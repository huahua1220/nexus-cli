use nexus_sdk::{stwo::seq::Stwo, Local, Prover};
use std::time::Duration;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use lazy_static::lazy_static;

use crate::orchestrator_client::OrchestratorClient;
use crate::{analytics, environment::Environment};
use colored::Colorize;
use log::{error, info};
use sha3::{Digest, Keccak256};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProverError {
    #[error("Orchestrator error: {0}")]
    Orchestrator(String),

    #[error("Stwo prover error: {0}")]
    Stwo(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] postcard::Error),
    
    #[error("Node stopped after {0} consecutive failures")]
    NodeStopped(u32),
    
    #[error("Rate limited (429): {0}")]
    RateLimited(String),
}

lazy_static! {
    static ref GLOBAL_PROVER: RwLock<Option<Arc<Stwo<Local>>>> = RwLock::new(None);
    static ref PROVER_INIT_LOCK: Mutex<()> = Mutex::new(());
}

/// 获取或创建证明器实例（双重检查锁优化）
pub async fn get_or_create_prover() -> Result<Arc<Stwo<Local>>, ProverError> {
    // 快速路径：已初始化直接返回
    if let Some(prover) = &*GLOBAL_PROVER.read().await {
        return Ok(prover.clone());
    }
    
    // 获取初始化锁（防止多个线程同时初始化）
    let _guard = PROVER_INIT_LOCK.lock().await;
    // 再次检查避免竞争条件
    if let Some(prover) = &*GLOBAL_PROVER.read().await {
        return Ok(prover.clone());
    }
    
    // 初始化证明器
    let prover = get_default_stwo_prover()
        .map_err(|e| ProverError::Stwo(format!("Failed to create prover: {}", e)))?;
    let prover_arc = Arc::new(prover);
    
    // 更新全局实例
    *GLOBAL_PROVER.write().await = Some(prover_arc.clone());
    
    Ok(prover_arc)
}

/// Starts the prover (original function for single node mode)
pub async fn start_prover(
    environment: Environment,
    node_id: Option<u64>,
) -> Result<(), ProverError> {
    match node_id {
        Some(id) => {
            info!("Starting authenticated proving loop for node ID: {}", id);
            run_authenticated_proving_loop(id, environment).await?;
        }
        None => {
            info!("Starting anonymous proving loop");
            run_anonymous_proving_loop(environment).await?;
        }
    }
    Ok(())
}

/// Optimized prover for batch mode with custom proof interval and failure limit
pub async fn start_prover_optimized(
    environment: Environment,
    node_id: Option<u64>,
    proof_interval: u64,
) -> Result<(), ProverError> {
    let node_prefix = match node_id {
        Some(id) => format!("[Node-{}]", id),
        None => "[Anonymous]".to_string(),
    };
    
    match node_id {
        Some(id) => {
            println!("{} 🚀 Started", node_prefix);
            run_authenticated_proving_loop_optimized(id, environment, node_prefix, proof_interval).await?;
        }
        None => {
            println!("{} 🚀 Started (anonymous mode)", node_prefix);
            run_anonymous_proving_loop_optimized(environment, node_prefix, proof_interval).await?;
        }
    }
    Ok(())
}

/// Original anonymous proving loop (for single node mode)
async fn run_anonymous_proving_loop(environment: Environment) -> Result<(), ProverError> {
    let client_id = format!("{:x}", md5::compute(b"anonymous"));
    let mut proof_count = 1;
    loop {
        info!("{}", "Starting proof (anonymous)".yellow());
        if let Err(e) = prove_anonymously() {
            error!("Failed to create proof: {}", e);
        } else {
            analytics::track(
                "cli_proof_anon_v2".to_string(),
                format!("Completed anon proof iteration #{}", proof_count),
                serde_json::json!({
                    "node_id": "anonymous",
                    "proof_count": proof_count,
                }),
                false,
                &environment,
                client_id.clone(),
            );
        }
        proof_count += 1;
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Optimized anonymous proving loop (for batch mode) with failure limit
async fn run_anonymous_proving_loop_optimized(
    environment: Environment,
    prefix: String,
    proof_interval: u64,
) -> Result<(), ProverError> {
    let client_id = format!("{:x}", md5::compute(b"anonymous"));
    let mut proof_count = 1;
    let mut consecutive_failures = 0;
    const MAX_CONSECUTIVE_FAILURES: u32 = 3;
    
    loop {
        if let Err(e) = prove_anonymously() {
            consecutive_failures += 1;
            println!("{} ❌ Proof #{} failed: {} (failure {}/{})", 
                     prefix, proof_count, e, consecutive_failures, MAX_CONSECUTIVE_FAILURES);
            
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                println!("{} 🛑 Node stopped after {} consecutive failures", 
                         prefix, MAX_CONSECUTIVE_FAILURES);
                return Err(ProverError::NodeStopped(MAX_CONSECUTIVE_FAILURES));
            }
        } else {
            consecutive_failures = 0; // 重置失败计数
            println!("{} ✅ Proof #{} completed successfully", prefix, proof_count);
            analytics::track(
                "cli_proof_anon_v2".to_string(),
                format!("Completed anon proof iteration #{}", proof_count),
                serde_json::json!({
                    "node_id": "anonymous",
                    "proof_count": proof_count,
                }),
                false,
                &environment,
                client_id.clone(),
            );
        }
        proof_count += 1;
        tokio::time::sleep(Duration::from_secs(proof_interval)).await;
    }
}

/// Original authenticated proving loop (for single node mode)
async fn run_authenticated_proving_loop(
    node_id: u64,
    environment: Environment,
) -> Result<(), ProverError> {
    let orchestrator_client = OrchestratorClient::new(environment);
    let mut proof_count = 1;
    loop {
        info!("{}", format!("Starting proof (node: {})", node_id).yellow());

        const MAX_ATTEMPTS: usize = 3;
        let mut attempt = 1;
        let mut success = false;

        while attempt <= MAX_ATTEMPTS {
            let stwo_prover = get_or_create_prover().await?;
            match authenticated_proving(node_id, &orchestrator_client, stwo_prover.clone()).await {
                Ok(_) => {
                    info!("Proving succeeded on attempt #{attempt}!");
                    success = true;
                    break;
                }
                Err(e) => {
                    error!("Attempt #{attempt} failed: {e}");
                    attempt += 1;
                    if attempt <= MAX_ATTEMPTS {
                        error!("Retrying in 2s...");
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                }
            }
        }

        if !success {
            error!(
                "All {} attempts to prove with node {} failed. Continuing to next proof iteration.",
                MAX_ATTEMPTS, node_id
            );
        }

        proof_count += 1;

        let client_id = format!("{:x}", md5::compute(node_id.to_le_bytes()));
        analytics::track(
            "cli_proof_node_v2".to_string(),
            format!("Completed proof iteration #{}", proof_count),
            serde_json::json!({
                "node_id": node_id,
                "proof_count": proof_count,
            }),
            false,
            &environment,
            client_id.clone(),
        );
    }
}

/// Optimized authenticated proving loop (for batch mode) with failure limit
async fn run_authenticated_proving_loop_optimized(
    node_id: u64,
    environment: Environment,
    prefix: String,
    proof_interval: u64,
) -> Result<(), ProverError> {
    let orchestrator_client = OrchestratorClient::new(environment);
    let prover = get_or_create_prover().await?;
    let mut proof_count = 1;
    let mut consecutive_failures = 0;
    const MAX_CONSECUTIVE_FAILURES: u32 = 5; // 其他错误最大重试5次
    
    loop {
        const MAX_ATTEMPTS: usize = 5; // 单次证明最大尝试5次
        let mut attempt = 1;
        let mut success = false;
        let mut last_error = String::new();

        while attempt <= MAX_ATTEMPTS {
            let current_prover = prover.clone();
            match authenticated_proving(node_id, &orchestrator_client, current_prover.clone()).await {
                Ok(_) => {
                    success = true;
                    break;
                }
                Err(ProverError::RateLimited(_)) => {
                    // 429错误，立即终止节点
                    println!("{} 🚫 Rate limited (429) - terminating node immediately", prefix);
                    return Err(ProverError::RateLimited("Node terminated due to rate limiting".to_string()));
                }
                Err(e) => {
                    last_error = format!("Attempt #{} failed: {}", attempt, e);
                    attempt += 1;
                    if attempt <= MAX_ATTEMPTS {
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                }
            }
        }

        if success {
            consecutive_failures = 0; // 重置失败计数
            println!("{} ✅ Proof #{} completed successfully", prefix, proof_count);
        } else {
            consecutive_failures += 1;
            println!("{} ❌ Proof #{} failed: {} (failure {}/{})", 
                     prefix, proof_count, last_error, consecutive_failures, MAX_CONSECUTIVE_FAILURES);
            
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                println!("{} 🛑 Node stopped after {} consecutive failures", 
                         prefix, MAX_CONSECUTIVE_FAILURES);
                return Err(ProverError::NodeStopped(MAX_CONSECUTIVE_FAILURES));
            }
        }

        proof_count += 1;

        let client_id = format!("{:x}", md5::compute(node_id.to_le_bytes()));
        analytics::track(
            "cli_proof_node_v2".to_string(),
            format!("Completed proof iteration #{}", proof_count),
            serde_json::json!({
                "node_id": node_id,
                "proof_count": proof_count,
            }),
            false,
            &environment,
            client_id.clone(),
        );
        
        tokio::time::sleep(Duration::from_secs(proof_interval)).await;
    }
}

/// Memory-optimized silent authenticated proving
async fn authenticated_proving_silent(
    node_id: u64,
    orchestrator_client: &OrchestratorClient,
    stwo_prover: Arc<Stwo<Local>>,
) -> Result<usize, ProverError> {
    let task = orchestrator_client
        .get_proof_task(&node_id.to_string())
        .await
        .map_err(|e| {
            let error_str = e.to_string();
            if error_str.contains("RATE_LIMITED:") {
                ProverError::RateLimited(error_str)
            } else {
                ProverError::Orchestrator(error_str)
            }
        })?;

    let public_input: u32 = task.public_inputs.first().cloned().unwrap_or_default() as u32;
    let proof_bytes = prove_helper(stwo_prover, public_input)?;
    let proof_hash = format!("{:x}", Keccak256::digest(&proof_bytes));
    let proof_size = proof_bytes.len();
    
    orchestrator_client
        .submit_proof(&task.task_id, &proof_hash, proof_bytes)
        .await
        .map_err(|e| {
            let error_str = e.to_string();
            if error_str.contains("RATE_LIMITED:") {
                ProverError::RateLimited(error_str)
            } else {
                ProverError::Orchestrator(error_str)
            }
        })?;

    Ok(proof_size)
}

/// Original authenticated proving (for single node mode and UI)
pub async fn authenticated_proving(
    node_id: u64,
    orchestrator_client: &OrchestratorClient,
    stwo_prover: Arc<Stwo<Local>>,
) -> Result<(), ProverError> {
    // 获取任务
    let task_response = orchestrator_client
        .get_proof_task(&node_id.to_string())
        .await
        .map_err(|e| {
            let error_str = e.to_string();
            if error_str.contains("RATE_LIMITED:") {
                ProverError::RateLimited(error_str)
            } else {
                ProverError::Orchestrator(error_str)
            }
        })?;

    // 证明生成
    let public_input: u32 = task_response.public_inputs.first().cloned().unwrap_or_default() as u32;
    let proof = prove_helper(stwo_prover, public_input)?;
    let proof_hash = format!("{:x}", Keccak256::digest(&proof));

    // 提交证明
    orchestrator_client
        .submit_proof(&task_response.task_id, &proof_hash, proof)
        .await
        .map_err(|e| {
            let error_str = e.to_string();
            if error_str.contains("RATE_LIMITED:") {
                ProverError::RateLimited(error_str)
            } else {
                ProverError::Orchestrator(error_str)
            }
        })?;

    Ok(())
}

/// Proves a program locally with hardcoded inputs.
pub fn prove_anonymously() -> Result<(), ProverError> {
    // 使用全局实例替代直接创建
    let rt = tokio::runtime::Runtime::new().unwrap();
    let stwo_prover = rt.block_on(get_or_create_prover())?;
    
    let public_input: u32 = 9;
    let proof_bytes = prove_helper(stwo_prover, public_input)?;
    
    let msg = format!(
        "ZK proof created (anonymous) with size: {} bytes",
        proof_bytes.len()
    );
    info!("{}", msg.green());
    Ok(())
}

/// Create a Stwo prover for the default program (deprecated - use get_or_create_prover)
pub fn get_default_stwo_prover() -> Result<Stwo, ProverError> {
    let elf_bytes = include_bytes!("../assets/fib_input");
    Stwo::new_from_bytes(elf_bytes).map_err(|e| {
        let msg = format!("Failed to load guest program: {}", e);
        error!("{}", msg);
        ProverError::Stwo(msg)
    })
}

fn prove_helper(_stwo_prover: Arc<Stwo<Local>>, public_input: u32) -> Result<Vec<u8>, ProverError> {
    // 重用证明器实例，避免重复创建
    // 注意：这里暂时还是要创建新实例，因为Stwo不支持Clone
    // 但我们应该考虑在更高层面缓存或重用
    let prover_instance = get_default_stwo_prover()?;
    let (_view, proof) = prover_instance
        .prove_with_input::<(), u32>(&(), &public_input)
        .map_err(|e| ProverError::Stwo(e.to_string()))?;

    // 直接序列化，让postcard处理内存分配
    let proof_bytes = postcard::to_allocvec(&proof).map_err(ProverError::from)?;
    
    Ok(proof_bytes)
}

/// Prover with status callback for fixed-line display
pub async fn start_prover_with_callback<F>(
    environment: Environment,
    node_id: Option<u64>,
    proof_interval: u64,
    status_callback: F,
) -> Result<(), ProverError> 
where
    F: Fn(String) + Send + Sync + 'static,
{
    let node_prefix = match node_id {
        Some(id) => format!("[Node-{}]", id),
        None => "[Anonymous]".to_string(),
    };
    
    match node_id {
        Some(id) => {
            status_callback(format!("🚀 Starting authenticated mode"));
            run_authenticated_proving_loop_with_callback(id, environment, node_prefix, proof_interval, status_callback).await?;
        }
        None => {
            status_callback(format!("🚀 Starting anonymous mode"));
            run_anonymous_proving_loop_with_callback(environment, node_prefix, proof_interval, status_callback).await?;
        }
    }
    Ok(())
}

/// Authenticated proving loop with status callback
async fn run_authenticated_proving_loop_with_callback<F>(
    node_id: u64,
    environment: Environment,
    _prefix: String,
    proof_interval: u64,
    status_callback: F,
) -> Result<(), ProverError> 
where
    F: Fn(String) + Send + Sync + 'static,
{
    let orchestrator_client = OrchestratorClient::new(environment);
    let prover = get_or_create_prover().await?;
    let mut proof_count = 1;
    let mut consecutive_failures = 0;
    const MAX_CONSECUTIVE_FAILURES: u32 = 5;
    
    loop {
        const MAX_ATTEMPTS: usize = 5;
        let mut attempt = 1;
        let mut success = false;

        while attempt <= MAX_ATTEMPTS {
            let current_prover = prover.clone();
            match authenticated_proving(node_id, &orchestrator_client, current_prover.clone()).await {
                Ok(_) => {
                    success = true;
                    break;
                }
                Err(ProverError::RateLimited(_)) => {
                    status_callback(format!("🚫 Rate limited (429) - terminating"));
                    return Err(ProverError::RateLimited("Node terminated due to rate limiting".to_string()));
                }
                Err(_e) => {
                    attempt += 1;
                    if attempt <= MAX_ATTEMPTS {
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                }
            }
        }

        if success {
            consecutive_failures = 0;
            status_callback(format!("✅ Proof #{} completed successfully", proof_count));
        } else {
            consecutive_failures += 1;
            status_callback(format!("❌ Proof #{} failed after {} attempts (failure {}/{})", 
                proof_count, MAX_ATTEMPTS, consecutive_failures, MAX_CONSECUTIVE_FAILURES));
            
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                status_callback(format!("🛑 Node stopped after {} consecutive failures", MAX_CONSECUTIVE_FAILURES));
                return Err(ProverError::NodeStopped(MAX_CONSECUTIVE_FAILURES));
            }
        }

        proof_count += 1;

        let client_id = format!("{:x}", md5::compute(node_id.to_le_bytes()));
        analytics::track(
            "cli_proof_node_v2".to_string(),
            format!("Completed proof iteration #{}", proof_count),
            serde_json::json!({
                "node_id": node_id,
                "proof_count": proof_count,
            }),
            false,
            &environment,
            client_id.clone(),
        );
        
        tokio::time::sleep(Duration::from_secs(proof_interval)).await;
    }
}

/// Anonymous proving loop with status callback
async fn run_anonymous_proving_loop_with_callback<F>(
    environment: Environment,
    _prefix: String,
    proof_interval: u64,
    status_callback: F,
) -> Result<(), ProverError> 
where
    F: Fn(String) + Send + Sync + 'static,
{
    let client_id = format!("{:x}", md5::compute(b"anonymous"));
    let mut proof_count = 1;
    let mut consecutive_failures = 0;
    const MAX_CONSECUTIVE_FAILURES: u32 = 5;
    
    loop {
        match prove_anonymously() {
            Ok(_) => {
                consecutive_failures = 0;
                status_callback(format!("✅ Proof #{} completed successfully", proof_count));
                
                analytics::track(
                    "cli_proof_anon_v2".to_string(),
                    format!("Completed anon proof iteration #{}", proof_count),
                    serde_json::json!({
                        "node_id": "anonymous",
                        "proof_count": proof_count,
                    }),
                    false,
                    &environment,
                    client_id.clone(),
                );
            }
            Err(_e) => {
                consecutive_failures += 1;
                status_callback(format!("❌ Proof #{} failed (failure {}/{})", 
                    proof_count, consecutive_failures, MAX_CONSECUTIVE_FAILURES));
                
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                    status_callback(format!("🛑 Node stopped after {} consecutive failures", MAX_CONSECUTIVE_FAILURES));
                    return Err(ProverError::NodeStopped(MAX_CONSECUTIVE_FAILURES));
                }
            }
        }
        
        proof_count += 1;
        tokio::time::sleep(Duration::from_secs(proof_interval)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_default_stwo_prover() {
        let prover = get_default_stwo_prover();
        match prover {
            Ok(_) => println!("Prover initialized successfully."),
            Err(e) => panic!("Failed to initialize prover: {}", e),
        }
    }

    #[tokio::test]
    async fn test_prove_anonymously() {
        let result = prove_anonymously();
        assert!(result.is_ok(), "Anonymous proving failed: {:?}", result);
    }
}
