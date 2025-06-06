// Copyright (c) 2024 Nexus. All rights reserved.

use clap::{Parser, Subcommand};
use crossterm::{
    event::{DisableMouseCapture, EnableMouseCapture},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    Terminal,
};
use std::error::Error;
use std::path::PathBuf;
use std::io::{self, Write};
use tokio::signal;
use tokio::task::JoinSet;
use std::sync::Arc;

mod analytics;
mod config;
mod environment;
#[path = "proto/nexus.orchestrator.rs"]
mod nexus_orchestrator;
mod orchestrator_client;
mod prover;
mod setup;
mod ui;
mod utils;
mod node_list;

use crate::config::Config;
use crate::environment::Environment;
use crate::setup::clear_node_config;
use crate::node_list::NodeList;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
/// Command-line arguments
struct Args {
    /// Command to execute
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start the prover
    Start {
        /// Node ID
        #[arg(long, value_name = "NODE_ID")]
        node_id: Option<u64>,

        /// Environment to connect to.
        #[arg(long, value_enum)]
        env: Option<Environment>,
    },
    
    /// Start multiple provers from node list file
    BatchFile {
        /// Path to node list file (.txt)
        #[arg(long, value_name = "FILE_PATH")]
        file: String,

        /// Environment to connect to.
        #[arg(long, value_enum)]
        env: Option<Environment>,

        /// Delay between starting each node (seconds)
        #[arg(long, default_value = "2")]
        start_delay: u64,

        /// Delay between proof submissions per node (seconds)
        #[arg(long, default_value = "3")]
        proof_interval: u64,

        /// Maximum number of concurrent nodes
        #[arg(long, default_value = "10")]
        max_concurrent: usize,
    },

    /// Create example node list files
    CreateExamples {
        /// Directory to create example files
        #[arg(long, default_value = "./examples")]
        dir: String,
    },
    
    /// Logout from the current session
    Logout,
}

/// Get the path to the Nexus config file, typically located at ~/.nexus/config.json.
fn get_config_path() -> Result<PathBuf, ()> {
    let home_path = home::home_dir().expect("Failed to get home directory");
    let config_path = home_path.join(".nexus").join("config.json");
    Ok(config_path)
}

// 节点池管理器
#[derive(Debug, Clone)]
struct NodePoolManager {
    all_nodes: Vec<u64>,
    active_nodes: Arc<tokio::sync::RwLock<std::collections::HashSet<u64>>>,
    available_nodes: Arc<tokio::sync::RwLock<Vec<u64>>>,
    max_concurrent: usize,
}

impl NodePoolManager {
    fn new(all_nodes: Vec<u64>, max_concurrent: usize) -> Self {
        let mut available_nodes = all_nodes.clone();
        let initial_nodes: std::collections::HashSet<u64> = available_nodes
            .drain(..std::cmp::min(max_concurrent, available_nodes.len()))
            .collect();
        
        Self {
            all_nodes,
            active_nodes: Arc::new(tokio::sync::RwLock::new(initial_nodes)),
            available_nodes: Arc::new(tokio::sync::RwLock::new(available_nodes)),
            max_concurrent,
        }
    }

    async fn get_initial_nodes(&self) -> Vec<u64> {
        self.active_nodes.read().await.iter().cloned().collect()
    }

    async fn replace_failed_node(&self, failed_node_id: u64) -> Option<u64> {
        let mut active = self.active_nodes.write().await;
        let mut available = self.available_nodes.write().await;
        
        // 移除失败的节点
        active.remove(&failed_node_id);
        
        // 从可用节点中获取新节点
        if let Some(new_node_id) = available.pop() {
            active.insert(new_node_id);
            Some(new_node_id)
        } else {
            None
        }
    }

    async fn get_active_count(&self) -> usize {
        self.active_nodes.read().await.len()
    }
}

// 固定行显示管理器
#[derive(Debug)]
struct FixedLineDisplay {
    max_lines: usize,
    node_lines: Arc<tokio::sync::RwLock<std::collections::HashMap<u64, String>>>,
    last_render_hash: Arc<tokio::sync::Mutex<u64>>,
    replacement_info: Arc<tokio::sync::RwLock<String>>,
}

impl FixedLineDisplay {
    fn new(max_lines: usize) -> Self {
        Self {
            max_lines,
            node_lines: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::with_capacity(max_lines))),
            last_render_hash: Arc::new(tokio::sync::Mutex::new(0)),
            replacement_info: Arc::new(tokio::sync::RwLock::new(String::new())),
        }
    }

    async fn update_node_status(&self, node_id: u64, status: String) {
        let needs_update = {
            let lines = self.node_lines.read().await;
            lines.get(&node_id) != Some(&status)
        };
        
        if needs_update {
            {
                let mut lines = self.node_lines.write().await;
                lines.insert(node_id, status.clone());
            }
            self.render_display_optimized().await;
        }
    }

    async fn remove_node(&self, node_id: u64) {
        {
            let mut lines = self.node_lines.write().await;
            lines.remove(&node_id);
        }
    }

    async fn update_replacement_info(&self, info: String) {
        {
            let mut replacement_info = self.replacement_info.write().await;
            *replacement_info = info;
        }
        self.render_display_optimized().await;
    }

    async fn render_display_optimized(&self) {
        let lines = self.node_lines.read().await;
        
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        for (id, status) in lines.iter() {
            std::hash::Hasher::write_u64(&mut hasher, *id);
            std::hash::Hasher::write(&mut hasher, status.as_bytes());
        }
        let current_hash = std::hash::Hasher::finish(&hasher);
        
        let mut last_hash = self.last_render_hash.lock().await;
        if *last_hash != current_hash {
            *last_hash = current_hash;
            drop(last_hash);
            self.render_display(&lines).await;
        }
    }

    async fn render_display(&self, lines: &std::collections::HashMap<u64, String>) {
        // 清屏并移动到顶部
        print!("\x1b[2J\x1b[H");
        
        // 标题
        println!("🔄 Nexus Prover Monitor");
        println!("═══════════════════════════════════════");
        
        // 显示替换信息
        let replacement_info = self.replacement_info.read().await;
        if !replacement_info.is_empty() {
            println!("🔄 Latest: {}", replacement_info);
            println!("───────────────────────────────────────");
        }
        
        // 按节点ID排序显示
        let mut sorted_lines: Vec<_> = lines.iter().collect();
        sorted_lines.sort_by_key(|(id, _)| *id);
        
        for (node_id, status) in sorted_lines {
            println!("Node-{}: {}", node_id, status);
        }
        
        println!("───────────────────────────────────────");
        println!("Press Ctrl+C to stop all provers");
        
        // 强制刷新输出
        use std::io::Write;
        std::io::stdout().flush().unwrap();
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize logger
    env_logger::init();
    
    let args = Args::parse();
    match args.command {
        Command::Start { node_id, env } => {
            let mut node_id = node_id;
            // If no node ID is provided, try to load it from the config file.
            let config_path = get_config_path().expect("Failed to get config path");
            if node_id.is_none() && config_path.exists() {
                if let Ok(config) = Config::load_from_file(&config_path) {
                    let node_id_as_u64 = config
                        .node_id
                        .parse::<u64>()
                        .expect("Failed to parse node ID");
                    node_id = Some(node_id_as_u64);
                }
            }

            let environment = env.unwrap_or_default();
            start(node_id, environment).await
        }
        
        Command::BatchFile {
            file,
            env,
            start_delay,
            proof_interval,
            max_concurrent,
        } => {
            let environment = env.unwrap_or_default();
            start_batch_from_file_with_pool(&file, environment, start_delay, proof_interval, max_concurrent).await
        }

        Command::CreateExamples { dir } => {
            NodeList::create_example_files(&dir)
                .map_err(|e| -> Box<dyn Error> { Box::new(e) })?;
            
            println!("🎉 Example node list files created successfully!");
            println!("📂 Location: {}", dir);
            println!("💡 Edit these files with your actual node IDs, then use:");
            println!("   nexus batch-file --file {}/example_nodes.txt", dir);
            Ok(())
        }
        
        Command::Logout => {
            let config_path = get_config_path().expect("Failed to get config path");
            clear_node_config(&config_path).map_err(Into::into)
        }
    }
}

/// Starts the Nexus CLI application.
async fn start(node_id: Option<u64>, env: Environment) -> Result<(), Box<dyn Error>> {
    if node_id.is_some() {
        // Use headless mode for single node with ID
        start_headless_prover(node_id, env).await
    } else {
        // Use UI mode for interactive setup
        start_with_ui(node_id, env).await
    }
}

/// Start with UI (original logic)
async fn start_with_ui(
    node_id: Option<u64>,
    env: Environment,
) -> Result<(), Box<dyn Error>> {
    // Terminal setup
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Create app and run it
    let res = ui::run(&mut terminal, ui::App::new(node_id, env, crate::orchestrator_client::OrchestratorClient::new(env)));

    // Restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    if let Err(err) = res {
        println!("{err:?}");
    }

    Ok(())
}

async fn start_headless_prover(
    node_id: Option<u64>,
    env: Environment,
) -> Result<(), Box<dyn Error>> {
    println!("🚀 Starting Nexus Prover in headless mode...");
    prover::start_prover(env, node_id).await?;
    Ok(())
}

async fn start_batch_from_file_with_pool(
    file_path: &str,
    env: Environment,
    start_delay: u64,
    proof_interval: u64,
    max_concurrent: usize,
) -> Result<(), Box<dyn Error>> {
    // 加载节点列表
    let node_list = NodeList::load_from_file(file_path)?;
    let all_nodes = node_list.node_ids().to_vec();
    
    if all_nodes.is_empty() {
        return Err("Node list is empty".into());
    }
    
    let actual_concurrent = std::cmp::min(max_concurrent, all_nodes.len());
    
    println!("🚀 Starting batch processing from file: {}", file_path);
    println!("📊 Total nodes: {}", all_nodes.len());
    println!("🔄 Max concurrent: {}", actual_concurrent);
    println!("⏱️  Start delay: {}s, Proof interval: {}s", start_delay, proof_interval);
    println!("🌍 Environment: {:?}", env);
    println!("═══════════════════════════════════════");

    // 创建节点池管理器和显示管理器
    let node_pool = NodePoolManager::new(all_nodes, actual_concurrent);
    let display = Arc::new(FixedLineDisplay::new(actual_concurrent));
    
    // 初始显示
    display.render_display(&std::collections::HashMap::new()).await;

    let mut join_set = JoinSet::new();
    let initial_nodes = node_pool.get_initial_nodes().await;
    
    // 启动初始节点
    for &node_id in &initial_nodes {
        let disp = display.clone();
        
        join_set.spawn(async move {
            let display_callback = {
                let disp = disp.clone();
                move |status: String| {
                    let disp = disp.clone();
                    let node_id = node_id;
                    tokio::spawn(async move {
                        disp.update_node_status(node_id, status).await;
                    });
                }
            };
            
            let result = prover::start_prover_with_callback(env, Some(node_id), proof_interval, display_callback).await;
            
            // 更新最终显示状态
            match &result {
                Ok(_) => disp.update_node_status(node_id, "✅ Completed".to_string()).await,
                Err(prover::ProverError::RateLimited(_)) => {
                    disp.update_node_status(node_id, "🚫 Rate Limited".to_string()).await;
                }
                Err(prover::ProverError::NodeStopped(_)) => {
                    disp.update_node_status(node_id, "🛑 Failed (max retries)".to_string()).await;
                }
                Err(_) => {
                    disp.update_node_status(node_id, "❌ Error".to_string()).await;
                }
            }
            
            (node_id, result)
        });
        
        tokio::time::sleep(std::time::Duration::from_secs(start_delay)).await;
    }
    
    println!("✅ All {} initial provers started!", initial_nodes.len());
    
    // 监控任务并处理节点替换
    monitor_with_node_replacement(join_set, node_pool, display, env, proof_interval, start_delay).await;
    
    Ok(())
}

/// 启动替换节点的通用函数
async fn spawn_replacement_node(
    join_set: &mut JoinSet<(u64, Result<(), prover::ProverError>)>,
    node_id: u64,
    display: Arc<FixedLineDisplay>,
    env: Environment,
    proof_interval: u64,
    start_delay: u64,
) {
    let disp = display.clone();
    
    join_set.spawn(async move {
        disp.update_node_status(node_id, "🚀 Starting (replacement)...".to_string()).await;
        
        tokio::time::sleep(std::time::Duration::from_secs(start_delay)).await;
        
        let display_callback = {
            let disp = disp.clone();
            move |status: String| {
                let disp = disp.clone();
                let node_id = node_id;
                tokio::spawn(async move {
                    disp.update_node_status(node_id, status).await;
                });
            }
        };
        
        let result = prover::start_prover_with_callback(env, Some(node_id), proof_interval, display_callback).await;
        
        match &result {
            Ok(_) => disp.update_node_status(node_id, "✅ Completed".to_string()).await,
            Err(prover::ProverError::RateLimited(_)) => {
                disp.update_node_status(node_id, "🚫 Rate Limited".to_string()).await;
            }
            Err(prover::ProverError::NodeStopped(_)) => {
                disp.update_node_status(node_id, "🛑 Failed (max retries)".to_string()).await;
            }
            Err(_) => {
                disp.update_node_status(node_id, "❌ Error".to_string()).await;
            }
        }
        
        (node_id, result)
    });
}

/// Monitor tasks with node replacement capability
async fn monitor_with_node_replacement(
    mut join_set: JoinSet<(u64, Result<(), prover::ProverError>)>,
    node_pool: NodePoolManager,
    display: Arc<FixedLineDisplay>,
    env: Environment,
    proof_interval: u64,
    start_delay: u64,
) {
    tokio::pin! {
        let ctrl_c = signal::ctrl_c();
    }
    
    loop {
        tokio::select! {
            // Handle completed tasks
            Some(result) = join_set.join_next() => {
                if let Ok((failed_node_id, prover_result)) = result {
                    match prover_result {
                        Ok(_) => {
                            println!("🎯 [Node-{}] Prover completed successfully", failed_node_id);
                            display.update_node_status(failed_node_id, "✅ Completed".to_string()).await;
                        },
                        Err(prover::ProverError::RateLimited(_)) => {
                            println!("🚨 [Node-{}] RATE LIMITED - Starting replacement...", failed_node_id);
                            display.remove_node(failed_node_id).await;
                            
                            if let Some(new_node_id) = node_pool.replace_failed_node(failed_node_id).await {
                                println!("🔄 [Node-{}] Starting replacement node", new_node_id);
                                
                                display.update_replacement_info(format!("Replaced {} with {} (Rate Limited)", failed_node_id, new_node_id)).await;
                                
                                spawn_replacement_node(&mut join_set, new_node_id, display.clone(), env, proof_interval, start_delay).await;
                            } else {
                                println!("⚠️ No more available nodes for replacement");
                                display.remove_node(failed_node_id).await;
                            }
                        },
                        Err(prover::ProverError::NodeStopped(failures)) => {
                            println!("🛑 [Node-{}] Stopped after {} failures - replacing with new node", failed_node_id, failures);
                            display.remove_node(failed_node_id).await;
                            
                            if let Some(new_node_id) = node_pool.replace_failed_node(failed_node_id).await {
                                println!("🔄 [Node-{}] Starting replacement node", new_node_id);
                                
                                display.update_replacement_info(format!("Replaced {} with {} (Node Stopped)", failed_node_id, new_node_id)).await;
                                
                                spawn_replacement_node(&mut join_set, new_node_id, display.clone(), env, proof_interval, start_delay).await;
                            } else {
                                println!("⚠️ No more available nodes for replacement");
                                display.remove_node(failed_node_id).await;
                            }
                        }
                        Err(e) => {
                            println!("❌ [Node-{}] Prover failed: {}", failed_node_id, e);
                            display.update_node_status(failed_node_id, format!("❌ Error: {}", e)).await;
                        }
                    }
                }
            }
            
            // Handle shutdown signal
            _ = &mut ctrl_c => {
                println!("🛑 Shutdown signal received. Stopping all provers...");
                join_set.abort_all();
                break;
            }
            
            // Exit when all tasks are done and no more nodes available
            else => {
                if node_pool.get_active_count().await == 0 {
                    println!("✅ All provers completed and no more nodes available.");
                    break;
                }
            }
        }
    }
    
    println!("✅ All provers stopped.");
} 