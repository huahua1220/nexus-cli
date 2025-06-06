## 快速安装

### Linux 安装
```bash
git clone https://github.com/huahua1220/nexus-cli.git
cd nexus-cli
cargo build --release
sudo cp target/release/nexus /usr/local/bin/

# 在同目录下生成节点文件
echo "节点ID1" > nodes.txt
echo "节点ID2" >> nodes.txt
echo "节点ID3" >> nodes.txt
```

### Mac 安装
```bash
brew install rust
git clone https://github.com/huahua1220/nexus-cli.git
cd nexus-cli
cargo build --release
sudo cp target/release/nexus /usr/local/bin/

# 在同目录下生成节点文件
echo "节点ID1" > nodes.txt
echo "节点ID2" >> nodes.txt
echo "节点ID3" >> nodes.txt
```

## 批量模式使用

```bash
# 批量运行节点 (10为线程数)
./nexus batch-file --file ./nodes.txt --max-concurrent 10
```
