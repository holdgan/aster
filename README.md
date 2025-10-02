
# Aster Trading System

基于Linus哲学的简洁分布式交易系统，专为Aster Genesis Stage 2积分最大化而设计。

## 📁 项目结构

```
aster/
├── scripts/                           # 可执行脚本目录
│   ├── coordinator_server.py          # 协调器服务 (A服务器)
│   ├── executor_server.py             # 执行器服务 (B/C服务器)
│   ├── system_monitor.py              # 系统监控脚本
│   ├── start_coordinator.sh           # 协调器启动脚本
│   └── start_executor.sh              # 执行器启动脚本
├── utils/                             # 工具模块目录
│   ├── __init__.py                    # 包初始化文件
│   ├── api_protocol.py                # 服务器间通信协议
│   ├── points_optimizer.py            # 积分最大化策略引擎
│   └── risk_manager.py                # 风控和安全机制
├── aster_btcusdt_genesis.py           # 原始交易客户端 (依赖)
├── .env.example                       # 环境配置模板
├── requirements.txt                   # Python依赖
├── deployment_guide.md                # 详细部署指南
└── README.md                          # 项目说明 (本文件)
```

## 🚀 快速开始

### 1. 配置环境
```bash
# 复制环境配置文件
cp .env.example .env

# 编辑配置文件，填入你的API密钥和服务器地址
vim .env
```

### 2. 安装依赖
```bash
# 创建虚拟环境
python3 -m venv venv
source venv/bin/activate

# 安装依赖
pip3 install -r requirements.txt

deactivate
```

### 3. 启动系统

**在服务器B上启动执行器:**
```bash
./scripts/start_executor.sh --account B --port 8081
```

**在服务器C上启动执行器:**
```bash
./scripts/start_executor.sh --account C --port 8082
```

**在服务器A上启动协调器:**
```bash
./scripts/start_coordinator.sh --account A
```

### 4. 监控系统
```bash
# 生成系统报告
python3 scripts/system_monitor.py --report

# 持续监控
python3 scripts/system_monitor.py --interval 30
```

## 🏗️ 系统架构

```
[服务器A - 协调器]     [服务器B - 执行器]     [服务器C - 执行器]
      (账户A)              (账户B)              (账户C)
   获取行情&决策            通用执行器            通用执行器
   执行taker应急单          接收交易信号          接收交易信号
   制定交易策略             买单/卖单maker        买单/卖单maker
```

**关键设计理念:**
- **动态策略**: 协调器根据市场情况和积分优化策略，动态分配B/C执行器的交易方向
- **通用执行器**: B/C服务器不预设买卖方向，完全由协调器的信号决定
- **灵活调度**: 可以让B做买单、C做卖单，也可以让B做卖单、C做买单，甚至两个都做同方向

## 📋 核心特性

### ✅ Linus式设计哲学
- **消除特殊情况**: B/C使用统一脚本，动态角色切换
- **直接实现**: HTTP API通信，无复杂抽象层
- **单一职责**: 每个组件功能明确，职责分离

### 🎯 积分最大化策略
- 基于Aster Genesis Stage 2规则优化
- 自动调整Maker/Taker比例(目标80% Maker)
- 持仓积分、交易量积分、抵押资产奖励
- 团队加成和连续交易奖励

### 🛡️ 多层风控体系
- 仓位限制、损失限制、频率限制
- API失败保护、市场波动监控
- 紧急停止机制、实时告警系统

### 🌐 分布式部署
- 三台不同IP服务器，规避风控检测
- 网络中断时各服务器独立保护
- 容错设计，系统稳定性高

## 📊 监控端点

### 健康检查
```bash
curl http://server-ip:port/health
```

### 详细状态
```bash
curl http://server-ip:port/status
```

### 紧急停止
```bash
curl -X POST http://server-ip:port/emergency_stop
```

## ⚙️ 配置说明

### 主要配置项
- `ASTER_A/B/C_KEY`: 各账户API密钥
- `SERVER_B/C_URL`: 服务器地址配置
- `MAX_POSITION_USD`: 单账户最大仓位限制
- `DAILY_LOSS_LIMIT`: 每日亏损限制
- `TARGET_VOLUME_PER_HOUR`: 每小时目标交易量

详细配置说明请查看 `.env.example` 文件。

## 📖 详细文档

- [部署指南](deployment_guide.md) - 完整的部署说明
- [API协议](utils/api_protocol.py) - 服务器间通信协议
- [积分优化](utils/points_optimizer.py) - 积分最大化策略
- [风控管理](utils/risk_manager.py) - 风险控制机制

## 🔧 开发说明

### 代码组织
- **scripts/**: 可执行的主程序和启动脚本
- **utils/**: 可复用的工具模块和库

### 添加新功能
1. 工具模块放入 `utils/` 目录
2. 可执行脚本放入 `scripts/` 目录
3. 更新 `utils/__init__.py` 导出新模块
4. 在脚本中添加路径以导入模块

### Python路径设置
脚本文件开头已添加父目录到Python路径:
```python
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
```

## 🆘 故障排除

### 常见问题
1. **导入错误**: 确保从项目根目录运行脚本
2. **连接失败**: 检查服务器IP地址和端口配置
3. **API错误**: 验证API密钥和权限设置

### 日志查看
```bash
# 系统日志
tail -f /var/log/aster/trading.log

# 或本地日志
tail -f logs/trading.log
```

## 📞 支持

如遇问题，请检查：
1. [部署指南](deployment_guide.md) 中的故障排除章节
2. 系统日志和健康检查端点
3. 网络连通性和API配置

---

**注意**: 此系统专为Aster Genesis Stage 2设计，请确保理解积分规则和风险后再使用。
