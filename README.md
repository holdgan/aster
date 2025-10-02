
# Aster Trading System

基于Linus哲学的简洁分布式做市系统，专为AsterDEX BTCUSDT交易设计。

## 📁 项目结构

```
aster/
├── scripts/                           # 核心交易脚本
│   ├── coordinator_cycle.py           # 🎯 循环库存策略 (推荐)
│   ├── coordinator_server.py          # 目标仓位策略
│   ├── executor_server.py             # 执行器服务 (B/C服务器)
│   ├── query_stats.py                 # 交易统计查询
│   ├── start_coordinator.sh           # 协调器启动脚本
│   └── start_executor.sh              # 执行器启动脚本
├── aster_btcusdt_genesis.py           # 交易客户端核心库
├── .env.example                       # 环境配置模板
├── requirements.txt                   # Python依赖
└── README.md                          # 本文件
```

## 🚀 快速开始

### 1. 配置环境
```bash
# 复制环境配置文件
cp .env.example .env

# 编辑配置文件，填入API密钥
vim .env
```

需要配置的变量：
```bash
ASTER_A_KEY=your_coordinator_api_key
ASTER_A_SECRET=your_coordinator_api_secret
ASTER_B_KEY=your_executor_b_api_key
ASTER_B_SECRET=your_executor_b_api_secret
ASTER_C_KEY=your_executor_c_api_key
ASTER_C_SECRET=your_executor_c_api_secret

# 服务器地址（可选，默认localhost）
SERVER_B_URL=http://server-b-ip:8081
SERVER_C_URL=http://server-c-ip:8082
```

### 2. 安装依赖
```bash
pip3 install -r requirements.txt
```

### 3. 启动系统

**步骤1: 在服务器B上启动执行器**
```bash
cd scripts
./start_executor.sh --account B --port 8081
```

**步骤2: 在服务器C上启动执行器**
```bash
cd scripts
./start_executor.sh --account C --port 8082
```

**步骤3: 在服务器A上启动协调器（循环库存策略）**
```bash
cd scripts
./start_coordinator.sh --strategy cycle
```

### 4. 查看交易统计
```bash
# 查询最近2小时的交易统计
python3 scripts/query_stats.py

# 查询最近6小时
python3 scripts/query_stats.py --hours 6

# 查询特定服务器
python3 scripts/query_stats.py --servers B
```

## 🏗️ 系统架构

```
┌─────────────────────┐
│  Server A (协调器)   │
│  - 获取行情数据      │
│  - 计算仓位策略      │
│  - 发送交易信号      │
└──────────┬──────────┘
           │
     ┌─────┴─────┐
     │           │
┌────▼────┐ ┌────▼────┐
│Server B │ │Server C │
│执行器   │ │执行器   │
│- Maker  │ │- Maker  │
│- Taker  │ │- Taker  │
└─────────┘ └─────────┘
```

### 循环库存策略 (coordinator_cycle.py)

**核心逻辑：**
1. **相位循环**: B和C交替达到最大多头/空头仓位
   - Phase 1: B→最大多头, C→最大空头
   - Phase 2: B→最大空头, C→最大多头
   - 循环往复，最大化交易量

2. **订单类型**:
   - **Maker订单**: 挂限价单提供流动性（费率0.01%）
   - **Taker订单**: 紧急平衡净敞口（费率0.035%）

3. **风控机制**:
   - 净敞口控制: 两服务器总敞口 < 4%总仓位
   - 单服务器限制: 5000 USD保证金
   - 订单过期时间: 5-30秒自动取消

## ⚙️ 策略参数配置

所有策略参数集中在文件开头，方便调整：

### coordinator_cycle.py 关键参数

**仓位限制** (第45-46行):
```python
MAX_POSITION_B_USD = 5000.0  # Server B最大仓位
MAX_POSITION_C_USD = 5000.0  # Server C最大仓位
```

**订单参数** (第64-78行):
```python
maker_step_btc: float = 0.002          # 单笔订单BTC数量
decision_interval: float = 5.0         # 决策频率（秒）
maker_expire_seconds: float = 30.0     # Maker订单过期时间
signal_delay_max: float = 5.0          # 随机延迟上限
maker_price_offset: float = 0.1        # 挂单价格偏移
```

**建议调整场景**：
- 加快建仓速度：增大 `maker_step_btc` (如0.005)
- 提高响应速度：减小 `decision_interval` (如2.0)
- 降低滑点：减小 `maker_price_offset` (如0.05)

## 📊 监控和统计

### 执行器健康检查
```bash
# 检查B服务器
curl http://server-b-ip:8081/health

# 检查C服务器
curl http://server-c-ip:8082/health
```

### 查看详细状态
```bash
curl http://server-b-ip:8081/status
```

### 交易统计报告
```bash
# 完整统计（包含费用和P&L）
python3 scripts/query_stats.py

# 输出示例：
# Server B Statistics:
#   Taker: 15 orders, $12,450 volume
#   Maker: 143 orders, $47,380 volume (成功率 95.3%)
#   Fees: $7.15
#   Realized P&L: -$13.70 (2.29 basis points)
```

### 紧急停止
```bash
curl -X POST http://server-b-ip:8081/emergency_stop
```

## 🛡️ 风控说明

### 多层防护机制

1. **仓位限制**: 单服务器最大5000 USD
2. **净敞口控制**: B+C总敞口 < 4% × 总仓位
3. **订单过期**: 5-30秒自动取消未成交订单
4. **价格保护**: Maker订单偏离市价 ±0.1 USD
5. **频率限制**: 5秒决策间隔，避免过度交易

### 异常情况处理

- **网络中断**: 执行器独立运行，不会无序交易
- **API失败**: 自动重试，3次失败后跳过
- **价格剧烈波动**: 净敞口超限时强制Taker平衡

## 🔧 常见问题

### Q: 如何调整仓位限制？
A: 修改 `coordinator_cycle.py` 第45-46行的 `MAX_POSITION_B_USD` 和 `MAX_POSITION_C_USD`

### Q: 如何加快建仓速度？
A: 增大 `maker_step_btc` 参数（第64行），如从0.002改为0.005

### Q: 订单成交率低怎么办？
A: 减小 `maker_price_offset`（第78行），如从0.1改为0.05，使挂单更接近市价

### Q: 如何查看历史P&L？
A: 运行 `python3 scripts/query_stats.py --hours 24` 查看24小时统计

---

**Linus哲学**: "Talk is cheap. Show me the code." - 所有参数集中在文件开头，直接修改即可生效。
