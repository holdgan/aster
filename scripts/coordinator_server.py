#!/usr/bin/env python3
"""
Aster BTCUSDT Coordinator Server (A服务器)
负责获取行情、计算全局敞口、向B/C服务器发送交易信号、执行紧急taker单
"""

import asyncio
import json
import time
import logging
import os
import signal
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from aiohttp import web, ClientSession, ClientTimeout
from dotenv import load_dotenv

# 添加父目录到Python路径以便导入模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 使用现有的交易客户端和配置
from aster_btcusdt_genesis import (
    AsterFuturesClient, TradingConfig, AccountConfig, AccountState, SymbolSpec,
    get_logger
)

# 常量
SYMBOL = "BTCUSDT"

load_dotenv()
logger = get_logger("coordinator")

# 导入工具模块 (简单测试)
try:
    from utils.api_protocol import TradingSignal
    from utils.points_optimizer import PointsOptimizer
    from utils.risk_manager import RiskManager
    logger.info("✅ Utils modules imported successfully")
except ImportError as e:
    logger.warning(f"⚠️  Utils modules not available: {e}")

# 简单的风控检查
def simple_risk_check(position_usd: float, daily_pnl: float) -> tuple[bool, str]:
    """简单风控检查"""
    if abs(position_usd) > MAX_POSITION_USD:
        return False, f"Position too large: ${position_usd:.2f}"
    if daily_pnl < -DAILY_LOSS_LIMIT:
        return False, f"Daily loss too high: ${daily_pnl:.2f}"
    return True, "OK"

# 净敞口检查
def check_net_exposure(positions: dict) -> tuple[bool, str, float]:
    """检查系统总净敞口"""
    net_exposure = sum(positions.values())

    if abs(net_exposure) > MAX_NET_EXPOSURE_USD:
        return False, f"Net exposure too high: ${net_exposure:.2f}", net_exposure
    elif abs(net_exposure) > NET_EXPOSURE_TOLERANCE:
        return True, f"Net exposure warning: ${net_exposure:.2f}", net_exposure

    return True, "Net exposure OK", net_exposure

# 读取基本配置
MAX_POSITION_USD = float(os.getenv('MAX_POSITION_USD', '2000.0'))
MAX_ORDER_USD = float(os.getenv('MAX_ORDER_USD', '500.0'))
DAILY_LOSS_LIMIT = float(os.getenv('DAILY_LOSS_LIMIT', '500.0'))

# 净敞口控制配置
MAX_NET_EXPOSURE_USD = float(os.getenv('MAX_NET_EXPOSURE_USD', '100.0'))
NET_EXPOSURE_TOLERANCE = float(os.getenv('NET_EXPOSURE_TOLERANCE', '50.0'))
NET_EXPOSURE_CHECK_INTERVAL = float(os.getenv('NET_EXPOSURE_CHECK_INTERVAL', '10.0'))
EMERGENCY_BALANCE_THRESHOLD = float(os.getenv('EMERGENCY_BALANCE_THRESHOLD', '200.0'))
BALANCE_PREFER_TAKER = os.getenv('BALANCE_PREFER_TAKER', 'true').lower() == 'true'

# 分账户仓位限制
MAX_POSITION_A_USD = float(os.getenv('MAX_POSITION_A_USD', os.getenv('MAX_POSITION_USD', '2000.0')))
MAX_POSITION_B_USD = float(os.getenv('MAX_POSITION_B_USD', os.getenv('MAX_POSITION_USD', '2000.0')))
MAX_POSITION_C_USD = float(os.getenv('MAX_POSITION_C_USD', os.getenv('MAX_POSITION_USD', '2000.0')))

# 风险管理配置
MAX_ACCOUNT_POSITION_DIFF = float(os.getenv('MAX_ACCOUNT_POSITION_DIFF', '300.0'))
REBALANCE_CHECK_INTERVAL = float(os.getenv('REBALANCE_CHECK_INTERVAL', '30.0'))

# 积分优化配置
TARGET_MAKER_RATIO = float(os.getenv('TARGET_MAKER_RATIO', '0.8'))
OPTIMIZE_FOR_POINTS = os.getenv('OPTIMIZE_FOR_POINTS', 'true').lower() == 'true'

# 订单大小配置
COORDINATOR_ORDER_SIZE_USD = float(os.getenv('COORDINATOR_ORDER_SIZE_USD', '150.0'))
COORDINATOR_MAX_ORDER_BTC = float(os.getenv('COORDINATOR_MAX_ORDER_BTC', '0.1'))

@dataclass
class ServerConfig:
    """服务器配置"""
    url: str
    timeout: float = 5.0
    retry_count: int = 3

@dataclass
class CoordinatorConfig:
    """协调器配置"""
    # 服务器端点 - 从环境变量或命令行参数获取
    server_b_url: str = ""
    server_c_url: str = ""

    # 信号发送配置
    signal_timeout: float = 3.0
    signal_expire_seconds: float = 10.0

    # 敞口控制
    max_total_exposure_usd: float = 5000.0
    emergency_exposure_threshold: float = 8000.0
    per_server_max_notional: float = 1000.0

    # 积分优化参数
    preferred_maker_ratio: float = 0.8  # 80%使用maker，20%使用taker
    volume_target_per_hour: float = 50000.0  # 每小时目标交易量

    # 决策间隔
    decision_interval: float = 2.0
    health_check_interval: float = 30.0

class CoordinatorBot:
    """协调器机器人 - 全局决策中心"""

    def __init__(self, account_id: str = "A"):
        self.config = CoordinatorConfig()
        self.trading_config = TradingConfig()
        self.account_id = account_id

        # 获取API密钥
        api_key = os.getenv(f'ASTER_{account_id}_KEY')
        api_secret = os.getenv(f'ASTER_{account_id}_SECRET')

        if not api_key or not api_secret:
            raise ValueError(f"Missing API credentials for account {account_id}")

        # 存储API密钥
        self.api_key = api_key
        self.api_secret = api_secret
        self.session = None

        # 创建BTCUSDT符号规格
        self.symbol_spec = SymbolSpec(
            symbol="BTCUSDT",
            tick_size=0.1,
            qty_step=0.001,
            min_qty=0.001,
            min_notional=5.0,
            price_precision=1,
            qty_precision=3
        )

        # 初始化自己的交易客户端 (用于taker单)
        self.account_state = AccountState(
            config=AccountConfig(
                identifier=account_id,
                role="coordinator",
                target_hold_notional=0.0,
                maker_order_notional=500.0
            ),
            client=None,  # 将在async初始化中设置
            symbol_spec=self.symbol_spec,
            metrics={}
        )

        # 服务器状态跟踪 - 延迟初始化
        self.server_b = None
        self.server_c = None
        self.last_signal_time = {}

        # 全局状态
        self.total_exposure = 0.0
        self.last_market_price = 0.0
        self.running = True
        self.tasks = []  # 任务列表
        self.executor_exposures: Dict[str, float] = {'A': 0.0, 'B': 0.0, 'C': 0.0}
        self.position_limits: Dict[str, float] = {
            'A': MAX_POSITION_A_USD,
            'B': MAX_POSITION_B_USD,
            'C': MAX_POSITION_C_USD
        }

        # 积分跟踪
        self.session_volume = 0.0
        self.session_start_time = time.time()

    def _normalize_price(self, price: float) -> float:
        """按照交易对精度规范化价格，避免浮点尾数"""
        rounded = self.symbol_spec.round_price(price)
        return float(f"{rounded:.{self.symbol_spec.price_precision}f}")

    def _normalize_quantity(self, quantity: float) -> float:
        """按照交易对精度规范化数量，确保满足最小步长"""
        rounded = self.symbol_spec.round_qty(quantity)
        if rounded < self.symbol_spec.min_qty:
            rounded = self.symbol_spec.min_qty
        return float(f"{rounded:.{self.symbol_spec.qty_precision}f}")

    def _within_position_limit(self, server: str, new_exposure: float) -> bool:
        limit = self.position_limits.get(server)
        if not limit or limit <= 0:
            return True
        return abs(new_exposure) <= limit

    async def _async_init(self):
        """异步初始化 - 创建HTTP会话和客户端"""
        self.session = ClientSession(timeout=ClientTimeout(total=30))

        # 创建交易客户端
        self.account_state.client = AsterFuturesClient(
            api_key=self.api_key,
            api_secret=self.api_secret,
            session=self.session,
            name=f"coordinator-{self.account_id}"
        )

    async def _cleanup(self):
        """清理资源"""
        if self.session:
            await self.session.close()

    async def start(self):
        """启动协调器"""
        logger.info("🚀 Starting Aster Coordinator Server")

        try:
            # 初始化服务器配置（必须在这里，因为URL是从命令行参数设置的）
            if not self.server_b:
                self.server_b = ServerConfig(self.config.server_b_url)
            if not self.server_c:
                self.server_c = ServerConfig(self.config.server_c_url)

            logger.info(f"📡 Server B: {self.config.server_b_url}")
            logger.info(f"📡 Server C: {self.config.server_c_url}")

            # AsterFuturesClient 不需要 initialize() 方法
            logger.info(f"✅ Trading client ready for account {self.account_id}")

            # 启动后台任务
            self.tasks.append(asyncio.create_task(self._main_decision_loop()))
            self.tasks.append(asyncio.create_task(self._health_check_loop()))
            self.tasks.append(asyncio.create_task(self._points_reporting_loop()))

            # 设置信号处理 - 在event loop中
            loop = asyncio.get_running_loop()
            loop.add_signal_handler(signal.SIGINT, self._handle_shutdown)
            loop.add_signal_handler(signal.SIGTERM, self._handle_shutdown)

            # 等待任务完成
            await asyncio.gather(*self.tasks, return_exceptions=True)

        except KeyboardInterrupt:
            logger.info("🛑 Keyboard interrupt received")
        except Exception as e:
            logger.error(f"❌ Coordinator failed: {e}")
        finally:
            # 取消所有任务
            for task in self.tasks:
                if not task.done():
                    task.cancel()

            # 等待任务清理
            await asyncio.gather(*self.tasks, return_exceptions=True)

            await self.cleanup()

    async def _main_decision_loop(self):
        """主决策循环"""
        logger.info("📊 Starting main decision loop")

        while self.running:
            try:
                # 1. 获取市场数据
                market_data = await self._get_market_data()
                if not market_data:
                    await asyncio.sleep(1.0)
                    continue

                # 2. 计算全局敞口
                total_exposure, exposure_usd, _ = await self._calculate_total_exposure(market_data['mid_price'])

                # 3. 做出交易决策
                decisions = await self._make_trading_decisions(market_data, total_exposure, exposure_usd)

                # 4. 执行决策
                await self._execute_decisions(decisions)

                # 5. 检查紧急情况
                await self._handle_emergency_situations(total_exposure)

                await asyncio.sleep(self.config.decision_interval)

            except Exception as e:
                logger.error(f"❌ Decision loop error: {e}")
                await asyncio.sleep(1.0)

    async def _get_market_data(self) -> Optional[Dict]:
        """获取市场数据"""
        try:
            # 获取深度数据
            depth = await self.account_state.client.get_depth(SYMBOL, limit=20)
            if not depth or 'bids' not in depth or 'asks' not in depth:
                return None

            bids = [[float(p), float(q)] for p, q in depth['bids'][:5]]
            asks = [[float(p), float(q)] for p, q in depth['asks'][:5]]

            if not bids or not asks:
                return None

            bid_price, bid_qty = bids[0]
            ask_price, ask_qty = asks[0]
            mid_price = (bid_price + ask_price) / 2

            self.last_market_price = mid_price

            return {
                'timestamp': time.time(),
                'symbol': SYMBOL,
                'bid_price': bid_price,
                'ask_price': ask_price,
                'mid_price': mid_price,
                'bid_qty': bid_qty,
                'ask_qty': ask_qty,
                'bids': bids,
                'asks': asks,
                'spread_bps': (ask_price - bid_price) / mid_price * 10000
            }

        except Exception as e:
            logger.error(f"❌ Failed to get market data: {e}")
            return None

    async def _fetch_executor_position(self, server_label: str) -> float:
        """从执行器获取仓位（BTC）"""
        server_config = self.server_b if server_label == 'B' else self.server_c

        if not server_config or not server_config.url:
            return 0.0

        try:
            timeout = ClientTimeout(total=3.0)
            async with self.session.get(f"{server_config.url}/position", timeout=timeout) as response:
                if response.status != 200:
                    text = await response.text()
                    logger.warning(
                        f"⚠️ Failed to fetch exposure from server {server_label}: "
                        f"status={response.status}, body={text[:200]}"
                    )
                    return 0.0

                payload = await response.json()
                position_btc = float(payload.get('position_btc', 0.0))
                logger.debug(
                    "Executor %s position fetched: %.6f BTC (raw payload=%s)",
                    server_label,
                    position_btc,
                    payload
                )
                return position_btc

        except asyncio.TimeoutError:
            logger.warning(f"⏰ Timeout fetching exposure from server {server_label}")
        except Exception as e:
            logger.warning(f"⚠️ Error fetching exposure from {server_label}: {e}")

        return 0.0

    async def _calculate_total_exposure(self, mid_price: float) -> Tuple[float, Dict[str, float], Dict[str, float]]:
        """计算全局敞口并返回各账户敞口 (USD 和 BTC)"""
        try:
            # 获取自己的仓位
            my_positions = await self.account_state.client.get_positions()
            my_position_btc = 0.0

            if my_positions:
                for pos in my_positions:
                    if pos.get('symbol') == SYMBOL:
                        my_position_btc = float(pos.get('positionAmt', 0))
                        break

            position_b = await self._fetch_executor_position('B')
            position_c = await self._fetch_executor_position('C')

            exposures_btc = {
                'A': my_position_btc,
                'B': position_b,
                'C': position_c
            }

            exposures_usd = {key: value * mid_price for key, value in exposures_btc.items()}

            total_exposure = sum(exposures_usd.values())
            self.executor_exposures = exposures_usd
            self.total_exposure = total_exposure

            logger.info(
                "📊 Exposure snapshot USD/BTC - A: %.2f / %.2f (%.4f BTC), B: %.2f / %.2f (%.4f BTC), C: %.2f / %.2f (%.4f BTC)",
                exposures_usd['A'], self.position_limits['A'], exposures_btc['A'],
                exposures_usd['B'], self.position_limits['B'], exposures_btc['B'],
                exposures_usd['C'], self.position_limits['C'], exposures_btc['C']
            )
            logger.debug(
                f"💰 Total exposure: ${total_exposure:.2f} "
                f"(A: ${exposures_usd['A']:.2f}, B: ${exposures_usd['B']:.2f}, C: ${exposures_usd['C']:.2f})"
            )
            return total_exposure, exposures_usd, exposures_btc

        except Exception as e:
            logger.error(f"❌ Failed to calculate exposure: {e}")
            fallback = {'A': 0.0, 'B': 0.0, 'C': 0.0}
            return 0.0, fallback, fallback

    async def _make_trading_decisions(
        self,
        market_data: Dict,
        total_exposure: float,
        exposures: Dict[str, float]
    ) -> List[Dict]:
        """制定交易决策"""
        decisions = []
        current_time = time.time()

        # 基于敞口和市场状况制定决策
        mid_price = market_data['mid_price']
        spread_bps = market_data['spread_bps']

        limit_decisions = self._enforce_position_limits(market_data, exposures)
        if limit_decisions:
            return limit_decisions

        exposure_b = exposures.get('B', 0.0)
        exposure_c = exposures.get('C', 0.0)

        # 敞口平衡逻辑
        exposure_threshold = self.config.per_server_max_notional * 0.5

        if total_exposure > exposure_threshold:
            # 敞口过多，需要减仓
            raw_price = market_data['bid_price'] + 0.01
            price = self._normalize_price(raw_price)
            raw_qty = min(COORDINATOR_ORDER_SIZE_USD / mid_price, COORDINATOR_MAX_ORDER_BTC)
            quantity = self._normalize_quantity(raw_qty)
            order_notional = quantity * price

            sell_candidates = sorted((('B', exposure_b), ('C', exposure_c)), key=lambda x: x[1], reverse=True)
            positive_candidates = [candidate for candidate in sell_candidates if candidate[1] > 0]
            if positive_candidates:
                sell_candidates = positive_candidates

            target_server = None
            for server, exposure in sell_candidates:
                new_exposure = exposure - order_notional
                if self._within_position_limit(server, new_exposure):
                    target_server = server
                    break

            if target_server:
                logger.debug(
                    "Using %s to reduce long exposure: current %.2f, order %.2f, new %.2f, limit %.2f",
                    target_server,
                    exposures.get(target_server, 0.0),
                    order_notional,
                    exposures.get(target_server, 0.0) - order_notional,
                    self.position_limits.get(target_server, 0.0)
                )
                decisions.append({
                    'server': target_server,
                    'action': 'sell_maker',
                    'price': price,
                    'quantity': quantity,
                    'expire_time': current_time + self.config.signal_expire_seconds,
                    'reason': f'Reduce long exposure: ${total_exposure:.2f} via {target_server}'
                })
            else:
                logger.warning(
                    "⚠️ No eligible executor to reduce long exposure (order %.2f) without breaching limits; exposures=%s",
                    order_notional,
                    exposures
                )

        elif total_exposure < -exposure_threshold:
            # 敞口过少，需要加仓
            raw_price = market_data['ask_price'] - 0.01
            price = self._normalize_price(raw_price)
            raw_qty = min(COORDINATOR_ORDER_SIZE_USD / mid_price, COORDINATOR_MAX_ORDER_BTC)
            quantity = self._normalize_quantity(raw_qty)
            order_notional = quantity * price

            buy_candidates = sorted((('B', exposure_b), ('C', exposure_c)), key=lambda x: x[1])
            negative_candidates = [candidate for candidate in buy_candidates if candidate[1] < 0]
            if negative_candidates:
                buy_candidates = negative_candidates

            target_server = None
            for server, exposure in buy_candidates:
                new_exposure = exposure + order_notional
                if self._within_position_limit(server, new_exposure):
                    target_server = server
                    break

            if target_server:
                logger.debug(
                    "Using %s to increase long exposure: current %.2f, order %.2f, new %.2f, limit %.2f",
                    target_server,
                    exposures.get(target_server, 0.0),
                    order_notional,
                    exposures.get(target_server, 0.0) + order_notional,
                    self.position_limits.get(target_server, 0.0)
                )
                decisions.append({
                    'server': target_server,
                    'action': 'buy_maker',
                    'price': price,
                    'quantity': quantity,
                    'expire_time': current_time + self.config.signal_expire_seconds,
                    'reason': f'Increase long exposure: ${total_exposure:.2f} via {target_server}'
                })
            else:
                logger.warning(
                    "⚠️ No eligible executor to increase long exposure (order %.2f) without breaching limits; exposures=%s",
                    order_notional,
                    exposures
                )

        # 积分优化逻辑 - 定期刷量
        time_since_last_b = current_time - self.last_signal_time.get('B', 0)
        time_since_last_c = current_time - self.last_signal_time.get('C', 0)

        # 如果spread合适且长时间没有交易，主动制造交易量
        if spread_bps < 5.0 and len(decisions) == 0:  # Spread < 5 bps 且没有敞口调整需求
            if time_since_last_b > 30:  # 30秒没有B交易
                raw_price = market_data['bid_price'] + 0.01
                price = self._normalize_price(raw_price)
                raw_qty = min(300.0 / mid_price, 0.05)
                quantity = self._normalize_quantity(raw_qty)
                notional = quantity * price
                new_exposure = exposure_b + notional
                if self._within_position_limit('B', new_exposure):
                    decisions.append({
                        'server': 'B',
                        'action': 'buy_maker',
                        'price': price,
                        'quantity': quantity,
                        'expire_time': current_time + self.config.signal_expire_seconds,
                        'reason': 'Volume generation - buy side'
                    })
                else:
                    logger.debug(
                        "Skipping B volume trade due to position limit: current %.2f, order %.2f, limit %.2f",
                        exposure_b,
                        notional,
                        self.position_limits.get('B', 0.0)
                    )

            if time_since_last_c > 30:  # 30秒没有C交易
                raw_price = market_data['ask_price'] - 0.01
                price = self._normalize_price(raw_price)
                raw_qty = min(300.0 / mid_price, 0.05)
                quantity = self._normalize_quantity(raw_qty)
                notional = quantity * price
                new_exposure = exposure_c - notional
                if self._within_position_limit('C', new_exposure):
                    decisions.append({
                        'server': 'C',
                        'action': 'sell_maker',
                        'price': price,
                        'quantity': quantity,
                        'expire_time': current_time + self.config.signal_expire_seconds,
                        'reason': 'Volume generation - sell side'
                    })
                else:
                    logger.debug(
                        "Skipping C volume trade due to position limit: current %.2f, order %.2f, limit %.2f",
                        exposure_c,
                        notional,
                        self.position_limits.get('C', 0.0)
                    )

        return decisions

    def _enforce_position_limits(self, market_data: Dict, exposures: Dict[str, float]) -> List[Dict]:
        """检查并纠正各执行器仓位上限"""
        corrections: List[Dict] = []
        mid_price = market_data['mid_price']

        for server in ('B', 'C'):
            exposure = exposures.get(server, 0.0)
            limit = self.position_limits.get(server, 0.0)

            if not limit or abs(exposure) <= limit:
                continue

            over_usd = abs(exposure) - limit
            order_notional = max(self.symbol_spec.min_notional, over_usd)
            raw_qty = order_notional / mid_price if mid_price > 0 else 0.0
            quantity = self._normalize_quantity(raw_qty)

            if quantity <= 0:
                logger.warning(
                    "⚠️ Unable to correct exposure for %s due to zero quantity (exposure=%.2f, limit=%.2f)",
                    server, exposure, limit
                )
                continue

            if exposure > 0:
                price = market_data['bid_price']
                action = 'sell_taker'
                reason = f'Reduce over-limit long exposure: ${exposure:.2f} > ${limit:.2f}'
            else:
                price = market_data['ask_price']
                action = 'buy_taker'
                reason = f'Reduce over-limit short exposure: ${exposure:.2f} < -${limit:.2f}'

            logger.error(
                "🚨 %s exposure %.2f exceeds limit %.2f; issuing %s of %.6f BTC",
                server, exposure, limit, action, quantity
            )

            corrections.append({
                'server': server,
                'action': action,
                'price': price,
                'quantity': quantity,
                'expire_time': time.time() + self.config.signal_expire_seconds,
                'reason': reason
            })

        return corrections

    async def _execute_decisions(self, decisions: List[Dict]):
        """执行交易决策"""
        for decision in decisions:
            try:
                server = decision['server']

                # 构造信号
                action = decision['action']

                if action in ('buy_taker', 'sell_taker'):
                    price = decision.get('price', self.last_market_price)
                else:
                    price = self._normalize_price(decision['price'])

                quantity = self._normalize_quantity(decision['quantity'])
                signal = {
                    'timestamp': time.time(),
                    'action': decision['action'],
                    'symbol': SYMBOL,
                    'price': price,
                    'quantity': quantity,
                    'expire_time': decision['expire_time'],
                    'source': 'coordinator',
                    'reason': decision['reason']
                }

                # 发送到对应服务器
                success = await self._send_signal_to_server(server, signal)

                if success:
                    self.last_signal_time[server] = time.time()
                    logger.info(f"📤 Sent {decision['action']} signal to server {server}: "
                              f"${quantity * price:.2f} @ {price}")
                else:
                    logger.warning(f"⚠️ Failed to send signal to server {server}")

            except Exception as e:
                logger.error(f"❌ Failed to execute decision: {e}")

    async def _send_signal_to_server(self, server: str, signal: Dict) -> bool:
        """向指定服务器发送信号"""
        server_config = self.server_b if server == 'B' else self.server_c

        logger.info(f"🔄 Sending signal to {server} at {server_config.url}: {signal.get('action')}")

        try:
            timeout = ClientTimeout(total=self.config.signal_timeout)
            async with ClientSession(timeout=timeout) as session:
                async with session.post(
                    f"{server_config.url}/signal",
                    json=signal,
                    headers={'Content-Type': 'application/json'}
                ) as response:
                    response_text = await response.text()
                    logger.info(f"📥 Server {server} response: status={response.status}, body={response_text[:200]}")

                    if response.status == 200:
                        result = await response.json()
                        success = result.get('success', False)
                        if not success:
                            logger.warning(f"⚠️ Server {server} returned success=False: {result.get('reason', 'unknown')}")
                        return success
                    else:
                        logger.warning(f"⚠️ Server {server} returned status {response.status}: {response_text}")
                        return False

        except asyncio.TimeoutError:
            logger.warning(f"⏰ Timeout sending signal to server {server}")
            return False
        except Exception as e:
            logger.error(f"❌ Network error sending to server {server}: {e}")
            return False

    async def _handle_emergency_situations(self, total_exposure: float):
        """处理紧急情况"""
        if abs(total_exposure) > self.config.emergency_exposure_threshold:
            logger.warning(f"🚨 EMERGENCY: Total exposure ${total_exposure:.2f} exceeds threshold")

            # 执行紧急taker单来平衡敞口
            try:
                if total_exposure > 0:
                    # 敞口过多，卖出
                    await self._execute_emergency_taker('SELL', total_exposure * 0.3)
                else:
                    # 敞口过少，买入
                    await self._execute_emergency_taker('BUY', abs(total_exposure) * 0.3)

            except Exception as e:
                logger.error(f"❌ Emergency action failed: {e}")

    async def _execute_emergency_taker(self, side: str, notional_amount: float):
        """执行紧急taker单"""
        try:
            if self.last_market_price <= 0:
                logger.error("❌ No market price available for emergency action")
                return

            # 计算数量并按照交易对规格取整
            raw_quantity = notional_amount / self.last_market_price
            quantity = self.symbol_spec.round_qty(raw_quantity)

            # 确保满足最小数量要求
            if quantity < self.symbol_spec.min_qty:
                logger.warning(f"⚠️ Emergency order quantity {quantity:.6f} below minimum {self.symbol_spec.min_qty}")
                quantity = self.symbol_spec.min_qty

            # 使用市价单快速成交
            order_params = {
                'symbol': SYMBOL,
                'side': side,
                'type': 'MARKET',
                'quantity': f"{quantity:.{self.symbol_spec.qty_precision}f}"
            }

            result = await self.account_state.client.place_order(order_params)

            if result and 'orderId' in result:
                logger.info(f"🚨 Emergency {side} executed: {quantity:.6f} BTC (${notional_amount:.2f})")
                self.session_volume += notional_amount
            else:
                logger.error(f"❌ Emergency {side} failed: {result}")

        except Exception as e:
            logger.error(f"❌ Emergency taker execution failed: {e}")

    async def _health_check_loop(self):
        """健康检查循环"""
        while self.running:
            try:
                # 检查B/C服务器状态
                b_healthy = await self._check_server_health('B')
                c_healthy = await self._check_server_health('C')

                if not b_healthy:
                    logger.warning("⚠️ Server B appears unhealthy")
                if not c_healthy:
                    logger.warning("⚠️ Server C appears unhealthy")

                # 检查自己的API连接
                try:
                    await self.account_state.client.get_account()
                    logger.debug("✅ Coordinator API connection healthy")
                except Exception as e:
                    logger.error(f"❌ Coordinator API connection failed: {e}")

                await asyncio.sleep(self.config.health_check_interval)

            except Exception as e:
                logger.error(f"❌ Health check error: {e}")
                await asyncio.sleep(10.0)

    async def _check_server_health(self, server: str) -> bool:
        """检查服务器健康状态"""
        server_config = self.server_b if server == 'B' else self.server_c

        try:
            timeout = ClientTimeout(total=5.0)
            async with ClientSession(timeout=timeout) as session:
                async with session.get(f"{server_config.url}/health") as response:
                    return response.status == 200
        except:
            return False

    async def _points_reporting_loop(self):
        """积分报告循环"""
        while self.running:
            try:
                await asyncio.sleep(300)  # 每5分钟报告一次

                session_time = time.time() - self.session_start_time
                volume_rate = self.session_volume / (session_time / 3600) if session_time > 0 else 0

                logger.info(f"📊 Session stats: Volume: ${self.session_volume:.2f}, "
                          f"Rate: ${volume_rate:.2f}/hour, Exposure: ${self.total_exposure:.2f}")

            except Exception as e:
                logger.error(f"❌ Points reporting error: {e}")

    def _handle_shutdown(self):
        """处理关闭信号"""
        logger.info(f"🛑 Shutdown signal received, stopping...")
        self.running = False

        # 主动取消所有任务以立即退出
        for task in self.tasks:
            if not task.done():
                task.cancel()

    async def cleanup(self):
        """清理资源"""
        try:
            if hasattr(self.account_state.client, 'close'):
                await self.account_state.client.close()
            logger.info("✅ Coordinator cleanup completed")
        except Exception as e:
            logger.error(f"❌ Cleanup error: {e}")

async def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description="Aster Coordinator Server")
    parser.add_argument("--account", default="A", help="Account identifier")
    parser.add_argument("--server-b", default="http://localhost:8081", help="Server B URL")
    parser.add_argument("--server-c", default="http://localhost:8082", help="Server C URL")
    parser.add_argument("--log-level", default="INFO", help="Log level")

    args = parser.parse_args()

    # 设置日志级别
    logging.getLogger().setLevel(getattr(logging, args.log_level.upper()))

    # 创建协调器
    coordinator = CoordinatorBot(args.account)
    coordinator.config.server_b_url = args.server_b
    coordinator.config.server_c_url = args.server_c

    # 异步初始化
    await coordinator._async_init()

    try:
        await coordinator.start()
    except KeyboardInterrupt:
        logger.info("👋 Coordinator stopped by user")
    except Exception as e:
        logger.error(f"💥 Coordinator crashed: {e}")
        sys.exit(1)
    finally:
        # 清理资源
        await coordinator._cleanup()

if __name__ == "__main__":
    asyncio.run(main())
