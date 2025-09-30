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
import random
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from collections import deque
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

@dataclass
class SystemConfig:
    """系统配置 - 统一管理所有配置参数"""
    # 仓位限制
    max_position_a: float = 3000.0
    max_position_b: float = 4000.0
    max_position_c: float = 4000.0
    max_order_usd: float = 500.0

    # 净敞口控制
    max_net_exposure: float = 100.0
    net_exposure_tolerance: float = 50.0
    emergency_threshold: float = 200.0

    # 订单配置
    order_size_usd: float = 150.0
    max_order_btc: float = 0.1
    volume_cooldown: float = 12.0

    # 风险管理
    daily_loss_limit: float = 500.0
    max_account_position_diff: float = 300.0

    # 积分优化
    target_maker_ratio: float = 0.8
    optimize_for_points: bool = True

    @classmethod
    def from_env(cls) -> 'SystemConfig':
        """从环境变量安全加载配置"""
        return cls(
            max_position_a=cls._get_float('MAX_POSITION_A_USD', 3000.0),
            max_position_b=cls._get_float('MAX_POSITION_B_USD', 4000.0),
            max_position_c=cls._get_float('MAX_POSITION_C_USD', 4000.0),
            max_order_usd=cls._get_float('MAX_ORDER_USD', 500.0),
            max_net_exposure=cls._get_float('MAX_NET_EXPOSURE_USD', 100.0),
            net_exposure_tolerance=cls._get_float('NET_EXPOSURE_TOLERANCE', 50.0),
            emergency_threshold=cls._get_float('EMERGENCY_BALANCE_THRESHOLD', 200.0),
            order_size_usd=cls._get_float('COORDINATOR_ORDER_SIZE_USD', 150.0),
            max_order_btc=cls._get_float('COORDINATOR_MAX_ORDER_BTC', 0.1),
            volume_cooldown=cls._get_float('VOLUME_COOLDOWN_SECONDS', 12.0),
            daily_loss_limit=cls._get_float('DAILY_LOSS_LIMIT', 500.0),
            max_account_position_diff=cls._get_float('MAX_ACCOUNT_POSITION_DIFF', 300.0),
            target_maker_ratio=cls._get_float('TARGET_MAKER_RATIO', 0.8),
            optimize_for_points=os.getenv('OPTIMIZE_FOR_POINTS', 'true').lower() == 'true',
        )

    @staticmethod
    def _get_float(key: str, default: float) -> float:
        """安全获取float配置，带验证"""
        try:
            value = float(os.getenv(key, str(default)))
            if value < 0:
                logger.warning(f"⚠️ Config {key}={value} is negative, using default {default}")
                return default
            return value
        except (ValueError, TypeError) as e:
            logger.error(f"❌ Invalid config {key}: {e}, using default {default}")
            return default

    def validate(self) -> bool:
        """验证配置合理性"""
        if self.max_position_b <= 0 or self.max_position_c <= 0:
            logger.error("❌ Position limits must be positive")
            return False

        if self.max_net_exposure > min(self.max_position_b, self.max_position_c):
            logger.warning(
                f"⚠️ Net exposure limit ({self.max_net_exposure}) larger than "
                f"min account limit ({min(self.max_position_b, self.max_position_c)})"
            )

        if self.volume_cooldown <= 0:
            logger.error("❌ Volume cooldown must be positive")
            return False

        return True

# 加载系统配置
SYSTEM_CONFIG = SystemConfig.from_env()
if not SYSTEM_CONFIG.validate():
    logger.error("❌ Configuration validation failed")
    sys.exit(1)

logger.info(
    f"✅ Config loaded: A={SYSTEM_CONFIG.max_position_a}, "
    f"B={SYSTEM_CONFIG.max_position_b}, C={SYSTEM_CONFIG.max_position_c}, "
    f"cooldown={SYSTEM_CONFIG.volume_cooldown}s"
)

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
            'A': SYSTEM_CONFIG.max_position_a,
            'B': SYSTEM_CONFIG.max_position_b,
            'C': SYSTEM_CONFIG.max_position_c
        }
        self.order_history: Dict[str, deque] = {
            'B': deque(maxlen=50),
            'C': deque(maxlen=50)
        }
        self.executor_fill_rates: Dict[str, float] = {'B': 0.5, 'C': 0.5}
        self.last_stats_fetch = 0.0

        # 积分跟踪
        self.session_volume = 0.0
        self.session_start_time = time.time()

        # 性能统计
        self.stats_window_start = time.time()
        self.stats_orders_sent = {'B': 0, 'C': 0}
        self.stats_volume_generated = 0.0
        self.last_summary_time = time.time()

        # 超限纠偏失败计数（用于taker fallback）
        self.limit_violation_rounds: Dict[str, int] = {'B': 0, 'C': 0}

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

    def _calculate_maker_price(self, market_data: Dict, side: str, aggressive: bool = False, server: Optional[str] = None) -> float:
        """计算安全的maker价格，结合成交率和随机扰动"""
        bid = market_data['bid_price']
        ask = market_data['ask_price']
        spread = max(ask - bid, self.symbol_spec.tick_size)
        if server:
            multiplier = self._calculate_adaptive_multiplier(server, aggressive)
        else:
            multiplier = 0.03 if aggressive else 0.1
        offset = max(self.symbol_spec.tick_size, spread * multiplier)
        offset *= random.uniform(0.8, 1.2)

        if side == 'SELL':
            raw_price = ask + offset
        else:
            raw_price = bid - offset

        return self._normalize_price(raw_price)

    def _calculate_optimal_order_size(self, server: str, exposure_usd: float, spread_bps: float) -> float:
        """根据 spread 和剩余额度计算刷量单大小 (USD)"""
        raw_limit = self.position_limits.get(server, 0.0)
        safe_limit = raw_limit * 0.9 if raw_limit else 0.0
        remaining = max(safe_limit - abs(exposure_usd), 0.0)

        # 降低刷量订单大小，避免过度震荡
        if spread_bps < 2.0:
            base_size = 250.0  # 从500降到250
        elif spread_bps < 5.0:
            base_size = 180.0  # 从300降到180
        else:
            base_size = 120.0  # 从150降到120

        # 限制最多用剩余额度的30%（从50%降低）
        max_safe_size = remaining * 0.3 if remaining > 0 else 0.0

        if max_safe_size <= 0:
            return 0.0

        return max(self.symbol_spec.min_notional, min(base_size, max_safe_size))

    def _append_volume_decisions(self, decisions: List[Dict], market_data: Dict, exposures: Dict[str, float]):
        current_time = time.time()
        spread_bps = market_data['spread_bps']
        mid_price = market_data['mid_price']
        jitter = 0.3
        cooldown_b = SYSTEM_CONFIG.volume_cooldown * random.uniform(1 - jitter, 1 + jitter)
        cooldown_c = SYSTEM_CONFIG.volume_cooldown * random.uniform(1 - jitter, 1 + jitter)

        # B 刷量（方向自适应）
        time_since_last_b = current_time - self.last_signal_time.get('B', 0)
        if time_since_last_b > cooldown_b and not any(d.get('server') == 'B' for d in decisions):
            exposure_b = exposures.get('B', 0.0)
            limit_b = self.position_limits.get('B', 0.0)
            if limit_b > 0:
                remaining_b = limit_b * 0.9 - abs(exposure_b)
                if remaining_b < 200.0:
                    logger.debug(f"B remaining {remaining_b:.2f} too low, skipping volume trade")
                else:
                    order_size_usd = self._calculate_optimal_order_size('B', exposure_b, spread_bps)
                    if order_size_usd > 0:
                        # 检查是否已有其他刷量单（避免同时发单）
                        has_volume_order = any('Volume generation' in d.get('reason', '') for d in decisions)
                        if has_volume_order:
                            logger.debug("B: Skipping volume trade, another server already sending")
                        else:
                            order_size_usd *= random.uniform(0.8, 1.2)
                            order_size_usd = max(self.symbol_spec.min_notional, order_size_usd)

                            # 刷量方向：优先考虑净敞口平衡
                            total_net_exposure = sum(exposures.values())

                            # 如果净敞口过大，刷量订单应该帮助平衡
                            if abs(total_net_exposure) > 200.0:
                                # 净敞口为正（多头过多） → 刷空单
                                # 净敞口为负（空头过多） → 刷多单
                                side = 'SELL' if total_net_exposure > 0 else 'BUY'
                                reason_suffix = f"balance net ${total_net_exposure:.0f}"
                            else:
                                # 净敞口正常，按个人仓位反向刷量
                                side_default = 'SELL' if exposure_b > 0 else 'BUY'
                                if self._should_reverse_direction('B', exposure_b, limit_b):
                                    side = 'BUY' if side_default == 'SELL' else 'SELL'
                                    reason_suffix = "reverse direction"
                                else:
                                    side = side_default
                                    reason_suffix = f"{'sell' if side == 'SELL' else 'buy'} side"

                            price = self._calculate_maker_price(market_data, side, aggressive=False, server='B')
                            quantity = self._normalize_quantity(order_size_usd / mid_price)
                            notional = quantity * price
                            new_exposure = exposure_b - notional if side == 'SELL' else exposure_b + notional

                            if self._within_position_limit('B', new_exposure):
                                reason = f"Volume generation - {reason_suffix}"
                                decisions.append({
                                    'server': 'B',
                                    'action': 'sell_maker' if side == 'SELL' else 'buy_maker',
                                    'price': price,
                                    'quantity': quantity,
                                    'expire_time': current_time + 60.0,
                                    'reason': reason
                                })
                            else:
                                logger.debug(
                                    "Skipping B volume trade due to limit: current %.2f, order %.2f, limit %.2f",
                                    exposure_b,
                                    notional,
                                    limit_b * 0.9
                                )

        # C 刷量（方向自适应）
        time_since_last_c = current_time - self.last_signal_time.get('C', 0)
        if time_since_last_c > cooldown_c and not any(d.get('server') == 'C' for d in decisions):
            exposure_c = exposures.get('C', 0.0)
            limit_c = self.position_limits.get('C', 0.0)
            if limit_c > 0:
                remaining_c = limit_c * 0.9 - abs(exposure_c)
                if remaining_c < 200.0:
                    logger.debug(f"C remaining {remaining_c:.2f} too low, skipping volume trade")
                else:
                    order_size_usd = self._calculate_optimal_order_size('C', exposure_c, spread_bps)
                    if order_size_usd > 0:
                        # 检查是否已有其他刷量单（避免同时发单）
                        has_volume_order = any('Volume generation' in d.get('reason', '') for d in decisions)
                        if has_volume_order:
                            logger.debug("C: Skipping volume trade, another server already sending")
                        else:
                            order_size_usd *= random.uniform(0.8, 1.2)
                            order_size_usd = max(self.symbol_spec.min_notional, order_size_usd)

                            # 刷量方向：优先考虑净敞口平衡
                            total_net_exposure = sum(exposures.values())

                            # 如果净敞口过大，刷量订单应该帮助平衡
                            if abs(total_net_exposure) > 200.0:
                                # 净敞口为正（多头过多） → 刷空单
                                # 净敞口为负（空头过多） → 刷多单
                                side = 'SELL' if total_net_exposure > 0 else 'BUY'
                                reason_suffix = f"balance net ${total_net_exposure:.0f}"
                            else:
                                # 净敞口正常，按个人仓位反向刷量
                                side_default = 'SELL' if exposure_c > 0 else 'BUY'
                                if self._should_reverse_direction('C', exposure_c, limit_c):
                                    side = 'BUY' if side_default == 'SELL' else 'SELL'
                                    reason_suffix = "reverse direction"
                                else:
                                    side = side_default
                                    reason_suffix = f"{'sell' if side == 'SELL' else 'buy'} side"

                            price = self._calculate_maker_price(market_data, side, aggressive=False, server='C')
                            quantity = self._normalize_quantity(order_size_usd / mid_price)
                            notional = quantity * price
                            new_exposure = exposure_c + notional if side == 'BUY' else exposure_c - notional

                            if self._within_position_limit('C', new_exposure):
                                reason = f"Volume generation - {reason_suffix}"
                                decisions.append({
                                    'server': 'C',
                                    'action': 'buy_maker' if side == 'BUY' else 'sell_maker',
                                    'price': price,
                                    'quantity': quantity,
                                    'expire_time': current_time + 60.0,
                                    'reason': reason
                                })
                            else:
                                logger.debug(
                                    "Skipping C volume trade due to limit: current %.2f, order %.2f, limit %.2f",
                                    exposure_c,
                                    notional,
                                    limit_c * 0.9
                                )

    def _within_position_limit(self, server: str, new_exposure: float) -> bool:
        """检查新敞口是否在安全限制内（90%限制）"""
        raw_limit = self.position_limits.get(server)
        if raw_limit is None or raw_limit <= 0:
            return True
        safe_limit = raw_limit * 0.9
        return abs(new_exposure) <= safe_limit

    def _record_order_result(self, server: str, filled: bool):
        history = self.order_history.get(server)
        if history is None:
            return
        history.append({'timestamp': time.time(), 'filled': filled})

    def _get_recent_fill_rate(self, server: str, window_seconds: float = 60.0) -> float:
        history = self.order_history.get(server)
        if not history:
            return self.executor_fill_rates.get(server, 0.5)

        cutoff = time.time() - window_seconds
        recent = [entry for entry in history if entry['timestamp'] >= cutoff]
        if not recent:
            return self.executor_fill_rates.get(server, 0.5)

        filled = sum(1 for entry in recent if entry['filled'])
        return filled / len(recent)

    async def _refresh_executor_stats(self):
        current_time = time.time()
        if current_time - self.last_stats_fetch < 5.0:
            return
        if not self.session:
            return
        self.last_stats_fetch = current_time

        for server in ('B', 'C'):
            fill_rate = await self._fetch_executor_fill_rate(server)
            if fill_rate is not None:
                self.executor_fill_rates[server] = fill_rate

    async def _fetch_executor_fill_rate(self, server: str) -> Optional[float]:
        server_config = self.server_b if server == 'B' else self.server_c
        if not server_config or not server_config.url:
            return None

        try:
            timeout = ClientTimeout(total=2.0)
            async with self.session.get(f"{server_config.url}/stats", timeout=timeout) as response:
                if response.status != 200:
                    return None
                data = await response.json()
                fill_rate = float(data.get('fill_rate', 0.5))

                # 记录挂单信息（用于日志）
                pending_count = data.get('pending_orders', 0)
                pending_timeout = data.get('pending_timeout_count', 0)
                max_pending = data.get('max_pending_duration', 0.0)

                if pending_timeout > 0:
                    logger.warning(
                        f"⚠️ {server} has {pending_timeout} stuck orders (max age: {max_pending:.1f}s) | FillRate: {fill_rate*100:.0f}%"
                    )

                return fill_rate
        except Exception as e:
            logger.debug(f"Stats fetch failed for {server}: {e}")
            return None

    def _calculate_adaptive_multiplier(self, server: str, base_aggressive: bool) -> float:
        fill_rate = self._get_recent_fill_rate(server)

        if fill_rate < 0.2:
            multiplier = 0.01
            logger.warning(
                f"⚠️ {server} fill rate {fill_rate*100:.1f}% very low, using multiplier {multiplier}"
            )
        elif fill_rate < 0.5:
            multiplier = 0.02
        elif fill_rate > 0.8:
            multiplier = 0.05
            logger.info(
                f"✅ {server} fill rate {fill_rate*100:.1f}% high, using multiplier {multiplier}"
            )
        else:
            multiplier = 0.03 if base_aggressive else 0.1

        return multiplier

    def _should_reverse_direction(self, server: str, current_exposure: float, raw_limit: float) -> bool:
        if raw_limit <= 0:
            return False

        usage = abs(current_exposure) / raw_limit
        fill_rate = self.executor_fill_rates.get(server, 0.5)

        if fill_rate < 0.2 and usage > 0.6:
            logger.warning(
                f"⚠️ {server} low fill rate {fill_rate*100:.1f}% with usage {usage*100:.1f}%, reversing direction"
            )
            return True

        if fill_rate < 0.4 and usage > 0.8:
            logger.warning(
                f"⚠️ {server} moderate fill rate {fill_rate*100:.1f}% with high usage {usage*100:.1f}%, reversing direction"
            )
            return True

        return False

    def _log_performance_summary(self):
        """输出60秒性能摘要"""
        current_time = time.time()
        window_duration = current_time - self.stats_window_start

        if window_duration < 1:
            return

        # 计算速率
        orders_per_min_b = (self.stats_orders_sent['B'] / window_duration) * 60
        orders_per_min_c = (self.stats_orders_sent['C'] / window_duration) * 60
        volume_per_hour = (self.stats_volume_generated / window_duration) * 3600

        logger.info(
            "📊 [60s Summary] Orders: B=%d(%.1f/min) C=%d(%.1f/min) | Volume: $%.0f/h | FillRate: B=%.0f%% C=%.0f%%",
            self.stats_orders_sent['B'], orders_per_min_b,
            self.stats_orders_sent['C'], orders_per_min_c,
            volume_per_hour,
            self.executor_fill_rates.get('B', 0.5) * 100,
            self.executor_fill_rates.get('C', 0.5) * 100
        )

        # 重置统计窗口
        self.stats_window_start = current_time
        self.stats_orders_sent = {'B': 0, 'C': 0}
        self.stats_volume_generated = 0.0

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

                # 2. 刷新executor统计（每5秒一次）
                await self._refresh_executor_stats()

                # 3. 计算全局敞口
                total_exposure, exposure_usd, _ = await self._calculate_total_exposure(market_data['mid_price'])

                # 4. 做出交易决策
                decisions = await self._make_trading_decisions(market_data, total_exposure, exposure_usd)

                # 5. 执行决策
                await self._execute_decisions(decisions)

                # 6. 检查紧急情况
                await self._handle_emergency_situations(total_exposure)

                # 7. 每60秒输出性能摘要
                current_time = time.time()
                if current_time - self.last_summary_time >= 60.0:
                    self._log_performance_summary()
                    self.last_summary_time = current_time

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

            # 计算仓位使用率
            usage_a = abs(exposures_usd['A']) / self.position_limits['A'] if self.position_limits['A'] > 0 else 0
            usage_b = abs(exposures_usd['B']) / self.position_limits['B'] if self.position_limits['B'] > 0 else 0
            usage_c = abs(exposures_usd['C']) / self.position_limits['C'] if self.position_limits['C'] > 0 else 0

            # 简化日志：每次只显示关键信息
            logger.info(
                "💰 Exposure | Net: $%.0f | A: $%.0f(%d%%) B: $%.0f(%d%%) C: $%.0f(%d%%) | FillRate B:%.0f%% C:%.0f%%",
                total_exposure,
                exposures_usd['A'], usage_a * 100,
                exposures_usd['B'], usage_b * 100,
                exposures_usd['C'], usage_c * 100,
                self.executor_fill_rates.get('B', 0.5) * 100,
                self.executor_fill_rates.get('C', 0.5) * 100
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
        decisions.extend(limit_decisions)

        exposure_b = exposures.get('B', 0.0)
        exposure_c = exposures.get('C', 0.0)

        # 敞口平衡逻辑
        exposure_threshold = self.config.per_server_max_notional * 0.5

        if total_exposure > exposure_threshold:
            # 敞口过多，需要减仓
            base_qty = min(SYSTEM_CONFIG.order_size_usd / mid_price, SYSTEM_CONFIG.max_order_btc)
            sell_candidates = sorted((('B', exposure_b), ('C', exposure_c)), key=lambda x: x[1], reverse=True)
            positive_candidates = [candidate for candidate in sell_candidates if candidate[1] > 0]
            if positive_candidates:
                sell_candidates = positive_candidates

            target = None
            for server, exposure in sell_candidates:
                price = self._calculate_maker_price(market_data, 'SELL', aggressive=True, server=server)
                quantity = self._normalize_quantity(base_qty)
                order_notional = quantity * price
                new_exposure = exposure - order_notional
                if self._within_position_limit(server, new_exposure):
                    target = (server, price, quantity, order_notional)
                    break

            if target:
                target_server, price, quantity, order_notional = target
                logger.debug(
                    "Using %s to reduce long exposure: current %.2f, order %.2f, new %.2f, safe limit %.2f",
                    target_server,
                    exposures.get(target_server, 0.0),
                    order_notional,
                    exposures.get(target_server, 0.0) - order_notional,
                    self.position_limits.get(target_server, 0.0) * 0.9
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
                    base_qty * mid_price,
                    exposures
                )

        elif total_exposure < -exposure_threshold:
            # 敞口过少，需要加仓
            base_qty = min(SYSTEM_CONFIG.order_size_usd / mid_price, SYSTEM_CONFIG.max_order_btc)
            buy_candidates = sorted((('B', exposure_b), ('C', exposure_c)), key=lambda x: x[1])
            negative_candidates = [candidate for candidate in buy_candidates if candidate[1] < 0]
            if negative_candidates:
                buy_candidates = negative_candidates

            target = None
            for server, exposure in buy_candidates:
                price = self._calculate_maker_price(market_data, 'BUY', aggressive=True, server=server)
                quantity = self._normalize_quantity(base_qty)
                order_notional = quantity * price
                new_exposure = exposure + order_notional
                if self._within_position_limit(server, new_exposure):
                    target = (server, price, quantity, order_notional)
                    break

            if target:
                target_server, price, quantity, order_notional = target
                logger.debug(
                    "Using %s to increase long exposure: current %.2f, order %.2f, new %.2f, safe limit %.2f",
                    target_server,
                    exposures.get(target_server, 0.0),
                    order_notional,
                    exposures.get(target_server, 0.0) + order_notional,
                    self.position_limits.get(target_server, 0.0) * 0.9
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
                    base_qty * mid_price,
                    exposures
                )

        # 积分优化逻辑 - 定期刷量
        time_since_last_b = current_time - self.last_signal_time.get('B', 0)
        time_since_last_c = current_time - self.last_signal_time.get('C', 0)

        # 如果spread合适且长时间没有交易，主动制造交易量
        if spread_bps < 5.0:
            self._append_volume_decisions(decisions, market_data, exposures)

        return decisions

    def _enforce_position_limits(self, market_data: Dict, exposures: Dict[str, float]) -> List[Dict]:
        """检查并纠正各执行器仓位上限（激进策略）"""
        corrections: List[Dict] = []
        mid_price = market_data['mid_price']

        for server in ('B', 'C'):
            exposure = exposures.get(server, 0.0)
            raw_limit = self.position_limits.get(server, 0.0)

            if raw_limit is None or raw_limit <= 0:
                continue

            safe_limit = raw_limit * 0.9

            if abs(exposure) <= safe_limit:
                # 仓位恢复正常，重置失败计数
                self.limit_violation_rounds[server] = 0
                continue

            # 增加失败轮次
            self.limit_violation_rounds[server] += 1
            violation_rounds = self.limit_violation_rounds[server]

            # 激进纠偏：平仓量 = 1.5倍超限部分（确保一次到位）
            over_usd = abs(exposure) - safe_limit
            order_notional = max(self.symbol_spec.min_notional, over_usd * 1.5)

            # 但不能超过账户当前仓位的绝对值
            order_notional = min(order_notional, abs(exposure) * 0.8)

            raw_qty = order_notional / mid_price if mid_price > 0 else 0.0
            quantity = self._normalize_quantity(raw_qty)

            if quantity <= 0:
                logger.warning(
                    "⚠️ Unable to correct exposure for %s due to zero quantity (exposure=%.2f, safe_limit=%.2f)",
                    server, exposure, safe_limit
                )
                continue

            # Taker fallback：连续3轮超限，改用taker强制平仓
            use_taker = violation_rounds >= 3

            if exposure > 0:
                if use_taker:
                    action = 'sell_taker'
                    price = mid_price  # taker使用市价
                    logger.error(
                        "🚨🚨 %s TAKER FALLBACK (round %d): %.2f > %.2f, force selling %.6f BTC",
                        server, violation_rounds, exposure, safe_limit, quantity
                    )
                else:
                    action = 'sell_maker'
                    price = self._calculate_maker_price(market_data, 'SELL', aggressive=True, server=server)
                reason = (
                    f'Reduce over-limit long exposure: ${exposure:.2f} > ${safe_limit:.2f} '
                    f'(limit ${raw_limit:.2f}, round {violation_rounds})'
                )
                new_exposure = exposure - quantity * price
            else:
                if use_taker:
                    action = 'buy_taker'
                    price = mid_price
                    logger.error(
                        "🚨🚨 %s TAKER FALLBACK (round %d): %.2f < -%.2f, force buying %.6f BTC",
                        server, violation_rounds, abs(exposure), safe_limit, quantity
                    )
                else:
                    action = 'buy_maker'
                    price = self._calculate_maker_price(market_data, 'BUY', aggressive=True, server=server)
                reason = (
                    f'Reduce over-limit short exposure: ${exposure:.2f} < -${safe_limit:.2f} '
                    f'(limit ${raw_limit:.2f}, round {violation_rounds})'
                )
                new_exposure = exposure + quantity * price

            if not use_taker:
                logger.error(
                    "🚨 %s exposure %.2f exceeds safe limit %.2f; issuing %s of %.6f BTC (round %d/3)",
                    server, exposure, safe_limit, action, quantity, violation_rounds
                )
            logger.debug(
                "Limit correction preview for %s: current %.2f -> %.2f (USD)",
                server, exposure, new_exposure
            )

            corrections.append({
                'server': server,
                'action': action,
                'price': price,
                'quantity': quantity,
                'expire_time': time.time() + (5.0 if use_taker else 15.0),
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
                    self.stats_orders_sent[server] += 1
                    self.stats_volume_generated += quantity * price
                    side_emoji = "🔵" if 'buy' in decision['action'] else "🔴"
                    logger.info(
                        f"{side_emoji} {server} {decision['action']} | $%.0f @ %.1f | Reason: %s",
                        quantity * price, price, decision['reason'][:30]
                    )
                else:
                    self._record_order_result(server, False)
                    logger.warning(f"⚠️ {server} signal failed")

            except Exception as e:
                logger.error(f"❌ Failed to execute decision: {e}")

    async def _send_signal_to_server(self, server: str, signal: Dict) -> bool:
        """向指定服务器发送信号"""
        server_config = self.server_b if server == 'B' else self.server_c

        logger.debug(f"🔄 Sending {signal.get('action')} to {server} at {server_config.url}")

        try:
            timeout = ClientTimeout(total=self.config.signal_timeout)
            async with ClientSession(timeout=timeout) as session:
                async with session.post(
                    f"{server_config.url}/signal",
                    json=signal,
                    headers={'Content-Type': 'application/json'}
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        success = result.get('success', False)
                        if not success:
                            logger.warning(f"⚠️ {server} rejected: {result.get('reason', 'unknown')[:50]}")
                        return success
                    else:
                        response_text = await response.text()
                        logger.warning(f"⚠️ {server} HTTP {response.status}: {response_text[:100]}")
                        return False

        except asyncio.TimeoutError:
            logger.warning(f"⏰ Timeout sending signal to server {server}")
            return False
        except Exception as e:
            logger.error(f"❌ Network error sending to server {server}: {e}")
            return False

    async def _handle_emergency_situations(self, total_exposure: float):
        """处理紧急情况 - A 仅做告警，不参与交易"""
        if abs(total_exposure) > self.config.emergency_exposure_threshold:
            logger.error(
                "🚨 EMERGENCY: Total exposure ${:.2f} exceeds threshold ${:.2f}"
                .format(total_exposure, self.config.emergency_exposure_threshold)
            )
            logger.error("⚠️ Manual intervention required - coordinator account does not trade")

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
