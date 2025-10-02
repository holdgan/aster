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
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from collections import deque
from aiohttp import web, ClientSession, ClientTimeout
from dotenv import load_dotenv

# 添加父目录到Python路径以便导入模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 使用现有的交易客户端和配置
from utils.trading_client import (
    AsterFuturesClient, TradingConfig, AccountConfig, AccountState, SymbolSpec,
    get_logger
)

# --- Trading Strategy Constants ------------------------------------------------
# All strategy parameters are collected here for easy management

SYMBOL = "BTCUSDT"

# Position and risk parameters (仓位和风控参数)
MAX_POSITION_B_USD = 5000.0  # Server B maximum position in USD
MAX_POSITION_C_USD = 5000.0  # Server C maximum position in USD
MAX_LEVERAGE = 10.0
SAFETY_MARGIN = 0.95
MIN_ORDER_QTY_BTC = 0.001

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
    max_position_b: float = MAX_POSITION_B_USD
    max_position_c: float = MAX_POSITION_C_USD
    max_order_usd: float = 600.0
    order_size_usd: float = 226.0
    order_quantity_btc: float = 0.002  # 新增：直接指定BTC数量
    max_order_btc: float = 0.005
    volume_cooldown: float = 12.0
    target_position_btc_b: float = 0.0
    target_position_btc_c: float = 0.0
    target_position_threshold_btc: float = 0.01
    limit_fallback_count: int = 3
    pending_count_threshold: int = 3  # 从2改为3
    pending_age_threshold_seconds: float = 12.0  # 从8改为12
    # 增仓策略配置
    increase_position_usage_threshold: float = 0.7  # 仓位使用率低于此值时优先增仓
    net_exposure_soft_limit: float = 50.0  # 净敞口软限制（触发方向调整）
    net_exposure_hard_limit: float = 200.0  # 净敞口硬限制（强制平衡）

    @classmethod
    def from_env(cls) -> 'SystemConfig':
        """从环境变量加载风控参数，其他参数使用硬编码默认值"""
        return cls(
            # 策略参数 - 使用类定义的默认值（硬编码）
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

        if self.volume_cooldown <= 0:
            logger.error("❌ Volume cooldown must be positive")
            return False

        if self.limit_fallback_count < 0:
            logger.warning("⚠️ limit_fallback_count negative, reset to 0")
            self.limit_fallback_count = 0

        minimum_threshold = max(MIN_ORDER_QTY_BTC, 0.01)
        if self.target_position_threshold_btc < minimum_threshold:
            logger.warning(
                "⚠️ target_position_threshold_btc %.4f too small, bumping to %.4f BTC",
                self.target_position_threshold_btc,
                minimum_threshold
            )
            self.target_position_threshold_btc = minimum_threshold

        if self.max_order_btc < MIN_ORDER_QTY_BTC:
            logger.warning(
                "⚠️ max_order_btc %.4f too small, bumping to %.4f BTC",
                self.max_order_btc,
                MIN_ORDER_QTY_BTC
            )
            self.max_order_btc = MIN_ORDER_QTY_BTC

        if self.pending_age_threshold_seconds <= 0:
            logger.warning("⚠️ pending_age_threshold_seconds must be positive, fallback to 8s")
            self.pending_age_threshold_seconds = 8.0

        if self.pending_count_threshold < 0:
            logger.warning("⚠️ pending_count_threshold negative, reset to 0")
            self.pending_count_threshold = 0

        # 校验新增字段: order_quantity_btc
        if self.order_quantity_btc < MIN_ORDER_QTY_BTC:
            logger.error(
                f"❌ order_quantity_btc {self.order_quantity_btc:.4f} < minimum {MIN_ORDER_QTY_BTC}"
            )
            return False

        if self.order_quantity_btc > self.max_order_btc:
            logger.error(
                f"❌ order_quantity_btc {self.order_quantity_btc:.4f} > max_order_btc {self.max_order_btc:.4f}"
            )
            return False

        # 校验新增字段: increase_position_usage_threshold
        if not (0 < self.increase_position_usage_threshold <= 1.0):
            logger.error(
                f"❌ increase_position_usage_threshold {self.increase_position_usage_threshold:.2f} "
                f"must be in (0, 1]"
            )
            return False

        # 校验净敞口阈值关系
        if self.net_exposure_soft_limit >= self.net_exposure_hard_limit:
            logger.error(
                f"❌ net_exposure_soft_limit {self.net_exposure_soft_limit:.0f} "
                f"must < hard_limit {self.net_exposure_hard_limit:.0f}"
            )
            return False

        if self.net_exposure_hard_limit <= 0:
            logger.error(
                f"❌ net_exposure_hard_limit {self.net_exposure_hard_limit:.0f} must > 0"
            )
            return False

        logger.info(
            f"✅ Config validated: order_qty={self.order_quantity_btc:.4f} BTC, "
            f"usage_threshold={self.increase_position_usage_threshold:.0%}, "
            f"net_exposure soft={self.net_exposure_soft_limit:.0f}/hard={self.net_exposure_hard_limit:.0f}"
        )
        return True

# 加载系统配置
SYSTEM_CONFIG = SystemConfig.from_env()
if not SYSTEM_CONFIG.validate():
    logger.error("❌ Configuration validation failed")
    sys.exit(1)

logger.info(
    "✅ Config loaded | max_position B=%.0f C=%.0f | order_size %.0f | cooldown %.1fs | pending threshold=%d/%.1fs",
    SYSTEM_CONFIG.max_position_b,
    SYSTEM_CONFIG.max_position_c,
    SYSTEM_CONFIG.order_size_usd,
    SYSTEM_CONFIG.volume_cooldown,
    SYSTEM_CONFIG.pending_count_threshold,
    SYSTEM_CONFIG.pending_age_threshold_seconds
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
    # 服务器端点 - from .env (网络配置)
    server_b_url: str = ""
    server_c_url: str = ""

    # 信号发送配置 - 硬编码（策略配置）
    signal_timeout: float = 3.0
    signal_expire_seconds: float = 10.0

    # 敞口控制 - 硬编码（策略配置，非风控）
    max_total_exposure_usd: float = 5000.0
    emergency_exposure_threshold: float = 8000.0
    per_server_max_notional: float = 1000.0

    # 决策间隔 - 硬编码（策略配置）
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
        self.executor_exposures: Dict[str, float] = {'B': 0.0, 'C': 0.0}
        self.position_limits: Dict[str, float] = {
            'B': SYSTEM_CONFIG.max_position_b,
            'C': SYSTEM_CONFIG.max_position_c
        }
        self.target_positions_btc: Dict[str, float] = {
            'B': SYSTEM_CONFIG.target_position_btc_b,
            'C': SYSTEM_CONFIG.target_position_btc_c
        }
        self.target_threshold_btc = SYSTEM_CONFIG.target_position_threshold_btc
        self.order_history: Dict[str, deque] = {
            'B': deque(maxlen=50),
            'C': deque(maxlen=50)
        }
        self.executor_fill_rates: Dict[str, float] = {'B': 0.5, 'C': 0.5}
        self.executor_pending_orders: Dict[str, int] = {'B': 0, 'C': 0}
        self.executor_pending_max_age: Dict[str, float] = {'B': 0.0, 'C': 0.0}
        self.executor_stats_blocked: Dict[str, bool] = {'B': False, 'C': False}
        self.limit_violation_rounds: Dict[str, int] = {'B': 0, 'C': 0}
        self.last_stats_fetch = 0.0
        self.executor_positions_btc: Dict[str, float] = {'B': 0.0, 'C': 0.0}
        self.summary_metrics = {
            'B': {'fill_sum': 0.0, 'fill_count': 0, 'pending_max': 0.0},
            'C': {'fill_sum': 0.0, 'fill_count': 0, 'pending_max': 0.0}
        }

        # 积分跟踪
        self.session_volume = 0.0
        self.session_start_time = time.time()

        # 性能统计
        self.stats_window_start = time.time()
        self.stats_orders_sent = {'B': 0, 'C': 0}
        self.stats_volume_generated = 0.0
        self.last_summary_time = time.time()

        # 刷量轮换（防止B和C同时刷量）
        self.last_volume_sender = 'C'  # 初始为C，这样B先发

        # Emergency控制
        self.emergency_trigger_count = 0  # 连续触发计数
        self.last_emergency_time = 0.0

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
            fill_rate = self._get_recent_fill_rate(server)
        else:
            multiplier = 0.03 if aggressive else 0.1
            fill_rate = 0.5

        offset = max(self.symbol_spec.tick_size, spread * multiplier)

        # 根据成交率调整随机扰动：成交率低时更激进（只向内扰动）
        if fill_rate < 0.2:
            # 成交率很低：只向内扰动 (0.5-0.9)
            offset *= random.uniform(0.5, 0.9)
        elif fill_rate < 0.5:
            # 成交率中等：适度向内扰动 (0.6-1.0)
            offset *= random.uniform(0.6, 1.0)
        else:
            # 成交率正常：正常扰动 (0.6-1.4)
            offset *= random.uniform(0.6, 1.4)

        if side == 'SELL':
            raw_price = ask + offset
        else:
            raw_price = bid - offset

        return self._normalize_price(raw_price)

    def _calculate_optimal_order_size(self, server: str, exposure_usd: float, spread_bps: float) -> float:
        """根据 spread 和剩余额度计算刷量单大小 (USD)"""
        raw_limit = self.position_limits.get(server, 0.0)
        safe_limit = raw_limit * SAFETY_MARGIN if raw_limit else 0.0
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
        leverage_cap = (safe_limit / MAX_LEVERAGE) if safe_limit > 0 else base_size
        if leverage_cap > 0:
            max_safe_size = min(max_safe_size, leverage_cap)

        if max_safe_size <= 0:
            return 0.0

        return max(self.symbol_spec.min_notional, min(base_size, max_safe_size))

    def _simulate_exposure_change(self, exposures_usd: Dict[str, float], exposures_btc: Dict[str, float], server: str,
                                  action: str, price: float, quantity: float):
        """根据指令预估敞口变化，供后续决策使用"""
        notional = quantity * price
        if action in ('buy_maker', 'buy_taker'):
            exposures_usd[server] = exposures_usd.get(server, 0.0) + notional
            exposures_btc[server] = exposures_btc.get(server, 0.0) + quantity
        elif action in ('sell_maker', 'sell_taker'):
            exposures_usd[server] = exposures_usd.get(server, 0.0) - notional
            exposures_btc[server] = exposures_btc.get(server, 0.0) - quantity

    def _ensure_target_inventory(self, decisions: List[Dict], market_data: Dict,
                                 exposures_usd: Dict[str, float], exposures_btc: Dict[str, float]):
        mid_price = market_data['mid_price']

        # 限制每轮最多调整1个server，避免同时建仓导致净敞口失控
        adjusted_count = 0
        max_adjustments_per_round = 1

        for server in ('B', 'C'):
            # 检查本轮是否已有该server的决策（避免重复）
            if any(d.get('server') == server for d in decisions):
                logger.debug(f"{server} already has decision in this round, skip target adjust")
                continue

            # 检查是否已达到本轮调整上限
            if adjusted_count >= max_adjustments_per_round:
                logger.debug(f"Target adjustment limit reached ({adjusted_count}/{max_adjustments_per_round}), defer remaining")
                break

            pending_count = self.executor_pending_orders.get(server, 0)
            max_pending_age = self.executor_pending_max_age.get(server, 0.0)
            count_threshold = SYSTEM_CONFIG.pending_count_threshold
            age_threshold = SYSTEM_CONFIG.pending_age_threshold_seconds
            pending_block = count_threshold > 0 and pending_count >= count_threshold
            age_block = age_threshold > 0 and max_pending_age >= age_threshold
            if self.executor_stats_blocked.get(server) or pending_block or age_block:
                logger.debug(
                    "%s target adjust skipped due to pending backlog (count=%d, age=%.1fs)",
                    server,
                    pending_count,
                    max_pending_age
                )
                continue

            target = self.target_positions_btc.get(server, 0.0)
            current = exposures_btc.get(server, 0.0)
            diff = current - target
            if abs(diff) < self.target_threshold_btc:
                continue

            raw_limit = self.position_limits.get(server, 0.0)
            safe_limit = raw_limit * SAFETY_MARGIN if raw_limit else 0.0

            side = 'SELL' if diff > 0 else 'BUY'
            # 限制单次调整量：最多调整1/4的偏差，避免订单过大触发executor限制
            max_adjust_qty = abs(diff) * 0.25
            desired_qty = min(max_adjust_qty, SYSTEM_CONFIG.max_order_btc, safe_limit / mid_price * 0.3)
            quantity = self._normalize_quantity(desired_qty)
            if quantity < MIN_ORDER_QTY_BTC:
                quantity = self._normalize_quantity(MIN_ORDER_QTY_BTC)
            if quantity <= 0:
                continue

            price = self._calculate_maker_price(market_data, side, aggressive=True, server=server)
            notional = quantity * price
            new_exposure_usd = exposures_usd.get(server, 0.0) - notional if side == 'SELL' else exposures_usd.get(server, 0.0) + notional
            if safe_limit and abs(new_exposure_usd) > safe_limit:
                continue

            decisions.append({
                'server': server,
                'action': 'sell_maker' if side == 'SELL' else 'buy_maker',
                'price': price,
                'quantity': quantity,
                'expire_time': time.time() + 30.0,
                'reason': f'Target inventory adjust to {target:.4f} BTC'
            })
            self._simulate_exposure_change(exposures_usd, exposures_btc, server,
                                           'sell_maker' if side == 'SELL' else 'buy_maker',
                                           price, quantity)
            adjusted_count += 1  # 记录本轮已调整的server数量

    def _append_emergency_net_exposure_correction(
        self,
        decisions: List[Dict],
        market_data: Dict,
        exposures_usd: Dict[str, float],
        exposures_btc: Dict[str, float],
        total_net_exposure: float
    ):
        """
        紧急净敞口纠偏（最高优先级，但要避免过度发单）

        选择与净敞口同号且abs最小的账户执行反向订单，避免单边负担。
        新增限制：如果目标账户已有pending订单，暂缓发单，等待成交。
        连续触发3次后切换taker确保成交。
        """
        mid_price = market_data['mid_price']

        # 确定需要的操作方向
        if total_net_exposure > 0:
            side = 'SELL'
            direction_desc = "short"
            # 找与净敞口同号(正值)且abs最小的账户
            candidates = {k: v for k, v in exposures_usd.items() if v >= 0}
        else:
            side = 'BUY'
            direction_desc = "long"
            # 找与净敞口同号(负值)且abs最小的账户
            candidates = {k: v for k, v in exposures_usd.items() if v <= 0}

        # 如果没有同号账户（极端情况），fallback到abs最小
        if not candidates:
            logger.warning("⚠️ No same-sign candidate, falling back to min abs(exposure)")
            target_server = min(exposures_usd.items(), key=lambda x: abs(x[1]))[0]
        else:
            # 选同号中abs最小的
            target_server = min(candidates.items(), key=lambda x: abs(x[1]))[0]

        # 检查目标账户的pending状态，避免过度发单
        pending_count = self.executor_pending_orders.get(target_server, 0)
        max_pending_age = self.executor_pending_max_age.get(target_server, 0.0)

        # 如果目标账户已有pending订单，暂缓发单（等待现有订单成交）
        if pending_count > 0:
            logger.warning(
                f"🚨 Emergency correction delayed: {target_server} has {pending_count} pending orders "
                f"(age={max_pending_age:.1f}s), waiting for settlement before new order"
            )
            return  # 不发新单，等待现有订单成交

        # 更新连续触发计数
        current_time = time.time()
        if current_time - self.last_emergency_time > 30.0:
            # 超过30秒没触发，重置计数
            self.emergency_trigger_count = 0
        self.emergency_trigger_count += 1
        self.last_emergency_time = current_time

        # 连续触发3次后切换taker确保成交
        use_taker = self.emergency_trigger_count >= 3

        if use_taker:
            # Taker模式：使用市价单
            action = f'{side.lower()}_taker'
            price = mid_price
            logger.warning(
                f"⚠️ Emergency triggered {self.emergency_trigger_count} times, switching to TAKER"
            )
        else:
            # Maker模式：使用最激进定价
            action = f'{side.lower()}_maker'
            price = self._calculate_maker_price(market_data, side, aggressive=True, server=target_server)

        quantity = SYSTEM_CONFIG.order_quantity_btc

        decision = {
            'server': target_server,
            'action': action,
            'price': price,
            'quantity': quantity,
            'expire_time': time.time() + 60.0,
            'reason': f'🚨 EMERGENCY net exposure balance: ${total_net_exposure:.0f} -> {direction_desc}'
        }

        decisions.append(decision)

        # 模拟更新exposures，避免后续逻辑使用旧数据
        self._simulate_exposure_change(exposures_usd, exposures_btc, target_server, action, price, quantity)

        logger.warning(
            f"🚨 Emergency ({self.emergency_trigger_count}x): {target_server} {side} {quantity:.4f} BTC "
            f"@ ${price:.1f} ({'TAKER' if use_taker else 'MAKER'}) to balance net ${total_net_exposure:.0f}"
        )

    def _append_volume_decisions(
        self,
        decisions: List[Dict],
        market_data: Dict,
        exposures_usd: Dict[str, float],
        exposures_btc: Dict[str, float]
    ):
        current_time = time.time()
        spread_bps = market_data['spread_bps']
        mid_price = market_data['mid_price']
        cooldown_b = random.uniform(6.0, 18.0)
        cooldown_c = random.uniform(6.0, 18.0)
        target_threshold = self.target_threshold_btc

        # B 刷量（方向自适应）
        time_since_last_b = current_time - self.last_signal_time.get('B', 0)
        if time_since_last_b > cooldown_b and not any(d.get('server') == 'B' for d in decisions):
            if self.executor_stats_blocked.get('B'):
                logger.debug("B pending watchdog active, skip volume")
            else:
                exposure_b = exposures_usd.get('B', 0.0)
                position_b = exposures_btc.get('B', 0.0)
                target_b = self.target_positions_btc.get('B', 0.0)
                delta_b = position_b - target_b
                if abs(delta_b) > target_threshold:
                    logger.debug(
                        "B target drift %.4f outside threshold %.4f, skip volume",
                        delta_b,
                        target_threshold
                    )
                else:
                    limit_b = self.position_limits.get('B', 0.0)
                    if limit_b > 0:
                        remaining_b = limit_b * SAFETY_MARGIN - abs(exposure_b)
                        if remaining_b < 200.0:
                            logger.debug(f"B remaining {remaining_b:.2f} too low, skipping volume trade")
                        else:
                            order_size_usd = self._calculate_optimal_order_size('B', exposure_b, spread_bps)
                            if order_size_usd > 0:
                                # 轮换机制：只允许非上次发单账户刷量
                                if self.last_volume_sender == 'B':
                                    logger.debug("B: Skipping volume trade, last sender was B (rotation)")
                                else:
                                    order_size_usd *= random.uniform(0.8, 1.2)
                                    order_size_usd = max(self.symbol_spec.min_notional, order_size_usd)

                                    usage = abs(exposure_b) / limit_b if limit_b > 0 else 0

                                    # 简化的静态互补策略: B=BUY，仓位使用率低于阈值时优先增仓
                                    if usage < SYSTEM_CONFIG.increase_position_usage_threshold:
                                        # 增仓模式：B固定做多
                                        side = 'BUY'
                                        reason_suffix = f"increase long (usage {usage*100:.0f}%)"
                                    else:
                                        # 保守模式：按当前仓位反向平衡
                                        side_default = 'SELL' if exposure_b > 0 else 'BUY'
                                        if self._should_reverse_direction('B', exposure_b, limit_b):
                                            side = 'BUY' if side_default == 'SELL' else 'SELL'
                                            reason_suffix = f"reverse (usage {usage*100:.0f}%)"
                                        else:
                                            side = side_default
                                            reason_suffix = f"maintain (usage {usage*100:.0f}%)"

                                    price = self._calculate_maker_price(market_data, side, aggressive=False, server='B')
                                    # 直接使用配置的BTC数量，避免USD换算精度问题
                                    quantity = SYSTEM_CONFIG.order_quantity_btc
                                    notional = quantity * price
                                    new_exposure = exposure_b - notional if side == 'SELL' else exposure_b + notional

                                    if self._within_position_limit('B', new_exposure):
                                        reason = f"Volume generation - {reason_suffix}"
                                        decision = {
                                            'server': 'B',
                                            'action': 'sell_maker' if side == 'SELL' else 'buy_maker',
                                            'price': price,
                                            'quantity': quantity,
                                            'expire_time': current_time + 60.0,
                                            'reason': reason
                                        }
                                        decisions.append(decision)
                                        self._simulate_exposure_change(
                                            exposures_usd,
                                            exposures_btc,
                                            decision['server'],
                                            decision['action'],
                                            decision['price'],
                                            decision['quantity']
                                        )
                                    else:
                                        logger.debug(
                                            "Skipping B volume trade due to limit: current %.2f, order %.2f, limit %.2f",
                                            exposure_b,
                                            notional,
                                            limit_b * SAFETY_MARGIN
                                        )

        # C 刷量（方向自适应）
        time_since_last_c = current_time - self.last_signal_time.get('C', 0)
        if time_since_last_c > cooldown_c and not any(d.get('server') == 'C' for d in decisions):
            if self.executor_stats_blocked.get('C'):
                logger.debug("C pending watchdog active, skip volume")
            else:
                exposure_c = exposures_usd.get('C', 0.0)
                position_c = exposures_btc.get('C', 0.0)
                target_c = self.target_positions_btc.get('C', 0.0)
                delta_c = position_c - target_c
                if abs(delta_c) > target_threshold:
                    logger.debug(
                        "C target drift %.4f outside threshold %.4f, skip volume",
                        delta_c,
                        target_threshold
                    )
                else:
                    limit_c = self.position_limits.get('C', 0.0)
                    if limit_c > 0:
                        remaining_c = limit_c * SAFETY_MARGIN - abs(exposure_c)
                        if remaining_c < 200.0:
                            logger.debug(f"C remaining {remaining_c:.2f} too low, skipping volume trade")
                        else:
                            order_size_usd = self._calculate_optimal_order_size('C', exposure_c, spread_bps)
                            if order_size_usd > 0:
                                # 轮换机制：只允许非上次发单账户刷量
                                if self.last_volume_sender == 'C':
                                    logger.debug("C: Skipping volume trade, last sender was C (rotation)")
                                else:
                                    order_size_usd *= random.uniform(0.8, 1.2)
                                    order_size_usd = max(self.symbol_spec.min_notional, order_size_usd)

                                    usage = abs(exposure_c) / limit_c if limit_c > 0 else 0

                                    # 简化的静态互补策略: C=SELL，仓位使用率低于阈值时优先增仓
                                    if usage < SYSTEM_CONFIG.increase_position_usage_threshold:
                                        # 增仓模式：C固定做空
                                        side = 'SELL'
                                        reason_suffix = f"increase short (usage {usage*100:.0f}%)"
                                    else:
                                        # 保守模式：按当前仓位反向平衡
                                        side_default = 'SELL' if exposure_c > 0 else 'BUY'
                                        if self._should_reverse_direction('C', exposure_c, limit_c):
                                            side = 'BUY' if side_default == 'SELL' else 'SELL'
                                            reason_suffix = f"reverse (usage {usage*100:.0f}%)"
                                        else:
                                            side = side_default
                                            reason_suffix = f"maintain (usage {usage*100:.0f}%)"

                                    price = self._calculate_maker_price(market_data, side, aggressive=False, server='C')
                                    # 直接使用配置的BTC数量，避免USD换算精度问题
                                    quantity = SYSTEM_CONFIG.order_quantity_btc
                                    notional = quantity * price
                                    new_exposure = exposure_c + notional if side == 'BUY' else exposure_c - notional

                                    if self._within_position_limit('C', new_exposure):
                                        reason = f"Volume generation - {reason_suffix}"
                                        decision = {
                                            'server': 'C',
                                            'action': 'buy_maker' if side == 'BUY' else 'sell_maker',
                                            'price': price,
                                            'quantity': quantity,
                                            'expire_time': current_time + 60.0,
                                            'reason': reason
                                        }
                                        decisions.append(decision)
                                        self._simulate_exposure_change(
                                            exposures_usd,
                                            exposures_btc,
                                            decision['server'],
                                            decision['action'],
                                            decision['price'],
                                            decision['quantity']
                                        )
                                    else:
                                        logger.debug(
                                            "Skipping C volume trade due to limit: current %.2f, order %.2f, limit %.2f",
                                            exposure_c,
                                            notional,
                                            limit_c * SAFETY_MARGIN
                                        )

    def _within_position_limit(self, server: str, new_exposure: float) -> bool:
        """检查新敞口是否在安全限制内（90%限制）"""
        raw_limit = self.position_limits.get(server)
        if raw_limit is None or raw_limit <= 0:
            return True
        safe_limit = raw_limit * SAFETY_MARGIN
        return abs(new_exposure) <= safe_limit

    def _record_order_result(self, server: str, filled: bool):
        history = self.order_history.get(server)
        if history is None:
            return
        history.append({'timestamp': time.time(), 'filled': filled})

    def _accumulate_summary_metrics(self, server: str, fill_rate: float, max_pending_age: float):
        bucket = self.summary_metrics.get(server)
        if bucket is None:
            return
        bucket['fill_sum'] += fill_rate
        bucket['fill_count'] += 1
        bucket['pending_max'] = max(bucket['pending_max'], max_pending_age)

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
            stats = await self._fetch_executor_stats(server)
            if not stats:
                continue

            fill_rate = float(stats.get('fill_rate', self.executor_fill_rates.get(server, 0.5)))
            pending_count = int(stats.get('pending_count', 0))
            max_pending_age = float(stats.get('max_pending_age', 0.0))

            prev_blocked = self.executor_stats_blocked.get(server, False)
            count_threshold = SYSTEM_CONFIG.pending_count_threshold
            age_threshold = SYSTEM_CONFIG.pending_age_threshold_seconds
            pending_block = count_threshold > 0 and pending_count >= count_threshold
            age_block = age_threshold > 0 and max_pending_age >= age_threshold
            blocked = pending_block or age_block

            if blocked:
                if not prev_blocked:
                    logger.warning(
                        "⚠️ %s pending backlog detected: count=%d, max_age=%.1fs; suspending volume",
                        server,
                        pending_count,
                        max_pending_age
                    )
                fill_rate = 0.0
            elif prev_blocked and not blocked:
                logger.info(
                    "✅ %s pending cleared (count=%d, max_age=%.1fs); resuming volume",
                    server,
                    pending_count,
                    max_pending_age
                )

            self.executor_stats_blocked[server] = blocked
            self.executor_fill_rates[server] = fill_rate
            self.executor_pending_orders[server] = pending_count
            self.executor_pending_max_age[server] = max_pending_age
            self._accumulate_summary_metrics(server, fill_rate, max_pending_age)

    async def _fetch_executor_stats(self, server: str) -> Optional[Dict[str, float]]:
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
                pending_count = int(data.get('pending_count', data.get('pending_orders', 0)))
                pending_timeout = int(data.get('pending_timeout_count', 0))
                max_pending = float(data.get('max_pending_age', data.get('max_pending_duration', 0.0)))

                if pending_timeout > 0:
                    logger.warning(
                        f"⚠️ {server} has {pending_timeout} stuck orders (max age: {max_pending:.1f}s) | Reported fill_rate={fill_rate*100:.0f}%"
                    )

                return {
                    'fill_rate': fill_rate,
                    'pending_count': pending_count,
                    'max_pending_age': max_pending
                }
        except Exception as e:
            logger.debug(f"Stats fetch failed for {server}: {e}")
            return None

    def _calculate_adaptive_multiplier(self, server: str, base_aggressive: bool) -> float:
        fill_rate = self._get_recent_fill_rate(server)
        violation_rounds = self.limit_violation_rounds.get(server, 0)
        fallback_threshold = max(0, SYSTEM_CONFIG.limit_fallback_count)

        if fill_rate < 0.05:
            multiplier = 0.008  # 5%以下极度激进
            logger.warning(
                f"⚠️ {server} fill rate {fill_rate*100:.1f}% critical, using ultra-aggressive multiplier {multiplier}"
            )
        elif fill_rate < 0.1:
            multiplier = 0.012  # 5-10%很激进
            logger.warning(
                f"⚠️ {server} fill rate {fill_rate*100:.1f}% extremely low, using multiplier {multiplier}"
            )
        elif fill_rate < 0.2:
            multiplier = 0.02  # 10-20%用2%
            logger.warning(
                f"⚠️ {server} fill rate {fill_rate*100:.1f}% very low, using multiplier {multiplier}"
            )
        elif fill_rate < 0.5:
            multiplier = 0.03  # 20-50%用3%
        elif fill_rate > 0.8:
            multiplier = 0.05
            logger.info(
                f"✅ {server} fill rate {fill_rate*100:.1f}% high, using multiplier {multiplier}"
            )
        else:
            multiplier = 0.03 if base_aggressive else 0.1

        if base_aggressive and fallback_threshold > 0 and violation_rounds >= max(1, fallback_threshold - 1):
            multiplier = min(multiplier, 0.005)
            logger.warning(
                "⚠️ %s close to taker fallback (round %d/%d), tightening maker offset",
                server,
                violation_rounds,
                fallback_threshold
            )
        elif base_aggressive and violation_rounds > 0:
            multiplier = min(multiplier, 0.015)

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

        bucket_b = self.summary_metrics.get('B', {'fill_sum': 0.0, 'fill_count': 0, 'pending_max': 0.0})
        bucket_c = self.summary_metrics.get('C', {'fill_sum': 0.0, 'fill_count': 0, 'pending_max': 0.0})
        avg_fill_b = (bucket_b['fill_sum'] / bucket_b['fill_count']) if bucket_b['fill_count'] else self.executor_fill_rates.get('B', 0.5)
        avg_fill_c = (bucket_c['fill_sum'] / bucket_c['fill_count']) if bucket_c['fill_count'] else self.executor_fill_rates.get('C', 0.5)
        max_pending_b = bucket_b['pending_max']
        max_pending_c = bucket_c['pending_max']

        logger.info(
            "📊 [60s Summary] Orders B=%d(%.1f/min) C=%d(%.1f/min) | Volume $%.0f/h | Fill(avg) B=%.0f%% C=%.0f%% | PendingMax B=%.1fs C=%.1fs | PendingCnt B=%d C=%d",
            self.stats_orders_sent['B'], orders_per_min_b,
            self.stats_orders_sent['C'], orders_per_min_c,
            volume_per_hour,
            avg_fill_b * 100,
            avg_fill_c * 100,
            max_pending_b,
            max_pending_c,
            self.executor_pending_orders.get('B', 0),
            self.executor_pending_orders.get('C', 0)
        )

        # 重置统计窗口
        self.stats_window_start = current_time
        self.stats_orders_sent = {'B': 0, 'C': 0}
        self.stats_volume_generated = 0.0
        for bucket in self.summary_metrics.values():
            bucket['fill_sum'] = 0.0
            bucket['fill_count'] = 0
            bucket['pending_max'] = 0.0

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
                total_exposure, exposure_usd, exposure_btc = await self._calculate_total_exposure(market_data['mid_price'])

                # 4. 做出交易决策
                decisions = await self._make_trading_decisions(market_data, total_exposure, exposure_usd, exposure_btc)

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
            position_b = await self._fetch_executor_position('B')
            position_c = await self._fetch_executor_position('C')

            exposures_btc = {
                'B': position_b,
                'C': position_c
            }

            exposures_usd = {key: value * mid_price for key, value in exposures_btc.items()}

            total_exposure = sum(exposures_usd.values())
            self.executor_exposures = exposures_usd
            self.executor_positions_btc = exposures_btc
            self.total_exposure = total_exposure

            # 计算仓位使用率
            usage_b = abs(exposures_usd['B']) / self.position_limits['B'] if self.position_limits['B'] > 0 else 0
            usage_c = abs(exposures_usd['C']) / self.position_limits['C'] if self.position_limits['C'] > 0 else 0

            target_b = self.target_positions_btc.get('B', 0.0)
            target_c = self.target_positions_btc.get('C', 0.0)
            diff_b = exposures_btc['B'] - target_b
            diff_c = exposures_btc['C'] - target_c

            pending_b = self.executor_pending_orders.get('B', 0)
            pending_c = self.executor_pending_orders.get('C', 0)
            pending_age_b = self.executor_pending_max_age.get('B', 0.0)
            pending_age_c = self.executor_pending_max_age.get('C', 0.0)

            # 简化日志：每次只显示关键信息
            logger.info(
                "💰 Exposure | Net:$%.0f | B:$%.0f(%d%%) C:$%.0f(%d%%) | ΔB:%.4f ΔC:%.4f | Pending B:%d(%.1fs) C:%d(%.1fs) | FillRate B:%.0f%% C:%.0f%%",
                total_exposure,
                exposures_usd['B'], usage_b * 100,
                exposures_usd['C'], usage_c * 100,
                diff_b,
                diff_c,
                pending_b,
                pending_age_b,
                pending_c,
                pending_age_c,
                self.executor_fill_rates.get('B', 0.5) * 100,
                self.executor_fill_rates.get('C', 0.5) * 100
            )
            return total_exposure, exposures_usd, exposures_btc

        except Exception as e:
            logger.error(f"❌ Failed to calculate exposure: {e}")
            fallback = {'B': 0.0, 'C': 0.0}
            return 0.0, fallback, fallback

    async def _make_trading_decisions(
        self,
        market_data: Dict,
        total_exposure: float,
        exposures_usd: Dict[str, float],
        exposures_btc: Dict[str, float]
    ) -> List[Dict]:
        """制定交易决策"""
        decisions: List[Dict] = []
        current_time = time.time()

        exposures_usd_local = dict(exposures_usd)
        exposures_btc_local = dict(exposures_btc)

        # 决策数量限制：每轮最多2个决策
        MAX_DECISIONS = 2

        # ========== 优先级0: 净敞口硬限制（最高优先级，但允许并行） ==========
        total_net_exposure = sum(exposures_usd_local.values())
        emergency_triggered = False
        if abs(total_net_exposure) > SYSTEM_CONFIG.net_exposure_hard_limit:
            logger.warning(
                f"🚨 Net exposure ${total_net_exposure:.0f} exceeds HARD LIMIT "
                f"${SYSTEM_CONFIG.net_exposure_hard_limit:.0f}, triggering emergency balance"
            )
            self._append_emergency_net_exposure_correction(
                decisions, market_data, exposures_usd_local, exposures_btc_local, total_net_exposure
            )
            emergency_triggered = True
            # 不再直接return，允许后续逻辑并行执行（如果决策数未满）

        # 基于敞口和市场状况制定决策
        mid_price = market_data['mid_price']
        spread_bps = market_data['spread_bps']

        # ========== 优先级1: 仓位限制纠偏 ==========
        if len(decisions) < MAX_DECISIONS:
            limit_decisions = self._enforce_position_limits(market_data, exposures_usd_local)
            for correction in limit_decisions:
                if len(decisions) >= MAX_DECISIONS:
                    break
                decisions.append(correction)
                self._simulate_exposure_change(
                    exposures_usd_local,
                    exposures_btc_local,
                    correction['server'],
                    correction['action'],
                    correction['price'],
                    correction['quantity']
                )

        # ========== 优先级2: 目标仓位调节 ==========
        # Emergency触发后跳过目标仓位调节，避免冲突
        if not emergency_triggered and len(decisions) < MAX_DECISIONS:
            self._ensure_target_inventory(decisions, market_data, exposures_usd_local, exposures_btc_local)

        exposure_b = exposures_usd_local.get('B', 0.0)
        exposure_c = exposures_usd_local.get('C', 0.0)

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
                # 净敞口纠偏是最高优先级，不受pending限制
                price = self._calculate_maker_price(market_data, 'SELL', aggressive=True, server=server)
                quantity = self._normalize_quantity(base_qty)
                order_notional = quantity * price
                new_exposure = exposure - order_notional
                if self._within_position_limit(server, new_exposure):
                    target = (server, price, quantity, order_notional)
                    break

            if target:
                target_server, price, quantity, order_notional = target
                action = 'sell_maker'
                logger.debug(
                    "Using %s to reduce long exposure: current %.2f, order %.2f, new %.2f, safe limit %.2f",
                    target_server,
                    exposures_usd_local.get(target_server, 0.0),
                    order_notional,
                    exposures_usd_local.get(target_server, 0.0) - order_notional,
                    self.position_limits.get(target_server, 0.0) * SAFETY_MARGIN
                )
                decisions.append({
                    'server': target_server,
                    'action': action,
                    'price': price,
                    'quantity': quantity,
                    'expire_time': current_time + self.config.signal_expire_seconds,
                    'reason': f'Reduce long exposure: ${total_exposure:.2f} via {target_server}'
                })
                self._simulate_exposure_change(
                    exposures_usd_local,
                    exposures_btc_local,
                    target_server,
                    action,
                    price,
                    quantity
                )
            else:
                logger.warning(
                    "⚠️ No eligible executor to reduce long exposure (order %.2f) without breaching limits; exposures=%s",
                    base_qty * mid_price,
                    exposures_usd_local
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
                # 净敞口纠偏是最高优先级，不受pending限制
                price = self._calculate_maker_price(market_data, 'BUY', aggressive=True, server=server)
                quantity = self._normalize_quantity(base_qty)
                order_notional = quantity * price
                new_exposure = exposure + order_notional
                if self._within_position_limit(server, new_exposure):
                    target = (server, price, quantity, order_notional)
                    break

            if target:
                target_server, price, quantity, order_notional = target
                action = 'buy_maker'
                logger.debug(
                    "Using %s to increase long exposure: current %.2f, order %.2f, new %.2f, safe limit %.2f",
                    target_server,
                    exposures_usd_local.get(target_server, 0.0),
                    order_notional,
                    exposures_usd_local.get(target_server, 0.0) + order_notional,
                    self.position_limits.get(target_server, 0.0) * SAFETY_MARGIN
                )
                decisions.append({
                    'server': target_server,
                    'action': action,
                    'price': price,
                    'quantity': quantity,
                    'expire_time': current_time + self.config.signal_expire_seconds,
                    'reason': f'Increase long exposure: ${total_exposure:.2f} via {target_server}'
                })
                self._simulate_exposure_change(
                    exposures_usd_local,
                    exposures_btc_local,
                    target_server,
                    action,
                    price,
                    quantity
                )
            else:
                logger.warning(
                    "⚠️ No eligible executor to increase long exposure (order %.2f) without breaching limits; exposures=%s",
                    base_qty * mid_price,
                    exposures_usd_local
                )

        # ========== 优先级3: 刷量逻辑 ==========
        # 仅在决策数未满时执行刷量
        if spread_bps < 5.0 and len(decisions) < MAX_DECISIONS:
            self._append_volume_decisions(
                decisions,
                market_data,
                exposures_usd_local,
                exposures_btc_local
            )

        return decisions

    def _enforce_position_limits(self, market_data: Dict, exposures: Dict[str, float]) -> List[Dict]:
        """检查并纠正各执行器仓位上限（支持taker fallback）"""
        corrections: List[Dict] = []
        mid_price = market_data['mid_price']
        fallback_threshold = max(0, SYSTEM_CONFIG.limit_fallback_count)

        for server in ('B', 'C'):
            if self.executor_stats_blocked.get(server):
                logger.debug("%s limit enforcement skipped due to pending backlog", server)
                continue

            exposure = exposures.get(server, 0.0)
            raw_limit = self.position_limits.get(server, 0.0)

            if raw_limit is None or raw_limit <= 0:
                continue

            safe_limit = raw_limit * SAFETY_MARGIN

            if abs(exposure) <= safe_limit:
                self.limit_violation_rounds[server] = 0
                continue

            self.limit_violation_rounds[server] += 1
            violation_rounds = self.limit_violation_rounds[server]

            over_usd = abs(exposure) - safe_limit
            order_notional = max(self.symbol_spec.min_notional, over_usd * 1.5)
            order_notional = min(order_notional, max(abs(exposure) * 0.8, self.symbol_spec.min_notional))

            raw_qty = order_notional / mid_price if mid_price > 0 else 0.0
            quantity = self._normalize_quantity(raw_qty)

            if quantity <= 0:
                logger.warning(
                    "⚠️ Unable to correct exposure for %s due to zero quantity (exposure=%.2f, safe_limit=%.2f)",
                    server, exposure, safe_limit
                )
                continue

            use_taker = fallback_threshold > 0 and violation_rounds >= fallback_threshold

            if exposure > 0:
                if use_taker:
                    action = 'sell_taker'
                    price = mid_price
                    logger.error(
                        "🚨🚨 %s TAKER FALLBACK (round %d/%d): %.2f > %.2f, force selling %.6f BTC",
                        server, violation_rounds, fallback_threshold, exposure, safe_limit, quantity
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
                        "🚨🚨 %s TAKER FALLBACK (round %d/%d): %.2f < -%.2f, force buying %.6f BTC",
                        server, violation_rounds, fallback_threshold, abs(exposure), safe_limit, quantity
                    )
                else:
                    action = 'buy_maker'
                    price = self._calculate_maker_price(market_data, 'BUY', aggressive=True, server=server)
                reason = (
                    f'Reduce over-limit short exposure: ${exposure:.2f} < -${safe_limit:.2f} '
                    f'(limit ${raw_limit:.2f}, round {violation_rounds})'
                )
                new_exposure = exposure + quantity * price

            logger.warning(
                "⚠️ %s exposure %.2f → %.2f / %.2f (USD)",
                server,
                exposure,
                new_exposure,
                safe_limit
            )

            if not use_taker:
                logger.error(
                    "🚨 %s exposure %.2f exceeds safe limit %.2f; issuing %s of %.6f BTC (round %d/%d)",
                    server, exposure, safe_limit, action, quantity,
                    violation_rounds, max(1, fallback_threshold)
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

                    # 如果是刷量订单，更新轮换标志
                    if 'Volume generation' in decision.get('reason', ''):
                        self.last_volume_sender = server

                    side_emoji = "🔵" if 'buy' in decision['action'] else "🔴"
                    logger.info(
                        f"{side_emoji} {server} {decision['action']} | $%.0f @ %.1f | Reason: %s",
                        quantity * price, price, decision['reason'][:30]
                    )
                else:
                    self._record_order_result(server, False)
                    logger.warning(f"⚠️ {server} signal failed")

            except Exception as e:
                self._record_order_result(server, False)
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
