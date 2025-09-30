#!/usr/bin/env python3
"""
Aster Trading System - Risk Management and Safety Engine
多层风控体系，保护交易系统免受极端市场波动和系统故障影响
"""

import time
import logging
import asyncio
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Literal
from enum import Enum
import json
from datetime import datetime, timezone

logger = logging.getLogger("risk_manager")

class RiskLevel(Enum):
    """风险等级"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class AlertType(Enum):
    """告警类型"""
    POSITION_LIMIT = "position_limit"
    LOSS_LIMIT = "loss_limit"
    VOLUME_ANOMALY = "volume_anomaly"
    API_FAILURE = "api_failure"
    MARKET_VOLATILITY = "market_volatility"
    SYSTEM_ERROR = "system_error"
    EMERGENCY_STOP = "emergency_stop"

@dataclass
class RiskConfig:
    """风控配置"""
    # 仓位风控
    max_position_usd: float = 5000.0
    max_position_percentage: float = 0.1  # 总资金的10%
    position_concentration_limit: float = 0.6  # 单方向最大60%仓位

    # 损失风控
    daily_loss_limit_usd: float = 500.0
    session_loss_limit_usd: float = 200.0
    max_drawdown_percentage: float = 0.05  # 最大回撤5%

    # 交易频率风控
    max_orders_per_minute: int = 20
    max_orders_per_hour: int = 500
    max_volume_per_hour_usd: float = 50000.0

    # 价格偏离风控
    max_price_deviation_percentage: float = 0.02  # 最大价格偏离2%
    market_impact_threshold: float = 0.001  # 市场冲击阈值0.1%

    # API风控
    max_api_failures_per_minute: int = 5
    api_timeout_seconds: float = 10.0
    max_consecutive_failures: int = 3

    # 系统风控
    max_memory_usage_mb: float = 512.0
    max_cpu_usage_percentage: float = 80.0
    emergency_stop_conditions: List[str] = field(
        default_factory=lambda: [
            "critical_api_failure",
            "extreme_market_movement",
            "system_resource_exhaustion"
        ]
    )

@dataclass
class RiskAlert:
    """风险告警"""
    timestamp: float
    alert_type: AlertType
    risk_level: RiskLevel
    message: str
    account_id: str = ""
    value: float = 0.0
    threshold: float = 0.0
    auto_action: str = ""

    def to_dict(self) -> Dict:
        return {
            'timestamp': self.timestamp,
            'alert_type': self.alert_type.value,
            'risk_level': self.risk_level.value,
            'message': self.message,
            'account_id': self.account_id,
            'value': self.value,
            'threshold': self.threshold,
            'auto_action': self.auto_action
        }

@dataclass
class AccountRiskMetrics:
    """账户风险指标"""
    account_id: str
    current_position_usd: float = 0.0
    max_position_reached: float = 0.0

    # PnL追踪
    unrealized_pnl: float = 0.0
    realized_pnl_today: float = 0.0
    max_profit_today: float = 0.0
    max_drawdown_today: float = 0.0

    # 交易统计
    orders_last_minute: int = 0
    orders_last_hour: int = 0
    volume_last_hour: float = 0.0

    # API统计
    api_calls_last_minute: int = 0
    api_failures_last_minute: int = 0
    consecutive_api_failures: int = 0

    # 最后更新时间
    last_update: float = 0.0

class RiskManager:
    """风险管理器"""

    def __init__(self, config: RiskConfig = None):
        self.config = config or RiskConfig()
        self.account_metrics: Dict[str, AccountRiskMetrics] = {}
        self.risk_alerts: List[RiskAlert] = []

        # 系统状态
        self.emergency_stop_active = False
        self.system_start_time = time.time()
        self.last_risk_check = 0.0

        # 市场数据缓存
        self.market_price_history: List[Tuple[float, float]] = []  # (timestamp, price)
        self.volatility_cache = 0.0

        # 风控规则统计
        self.rule_violations: Dict[str, int] = {}

    def update_account_metrics(self, account_id: str, position_usd: float,
                             unrealized_pnl: float, realized_pnl: float):
        """更新账户风险指标"""
        if account_id not in self.account_metrics:
            self.account_metrics[account_id] = AccountRiskMetrics(account_id=account_id)

        metrics = self.account_metrics[account_id]
        current_time = time.time()

        # 更新仓位数据
        metrics.current_position_usd = position_usd
        metrics.max_position_reached = max(metrics.max_position_reached, abs(position_usd))

        # 更新PnL数据
        metrics.unrealized_pnl = unrealized_pnl

        # 计算当日实现PnL (简化版)
        if metrics.last_update == 0 or self._is_new_day(metrics.last_update, current_time):
            metrics.realized_pnl_today = realized_pnl
            metrics.max_profit_today = max(0, unrealized_pnl)
            metrics.max_drawdown_today = min(0, unrealized_pnl)
        else:
            metrics.realized_pnl_today += realized_pnl
            metrics.max_profit_today = max(metrics.max_profit_today, metrics.unrealized_pnl)

            # 计算回撤
            if metrics.max_profit_today > 0:
                current_drawdown = (metrics.unrealized_pnl - metrics.max_profit_today) / metrics.max_profit_today
                metrics.max_drawdown_today = min(metrics.max_drawdown_today, current_drawdown)

        metrics.last_update = current_time

        # 实时风控检查
        self._check_account_risks(account_id)

    def record_order(self, account_id: str, volume_usd: float, success: bool = True):
        """记录订单事件"""
        if account_id not in self.account_metrics:
            self.account_metrics[account_id] = AccountRiskMetrics(account_id=account_id)

        metrics = self.account_metrics[account_id]
        current_time = time.time()

        # 更新订单计数 (简化版 - 应该使用滑动窗口)
        if current_time - metrics.last_update < 60:  # 同一分钟内
            metrics.orders_last_minute += 1
        else:
            metrics.orders_last_minute = 1

        metrics.orders_last_hour += 1  # 简化版
        metrics.volume_last_hour += volume_usd

        # 检查频率限制
        self._check_trading_frequency_limits(account_id)

    def record_api_call(self, account_id: str, success: bool):
        """记录API调用"""
        if account_id not in self.account_metrics:
            self.account_metrics[account_id] = AccountRiskMetrics(account_id=account_id)

        metrics = self.account_metrics[account_id]
        metrics.api_calls_last_minute += 1

        if not success:
            metrics.api_failures_last_minute += 1
            metrics.consecutive_api_failures += 1
        else:
            metrics.consecutive_api_failures = 0

        # 检查API限制
        self._check_api_limits(account_id)

    def update_market_price(self, price: float):
        """更新市场价格"""
        current_time = time.time()
        self.market_price_history.append((current_time, price))

        # 保留最近1小时的数据
        cutoff_time = current_time - 3600
        self.market_price_history = [
            (t, p) for t, p in self.market_price_history if t > cutoff_time
        ]

        # 计算波动率
        self._calculate_volatility()

        # 检查市场异常
        self._check_market_volatility()

    def _calculate_volatility(self):
        """计算市场波动率"""
        if len(self.market_price_history) < 10:
            return

        prices = [p for _, p in self.market_price_history[-20:]]  # 最近20个价格点

        # 计算对数收益率
        returns = []
        for i in range(1, len(prices)):
            ret = (prices[i] - prices[i-1]) / prices[i-1]
            returns.append(ret)

        if returns:
            # 简化的波动率计算
            avg_return = sum(returns) / len(returns)
            variance = sum((r - avg_return) ** 2 for r in returns) / len(returns)
            self.volatility_cache = variance ** 0.5

    def _check_account_risks(self, account_id: str):
        """检查账户风险"""
        metrics = self.account_metrics[account_id]

        # 1. 仓位风控
        if abs(metrics.current_position_usd) > self.config.max_position_usd:
            self._create_alert(
                AlertType.POSITION_LIMIT,
                RiskLevel.HIGH,
                f"Position limit exceeded: ${abs(metrics.current_position_usd):.2f}",
                account_id,
                abs(metrics.current_position_usd),
                self.config.max_position_usd,
                "reduce_position"
            )

        # 2. 损失风控
        total_loss = metrics.realized_pnl_today + metrics.unrealized_pnl
        if total_loss < -self.config.daily_loss_limit_usd:
            self._create_alert(
                AlertType.LOSS_LIMIT,
                RiskLevel.CRITICAL,
                f"Daily loss limit exceeded: ${total_loss:.2f}",
                account_id,
                abs(total_loss),
                self.config.daily_loss_limit_usd,
                "stop_trading"
            )

        # 3. 回撤风控
        if metrics.max_drawdown_today < -self.config.max_drawdown_percentage:
            self._create_alert(
                AlertType.LOSS_LIMIT,
                RiskLevel.HIGH,
                f"Max drawdown exceeded: {metrics.max_drawdown_today:.1%}",
                account_id,
                abs(metrics.max_drawdown_today),
                self.config.max_drawdown_percentage,
                "reduce_position"
            )

    def _check_trading_frequency_limits(self, account_id: str):
        """检查交易频率限制"""
        metrics = self.account_metrics[account_id]

        if metrics.orders_last_minute > self.config.max_orders_per_minute:
            self._create_alert(
                AlertType.VOLUME_ANOMALY,
                RiskLevel.MEDIUM,
                f"Order frequency too high: {metrics.orders_last_minute}/min",
                account_id,
                metrics.orders_last_minute,
                self.config.max_orders_per_minute,
                "throttle_orders"
            )

        if metrics.volume_last_hour > self.config.max_volume_per_hour_usd:
            self._create_alert(
                AlertType.VOLUME_ANOMALY,
                RiskLevel.HIGH,
                f"Hourly volume limit exceeded: ${metrics.volume_last_hour:.2f}",
                account_id,
                metrics.volume_last_hour,
                self.config.max_volume_per_hour_usd,
                "reduce_trading"
            )

    def _check_api_limits(self, account_id: str):
        """检查API限制"""
        metrics = self.account_metrics[account_id]

        if metrics.api_failures_last_minute > self.config.max_api_failures_per_minute:
            self._create_alert(
                AlertType.API_FAILURE,
                RiskLevel.HIGH,
                f"API failure rate too high: {metrics.api_failures_last_minute}/min",
                account_id,
                metrics.api_failures_last_minute,
                self.config.max_api_failures_per_minute,
                "pause_trading"
            )

        if metrics.consecutive_api_failures >= self.config.max_consecutive_failures:
            self._create_alert(
                AlertType.API_FAILURE,
                RiskLevel.CRITICAL,
                f"Consecutive API failures: {metrics.consecutive_api_failures}",
                account_id,
                metrics.consecutive_api_failures,
                self.config.max_consecutive_failures,
                "emergency_stop"
            )

    def _check_market_volatility(self):
        """检查市场波动异常"""
        if self.volatility_cache > 0.05:  # 5%的异常波动
            self._create_alert(
                AlertType.MARKET_VOLATILITY,
                RiskLevel.HIGH,
                f"Extreme market volatility: {self.volatility_cache:.1%}",
                "",
                self.volatility_cache,
                0.05,
                "reduce_all_positions"
            )

    def _create_alert(self, alert_type: AlertType, risk_level: RiskLevel,
                     message: str, account_id: str = "", value: float = 0.0,
                     threshold: float = 0.0, auto_action: str = ""):
        """创建风险告警"""
        alert = RiskAlert(
            timestamp=time.time(),
            alert_type=alert_type,
            risk_level=risk_level,
            message=message,
            account_id=account_id,
            value=value,
            threshold=threshold,
            auto_action=auto_action
        )

        self.risk_alerts.append(alert)

        # 保留最近100条告警
        if len(self.risk_alerts) > 100:
            self.risk_alerts = self.risk_alerts[-100:]

        # 记录违规统计
        rule_key = f"{alert_type.value}_{account_id}"
        self.rule_violations[rule_key] = self.rule_violations.get(rule_key, 0) + 1

        # 日志记录
        logger.warning(f"🚨 RISK ALERT [{risk_level.value.upper()}] {alert_type.value}: {message}")

        # 自动处理关键风险
        if risk_level == RiskLevel.CRITICAL:
            self._handle_critical_risk(alert)

    def _handle_critical_risk(self, alert: RiskAlert):
        """处理关键风险"""
        if alert.auto_action == "emergency_stop":
            self.activate_emergency_stop(f"Auto-triggered by {alert.alert_type.value}")
        elif alert.auto_action == "stop_trading":
            logger.error(f"🛑 Auto-stop trading for account {alert.account_id}")
            # 这里应该调用停止交易的接口

        # 发送紧急通知
        self._send_emergency_notification(alert)

    def _send_emergency_notification(self, alert: RiskAlert):
        """发送紧急通知 (这里可以集成邮件、短信、Webhook等)"""
        notification = {
            'timestamp': alert.timestamp,
            'level': 'EMERGENCY',
            'alert_type': alert.alert_type.value,
            'message': alert.message,
            'account_id': alert.account_id,
            'auto_action': alert.auto_action
        }

        # TODO: 实现实际的通知机制
        logger.critical(f"📱 EMERGENCY NOTIFICATION: {json.dumps(notification)}")

    def activate_emergency_stop(self, reason: str = "Manual"):
        """激活紧急停止"""
        if self.emergency_stop_active:
            return

        self.emergency_stop_active = True

        alert = RiskAlert(
            timestamp=time.time(),
            alert_type=AlertType.EMERGENCY_STOP,
            risk_level=RiskLevel.CRITICAL,
            message=f"Emergency stop activated: {reason}",
            auto_action="stop_all_trading"
        )

        self.risk_alerts.append(alert)

        logger.critical(f"🚨 EMERGENCY STOP ACTIVATED: {reason}")
        self._send_emergency_notification(alert)

    def deactivate_emergency_stop(self, reason: str = "Manual"):
        """解除紧急停止"""
        if not self.emergency_stop_active:
            return

        self.emergency_stop_active = False
        logger.info(f"✅ Emergency stop deactivated: {reason}")

    def is_trading_allowed(self, account_id: str) -> Tuple[bool, str]:
        """检查是否允许交易"""
        if self.emergency_stop_active:
            return False, "Emergency stop is active"

        if account_id not in self.account_metrics:
            return True, "OK"

        metrics = self.account_metrics[account_id]

        # 检查损失限制
        total_loss = metrics.realized_pnl_today + metrics.unrealized_pnl
        if total_loss < -self.config.daily_loss_limit_usd:
            return False, "Daily loss limit exceeded"

        # 检查API失败
        if metrics.consecutive_api_failures >= self.config.max_consecutive_failures:
            return False, "Too many consecutive API failures"

        # 检查频率限制
        if metrics.orders_last_minute > self.config.max_orders_per_minute:
            return False, "Order frequency limit exceeded"

        return True, "OK"

    def can_open_position(self, account_id: str, position_change_usd: float) -> Tuple[bool, str]:
        """检查是否可以开仓"""
        allowed, reason = self.is_trading_allowed(account_id)
        if not allowed:
            return False, reason

        if account_id not in self.account_metrics:
            return True, "OK"

        metrics = self.account_metrics[account_id]
        new_position = metrics.current_position_usd + position_change_usd

        # 检查仓位限制
        if abs(new_position) > self.config.max_position_usd:
            return False, f"Position limit would be exceeded: ${abs(new_position):.2f}"

        return True, "OK"

    def get_risk_summary(self) -> Dict:
        """获取风险摘要"""
        current_time = time.time()

        # 统计活跃告警
        recent_alerts = [a for a in self.risk_alerts if current_time - a.timestamp < 3600]  # 最近1小时
        alert_counts = {}
        for alert in recent_alerts:
            level = alert.risk_level.value
            alert_counts[level] = alert_counts.get(level, 0) + 1

        # 计算总体风险等级
        overall_risk = RiskLevel.LOW
        if self.emergency_stop_active:
            overall_risk = RiskLevel.CRITICAL
        elif alert_counts.get('critical', 0) > 0:
            overall_risk = RiskLevel.CRITICAL
        elif alert_counts.get('high', 0) > 0:
            overall_risk = RiskLevel.HIGH
        elif alert_counts.get('medium', 0) > 0:
            overall_risk = RiskLevel.MEDIUM

        # 账户风险概览
        account_risks = {}
        for account_id, metrics in self.account_metrics.items():
            account_risks[account_id] = {
                'position_usd': metrics.current_position_usd,
                'position_limit_usage': abs(metrics.current_position_usd) / self.config.max_position_usd,
                'daily_pnl': metrics.realized_pnl_today + metrics.unrealized_pnl,
                'max_drawdown': metrics.max_drawdown_today,
                'api_health': 1.0 - (metrics.api_failures_last_minute / max(metrics.api_calls_last_minute, 1))
            }

        return {
            'timestamp': current_time,
            'overall_risk_level': overall_risk.value,
            'emergency_stop_active': self.emergency_stop_active,
            'market_volatility': self.volatility_cache,
            'alert_counts': alert_counts,
            'total_violations': sum(self.rule_violations.values()),
            'account_risks': account_risks,
            'system_uptime_hours': (current_time - self.system_start_time) / 3600
        }

    def get_recent_alerts(self, hours: int = 1) -> List[Dict]:
        """获取最近的告警"""
        cutoff_time = time.time() - (hours * 3600)
        recent_alerts = [
            alert.to_dict() for alert in self.risk_alerts
            if alert.timestamp > cutoff_time
        ]
        return sorted(recent_alerts, key=lambda x: x['timestamp'], reverse=True)

    def _is_new_day(self, last_time: float, current_time: float) -> bool:
        """检查是否是新的一天 (UTC)"""
        last_day = datetime.fromtimestamp(last_time, tz=timezone.utc).date()
        current_day = datetime.fromtimestamp(current_time, tz=timezone.utc).date()
        return last_day != current_day

    def reset_daily_counters(self):
        """重置每日计数器"""
        for metrics in self.account_metrics.values():
            metrics.realized_pnl_today = 0.0
            metrics.max_profit_today = 0.0
            metrics.max_drawdown_today = 0.0

        logger.info("🔄 Daily risk counters reset")

# 使用示例
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # 创建风险管理器
    risk_manager = RiskManager()

    # 模拟账户更新
    risk_manager.update_account_metrics("A", position_usd=1000.0, unrealized_pnl=50.0, realized_pnl=10.0)
    risk_manager.update_account_metrics("B", position_usd=-500.0, unrealized_pnl=-20.0, realized_pnl=5.0)

    # 模拟订单
    risk_manager.record_order("A", volume_usd=100.0, success=True)
    risk_manager.record_api_call("A", success=True)

    # 模拟市场价格
    risk_manager.update_market_price(50000.0)
    risk_manager.update_market_price(50100.0)

    # 检查交易权限
    allowed, reason = risk_manager.is_trading_allowed("A")
    print(f"Trading allowed for A: {allowed} - {reason}")

    # 获取风险摘要
    summary = risk_manager.get_risk_summary()
    print(f"Overall risk level: {summary['overall_risk_level']}")
    print(f"Alert counts: {summary['alert_counts']}")

    # 模拟极端情况
    risk_manager.update_account_metrics("A", position_usd=6000.0, unrealized_pnl=-600.0, realized_pnl=0.0)

    # 获取最近告警
    alerts = risk_manager.get_recent_alerts()
    print(f"Recent alerts: {len(alerts)}")