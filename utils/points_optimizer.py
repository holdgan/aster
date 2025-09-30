#!/usr/bin/env python3
"""
Aster Genesis Stage 2 Points Optimization Engine
基于交易量、持仓、抵押资产、PnL等多维度积分规则，最大化积分收益
"""

import time
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone
import math

logger = logging.getLogger("points_optimizer")

@dataclass
class PointsConfig:
    """积分系统配置"""
    # 基础积分率 (基于原始代码中的配置)
    holding_rate_per_usd_hour: float = 1 / 24      # 持仓积分: 1 USD/小时 = 1/24 积分
    collateral_rate_per_usd_hour: float = 1 / 24   # 抵押积分: 1 USD/小时 = 1/24 积分
    pnl_points_rate: float = 0.25                  # PnL积分: 1 USD PnL = 0.25 积分

    # 交易量积分 (推测的规则)
    volume_points_rate: float = 0.001              # 1 USD交易量 = 0.001 积分
    maker_bonus_multiplier: float = 1.2             # Maker订单额外20%积分
    taker_penalty_multiplier: float = 0.8           # Taker订单减少20%积分

    # 团队加成
    team_boost_multiplier: float = 1.5              # 团队加成50%

    # 优先抵押资产
    preferred_collateral_assets: frozenset = field(
        default_factory=lambda: frozenset({"USDF", "ASBNB"})
    )
    preferred_collateral_bonus: float = 1.3         # 优先抵押资产额外30%积分

    # 连续交易奖励
    consecutive_trading_days_bonus: Dict[int, float] = field(
        default_factory=lambda: {
            7: 1.1,   # 连续7天 +10%
            14: 1.25, # 连续14天 +25%
            30: 1.5   # 连续30天 +50%
        }
    )

@dataclass
class AccountMetrics:
    """账户指标"""
    # 基础数据
    current_position_usd: float = 0.0
    collateral_balance_usd: float = 0.0
    preferred_collateral_usd: float = 0.0

    # 交易数据
    maker_volume_24h: float = 0.0
    taker_volume_24h: float = 0.0
    realized_pnl_24h: float = 0.0

    # 时间追踪
    last_update_time: float = 0.0
    position_hold_start_time: float = 0.0

@dataclass
class PointsBreakdown:
    """积分明细"""
    # 基础积分
    holding_points: float = 0.0
    collateral_points: float = 0.0
    volume_points: float = 0.0
    pnl_points: float = 0.0

    # 加成积分
    maker_bonus_points: float = 0.0
    preferred_collateral_bonus: float = 0.0
    team_bonus_points: float = 0.0
    consecutive_trading_bonus: float = 0.0

    # 总计
    total_points: float = 0.0
    timestamp: float = 0.0

    def calculate_total(self) -> float:
        """计算总积分"""
        base_points = (self.holding_points + self.collateral_points +
                      self.volume_points + self.pnl_points)

        bonus_points = (self.maker_bonus_points + self.preferred_collateral_bonus +
                       self.team_bonus_points + self.consecutive_trading_bonus)

        self.total_points = base_points + bonus_points
        return self.total_points

class PointsOptimizer:
    """积分优化引擎"""

    def __init__(self, config: PointsConfig = None):
        self.config = config or PointsConfig()
        self.session_start_time = time.time()

        # 多账户积分追踪
        self.account_metrics: Dict[str, AccountMetrics] = {}
        self.points_history: List[PointsBreakdown] = []

        # 优化策略状态
        self.optimal_position_target = 0.0
        self.optimal_volume_target = 0.0
        self.last_strategy_update = 0.0

    def update_account_metrics(self, account_id: str, position_usd: float,
                             collateral_usd: float, preferred_collateral_usd: float):
        """更新账户基础指标"""
        if account_id not in self.account_metrics:
            self.account_metrics[account_id] = AccountMetrics()

        metrics = self.account_metrics[account_id]
        current_time = time.time()

        # 更新仓位相关数据
        if metrics.current_position_usd == 0 and position_usd != 0:
            metrics.position_hold_start_time = current_time
        elif metrics.current_position_usd != 0 and position_usd == 0:
            metrics.position_hold_start_time = 0.0

        metrics.current_position_usd = position_usd
        metrics.collateral_balance_usd = collateral_usd
        metrics.preferred_collateral_usd = preferred_collateral_usd
        metrics.last_update_time = current_time

        logger.debug(f"Updated metrics for {account_id}: "
                    f"Position=${position_usd:.2f}, Collateral=${collateral_usd:.2f}")

    def record_trade(self, account_id: str, volume_usd: float, is_maker: bool, pnl_usd: float = 0.0):
        """记录交易数据"""
        if account_id not in self.account_metrics:
            self.account_metrics[account_id] = AccountMetrics()

        metrics = self.account_metrics[account_id]

        if is_maker:
            metrics.maker_volume_24h += volume_usd
        else:
            metrics.taker_volume_24h += volume_usd

        metrics.realized_pnl_24h += pnl_usd

        logger.info(f"Recorded trade for {account_id}: "
                   f"${volume_usd:.2f} ({'Maker' if is_maker else 'Taker'}), "
                   f"PnL=${pnl_usd:.2f}")

    def calculate_current_points(self, account_id: str = None) -> PointsBreakdown:
        """计算当前积分"""
        current_time = time.time()
        breakdown = PointsBreakdown(timestamp=current_time)

        if account_id:
            # 单账户积分
            if account_id in self.account_metrics:
                self._calculate_account_points(self.account_metrics[account_id], breakdown, current_time)
        else:
            # 所有账户积分总和
            for metrics in self.account_metrics.values():
                temp_breakdown = PointsBreakdown()
                self._calculate_account_points(metrics, temp_breakdown, current_time)

                breakdown.holding_points += temp_breakdown.holding_points
                breakdown.collateral_points += temp_breakdown.collateral_points
                breakdown.volume_points += temp_breakdown.volume_points
                breakdown.pnl_points += temp_breakdown.pnl_points
                breakdown.maker_bonus_points += temp_breakdown.maker_bonus_points
                breakdown.preferred_collateral_bonus += temp_breakdown.preferred_collateral_bonus

        # 计算团队加成 (基于总积分)
        base_total = (breakdown.holding_points + breakdown.collateral_points +
                     breakdown.volume_points + breakdown.pnl_points +
                     breakdown.maker_bonus_points + breakdown.preferred_collateral_bonus)

        breakdown.team_bonus_points = base_total * (self.config.team_boost_multiplier - 1.0)

        # 计算连续交易奖励
        breakdown.consecutive_trading_bonus = self._calculate_consecutive_bonus(base_total)

        breakdown.calculate_total()
        return breakdown

    def _calculate_account_points(self, metrics: AccountMetrics, breakdown: PointsBreakdown, current_time: float):
        """计算单个账户的积分"""
        # 1. 持仓积分
        if metrics.current_position_usd > 0 and metrics.position_hold_start_time > 0:
            hold_duration_hours = (current_time - metrics.position_hold_start_time) / 3600
            breakdown.holding_points += (
                metrics.current_position_usd *
                hold_duration_hours *
                self.config.holding_rate_per_usd_hour
            )

        # 2. 抵押积分
        if metrics.collateral_balance_usd > 0:
            # 按照24小时计算 (这里简化为1小时的比例)
            collateral_hours = 1.0  # 假设查询间隔
            breakdown.collateral_points += (
                metrics.collateral_balance_usd *
                collateral_hours *
                self.config.collateral_rate_per_usd_hour
            )

            # 优先抵押资产奖励
            if metrics.preferred_collateral_usd > 0:
                breakdown.preferred_collateral_bonus += (
                    metrics.preferred_collateral_usd *
                    collateral_hours *
                    self.config.collateral_rate_per_usd_hour *
                    (self.config.preferred_collateral_bonus - 1.0)
                )

        # 3. 交易量积分
        total_volume = metrics.maker_volume_24h + metrics.taker_volume_24h
        breakdown.volume_points += total_volume * self.config.volume_points_rate

        # Maker奖励
        breakdown.maker_bonus_points += (
            metrics.maker_volume_24h *
            self.config.volume_points_rate *
            (self.config.maker_bonus_multiplier - 1.0)
        )

        # 4. PnL积分
        if metrics.realized_pnl_24h > 0:
            breakdown.pnl_points += metrics.realized_pnl_24h * self.config.pnl_points_rate

    def _calculate_consecutive_bonus(self, base_points: float) -> float:
        """计算连续交易奖励"""
        session_days = (time.time() - self.session_start_time) / 86400

        # 简化版：基于会话时长给予奖励
        for days, multiplier in sorted(self.config.consecutive_trading_days_bonus.items()):
            if session_days >= days:
                return base_points * (multiplier - 1.0)

        return 0.0

    def optimize_trading_strategy(self, market_price: float, spread_bps: float) -> Dict:
        """优化交易策略以最大化积分"""
        current_time = time.time()

        # 每分钟更新一次策略
        if current_time - self.last_strategy_update < 60:
            return self._get_current_strategy()

        self.last_strategy_update = current_time

        # 计算当前积分情况
        current_points = self.calculate_current_points()

        # 分析积分来源分布
        total_base = (current_points.holding_points + current_points.collateral_points +
                     current_points.volume_points + current_points.pnl_points)

        if total_base == 0:
            total_base = 1  # 避免除零

        holding_ratio = current_points.holding_points / total_base
        volume_ratio = current_points.volume_points / total_base
        maker_ratio = current_points.maker_bonus_points / total_base

        logger.info(f"📊 Points distribution: Holding={holding_ratio:.1%}, "
                   f"Volume={volume_ratio:.1%}, Maker Bonus={maker_ratio:.1%}")

        # 制定优化策略
        strategy = self._generate_optimization_strategy(
            holding_ratio, volume_ratio, maker_ratio, market_price, spread_bps
        )

        return strategy

    def _generate_optimization_strategy(self, holding_ratio: float, volume_ratio: float,
                                      maker_ratio: float, market_price: float, spread_bps: float) -> Dict:
        """生成积分优化策略"""
        strategy = {
            'timestamp': time.time(),
            'primary_focus': 'volume',  # 默认主要关注交易量
            'position_target_usd': 1000.0,
            'volume_target_per_hour': 2000.0,
            'maker_ratio_target': 0.8,  # 80% maker交易
            'rebalance_frequency_minutes': 5.0,
            'aggressive_mode': False
        }

        # 策略调整逻辑
        if holding_ratio < 0.2:  # 持仓积分占比太低
            strategy['primary_focus'] = 'holding'
            strategy['position_target_usd'] = 2000.0
            strategy['rebalance_frequency_minutes'] = 10.0

        elif volume_ratio < 0.3:  # 交易量积分占比太低
            strategy['primary_focus'] = 'volume'
            strategy['volume_target_per_hour'] = 5000.0
            strategy['maker_ratio_target'] = 0.9
            strategy['rebalance_frequency_minutes'] = 3.0

        elif maker_ratio < 0.1:  # Maker奖励太少
            strategy['primary_focus'] = 'maker_bonus'
            strategy['maker_ratio_target'] = 0.95
            strategy['rebalance_frequency_minutes'] = 2.0

        # 市场条件调整
        if spread_bps > 10:  # Spread较大，适合做市
            strategy['aggressive_mode'] = True
            strategy['maker_ratio_target'] = min(0.95, strategy['maker_ratio_target'] + 0.1)
        elif spread_bps < 3:  # Spread很小，降低频率
            strategy['rebalance_frequency_minutes'] *= 1.5

        logger.info(f"🎯 Strategy: Focus={strategy['primary_focus']}, "
                   f"Position=${strategy['position_target_usd']:.0f}, "
                   f"Volume=${strategy['volume_target_per_hour']:.0f}/hr, "
                   f"Maker={strategy['maker_ratio_target']:.0%}")

        return strategy

    def _get_current_strategy(self) -> Dict:
        """获取当前策略（缓存版本）"""
        return {
            'timestamp': self.last_strategy_update,
            'primary_focus': 'volume',
            'position_target_usd': self.optimal_position_target,
            'volume_target_per_hour': self.optimal_volume_target,
            'maker_ratio_target': 0.8,
            'rebalance_frequency_minutes': 5.0,
            'aggressive_mode': False
        }

    def get_points_report(self) -> Dict:
        """生成积分报告"""
        current_points = self.calculate_current_points()
        session_duration_hours = (time.time() - self.session_start_time) / 3600

        total_volume = sum(
            metrics.maker_volume_24h + metrics.taker_volume_24h
            for metrics in self.account_metrics.values()
        )

        total_position = sum(
            metrics.current_position_usd
            for metrics in self.account_metrics.values()
        )

        report = {
            'timestamp': time.time(),
            'session_duration_hours': session_duration_hours,
            'total_points': current_points.total_points,
            'points_per_hour': current_points.total_points / max(session_duration_hours, 0.1),

            # 积分明细
            'points_breakdown': {
                'holding': current_points.holding_points,
                'collateral': current_points.collateral_points,
                'volume': current_points.volume_points,
                'pnl': current_points.pnl_points,
                'maker_bonus': current_points.maker_bonus_points,
                'collateral_bonus': current_points.preferred_collateral_bonus,
                'team_bonus': current_points.team_bonus_points,
                'consecutive_bonus': current_points.consecutive_trading_bonus
            },

            # 账户统计
            'accounts': len(self.account_metrics),
            'total_volume_24h': total_volume,
            'total_position_usd': total_position,

            # 效率指标
            'points_per_dollar_volume': current_points.total_points / max(total_volume, 1),
            'points_per_dollar_position': current_points.total_points / max(total_position, 1),

            # 建议
            'optimization_suggestions': self._generate_suggestions(current_points, total_volume, total_position)
        }

        return report

    def _generate_suggestions(self, points: PointsBreakdown, volume: float, position: float) -> List[str]:
        """生成优化建议"""
        suggestions = []

        total_base = points.holding_points + points.collateral_points + points.volume_points + points.pnl_points

        if total_base > 0:
            volume_ratio = points.volume_points / total_base
            holding_ratio = points.holding_points / total_base

            if volume_ratio < 0.3:
                suggestions.append("增加交易频率以提升交易量积分")

            if holding_ratio < 0.2:
                suggestions.append("适当增加持仓规模以获得更多持仓积分")

            if points.maker_bonus_points < points.volume_points * 0.2:
                suggestions.append("提高Maker订单比例以获得更多奖励积分")

            if points.preferred_collateral_bonus < points.collateral_points * 0.1:
                suggestions.append("考虑使用USDF或ASBNB作为抵押资产获得额外奖励")

        if len(suggestions) == 0:
            suggestions.append("当前策略表现良好，继续保持")

        return suggestions

    def reset_daily_metrics(self):
        """重置每日指标（通常在每日UTC 00:00调用）"""
        for metrics in self.account_metrics.values():
            metrics.maker_volume_24h = 0.0
            metrics.taker_volume_24h = 0.0
            metrics.realized_pnl_24h = 0.0

        logger.info("🔄 Daily metrics reset completed")

# 使用示例
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # 创建积分优化器
    optimizer = PointsOptimizer()

    # 模拟账户数据
    optimizer.update_account_metrics("A", position_usd=1000.0, collateral_usd=5000.0, preferred_collateral_usd=3000.0)
    optimizer.update_account_metrics("B", position_usd=500.0, collateral_usd=2000.0, preferred_collateral_usd=1000.0)
    optimizer.update_account_metrics("C", position_usd=-300.0, collateral_usd=1500.0, preferred_collateral_usd=500.0)

    # 模拟交易
    optimizer.record_trade("A", volume_usd=100.0, is_maker=True, pnl_usd=5.0)
    optimizer.record_trade("B", volume_usd=200.0, is_maker=True, pnl_usd=-2.0)
    optimizer.record_trade("C", volume_usd=150.0, is_maker=False, pnl_usd=3.0)

    # 计算积分
    points = optimizer.calculate_current_points()
    print(f"Total Points: {points.total_points:.2f}")

    # 生成策略
    strategy = optimizer.optimize_trading_strategy(market_price=50000.0, spread_bps=5.0)
    print(f"Strategy Focus: {strategy['primary_focus']}")

    # 生成报告
    report = optimizer.get_points_report()
    print(f"Points per hour: {report['points_per_hour']:.2f}")
    print("Suggestions:", report['optimization_suggestions'])