"""
Aster Trading System - Utility Modules
工具模块包，包含通信协议、积分优化、风控管理、交易客户端等功能
"""

from .trading_client import (
    AsterFuturesClient,
    TradingConfig,
    AccountConfig,
    AccountState,
    SymbolSpec,
    get_logger
)

from .api_protocol import (
    TradingSignal,
    ServerStatus,
    OrderInfo,
    ProtocolValidator,
    create_signal_message,
    create_error_response,
    create_success_response
)

from .points_optimizer import (
    PointsOptimizer,
    PointsConfig,
    AccountMetrics,
    PointsBreakdown
)

from .risk_manager import (
    RiskManager,
    RiskConfig,
    RiskAlert,
    RiskLevel,
    AlertType
)

__all__ = [
    'AsterFuturesClient', 'TradingConfig', 'AccountConfig', 'AccountState', 'SymbolSpec', 'get_logger',
    'TradingSignal', 'ServerStatus', 'OrderInfo', 'ProtocolValidator',
    'create_signal_message', 'create_error_response', 'create_success_response',
    'PointsOptimizer', 'PointsConfig', 'AccountMetrics', 'PointsBreakdown',
    'RiskManager', 'RiskConfig', 'RiskAlert', 'RiskLevel', 'AlertType'
]