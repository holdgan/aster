#!/usr/bin/env python3
"""
Aster Trading System - Server Communication Protocol
定义协调器(A)与执行器(B/C)之间的通信协议和数据结构
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Literal
import time
import json

# 消息类型定义
MessageType = Literal[
    "signal",           # 交易信号
    "health_check",     # 健康检查
    "status_query",     # 状态查询
    "emergency_stop",   # 紧急停止
    "position_query",   # 仓位查询
    "order_status"      # 订单状态
]

ActionType = Literal[
    "buy_maker",        # 买单maker
    "sell_maker",       # 卖单maker
    "cancel_all",       # 取消所有订单
    "reduce_position"   # 减仓
]

@dataclass
class TradingSignal:
    """交易信号数据结构"""
    # 基本信息
    timestamp: float
    action: ActionType
    symbol: str = "BTCUSDT"

    # 订单参数
    price: float = 0.0
    quantity: float = 0.0
    expire_time: float = 0.0

    # 元数据
    source: str = "coordinator"
    reason: str = ""
    priority: int = 1  # 1=低, 2=中, 3=高
    signal_id: str = ""

    def to_dict(self) -> Dict:
        """转换为字典格式用于JSON序列化"""
        return {
            'timestamp': self.timestamp,
            'action': self.action,
            'symbol': self.symbol,
            'price': self.price,
            'quantity': self.quantity,
            'expire_time': self.expire_time,
            'source': self.source,
            'reason': self.reason,
            'priority': self.priority,
            'signal_id': self.signal_id or f"{self.source}_{int(self.timestamp)}"
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'TradingSignal':
        """从字典创建信号对象"""
        return cls(
            timestamp=data['timestamp'],
            action=data['action'],
            symbol=data.get('symbol', 'BTCUSDT'),
            price=data.get('price', 0.0),
            quantity=data.get('quantity', 0.0),
            expire_time=data.get('expire_time', 0.0),
            source=data.get('source', 'coordinator'),
            reason=data.get('reason', ''),
            priority=data.get('priority', 1),
            signal_id=data.get('signal_id', '')
        )

    def is_expired(self) -> bool:
        """检查信号是否过期"""
        return time.time() > self.expire_time

@dataclass
class ServerStatus:
    """服务器状态数据结构"""
    server_name: str
    timestamp: float
    status: Literal["healthy", "warning", "error", "offline"]

    # 交易状态
    active_orders: int = 0
    current_position: float = 0.0
    session_volume: float = 0.0

    # 性能指标
    volume_rate_per_hour: float = 0.0
    order_success_rate: float = 0.0
    avg_response_time_ms: float = 0.0

    # 风控状态
    emergency_stop: bool = False
    position_limit_used_percent: float = 0.0

    # API连接状态
    api_connected: bool = True
    last_api_call: float = 0.0

    def to_dict(self) -> Dict:
        """转换为字典格式"""
        return {
            'server_name': self.server_name,
            'timestamp': self.timestamp,
            'status': self.status,
            'active_orders': self.active_orders,
            'current_position': self.current_position,
            'session_volume': self.session_volume,
            'volume_rate_per_hour': self.volume_rate_per_hour,
            'order_success_rate': self.order_success_rate,
            'avg_response_time_ms': self.avg_response_time_ms,
            'emergency_stop': self.emergency_stop,
            'position_limit_used_percent': self.position_limit_used_percent,
            'api_connected': self.api_connected,
            'last_api_call': self.last_api_call
        }

@dataclass
class OrderInfo:
    """订单信息数据结构"""
    order_id: str
    symbol: str
    side: Literal["BUY", "SELL"]
    type: Literal["LIMIT", "MARKET"]
    status: str
    price: float
    quantity: float
    executed_qty: float
    timestamp: float

    def to_dict(self) -> Dict:
        return {
            'order_id': self.order_id,
            'symbol': self.symbol,
            'side': self.side,
            'type': self.type,
            'status': self.status,
            'price': self.price,
            'quantity': self.quantity,
            'executed_qty': self.executed_qty,
            'timestamp': self.timestamp
        }

class ProtocolValidator:
    """协议验证器"""

    @staticmethod
    def validate_signal(signal_data: Dict) -> tuple[bool, str]:
        """验证交易信号格式"""
        required_fields = ['timestamp', 'action', 'symbol']

        # 检查必要字段
        for field in required_fields:
            if field not in signal_data:
                return False, f"Missing required field: {field}"

        # 检查action类型
        valid_actions = ['buy_maker', 'sell_maker', 'cancel_all', 'reduce_position']
        if signal_data['action'] not in valid_actions:
            return False, f"Invalid action: {signal_data['action']}"

        # 检查价格和数量（对于交易动作）
        if signal_data['action'] in ['buy_maker', 'sell_maker']:
            if 'price' not in signal_data or 'quantity' not in signal_data:
                return False, "Trading actions require price and quantity"

            if signal_data['price'] <= 0 or signal_data['quantity'] <= 0:
                return False, "Price and quantity must be positive"

        # 检查时间戳
        current_time = time.time()
        if signal_data['timestamp'] > current_time + 60:  # 未来1分钟内
            return False, "Timestamp is too far in the future"

        if signal_data['timestamp'] < current_time - 3600:  # 过去1小时内
            return False, "Timestamp is too old"

        return True, "Valid"

class APIEndpoints:
    """API端点定义"""

    # 执行器端点 (B/C服务器)
    EXECUTOR_ENDPOINTS = {
        'signal': '/signal',           # POST - 接收交易信号
        'health': '/health',           # GET - 健康检查
        'status': '/status',           # GET - 获取详细状态
        'emergency_stop': '/emergency_stop',  # POST - 紧急停止
        'position': '/position',       # GET - 查询当前仓位
        'orders': '/orders',          # GET - 查询活跃订单
        'cancel_all': '/cancel_all'   # POST - 取消所有订单
    }

    # 协调器端点 (A服务器，可选)
    COORDINATOR_ENDPOINTS = {
        'status': '/status',          # GET - 协调器状态
        'config': '/config',          # GET - 获取配置
        'servers': '/servers',        # GET - 查询所有服务器状态
        'emergency': '/emergency'     # POST - 全局紧急停止
    }

class ResponseCodes:
    """响应代码定义"""
    SUCCESS = 200
    BAD_REQUEST = 400
    UNAUTHORIZED = 401
    NOT_FOUND = 404
    RATE_LIMITED = 429
    INTERNAL_ERROR = 500
    SERVICE_UNAVAILABLE = 503

# HTTP请求/响应模板
REQUEST_TEMPLATES = {
    'signal': {
        'method': 'POST',
        'path': '/signal',
        'headers': {'Content-Type': 'application/json'},
        'body': {
            'timestamp': 1700000000.0,
            'action': 'buy_maker',
            'symbol': 'BTCUSDT',
            'price': 50000.0,
            'quantity': 0.01,
            'expire_time': 1700000060.0,
            'source': 'coordinator',
            'reason': 'market_making'
        }
    },
    'health_check': {
        'method': 'GET',
        'path': '/health',
        'headers': {},
        'body': None
    },
    'status_query': {
        'method': 'GET',
        'path': '/status',
        'headers': {},
        'body': None
    }
}

RESPONSE_TEMPLATES = {
    'signal_success': {
        'status': 200,
        'body': {
            'success': True,
            'orderId': '12345678',
            'side': 'BUY',
            'quantity': 0.01,
            'price': 50000.0,
            'timestamp': 1700000000.0
        }
    },
    'signal_failure': {
        'status': 400,
        'body': {
            'success': False,
            'reason': 'Position limit would be exceeded',
            'error_code': 'POSITION_LIMIT_EXCEEDED'
        }
    },
    'health_check': {
        'status': 200,
        'body': {
            'status': 'healthy',
            'server': 'executor-B',
            'timestamp': 1700000000.0,
            'active_orders': 3,
            'emergency_stop': False,
            'api_connected': True
        }
    },
    'status_query': {
        'status': 200,
        'body': {
            'server': 'executor-B',
            'timestamp': 1700000000.0,
            'session_volume': 1500.0,
            'volume_rate_per_hour': 300.0,
            'current_position': 0.05,
            'active_orders': 2,
            'emergency_stop': False,
            'order_count_last_minute': 5
        }
    }
}

# 错误代码定义
ERROR_CODES = {
    'INVALID_SIGNAL': 'Signal validation failed',
    'SIGNAL_EXPIRED': 'Signal has expired',
    'POSITION_LIMIT_EXCEEDED': 'Order would exceed position limit',
    'ORDER_SIZE_TOO_LARGE': 'Order size exceeds maximum allowed',
    'RATE_LIMIT_EXCEEDED': 'Too many orders in short time period',
    'EMERGENCY_STOP_ACTIVE': 'Emergency stop is currently active',
    'API_CONNECTION_FAILED': 'Unable to connect to exchange API',
    'INSUFFICIENT_BALANCE': 'Insufficient account balance',
    'INVALID_PRICE': 'Price is outside acceptable range',
    'MARKET_CLOSED': 'Market is currently closed',
    'INTERNAL_ERROR': 'Internal server error occurred'
}

def create_signal_message(action: ActionType, price: float, quantity: float,
                         reason: str = "", expire_seconds: float = 30.0) -> Dict:
    """创建标准的交易信号消息"""
    current_time = time.time()

    signal = TradingSignal(
        timestamp=current_time,
        action=action,
        price=price,
        quantity=quantity,
        expire_time=current_time + expire_seconds,
        reason=reason
    )

    return signal.to_dict()

def create_error_response(error_code: str, details: str = "") -> Dict:
    """创建标准的错误响应"""
    return {
        'success': False,
        'error_code': error_code,
        'reason': ERROR_CODES.get(error_code, 'Unknown error'),
        'details': details,
        'timestamp': time.time()
    }

def create_success_response(data: Dict = None) -> Dict:
    """创建标准的成功响应"""
    response = {
        'success': True,
        'timestamp': time.time()
    }

    if data:
        response.update(data)

    return response

# 使用示例
if __name__ == "__main__":
    # 创建交易信号
    signal = create_signal_message(
        action="buy_maker",
        price=50000.0,
        quantity=0.01,
        reason="Market making opportunity"
    )
    print("Trading Signal:")
    print(json.dumps(signal, indent=2))

    # 验证信号
    is_valid, message = ProtocolValidator.validate_signal(signal)
    print(f"\nValidation: {is_valid} - {message}")

    # 创建响应
    if is_valid:
        response = create_success_response({
            'orderId': '123456789',
            'side': 'BUY',
            'quantity': signal['quantity'],
            'price': signal['price']
        })
    else:
        response = create_error_response('INVALID_SIGNAL', message)

    print("\nResponse:")
    print(json.dumps(response, indent=2))