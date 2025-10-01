#!/usr/bin/env python3
"""
Aster BTCUSDT Executor Server (B/C服务器通用脚本)
接收协调器信号，执行maker订单，维护本地风控
"""
from collections import deque

import asyncio
import json
import time
import logging
import os
import signal
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
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

# 导入工具模块 (简单测试)
try:
    from utils.api_protocol import TradingSignal
    from utils.points_optimizer import PointsOptimizer
    from utils.risk_manager import RiskManager
    print("✅ Utils modules imported successfully")
except ImportError as e:
    print(f"⚠️  Utils modules not available: {e}")

load_dotenv()
logger = get_logger("executor")

# 读取基础配置（执行器仅做执行，风险由协调器负责）
DEFAULT_MAX_ORDER_USD = os.getenv('MAX_ORDER_USD')
DEFAULT_MAX_ORDER_USD = float(DEFAULT_MAX_ORDER_USD) if DEFAULT_MAX_ORDER_USD else None

# 读取订单超时配置
DEFAULT_ORDER_EXPIRY = float(os.getenv('ORDER_EXPIRY_TIME', '30.0'))
DEFAULT_PENDING_FAILURE = float(os.getenv('PENDING_FAILURE_SECONDS', '20.0'))

@dataclass
class ExecutorConfig:
    """执行器配置"""
    # 服务器配置
    listen_host: str = "0.0.0.0"
    listen_port: int = 8080

    # 风控参数
    max_position_usd: Optional[float] = None  # 仅用于监控日志，不阻断执行
    max_order_usd: Optional[float] = field(default_factory=lambda: DEFAULT_MAX_ORDER_USD)
    max_orders_per_minute: int = 30
    order_expire_seconds: float = field(default_factory=lambda: DEFAULT_ORDER_EXPIRY)
    pending_failure_seconds: float = field(default_factory=lambda: DEFAULT_PENDING_FAILURE)

class ExecutorBot:
    """执行器机器人 - 接收信号执行maker订单"""

    def __init__(self, account_id: str, server_name: str = "executor"):
        self.config = ExecutorConfig()
        self.trading_config = TradingConfig()
        self.server_name = server_name
        self.account_id = account_id

        # 获取API密钥
        api_key = os.getenv(f'ASTER_{account_id}_KEY')
        api_secret = os.getenv(f'ASTER_{account_id}_SECRET')

        if not api_key or not api_secret:
            raise ValueError(f"Missing API credentials for account {account_id}")

        # 初始化HTTP会话（将在运行时创建）
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

        # 初始化交易客户端（延迟到async环境中）
        self.account_state = AccountState(
            config=AccountConfig(
                identifier=account_id,
                role="executor",
                target_hold_notional=0.0,
                maker_order_notional=300.0
            ),
            client=None,  # 将在async初始化中设置
            symbol_spec=self.symbol_spec,
            metrics={}
        )

        # 状态管理
        self.running = True
        self.active_orders = {}  # orderId -> order_info
        self.signal_history = []  # 信号历史
        self.order_count_last_minute = 0
        self.last_minute_timestamp = time.time()
        self.background_tasks = None  # 后台任务引用
        self.recent_fills: deque[Dict[str, Any]] = deque(maxlen=50)

        # 风控状态
        self.emergency_stop = False
        self.total_pnl = 0.0
        self.session_volume = 0.0
        self.session_start_time = time.time()

        # 存储API密钥
        self.api_key = api_key
        self.api_secret = api_secret

        # Web服务器
        self.app = web.Application()
        self._setup_routes()

    def _format_price(self, price: float) -> str:
        """格式化价格，去除多余浮点尾数"""
        rounded = self.symbol_spec.round_price(price)
        return f"{rounded:.{self.symbol_spec.price_precision}f}"

    def _format_quantity(self, quantity: float) -> str:
        """格式化数量以匹配交易步长"""
        rounded = self.symbol_spec.round_qty(quantity)
        if rounded < self.symbol_spec.min_qty:
            rounded = self.symbol_spec.min_qty
        return f"{rounded:.{self.symbol_spec.qty_precision}f}"

    async def _async_init(self):
        """异步初始化 - 创建HTTP会话和客户端"""
        self.session = ClientSession(timeout=ClientTimeout(total=30))

        # 创建交易客户端
        self.account_state.client = AsterFuturesClient(
            api_key=self.api_key,
            api_secret=self.api_secret,
            session=self.session,
            name=f"executor-{self.account_id}"
        )

    async def _cleanup(self):
        """清理资源"""
        if self.session:
            await self.session.close()

    def _setup_routes(self):
        """设置HTTP路由"""
        self.app.router.add_post('/signal', self._handle_signal)
        self.app.router.add_get('/health', self._handle_health)
        self.app.router.add_get('/status', self._handle_status)
        self.app.router.add_post('/emergency_stop', self._handle_emergency_stop)
        self.app.router.add_get('/stats', self._handle_stats)
        self.app.router.add_get('/position', self._handle_position)

    async def start(self):
        """启动执行器服务器"""
        logger.info(f"🚀 Starting Executor Server: {self.server_name}")

        runner = None

        try:
            # AsterFuturesClient 不需要 initialize() 方法
            logger.info(f"✅ Trading client ready for account {self.account_id}")

            # 启动后台任务
            self.background_tasks = asyncio.create_task(self._background_tasks())

            # 启动web服务器
            runner = web.AppRunner(self.app)
            await runner.setup()

            site = web.TCPSite(runner, self.config.listen_host, self.config.listen_port)
            await site.start()

            logger.info(f"🌐 Executor server listening on {self.config.listen_host}:{self.config.listen_port}")

            # 设置信号处理 - 在event loop中
            loop = asyncio.get_running_loop()
            loop.add_signal_handler(signal.SIGINT, self._handle_shutdown)
            loop.add_signal_handler(signal.SIGTERM, self._handle_shutdown)

            # 等待关闭信号
            await self.background_tasks

        except KeyboardInterrupt:
            logger.info("🛑 Keyboard interrupt received")
        except asyncio.CancelledError:
            logger.info("🛑 Tasks cancelled")
        except Exception as e:
            logger.error(f"❌ Executor failed: {e}")
        finally:
            # 取消后台任务
            if self.background_tasks and not self.background_tasks.done():
                self.background_tasks.cancel()
                try:
                    await self.background_tasks
                except asyncio.CancelledError:
                    pass

            # 清理web服务器
            if runner:
                await runner.cleanup()

            await self.cleanup()

    async def _background_tasks(self):
        """后台任务协程"""
        try:
            await asyncio.gather(
                self._order_management_loop(),
                self._risk_monitoring_loop(),
                self._performance_reporting_loop(),
                return_exceptions=True
            )
        except asyncio.CancelledError:
            logger.info("🛑 Background tasks cancelled")
            raise

    async def _handle_signal(self, request):
        """处理来自协调器的交易信号"""
        try:
            signal_data = await request.json()
            action = signal_data.get('action')

            raw_quantity = signal_data.get('quantity', 0.0)
            raw_price = signal_data.get('price', 0.0)

            try:
                quantity_val = float(raw_quantity)
            except (TypeError, ValueError):
                quantity_val = 0.0

            try:
                price_val = float(raw_price)
            except (TypeError, ValueError):
                price_val = 0.0

            side_emoji = "🔵" if 'buy' in action.lower() else "🔴"
            logger.info(
                f"{side_emoji} Signal: {action} {self._format_quantity(quantity_val)} @ {self._format_price(price_val)}"
            )

            # 验证信号
            validation_result = self._validate_signal(signal_data)
            if not validation_result['valid']:
                logger.warning(f"⚠️ Signal validation failed: {validation_result['reason']}")
                return web.json_response({
                    'success': False,
                    'reason': validation_result['reason']
                })

            # 处理信号
            result = await self._process_signal(signal_data)

            # 记录信号历史
            self.signal_history.append({
                'timestamp': time.time(),
                'signal': signal_data,
                'result': result
            })

            # 清理历史记录 (只保留最近100条)
            if len(self.signal_history) > 100:
                self.signal_history = self.signal_history[-100:]

            return web.json_response(result)

        except Exception as e:
            logger.error(f"❌ Error handling signal: {e}")
            return web.json_response({
                'success': False,
                'reason': f'Internal error: {str(e)}'
            })

    def _validate_signal(self, signal: Dict) -> Dict:
        """验证交易信号"""
        # 检查必要字段
        required_fields = ['action', 'symbol', 'price', 'quantity', 'expire_time']
        for field in required_fields:
            if field not in signal:
                return {'valid': False, 'reason': f'Missing field: {field}'}

        # 检查信号是否过期
        if time.time() > signal['expire_time']:
            return {'valid': False, 'reason': 'Signal expired'}

        # 检查交易对
        if signal['symbol'] != SYMBOL:
            return {'valid': False, 'reason': f'Wrong symbol: {signal["symbol"]}'}

        # 检查动作类型
        if signal['action'] not in ['buy_maker', 'sell_maker', 'buy_taker', 'sell_taker']:
            return {'valid': False, 'reason': f'Invalid action: {signal["action"]}'}

        # 检查价格和数量
        if signal['quantity'] <= 0:
            return {'valid': False, 'reason': 'Invalid price or quantity'}

        if signal['action'] in ['buy_maker', 'sell_maker'] and signal['price'] <= 0:
            return {'valid': False, 'reason': 'Invalid price for maker order'}

        # 检查订单大小
        notional = signal['price'] * signal['quantity']
        if self.config.max_order_usd and notional > self.config.max_order_usd:
            return {'valid': False, 'reason': f'Order too large: ${notional:.2f}'}

        # 检查频率限制
        current_minute = int(time.time() / 60)
        if current_minute > int(self.last_minute_timestamp / 60):
            self.order_count_last_minute = 0
            self.last_minute_timestamp = time.time()

        if self.order_count_last_minute >= self.config.max_orders_per_minute:
            return {'valid': False, 'reason': 'Rate limit exceeded'}

        # 检查紧急停止状态
        if self.emergency_stop:
            return {'valid': False, 'reason': 'Emergency stop activated'}

        return {'valid': True, 'reason': 'OK'}

    async def _process_signal(self, signal: Dict) -> Dict:
        """处理交易信号"""
        try:
            action = signal['action']
            price = float(signal['price'])
            quantity = float(signal['quantity'])

            # 执行订单
            if action == 'buy_maker':
                order_result = await self._place_buy_maker_order(price, quantity)
            elif action == 'sell_maker':
                order_result = await self._place_sell_maker_order(price, quantity)
            elif action == 'buy_taker':
                order_result = await self._place_buy_taker_order(quantity)
            else:  # sell_taker
                order_result = await self._place_sell_taker_order(quantity)

            if order_result['success']:
                self.order_count_last_minute += 1
                self.session_volume += price * quantity

                # 记录活跃订单
                if 'orderId' in order_result:
                    self.active_orders[order_result['orderId']] = {
                        'symbol': SYMBOL,
                        'side': 'BUY' if action == 'buy_maker' else 'SELL',
                        'price': price,
                        'quantity': quantity,
                        'timestamp': time.time(),
                        'signal': signal,
                        'outcome_recorded': False
                    }

            return order_result

        except Exception as e:
            logger.error(f"❌ Error processing signal: {e}")
            return {'success': False, 'reason': str(e)}

    async def _place_buy_maker_order(self, price: float, quantity: float) -> Dict:
        """下买单maker订单"""
        try:
            # 使用symbol_spec进行精度取整
            rounded_price = self.symbol_spec.round_price(price)
            rounded_qty = self.symbol_spec.round_qty(quantity)

            order_params = {
                'symbol': SYMBOL,
                'side': 'BUY',
                'type': 'LIMIT',
                'quantity': f"{rounded_qty:.{self.symbol_spec.qty_precision}f}",
                'price': f"{rounded_price:.{self.symbol_spec.price_precision}f}",
                'timeInForce': 'GTX'  # GTX确保只做maker
            }

            result = await self.account_state.client.place_order(order_params)

            if result and 'orderId' in result:
                logger.debug(f"✅ Buy maker: {quantity:.6f} @ ${price:.2f} (ID: {result['orderId']})")
                return {
                    'success': True,
                    'orderId': result['orderId'],
                    'side': 'BUY',
                    'quantity': quantity,
                    'price': price
                }
            else:
                logger.error(f"❌ Buy order failed: {result}")
                return {'success': False, 'reason': 'Order placement failed'}

        except Exception as e:
            logger.error(f"❌ Buy maker order error: {e}")
            return {'success': False, 'reason': str(e)}

    async def _place_sell_maker_order(self, price: float, quantity: float) -> Dict:
        """下卖单maker订单"""
        try:
            # 使用symbol_spec进行精度取整
            rounded_price = self.symbol_spec.round_price(price)
            rounded_qty = self.symbol_spec.round_qty(quantity)

            order_params = {
                'symbol': SYMBOL,
                'side': 'SELL',
                'type': 'LIMIT',
                'quantity': f"{rounded_qty:.{self.symbol_spec.qty_precision}f}",
                'price': f"{rounded_price:.{self.symbol_spec.price_precision}f}",
                'timeInForce': 'GTX'  # GTX确保只做maker
            }

            result = await self.account_state.client.place_order(order_params)

            if result and 'orderId' in result:
                logger.debug(f"✅ Sell maker: {quantity:.6f} @ ${price:.2f} (ID: {result['orderId']})")
                return {
                    'success': True,
                    'orderId': result['orderId'],
                    'side': 'SELL',
                    'quantity': quantity,
                    'price': price
                }
            else:
                logger.error(f"❌ Sell order failed: {result}")
                return {'success': False, 'reason': 'Order placement failed'}

        except Exception as e:
            logger.error(f"❌ Sell maker order error: {e}")
            return {'success': False, 'reason': str(e)}

    async def _place_buy_taker_order(self, quantity: float) -> Dict:
        """下买单taker订单"""
        try:
            rounded_qty = self.symbol_spec.round_qty(quantity)
            order_params = {
                'symbol': SYMBOL,
                'side': 'BUY',
                'type': 'MARKET',
                'quantity': f"{rounded_qty:.{self.symbol_spec.qty_precision}f}"
            }

            result = await self.account_state.client.place_order(order_params)

            if result and 'orderId' in result:
                logger.info(f"✅ Buy taker order executed: {quantity:.6f} BTC (Order ID: {result['orderId']})")
                return {
                    'success': True,
                    'orderId': result['orderId'],
                    'side': 'BUY',
                    'quantity': quantity,
                    'price': float(result.get('avgPrice', 0.0)) if result else 0.0
                }
            else:
                logger.error(f"❌ Buy taker order failed: {result}")
                return {'success': False, 'reason': 'Order placement failed'}

        except Exception as e:
            logger.error(f"❌ Buy taker order error: {e}")
            return {'success': False, 'reason': str(e)}

    async def _place_sell_taker_order(self, quantity: float) -> Dict:
        """下卖单taker订单"""
        try:
            rounded_qty = self.symbol_spec.round_qty(quantity)
            order_params = {
                'symbol': SYMBOL,
                'side': 'SELL',
                'type': 'MARKET',
                'quantity': f"{rounded_qty:.{self.symbol_spec.qty_precision}f}"
            }

            result = await self.account_state.client.place_order(order_params)

            if result and 'orderId' in result:
                logger.info(f"✅ Sell taker order executed: {quantity:.6f} BTC (Order ID: {result['orderId']})")
                return {
                    'success': True,
                    'orderId': result['orderId'],
                    'side': 'SELL',
                    'quantity': quantity,
                    'price': float(result.get('avgPrice', 0.0)) if result else 0.0
                }
            else:
                logger.error(f"❌ Sell taker order failed: {result}")
                return {'success': False, 'reason': 'Order placement failed'}

        except Exception as e:
            logger.error(f"❌ Sell taker order error: {e}")
            return {'success': False, 'reason': str(e)}

    async def _get_current_position(self) -> float:
        """获取当前仓位"""
        try:
            positions = await self.account_state.client.get_positions()
            for pos in positions:
                if pos.get('symbol') == 'BTCUSDT':
                    return float(pos.get('positionAmt', 0))
            return 0.0
        except Exception as e:
            logger.error(f"❌ Failed to get position: {e}")
            return 0.0

    async def _order_management_loop(self):
        """订单管理循环"""
        logger.info("📋 Starting order management loop")

        while self.running:
            try:
                current_time = time.time()
                orders_to_remove = []

                # 检查活跃订单状态
                for order_id, order_info in list(self.active_orders.items()):
                    try:
                        order_age = current_time - order_info['timestamp']

                        # 超过失败阈值直接标记失败并撤单
                        if order_age > self.config.pending_failure_seconds:
                            logger.warning(
                                "⚠️ Order %s stuck %.1fs (>%.1fs), marking as failed",
                                order_id,
                                order_age,
                                self.config.pending_failure_seconds
                            )
                            self._mark_order_outcome(order_id, filled=False)
                            await self._cancel_order(order_id, record_failure=False)
                            orders_to_remove.append(order_id)
                            continue

                        # 正常过期检查（保留兜底）
                        if order_age > self.config.order_expire_seconds:
                            logger.debug(f"⏰ Canceling expired order: {order_id}")
                            await self._cancel_order(order_id)
                            orders_to_remove.append(order_id)
                            continue

                        # 查询订单状态
                        order_status = await self.account_state.client.get_order(SYMBOL, order_id)

                        if order_status:
                            status = order_status.get('status')
                            if status in ['FILLED', 'CANCELED', 'REJECTED', 'EXPIRED']:
                                if status == 'FILLED':
                                    filled_qty = float(order_status.get('executedQty', 0))
                                    filled_price = float(order_status.get('avgPrice', order_info['price']))
                                    side_emoji = "🔵" if order_info.get('side') == 'BUY' else "🔴"
                                    # Calculate current fill rate
                                    recent_60s = [e for e in self.recent_fills if current_time - e['timestamp'] <= 60]
                                    if recent_60s:
                                        current_fill_rate = sum(1 for e in recent_60s if e['filled']) / len(recent_60s)
                                    else:
                                        current_fill_rate = 0.5
                                    logger.info(f"{side_emoji} Filled {filled_qty:.6f} @ ${filled_price:.2f} | FillRate: {current_fill_rate*100:.0f}%")
                                    self._mark_order_outcome(order_id, filled=True)
                                else:
                                    self._mark_order_outcome(order_id, filled=False)
                                orders_to_remove.append(order_id)

                    except Exception as e:
                        logger.error(f"❌ Error checking order {order_id}: {e}")

                # 清理完成的订单
                for order_id in orders_to_remove:
                    if order_id in self.active_orders:
                        self.active_orders.pop(order_id, None)

                await asyncio.sleep(2.0)

            except Exception as e:
                logger.error(f"❌ Order management loop error: {e}")
                await asyncio.sleep(5.0)

    def _mark_order_outcome(self, order_id: str, filled: bool):
        """记录订单结果，避免重复写入"""
        order_info = self.active_orders.get(order_id)
        if order_info and order_info.get('outcome_recorded'):
            return

        if order_info is not None:
            order_info['outcome_recorded'] = True

        self.recent_fills.append({
            'order_id': order_id,
            'timestamp': time.time(),
            'filled': filled
        })

    async def _cancel_order(self, order_id: str, record_failure: bool = True):
        """取消订单"""
        try:
            result = await self.account_state.client.cancel_order(SYMBOL, order_id)
            if result:
                logger.debug(f"✅ Order canceled: {order_id}")
                if record_failure:
                    self._mark_order_outcome(order_id, filled=False)
            else:
                logger.warning(f"⚠️ Cancel order failed: {order_id}")
                if record_failure:
                    self._mark_order_outcome(order_id, filled=False)
        except Exception as e:
            logger.error(f"❌ Cancel order error: {e}")
            if record_failure:
                self._mark_order_outcome(order_id, filled=False)

    async def _risk_monitoring_loop(self):
        """风控监控循环"""
        logger.info("🛡️ Starting risk monitoring loop")

        while self.running:
            try:
                # 简单监控仓位用于日志
                current_position = await self._get_current_position()
                if current_position != 0:
                    market_data = await self._get_simple_market_price()
                    if market_data and self.config.max_position_usd:
                        position_value = current_position * market_data['price']
                        if abs(position_value) > self.config.max_position_usd:
                            logger.warning(f"⚠️ Position limit exceeded: ${position_value:.2f}")

                await asyncio.sleep(10.0)

            except Exception as e:
                logger.error(f"❌ Risk monitoring error: {e}")
                await asyncio.sleep(30.0)

    async def _get_simple_market_price(self) -> Optional[Dict]:
        """保留兼容接口，后续由协调器负责价格"""
        return None

    async def _performance_reporting_loop(self):
        """性能报告循环"""
        while self.running:
            try:
                await asyncio.sleep(300)  # 每5分钟报告一次

                session_time = time.time() - self.session_start_time
                volume_rate = self.session_volume / (session_time / 3600) if session_time > 0 else 0

                current_position = await self._get_current_position()

                logger.info(f"📊 {self.server_name} Performance: "
                          f"Volume: ${self.session_volume:.2f}, "
                          f"Rate: ${volume_rate:.2f}/hr, "
                          f"Position: {current_position:.6f} BTC, "
                          f"Active Orders: {len(self.active_orders)}")

            except Exception as e:
                logger.error(f"❌ Performance reporting error: {e}")

    async def _handle_health(self, request):
        """健康检查端点"""
        try:
            # 简单的健康检查
            account_info = await self.account_state.client.get_account()

            return web.json_response({
                'status': 'healthy',
                'server': self.server_name,
                'timestamp': time.time(),
                'active_orders': len(self.active_orders),
                'emergency_stop': self.emergency_stop,
                'api_connected': account_info is not None
            })
        except Exception as e:
            return web.json_response({
                'status': 'unhealthy',
                'server': self.server_name,
                'error': str(e)
            }, status=500)

    async def _handle_status(self, request):
        """状态查询端点"""
        try:
            current_position = await self._get_current_position()

            session_time = time.time() - self.session_start_time
            volume_rate = self.session_volume / (session_time / 3600) if session_time > 0 else 0

            return web.json_response({
                'server': self.server_name,
                'timestamp': time.time(),
                'session_volume': self.session_volume,
                'volume_rate_per_hour': volume_rate,
                'current_position': current_position,
                'active_orders': len(self.active_orders),
                'emergency_stop': self.emergency_stop,
                'order_count_last_minute': self.order_count_last_minute,
                'recent_signals': len([s for s in self.signal_history if time.time() - s['timestamp'] < 300])  # 最近5分钟
            })
        except Exception as e:
            return web.json_response({'error': str(e)}, status=500)

    async def _handle_position(self, request):
        """返回当前仓位敞口信息"""
        try:
            current_position = await self._get_current_position()
            logger.info(
                "Position endpoint: btc=%.6f",
                current_position
            )

            return web.json_response({
                'server': self.server_name,
                'symbol': SYMBOL,
                'position_btc': current_position,
                'timestamp': time.time()
            })
        except Exception as e:
            return web.json_response({'error': str(e)}, status=500)

    async def _handle_stats(self, request):
        """返回近期订单统计（包含挂单惩罚）"""
        current_time = time.time()
        window = float(request.query.get('window', 60.0))

        # 历史订单统计
        recent = [entry for entry in self.recent_fills if current_time - entry['timestamp'] <= window]

        # 当前挂单统计 - 关键修复！
        pending_orders = []
        max_pending_duration = 0.0
        pending_timeout = self.config.pending_failure_seconds

        for order_id, order_info in self.active_orders.items():
            age = current_time - order_info['timestamp']
            pending_orders.append({
                'order_id': order_id,
                'age': age
            })
            max_pending_duration = max(max_pending_duration, age)

        # 计算实际成交率：历史成交 / (历史总数 + 超时挂单数)
        if not recent and not pending_orders:
            fill_rate = 0.5
        else:
            filled_count = sum(1 for entry in recent if entry['filled'])
            total_evaluated = len(recent)

            # 挂单超过15秒的，视为"即将失败"，计入分母
            pending_failures = sum(1 for order in pending_orders if order['age'] > pending_timeout)
            total_evaluated += pending_failures

            if total_evaluated > 0:
                fill_rate = filled_count / total_evaluated
            else:
                fill_rate = 0.5

        pending_count = len(pending_orders)
        max_pending_age = max_pending_duration

        return web.json_response({
            'server': self.server_name,
            'window_seconds': window,
            'total_orders': len(recent),
            'filled_orders': sum(1 for entry in recent if entry['filled']),
            'pending_orders': pending_count,
            'pending_timeout_count': sum(1 for o in pending_orders if o['age'] > pending_timeout),
            'max_pending_duration': max_pending_duration,
            'pending_count': pending_count,
            'max_pending_age': max_pending_age,
            'fill_rate': fill_rate,
            'timestamp': current_time
        })

    async def _handle_emergency_stop(self, request):
        """紧急停止端点"""
        self.emergency_stop = True

        # 取消所有活跃订单
        cancel_tasks = []
        for order_id in list(self.active_orders.keys()):
            cancel_tasks.append(self._cancel_order(order_id))

        if cancel_tasks:
            await asyncio.gather(*cancel_tasks, return_exceptions=True)

        logger.warning("🚨 EMERGENCY STOP ACTIVATED - All orders canceled")

        return web.json_response({
            'success': True,
            'message': 'Emergency stop activated',
            'canceled_orders': len(cancel_tasks)
        })

    def _handle_shutdown(self):
        """处理关闭信号"""
        logger.info(f"🛑 Shutdown signal received, stopping {self.server_name}...")
        self.running = False

        # 主动取消后台任务以立即退出
        if self.background_tasks and not self.background_tasks.done():
            self.background_tasks.cancel()

    async def cleanup(self):
        """清理资源"""
        try:
            # 取消所有活跃订单
            if self.active_orders:
                logger.info(f"🧹 Canceling {len(self.active_orders)} active orders...")
                cancel_tasks = [self._cancel_order(order_id) for order_id in self.active_orders.keys()]
                await asyncio.gather(*cancel_tasks, return_exceptions=True)

            # 关闭客户端连接
            if hasattr(self.account_state.client, 'close'):
                await self.account_state.client.close()

            logger.info(f"✅ {self.server_name} cleanup completed")
        except Exception as e:
            logger.error(f"❌ Cleanup error: {e}")

async def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description="Aster Executor Server")
    parser.add_argument("--account", required=True, help="Account identifier (B or C)")
    parser.add_argument("--port", type=int, default=8080, help="Listen port")
    parser.add_argument("--name", help="Server name for logging")
    parser.add_argument("--log-level", default="INFO", help="Log level")
    parser.add_argument("--max-position", type=float, default=None, help="Max position USD (optional)")
    parser.add_argument("--max-order", type=float, default=None, help="Max order USD (optional)")

    args = parser.parse_args()

    # 设置日志级别
    logging.getLogger().setLevel(getattr(logging, args.log_level.upper()))

    # 设置服务器名称
    server_name = args.name if args.name else f"executor-{args.account}"

    # 创建执行器
    executor = ExecutorBot(args.account, server_name)
    executor.config.listen_port = args.port
    if args.max_position is not None:
        executor.config.max_position_usd = args.max_position
    if args.max_order is not None:
        executor.config.max_order_usd = args.max_order

    # 异步初始化
    await executor._async_init()

    try:
        await executor.start()
    except KeyboardInterrupt:
        logger.info(f"👋 {server_name} stopped by user")
    except Exception as e:
        logger.error(f"💥 {server_name} crashed: {e}")
        sys.exit(1)
    finally:
        # 清理资源
        await executor._cleanup()

if __name__ == "__main__":
    asyncio.run(main())
