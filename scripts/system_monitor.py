#!/usr/bin/env python3
"""
Aster Trading System - System Monitor
监控整个分布式交易系统的运行状态、性能指标和风险情况
"""

import asyncio
import time
import json
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional
import aiohttp
from datetime import datetime, timezone

logger = logging.getLogger("system_monitor")

@dataclass
class ServerHealth:
    """服务器健康状态"""
    server_name: str
    url: str
    status: str = "unknown"  # healthy, unhealthy, offline
    response_time_ms: float = 0.0
    last_check: float = 0.0

    # 业务指标
    active_orders: int = 0
    current_position: float = 0.0
    session_volume: float = 0.0
    volume_rate_per_hour: float = 0.0
    emergency_stop: bool = False

@dataclass
class SystemMetrics:
    """系统整体指标"""
    timestamp: float
    total_volume_24h: float = 0.0
    total_position_usd: float = 0.0
    total_active_orders: int = 0

    # 积分相关
    estimated_points_per_hour: float = 0.0

    # 风险指标
    max_drawdown: float = 0.0
    total_unrealized_pnl: float = 0.0

    # 系统健康
    healthy_servers: int = 0
    total_servers: int = 0

class SystemMonitor:
    """系统监控器"""

    def __init__(self, servers: Dict[str, str]):
        """
        初始化监控器
        servers: {"coordinator": "http://ip:8080", "executor-b": "http://ip:8081", ...}
        """
        self.servers = {name: ServerHealth(name, url) for name, url in servers.items()}
        self.metrics_history: List[SystemMetrics] = []
        self.alert_thresholds = {
            'max_response_time_ms': 5000,
            'min_healthy_servers': 2,
            'max_total_position': 5000,
            'max_session_loss': 1000
        }
        coordinator_url = servers.get('coordinator', '').rstrip('/')
        self.coordinator_exposure_url = f"{coordinator_url}/exposure" if coordinator_url else ''

    async def start_monitoring(self, interval_seconds: float = 30.0):
        """开始监控循环"""
        logger.info("🔍 Starting system monitoring...")

        while True:
            try:
                # 检查所有服务器健康状态
                await self._check_all_servers()

                # 计算系统指标
                metrics = await self._calculate_system_metrics()
                self.metrics_history.append(metrics)

                # 保留最近100条记录
                if len(self.metrics_history) > 100:
                    self.metrics_history = self.metrics_history[-100:]

                # 检查告警条件
                await self._check_alerts(metrics)

                # 生成报告
                self._log_system_status(metrics)

                await asyncio.sleep(interval_seconds)

            except Exception as e:
                logger.error(f"❌ Monitoring error: {e}")
                await asyncio.sleep(10.0)

    async def _check_all_servers(self):
        """检查所有服务器健康状态"""
        tasks = []
        for server in self.servers.values():
            tasks.append(self._check_server_health(server))

        await asyncio.gather(*tasks, return_exceptions=True)

    async def _check_server_health(self, server: ServerHealth):
        """检查单个服务器健康状态"""
        start_time = time.time()

        try:
            timeout = aiohttp.ClientTimeout(total=10.0)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                # 检查健康状态
                async with session.get(f"{server.url}/health") as response:
                    server.response_time_ms = (time.time() - start_time) * 1000
                    server.last_check = time.time()

                    if response.status == 200:
                        health_data = await response.json()
                        server.status = "healthy"
                        server.active_orders = health_data.get('active_orders', 0)
                        server.emergency_stop = health_data.get('emergency_stop', False)
                    else:
                        server.status = "unhealthy"

                # 获取详细状态 (如果健康的话)
                if server.status == "healthy":
                    try:
                        async with session.get(f"{server.url}/status") as response:
                            if response.status == 200:
                                status_data = await response.json()
                                server.current_position = status_data.get('current_position', 0.0)
                                server.session_volume = status_data.get('session_volume', 0.0)
                                server.volume_rate_per_hour = status_data.get('volume_rate_per_hour', 0.0)
                    except:
                        pass  # 状态查询失败不影响健康检查

        except asyncio.TimeoutError:
            server.status = "offline"
            server.response_time_ms = 10000  # 超时
            server.last_check = time.time()
            logger.warning(f"⏰ Server {server.server_name} timeout")

        except Exception as e:
            server.status = "offline"
            server.response_time_ms = 0
            server.last_check = time.time()
            logger.error(f"❌ Server {server.server_name} check failed: {e}")

    async def _calculate_system_metrics(self) -> SystemMetrics:
        """计算系统整体指标"""
        current_time = time.time()
        metrics = SystemMetrics(timestamp=current_time)

        # 统计服务器状态
        healthy_count = sum(1 for s in self.servers.values() if s.status == "healthy")
        metrics.healthy_servers = healthy_count
        metrics.total_servers = len(self.servers)

        for server_name, server in self.servers.items():
            if server.status == "healthy":
                metrics.total_active_orders += server.active_orders
                metrics.total_volume_24h += server.session_volume
                metrics.estimated_points_per_hour += server.volume_rate_per_hour * 0.001  # 简化计算

                if server_name == 'coordinator':
                    exposure = await self._fetch_coordinator_exposure()
                    metrics.total_position_usd = exposure
                else:
                    metrics.total_position_usd += server.current_position

        return metrics

    async def _check_alerts(self, metrics: SystemMetrics):
        """检查告警条件"""
        alerts = []

        # 服务器健康度告警
        if metrics.healthy_servers < self.alert_thresholds['min_healthy_servers']:
            alerts.append(f"🚨 Only {metrics.healthy_servers}/{metrics.total_servers} servers healthy")

        # 响应时间告警
        slow_servers = [
            s.server_name for s in self.servers.values()
            if s.response_time_ms > self.alert_thresholds['max_response_time_ms']
        ]
        if slow_servers:
            alerts.append(f"🐌 Slow servers: {', '.join(slow_servers)}")

        # 仓位风险告警
        if abs(metrics.total_position_usd) > self.alert_thresholds['max_total_position']:
            alerts.append(f"⚠️ High total position: ${metrics.total_position_usd:.2f}")

        # 紧急停止告警
        emergency_servers = [s.server_name for s in self.servers.values() if s.emergency_stop]
        if emergency_servers:
            alerts.append(f"🚨 Emergency stop active: {', '.join(emergency_servers)}")

        # 记录告警
        for alert in alerts:
            logger.warning(alert)

    def _log_system_status(self, metrics: SystemMetrics):
        """记录系统状态"""
        healthy_servers = [s.server_name for s in self.servers.values() if s.status == "healthy"]
        offline_servers = [s.server_name for s in self.servers.values() if s.status == "offline"]

        logger.info(f"📊 System Status: "
                   f"Servers: {metrics.healthy_servers}/{metrics.total_servers} healthy, "
                   f"Position: ${metrics.total_position_usd:.2f}, "
                   f"Volume: ${metrics.total_volume_24h:.2f}, "
                   f"Orders: {metrics.total_active_orders}")

        if offline_servers:
            logger.warning(f"⚠️ Offline servers: {', '.join(offline_servers)}")

    def get_system_report(self) -> Dict:
        """生成系统报告"""
        if not self.metrics_history:
            return {"error": "No metrics available"}

        latest_metrics = self.metrics_history[-1]

        # 计算趋势 (最近10分钟)
        recent_metrics = [m for m in self.metrics_history if latest_metrics.timestamp - m.timestamp < 600]

        if len(recent_metrics) > 1:
            volume_trend = recent_metrics[-1].total_volume_24h - recent_metrics[0].total_volume_24h
            position_trend = recent_metrics[-1].total_position_usd - recent_metrics[0].total_position_usd
        else:
            volume_trend = 0
            position_trend = 0

        # 服务器详情
        server_details = {}
        for name, server in self.servers.items():
            server_details[name] = {
                'status': server.status,
                'response_time_ms': server.response_time_ms,
                'position': server.current_position,
                'volume_rate': server.volume_rate_per_hour,
                'active_orders': server.active_orders,
                'emergency_stop': server.emergency_stop,
                'last_check': server.last_check
            }

        return {
            'timestamp': latest_metrics.timestamp,
            'system_health': {
                'healthy_servers': latest_metrics.healthy_servers,
                'total_servers': latest_metrics.total_servers,
                'health_ratio': latest_metrics.healthy_servers / max(latest_metrics.total_servers, 1)
            },
            'trading_metrics': {
                'total_position_usd': latest_metrics.total_position_usd,
                'total_volume_24h': latest_metrics.total_volume_24h,
                'total_active_orders': latest_metrics.total_active_orders,
                'estimated_points_per_hour': latest_metrics.estimated_points_per_hour
            },
            'trends': {
                'volume_trend_10min': volume_trend,
                'position_trend_10min': position_trend
            },
            'servers': server_details,
            'alerts': {
                'slow_servers': [s.server_name for s in self.servers.values()
                               if s.response_time_ms > 5000],
                'offline_servers': [s.server_name for s in self.servers.values()
                                  if s.status == "offline"],
                'emergency_stops': [s.server_name for s in self.servers.values()
                                  if s.emergency_stop]
            }
        }

    async def emergency_stop_all(self) -> Dict:
        """紧急停止所有服务器"""
        logger.critical("🚨 EMERGENCY STOP ALL SERVERS")

        results = {}
        tasks = []

        for name, server in self.servers.items():
            if server.status == "healthy":
                tasks.append(self._emergency_stop_server(name, server))

        responses = await asyncio.gather(*tasks, return_exceptions=True)

        for i, (name, _) in enumerate(self.servers.items()):
            if i < len(responses):
                results[name] = responses[i] if not isinstance(responses[i], Exception) else str(responses[i])

        return results

    async def _emergency_stop_server(self, name: str, server: ServerHealth):
        """紧急停止单个服务器"""
        try:
            timeout = aiohttp.ClientTimeout(total=5.0)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(f"{server.url}/emergency_stop") as response:
                    if response.status == 200:
                        result = await response.json()
                        logger.info(f"✅ Emergency stop sent to {name}")
                        return result
                    else:
                        logger.error(f"❌ Emergency stop failed for {name}: HTTP {response.status}")
                        return {"error": f"HTTP {response.status}"}
        except Exception as e:
            logger.error(f"❌ Emergency stop failed for {name}: {e}")
            return {"error": str(e)}

async def main():
    """主函数 - 示例用法"""
    import argparse

    parser = argparse.ArgumentParser(description="Aster System Monitor")
    parser.add_argument("--coordinator", default="http://localhost:8080", help="Coordinator URL")
    parser.add_argument("--executor-b", default="http://localhost:8081", help="Executor B URL")
    parser.add_argument("--executor-c", default="http://localhost:8082", help="Executor C URL")
    parser.add_argument("--interval", type=float, default=30.0, help="Check interval in seconds")
    parser.add_argument("--report", action="store_true", help="Generate report and exit")

    args = parser.parse_args()

    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # 创建监控器
    servers = {
        "coordinator": args.coordinator,
        "executor-b": args.executor_b,
        "executor-c": args.executor_c
    }

    monitor = SystemMonitor(servers)

    if args.report:
        # 生成一次性报告
        await monitor._check_all_servers()
        metrics = await monitor._calculate_system_metrics()
        monitor.metrics_history.append(metrics)

        report = monitor.get_system_report()
        print(json.dumps(report, indent=2))
    else:
        # 持续监控
        try:
            await monitor.start_monitoring(args.interval)
        except KeyboardInterrupt:
            logger.info("👋 Monitoring stopped by user")

if __name__ == "__main__":
    asyncio.run(main())
    async def _fetch_coordinator_exposure(self) -> float:
        if not self.coordinator_exposure_url:
            return 0.0

        try:
            timeout = aiohttp.ClientTimeout(total=5.0)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(self.coordinator_exposure_url) as response:
                    if response.status != 200:
                        return 0.0
                    payload = await response.json()
                    return float(payload.get('total_exposure_usd', 0.0))
        except Exception:
            return 0.0
