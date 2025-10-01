#!/usr/bin/env python3
"""
Aster Trading System - Order Statistics Query Tool

Usage:
    python3 query_stats.py                    # Query last 2 hours
    python3 query_stats.py --hours 24         # Query last 24 hours
    python3 query_stats.py --strategy cycle   # Filter by strategy (cycle or server)
    python3 query_stats.py --server B         # Only query server B
"""

import argparse
import asyncio
import os
import sys
from dataclasses import dataclass
from typing import Dict, List, Set

from aiohttp import ClientSession, ClientTimeout
from dotenv import load_dotenv

SYMBOL = "BTCUSDT"


@dataclass
class ServerStats:
    """Per-server statistics for orders"""
    server_id: str

    # Taker statistics
    taker_success_count: int = 0
    taker_buy_total_usd: float = 0.0
    taker_buy_total_btc: float = 0.0
    taker_sell_total_usd: float = 0.0
    taker_sell_total_btc: float = 0.0

    # Maker statistics
    maker_success_count: int = 0
    maker_buy_total_usd: float = 0.0
    maker_buy_total_btc: float = 0.0
    maker_sell_total_usd: float = 0.0
    maker_sell_total_btc: float = 0.0
    maker_unfilled_count: int = 0

    # Fee rates
    MAKER_FEE_RATE: float = 0.0001  # 0.01%
    TAKER_FEE_RATE: float = 0.00035  # 0.035%

    def taker_buy_avg(self) -> float:
        return self.taker_buy_total_usd / self.taker_buy_total_btc if self.taker_buy_total_btc > 0 else 0.0

    def taker_sell_avg(self) -> float:
        return self.taker_sell_total_usd / self.taker_sell_total_btc if self.taker_sell_total_btc > 0 else 0.0

    def maker_buy_avg(self) -> float:
        return self.maker_buy_total_usd / self.maker_buy_total_btc if self.maker_buy_total_btc > 0 else 0.0

    def maker_sell_avg(self) -> float:
        return self.maker_sell_total_usd / self.maker_sell_total_btc if self.maker_sell_total_btc > 0 else 0.0

    def maker_success_rate(self) -> float:
        total = self.maker_success_count + self.maker_unfilled_count
        return self.maker_success_count / total if total > 0 else 0.0

    def taker_total_volume_usd(self) -> float:
        return self.taker_buy_total_usd + self.taker_sell_total_usd

    def maker_total_volume_usd(self) -> float:
        return self.maker_buy_total_usd + self.maker_sell_total_usd

    def taker_fees(self) -> float:
        """Calculate taker fees"""
        return (self.taker_buy_total_usd + self.taker_sell_total_usd) * self.TAKER_FEE_RATE

    def maker_fees(self) -> float:
        """Calculate maker fees"""
        return (self.maker_buy_total_usd + self.maker_sell_total_usd) * self.MAKER_FEE_RATE

    def total_fees(self) -> float:
        """Calculate total fees"""
        return self.taker_fees() + self.maker_fees()

    def net_position_btc(self) -> float:
        """Calculate net BTC position (买入 - 卖出)"""
        total_buy = self.taker_buy_total_btc + self.maker_buy_total_btc
        total_sell = self.taker_sell_total_btc + self.maker_sell_total_btc
        return total_buy - total_sell

    def net_position_usd(self) -> float:
        """Calculate net USD position (卖出收入 - 买入支出)"""
        total_buy_cost = self.taker_buy_total_usd + self.maker_buy_total_usd
        total_sell_revenue = self.taker_sell_total_usd + self.maker_sell_total_usd
        return total_sell_revenue - total_buy_cost

    def realized_pnl(self) -> float:
        """Calculate trading P&L based on matched volume (简化版)

        计算逻辑：
        1. 找出配对成交的数量（买卖较小的一方）
        2. 计算价差盈亏 = 配对数量 × (平均卖价 - 平均买价)
        3. 扣除手续费

        这个方法假设买卖是对冲的，只计算周转盈亏，不考虑未平仓位。
        """
        total_buy_btc = self.taker_buy_total_btc + self.maker_buy_total_btc
        total_sell_btc = self.taker_sell_total_btc + self.maker_sell_total_btc

        # 如果没有交易，只有手续费损失
        if total_buy_btc == 0 or total_sell_btc == 0:
            return -self.total_fees()

        # 计算平均买入价和卖出价
        total_buy_usd = self.taker_buy_total_usd + self.maker_buy_total_usd
        total_sell_usd = self.taker_sell_total_usd + self.maker_sell_total_usd

        avg_buy_price = total_buy_usd / total_buy_btc
        avg_sell_price = total_sell_usd / total_sell_btc

        # 配对成交的数量（取较小方）
        matched_btc = min(total_buy_btc, total_sell_btc)

        # 价差盈亏
        spread_pnl = matched_btc * (avg_sell_price - avg_buy_price)

        # 扣除手续费
        return spread_pnl - self.total_fees()


class StatsQuery:
    def __init__(self, hours: float = 2.0, strategy: str = None, servers: List[str] = None, debug: bool = False):
        self.hours = hours
        self.strategy = strategy  # 'cycle' or 'server' or None (all)
        self.servers = servers or ['B', 'C']
        self.debug = debug
        self.processed_order_ids: Set[int] = set()

        # Get executor URLs from environment
        self.executor_urls = {
            'B': os.getenv('SERVER_B_URL', 'http://localhost:8081'),
            'C': os.getenv('SERVER_C_URL', 'http://localhost:8082')
        }

    async def query(self) -> Dict[str, ServerStats]:
        """Query statistics from executor servers"""
        stats: Dict[str, ServerStats] = {}

        # Create shared HTTP session
        timeout = ClientTimeout(total=30)
        async with ClientSession(timeout=timeout) as session:
            for server_id in self.servers:
                print(f"🔍 Querying {server_id} orders from {self.executor_urls[server_id]} (last {self.hours:.1f} hours)...")

                try:
                    # Query executor's /order_history endpoint
                    url = f"{self.executor_urls[server_id]}/order_history"
                    params = {'hours': self.hours}
                    if self.strategy:
                        params['strategy'] = self.strategy

                    async with session.get(url, params=params) as response:
                        if response.status != 200:
                            text = await response.text()
                            print(f"❌ Failed to query {server_id}: HTTP {response.status} - {text}")
                            continue

                        data = await response.json()
                        orders = data.get('orders', [])

                        print(f"   Found {data.get('total_orders', 0)} total orders, {data.get('filtered_orders', 0)} matched")

                        # Debug: show first order structure
                        if orders and self.debug:
                            print(f"\n   📋 Sample order structure:")
                            sample = orders[0]
                            for key in ['orderId', 'clientOrderId', 'status', 'side', 'price', 'origQty',
                                       'executedQty', 'avgPrice', 'cumQuote', 'cummulativeQuoteQty']:
                                if key in sample:
                                    print(f"      {key}: {sample[key]}")

                        # Process orders
                        server_stats = ServerStats(server_id=server_id)
                        self._process_orders(server_id, orders, server_stats)
                        stats[server_id] = server_stats

                        print(f"   ✅ Processed {server_stats.taker_success_count + server_stats.maker_success_count + server_stats.maker_unfilled_count} strategy orders")

                except Exception as e:
                    print(f"❌ Failed to query {server_id}: {e}")
                    if self.debug:
                        import traceback
                        print(traceback.format_exc())

        return stats

    def _process_orders(self, server_id: str, orders: List[Dict], stats: ServerStats) -> None:
        """Process order list and update statistics (orders are already filtered by executor)"""
        for order in orders:
            order_id = order.get('orderId')
            if not order_id or order_id in self.processed_order_ids:
                continue

            # Mark as processed
            self.processed_order_ids.add(order_id)

            client_order_id = order.get('clientOrderId', '')
            status = order.get('status')
            executed_qty = float(order.get('executedQty', 0))

            # Try different field names for quote amount
            cum_quote = float(order.get('cummulativeQuoteQty', 0) or
                            order.get('cumQuote', 0) or
                            order.get('cumQty', 0) or 0)

            # If still 0, calculate from price * quantity
            if cum_quote == 0 and executed_qty > 0:
                # Try avgPrice first, then fall back to order price
                avg_price = float(order.get('avgPrice', 0) or order.get('price', 0) or 0)
                if avg_price > 0:
                    cum_quote = executed_qty * avg_price

            side = order.get('side')

            # Determine type
            is_taker = 'taker' in client_order_id
            is_buy = side == 'BUY'

            # Update statistics
            if status == 'FILLED':
                if is_taker:
                    stats.taker_success_count += 1
                    if is_buy:
                        stats.taker_buy_total_usd += cum_quote
                        stats.taker_buy_total_btc += executed_qty
                    else:
                        stats.taker_sell_total_usd += cum_quote
                        stats.taker_sell_total_btc += executed_qty
                else:
                    stats.maker_success_count += 1
                    if is_buy:
                        stats.maker_buy_total_usd += cum_quote
                        stats.maker_buy_total_btc += executed_qty
                    else:
                        stats.maker_sell_total_usd += cum_quote
                        stats.maker_sell_total_btc += executed_qty
            elif status in ['CANCELED', 'EXPIRED'] and not is_taker:
                stats.maker_unfilled_count += 1

    def print_report(self, stats: Dict[str, ServerStats]) -> None:
        """Print formatted statistics report"""
        print("\n" + "=" * 90)
        print("📊 Order Statistics Report")
        print("=" * 90)
        print(f"Time Range: Last {self.hours:.1f} hours")
        if self.strategy:
            print(f"Strategy Filter: {self.strategy}")
        print("=" * 90)

        total_taker_volume = 0.0
        total_maker_volume = 0.0
        total_taker_fees = 0.0
        total_maker_fees = 0.0
        total_pnl = 0.0

        for server_id in sorted(stats.keys()):
            server_stats = stats[server_id]
            print(f"\n🔷 Server {server_id}:")
            print("-" * 90)

            # Taker statistics
            taker_count = server_stats.taker_success_count
            if taker_count > 0:
                print(f"  📍 Taker Orders: {taker_count} filled")
                print(f"     • Buy:  {server_stats.taker_buy_total_btc:.4f} BTC @ avg ${server_stats.taker_buy_avg():.2f} = ${server_stats.taker_buy_total_usd:.2f}")
                print(f"     • Sell: {server_stats.taker_sell_total_btc:.4f} BTC @ avg ${server_stats.taker_sell_avg():.2f} = ${server_stats.taker_sell_total_usd:.2f}")
                print(f"     • Total Volume: ${server_stats.taker_total_volume_usd():.2f}")
                total_taker_volume += server_stats.taker_total_volume_usd()
            else:
                print(f"  📍 Taker Orders: None")

            print()

            # Maker statistics
            maker_filled = server_stats.maker_success_count
            maker_unfilled = server_stats.maker_unfilled_count
            maker_total = maker_filled + maker_unfilled

            if maker_total > 0:
                print(f"  📍 Maker Orders: {maker_filled} filled, {maker_unfilled} unfilled ({server_stats.maker_success_rate():.1%} success)")
                if maker_filled > 0:
                    print(f"     • Buy:  {server_stats.maker_buy_total_btc:.4f} BTC @ avg ${server_stats.maker_buy_avg():.2f} = ${server_stats.maker_buy_total_usd:.2f}")
                    print(f"     • Sell: {server_stats.maker_sell_total_btc:.4f} BTC @ avg ${server_stats.maker_sell_avg():.2f} = ${server_stats.maker_sell_total_usd:.2f}")
                    print(f"     • Total Volume: ${server_stats.maker_total_volume_usd():.2f}")
                    total_maker_volume += server_stats.maker_total_volume_usd()
            else:
                print(f"  📍 Maker Orders: None")

            # Fees and P&L for this server
            total_buy_btc = server_stats.taker_buy_total_btc + server_stats.maker_buy_total_btc
            total_sell_btc = server_stats.taker_sell_total_btc + server_stats.maker_sell_total_btc
            matched_btc = min(total_buy_btc, total_sell_btc)
            unmatched_btc = abs(total_buy_btc - total_sell_btc)

            print()
            print(f"  💰 Fees & P&L:")
            print(f"     • Taker Fees:  ${server_stats.taker_fees():.2f} (0.035% of ${server_stats.taker_total_volume_usd():.2f})")
            print(f"     • Maker Fees:  ${server_stats.maker_fees():.2f} (0.01% of ${server_stats.maker_total_volume_usd():.2f})")
            print(f"     • Total Fees:  ${server_stats.total_fees():.2f}")
            print(f"     • Matched Volume: {matched_btc:.4f} BTC (paired trades)")
            print(f"     • Unmatched: {unmatched_btc:+.4f} BTC (open position)")
            print(f"     • Trading P&L (matched only): ${server_stats.realized_pnl():+.2f}")

            # Accumulate totals
            total_taker_fees += server_stats.taker_fees()
            total_maker_fees += server_stats.maker_fees()
            total_pnl += server_stats.realized_pnl()

        # Summary
        print("\n" + "=" * 90)
        print("📈 Summary:")
        print(f"  • Total Taker Volume: ${total_taker_volume:.2f}")
        print(f"  • Total Maker Volume: ${total_maker_volume:.2f}")
        print(f"  • Combined Volume:    ${total_taker_volume + total_maker_volume:.2f}")
        print()
        print(f"  💸 Total Fees Paid:")
        print(f"     • Taker Fees: ${total_taker_fees:.2f}")
        print(f"     • Maker Fees: ${total_maker_fees:.2f}")
        print(f"     • Total:      ${total_taker_fees + total_maker_fees:.2f}")
        print()
        print(f"  📊 Total Trading P&L (matched volume only): ${total_pnl:+.2f}")
        print(f"      Note: This only includes paired buy/sell trades.")
        print(f"            Open positions are NOT included in this calculation.")
        print("=" * 90 + "\n")


async def main():
    parser = argparse.ArgumentParser(
        description="Query Aster trading statistics from exchange API"
    )
    parser.add_argument(
        "--hours",
        type=float,
        default=2.0,
        help="Time range in hours (default: 2.0)"
    )
    parser.add_argument(
        "--strategy",
        choices=['cycle', 'server', 'all'],
        help="Filter by strategy (cycle, server, or all for no filter)"
    )
    parser.add_argument(
        "--server",
        choices=['B', 'C'],
        help="Only query specific server (B or C)"
    )
    parser.add_argument(
        "--debug",
        action='store_true',
        help="Show sample client_order_ids for debugging"
    )

    args = parser.parse_args()

    # Load environment variables
    load_dotenv()

    # Determine servers to query
    servers = [args.server] if args.server else ['B', 'C']

    # Create query instance
    query = StatsQuery(
        hours=args.hours,
        strategy=args.strategy,
        servers=servers,
        debug=args.debug
    )

    # Run query
    stats = await query.query()

    # Print report
    query.print_report(stats)


if __name__ == "__main__":
    asyncio.run(main())
