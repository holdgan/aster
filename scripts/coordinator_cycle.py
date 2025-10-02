#!/usr/bin/env python3
"""Alternate coordinator strategy: cyclic inventory maximizer.

Behaviour summary:
- Drive B and C to their respective maximum maker positions (phase = LONG),
  then flip and drive them to the opposite extreme (phase = SHORT), repeating continuously.
- Orders are sent via maker limits using small BTC quanta for smooth ramps.
- If combined net exposure exceeds the hard ratio (10% of total limits),
  immediately place taker orders to pull exposure back within bounds.

This file leaves the original coordinator untouched; run this one if you want
simple inventory cycling behaviour.
"""

import asyncio
import logging
import os
import sys
import signal
import time
import random
from dataclasses import dataclass
from typing import Dict, Optional

from aiohttp import ClientSession, ClientTimeout
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.trading_client import (
    AsterFuturesClient,
    TradingConfig,
    AccountConfig,
    AccountState,
    SymbolSpec,
    get_logger,
)

# --- Trading Strategy Constants ------------------------------------------------
# All strategy parameters are collected here for easy management

SYMBOL = "BTCUSDT"

# Position and risk parameters (仓位和风控参数)
MAX_POSITION_B_USD = 5000.0  # Server B maximum position in USD
MAX_POSITION_C_USD = 5000.0  # Server C maximum position in USD
MIN_ORDER_QTY_BTC = 0.001  # Minimum order quantity
SAFETY_MARGIN = 0.85  # 85% of max position as target
PHASE_TOLERANCE_USD = 100.0  # How close (USD) before we consider target reached
NET_EXPOSURE_RATIO_LIMIT = 0.04  # 4% of combined max position
MAKER_COOLDOWN_SECONDS = 2.0  # Cooldown between maker orders

load_dotenv()
logger = get_logger("coordinator-cycle")


@dataclass
class CycleConfig:
    # Server endpoints - from .env (网络配置)
    server_b_url: str = os.getenv("SERVER_B_URL", "http://localhost:8081")
    server_c_url: str = os.getenv("SERVER_C_URL", "http://localhost:8082")

    # Strategy parameters - hardcoded in file (策略配置)
    maker_step_btc: float = 0.002  # BTC per order
    decision_interval: float = 5.0  # seconds between iterations
    signal_timeout: float = 3.0  # HTTP timeout for signals

    # Order timing parameters (订单时间参数)
    taker_expire_seconds: float = 5.0  # Taker emergency orders
    maker_expire_seconds: float = 30.0  # Maker normal adjustment
    maker_overlimit_expire_seconds: float = 20.0  # Maker force reduction
    signal_delay_min: float = 0.0  # Random delay before sending signal
    signal_delay_max: float = 5.0  # Random delay before sending signal

    # Maker order price offset (挂单价格偏移)
    maker_price_offset: float = 0.1  # Buy at bid-offset, sell at ask+offset

    @classmethod
    def from_env(cls) -> "CycleConfig":
        return cls()


class InventoryCycler:
    def __init__(self) -> None:
        self.config = CycleConfig.from_env()
        self.trading_config = TradingConfig()

        api_key = os.getenv("ASTER_A_KEY")
        api_secret = os.getenv("ASTER_A_SECRET")
        if not api_key or not api_secret:
            raise RuntimeError("ASTER_A_KEY/SECRET missing in environment")

        symbol_spec = SymbolSpec(
            symbol=SYMBOL,
            tick_size=0.1,
            qty_step=0.001,
            min_qty=0.001,
            min_notional=5.0,
            price_precision=1,
            qty_precision=3,
        )

        self.account_state = AccountState(
            config=AccountConfig(
                identifier="A",
                role="coordinator-cycle",
                target_hold_notional=0.0,
                maker_order_notional=500.0,
            ),
            client=None,
            symbol_spec=symbol_spec,
            metrics={},
        )

        self.session: Optional[ClientSession] = None
        self.running = True

        self.server_urls = {"B": self.config.server_b_url, "C": self.config.server_c_url}
        self.position_limits = {
            "B": MAX_POSITION_B_USD,
            "C": MAX_POSITION_C_USD,
        }
        # phases per server: 'LONG' or 'SHORT' (complementary: B LONG = C SHORT)
        self.phases: Dict[str, str] = {"B": "LONG", "C": "SHORT"}
        self.last_order_time = 0.0
        self.last_emergency_time = 0.0  # Track last emergency to prevent rapid-fire
        self.last_maker_server = None  # Track last server to alternate between B and C

    # ------------------------------------------------------------------ lifecycle

    async def start(self) -> None:
        logger.info("🚀 Starting InventoryCycler coordinator")
        timeout = ClientTimeout(total=30)
        self.session = ClientSession(timeout=timeout)
        self.account_state.client = AsterFuturesClient(
            api_key=os.getenv("ASTER_A_KEY"),
            api_secret=os.getenv("ASTER_A_SECRET"),
            session=self.session,
            name="coordinator-cycle",
        )

        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT, self.stop)
        loop.add_signal_handler(signal.SIGTERM, self.stop)

        try:
            while self.running:
                await self._iteration()
                await asyncio.sleep(self.config.decision_interval)
        finally:
            await self.cleanup()

    def stop(self) -> None:
        if self.running:
            logger.info("🛑 Stop requested")
            self.running = False

    async def cleanup(self) -> None:
        try:
            if self.session:
                await self.session.close()
        except Exception as exc:  # noqa: broad-except
            logger.warning("Cleanup error: %s", exc)

    # ------------------------------------------------------------------- iteration

    async def _iteration(self) -> None:
        market = await self._get_market_data()
        if not market:
            logger.warning("No market data, skip iteration")
            return

        exposures_btc = await self._fetch_positions()
        exposures_usd = {
            server: exposures_btc[server] * market["mid_price"]
            for server in exposures_btc
        }
        net_exposure = sum(exposures_usd.values())
        max_total = sum(self.position_limits.values())
        hard_limit_usd = max_total * NET_EXPOSURE_RATIO_LIMIT

        # Log current state
        usage_b = abs(exposures_usd['B']) / self.position_limits['B'] * 100 if self.position_limits['B'] > 0 else 0
        usage_c = abs(exposures_usd['C']) / self.position_limits['C'] * 100 if self.position_limits['C'] > 0 else 0
        logger.info(
            "💰 Net:$%.0f (%.0f%%) | B:$%.0f(%.0f%% %s) C:$%.0f(%.0f%% %s)",
            net_exposure,
            abs(net_exposure) / max_total * 100 if max_total > 0 else 0,
            exposures_usd['B'], usage_b, self.phases['B'],
            exposures_usd['C'], usage_c, self.phases['C']
        )

        decisions: list[Dict[str, object]] = []

        # Emergency cooldown: 10 seconds (give taker time to fill)
        now = market["timestamp"]
        emergency_cooldown = 10.0
        time_since_emergency = now - self.last_emergency_time

        if abs(net_exposure) > hard_limit_usd:
            if time_since_emergency < emergency_cooldown:
                logger.debug(
                    f"Emergency cooldown active ({time_since_emergency:.1f}s/{emergency_cooldown}s), skip"
                )
            else:
                logger.warning(
                    "🚨 Net exposure $%.0f exceeds HARD LIMIT $%.0f, triggering balance",
                    net_exposure,
                    hard_limit_usd,
                )
                emergency_decision = self._build_emergency_decision(net_exposure, exposures_usd, market)
                if emergency_decision:
                    decisions.append(emergency_decision)
                    self._apply_simulated_exposure_change(decisions[-1], exposures_usd)
                    self.last_emergency_time = now
                    # Recalculate net exposure after simulated change
                    net_exposure = sum(exposures_usd.values())

        # normal maker decisions (max one maker per iteration, alternate between B and C)
        if abs(net_exposure) <= hard_limit_usd:
            # Alternate: if last was B, try C first; if last was C, try B first
            servers = ["C", "B"] if self.last_maker_server == "B" else ["B", "C"]
            for server in servers:
                decision = self._build_maker_adjustment(server, market, exposures_usd)
                if decision:
                    decisions.append(decision)
                    self._apply_simulated_exposure_change(decision, exposures_usd)
                    self.last_maker_server = server
                    break  # Only one maker per iteration

        # phase flip check
        if self._phase_targets_reached(exposures_usd):
            self._flip_phases()

        for decision in decisions:
            # Random delay before sending signal
            delay = random.uniform(self.config.signal_delay_min, self.config.signal_delay_max)
            await asyncio.sleep(delay)

            await self._send_signal(decision)
            # Log decision
            action = decision.get("action", "")
            side_emoji = "🔵" if "buy" in action else "🔴"
            logger.info(
                "%s %s %s %.4f BTC @ $%.1f | %s (delayed %.1fs)",
                side_emoji,
                decision.get("server"),
                action,
                decision.get("quantity", 0),
                decision.get("price", 0),
                decision.get("reason", "")[:50],
                delay
            )

    # ------------------------------------------------------------------ helpers

    def _target_usd(self, server: str) -> float:
        sign = 1 if self.phases[server] == "LONG" else -1
        return self.position_limits[server] * SAFETY_MARGIN * sign

    def _build_emergency_decision(
        self,
        net_exposure: float,
        exposures_usd: Dict[str, float],
        market: Dict[str, float],
    ) -> Optional[Dict[str, object]]:
        # Choose the account that is LAGGING (further from target) to accelerate it
        # This way we don't slow down the leading account
        targets = {s: self._target_usd(s) for s in ("B", "C")}
        gaps = {s: targets[s] - exposures_usd[s] for s in ("B", "C")}

        if net_exposure > 0:
            # Net too long → need to accelerate SHORT account (negative target)
            # Choose account with largest negative gap (most behind on SHORT)
            server = min(gaps.items(), key=lambda x: x[1])[0]
            side = "SELL"
        else:
            # Net too short → need to accelerate LONG account (positive target)
            # Choose account with largest positive gap (most behind on LONG)
            server = max(gaps.items(), key=lambda x: x[1])[0]
            side = "BUY"

        quantity = max(self.config.maker_step_btc, MIN_ORDER_QTY_BTC)
        action = "sell_taker" if side == "SELL" else "buy_taker"
        return {
            "server": server,
            "action": action,
            "price": market["mid_price"],
            "quantity": quantity,
            "expire_time": market["timestamp"] + self.config.taker_expire_seconds,
            "reason": f"🚨 EMERGENCY net exposure balance ${net_exposure:.0f}",
        }

    def _build_maker_adjustment(
        self,
        server: str,
        market: Dict[str, float],
        exposures_usd: Dict[str, float],
    ) -> Optional[Dict[str, object]]:
        current_usd = exposures_usd[server]
        target_usd = self._target_usd(server)

        # Hard limit check: if already over max position, force reduction
        max_limit = self.position_limits[server]
        if abs(current_usd) > max_limit:
            logger.warning(
                f"⚠️ {server} position ${current_usd:.0f} exceeds hard limit ${max_limit:.0f}, forcing reduction"
            )
            # Force opposite direction to reduce position
            side = "SELL" if current_usd > 0 else "BUY"
            quantity = max(self.config.maker_step_btc, MIN_ORDER_QTY_BTC)
            price = self._calculate_maker_price(market, side)
            return {
                "server": server,
                "action": "sell_maker" if side == "SELL" else "buy_maker",
                "price": price,
                "quantity": quantity,
                "expire_time": market["timestamp"] + self.config.maker_overlimit_expire_seconds,
                "reason": f"🚨 Reduce over-limit position ${current_usd:.0f} > ${max_limit:.0f}",
            }

        gap = target_usd - current_usd
        if abs(gap) <= PHASE_TOLERANCE_USD:
            return None

        quantity = max(self.config.maker_step_btc, MIN_ORDER_QTY_BTC)
        side = "BUY" if gap > 0 else "SELL"
        # clamp notional so we do not overshoot the target in one shot
        max_adjust_usd = min(abs(gap), self.position_limits[server] * 0.1)
        tentative_qty = max_adjust_usd / market["mid_price"]
        quantity = max(MIN_ORDER_QTY_BTC, min(quantity, self.account_state.symbol_spec.round_qty(tentative_qty)))
        if quantity <= 0:
            quantity = MIN_ORDER_QTY_BTC

        action = "buy_maker" if side == "BUY" else "sell_maker"
        price = self._calculate_maker_price(market, side)
        return {
            "server": server,
            "action": action,
            "price": price,
            "quantity": quantity,
            "expire_time": market["timestamp"] + self.config.maker_expire_seconds,
            "reason": f"Phase adjust toward {target_usd:+.0f} USD",
        }

    def _phase_targets_reached(self, exposures_usd: Dict[str, float]) -> bool:
        return all(abs(exposures_usd[s] - self._target_usd(s)) <= PHASE_TOLERANCE_USD for s in ("B", "C"))

    def _flip_phases(self) -> None:
        # Flip while maintaining complementary relationship (B LONG = C SHORT)
        if self.phases['B'] == 'LONG':
            self.phases['B'] = 'SHORT'
            self.phases['C'] = 'LONG'
        else:
            self.phases['B'] = 'LONG'
            self.phases['C'] = 'SHORT'
        logger.info(
            "🔁 Phase flip: B -> %s, C -> %s",
            self.phases['B'],
            self.phases['C'],
        )

    def _apply_simulated_exposure_change(self, decision: Dict[str, object], exposures_usd: Dict[str, float]) -> None:
        server = decision["server"]  # type: ignore[index]
        quantity = float(decision["quantity"])  # type: ignore[index]
        price = float(decision.get("price", 0.0))  # maker use computed price, taker mid
        if "taker" in decision["action"]:  # type: ignore[index]
            price = float(decision.get("price", 0.0))
        notional = quantity * price
        if "buy" in decision["action"]:  # type: ignore[index]
            exposures_usd[server] = exposures_usd.get(server, 0.0) + notional
        else:
            exposures_usd[server] = exposures_usd.get(server, 0.0) - notional

    async def _send_signal(self, decision: Dict[str, object]) -> None:
        server = decision["server"]  # type: ignore[index]
        url = self.server_urls[server]

        # Generate client_order_id for tracking
        action = decision["action"]  # type: ignore[index]
        timestamp = int(time.time() * 1000)
        client_order_id = f"cycle_{server}_{action}_{timestamp}"

        signal_payload = {
            "timestamp": time.time(),  # Use wall-clock time for signal validation
            "action": decision["action"],
            "symbol": SYMBOL,
            "price": self.account_state.symbol_spec.round_price(decision["price"]),
            "quantity": self.account_state.symbol_spec.round_qty(decision["quantity"]),
            "expire_time": decision["expire_time"],
            "source": "coordinator-cycle",
            "reason": decision["reason"],
            "client_order_id": client_order_id,
        }
        try:
            timeout = ClientTimeout(total=self.config.signal_timeout)
            async with ClientSession(timeout=timeout) as session:
                async with session.post(
                    f"{url}/signal",
                    json=signal_payload,
                    headers={"Content-Type": "application/json"},
                ) as response:
                    if response.status != 200:
                        body = await response.text()
                        logger.warning("%s signal failed HTTP %s body=%s", server, response.status, body[:120])
        except asyncio.TimeoutError:
            logger.warning("⏰ Timeout sending signal to %s", server)
        except Exception as exc:  # noqa: broad-except
            logger.error("Network error sending to %s: %s", server, exc)

    # ----------------------------------------------------------------- data fetch

    async def _fetch_positions(self) -> Dict[str, float]:
        results: Dict[str, float] = {}
        for server in ("B", "C"):
            try:
                endpoint = f"{self.server_urls[server]}/position"
                async with self.session.get(endpoint) as response:  # type: ignore[union-attr]
                    data = await response.json()
                    results[server] = float(data.get("position_btc", 0.0))
            except Exception as exc:  # noqa: broad-except
                logger.warning("Failed to fetch position for %s: %s", server, exc)
                results[server] = 0.0
        return results

    async def _get_market_data(self) -> Optional[Dict[str, float]]:
        try:
            depth = await self.account_state.client.get_depth(SYMBOL, limit=5)
            bids = depth.get("bids")
            asks = depth.get("asks")
            if not bids or not asks:
                return None
            bid = float(bids[0][0])
            ask = float(asks[0][0])
            mid = (bid + ask) / 2
            return {
                "timestamp": time.time(),  # Use wall-clock time for signal validation
                "bid_price": bid,
                "ask_price": ask,
                "mid_price": mid,
            }
        except Exception as exc:
            logger.error("Failed to fetch market data: %s", exc)
            return None

    def _calculate_maker_price(self, market: Dict[str, float], side: str) -> float:
        """Calculate maker order price

        买单：挂在买1价格 - offset（bid_price - offset）
        卖单：挂在卖1价格 + offset（ask_price + offset）
        """
        if side == "BUY":
            price = market["bid_price"] - self.config.maker_price_offset
        else:
            price = market["ask_price"] + self.config.maker_price_offset
        return self.account_state.symbol_spec.round_price(price)


async def main() -> None:
    logging.getLogger().setLevel(os.getenv("LOG_LEVEL", "INFO"))
    coordinator = InventoryCycler()
    await coordinator.start()


if __name__ == "__main__":
    asyncio.run(main())
