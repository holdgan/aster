#!/usr/bin/env python3
"""Aster BTCUSDT Genesis Stage 2 dual-account trading bot."""

import argparse
import asyncio
import json
import hmac
import hashlib
import math
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple, Set

import aiohttp
from urllib.parse import urlencode
from dotenv import load_dotenv
import logging

# Simple logger setup - no external dependencies
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def get_logger(name: str):
    return logging.getLogger(name)

logger = get_logger("aster_btcusdt_bot")
API_BASE = "https://fapi.asterdex.com"
SYMBOL = "BTCUSDT"
BASE_ASSET = "BTC"
QUOTE_ASSET = "USDT"

DEFAULT_LOOP_INTERVAL = 1.0  # seconds
ACCOUNT_REFRESH_INTERVAL = 6.0  # seconds between full account syncs
TIME_SYNC_INTERVAL = 60.0  # seconds between server time syncs

TEAM_BOOST_MULTIPLIER = 1.5

@dataclass
class TradingConfig:
    """Unified configuration - eliminates magic numbers scattered throughout code."""

    # Risk Management
    leverage: int = 5
    margin_buffer_ratio: float = 0.05
    binance_price_tolerance: float = 0.01  # 1%

    # Order Management - Conservative settings for risk reduction
    price_offset_ticks: int = 1               # Add 1 tick offset for safer pricing
    order_size_percentage: float = 0.025     # Reduced from 4% to 2.5% (smaller orders)
    min_order_notional: float = 300.0        # Reduced minimum for more flexibility
    max_order_notional: float = 1000.0       # Reduced cap from $1500 to $1000
    ping_pong_percentage: float = 0.02       # More conservative ping-pong (2% vs 3%)
    min_ping_pong_notional: float = 300.0    # Lower minimum for more opportunities

    # Queue Management - More conservative approach
    queue_dominance_ratio: float = 100.0      # Less aggressive queue handling (back to 100.0)
    extreme_queue_ratio: float = 1000.0       # Higher threshold for aggressive pricing (back to 1000.0)
    fill_rate_target: float = 0.20            # Lower target (20% vs 25%) - quality over quantity
    queue_notional_threshold: float = 8000.0  # Higher threshold for more patience (8K vs 5K)
    price_aggression_adjustment: float = 2.0  # Gentler aggression (back to 2.0)

    # Timing
    snapshot_interval: float = 5.0
    reprice_seconds: float = 3.0
    loop_interval: float = 1.0

    # Points System
    holding_rate_per_usd_hour: float = 1 / 24
    collateral_rate_per_usd_hour: float = 1 / 24
    pnl_points_rate: float = 0.25

    # Static Settings
    collateral_preferred_assets: frozenset = frozenset({"USDF", "ASBNB"})
    continuous_trading_mode: bool = True

# Global config instance - single source of truth
TRADING_CONFIG = TradingConfig()

# Backwards compatibility aliases (will remove these gradually)
MAX_LEVERAGE = TRADING_CONFIG.leverage
MARGIN_BUFFER_RATIO = TRADING_CONFIG.margin_buffer_ratio
BINANCE_PRICE_TOLERANCE = TRADING_CONFIG.binance_price_tolerance
PRICE_OFFSET_TICKS = TRADING_CONFIG.price_offset_ticks
QUEUE_DOMINANCE_RATIO = TRADING_CONFIG.queue_dominance_ratio
EXTREME_QUEUE_THRESHOLD = TRADING_CONFIG.extreme_queue_ratio
FILL_RATE_TARGET = TRADING_CONFIG.fill_rate_target
QUEUE_NOTIONAL_THRESHOLD = TRADING_CONFIG.queue_notional_threshold
PRICE_AGGRESSION_ADJUSTMENT = TRADING_CONFIG.price_aggression_adjustment
SNAPSHOT_INTERVAL = TRADING_CONFIG.snapshot_interval
REPRICE_SECONDS = TRADING_CONFIG.reprice_seconds
HOLDING_RATE_PER_USD_HOUR = TRADING_CONFIG.holding_rate_per_usd_hour
COLLATERAL_RATE_PER_USD_HOUR = TRADING_CONFIG.collateral_rate_per_usd_hour
PNL_POINTS_RATE = TRADING_CONFIG.pnl_points_rate
CONTINUOUS_TRADING_MODE = TRADING_CONFIG.continuous_trading_mode

# Remove these deprecated constants
UTC = timezone.utc


@dataclass
class Quote:
    bid: float = 0.0
    ask: float = 0.0
    bid_qty: float = 0.0
    ask_qty: float = 0.0
    ts: float = 0.0
@dataclass
class SymbolSpec:
    symbol: str
    tick_size: float
    qty_step: float
    min_qty: float
    min_notional: float
    price_precision: int
    qty_precision: int

    def round_price(self, value: float) -> float:
        ticks = round(value / self.tick_size)
        return max(self.tick_size, ticks * self.tick_size)

    def round_qty(self, value: float) -> float:
        steps = math.floor(value / self.qty_step + 1e-8)
        return max(self.min_qty, steps * self.qty_step)

    def clamp_notional(self, price: float, qty: float) -> float:
        notional = abs(price * qty)
        return max(self.min_notional, notional)


@dataclass
class AccountConfig:
    identifier: str  # email or label stored in userdata
    role: str  # "buy" or "sell"
    target_hold_notional: float
    maker_order_notional: float = 50.0  # Default value, now managed by TradingConfig
    price_offset_ticks: int = 0  # Use TradingConfig default
    leverage: int = 5  # Updated to match TradingConfig
    margin_buffer: float = 0.05  # Use TradingConfig default
    team_boost: float = TEAM_BOOST_MULTIPLIER
    prefer_collateral_assets: Tuple[str, ...] = frozenset({"USDF", "ASBNB"})
    base_target_notional: float = field(init=False)
    max_notional: float = field(default=0.0, init=False)

    def __post_init__(self) -> None:
        self.base_target_notional = abs(self.target_hold_notional)
        self.apply_role_sign()

    def apply_role_sign(self) -> None:
        sign = 1 if self.role.lower() == "buy" else -1
        self.target_hold_notional = sign * self.base_target_notional

    def set_role(self, new_role: str) -> None:
        self.role = new_role
        self.apply_role_sign()


@dataclass
class PositionSnapshot:
    quantity: float = 0.0
    entry_price: float = 0.0
    mark_price: float = 0.0
    unrealized_pnl: float = 0.0
    notional: float = 0.0


@dataclass
class AccountMetrics:
    wallet_balance: float = 0.0
    available_balance: float = 0.0
    initial_margin: float = 0.0
    position_margin: float = 0.0
    margin_ratio: float = 0.0
    collateral_values: Dict[str, float] = field(default_factory=dict)


@dataclass
class PointsState:
    maker_volume: float = 0.0
    taker_volume: float = 0.0
    holding_usd_hours: float = 0.0
    collateral_usd_hours: float = 0.0
    realized_pnl: float = 0.0

    def weekly_volume(self) -> float:
        return self.maker_volume + self.taker_volume


@dataclass
class AccountState:
    config: AccountConfig
    client: "AsterFuturesClient"
    symbol_spec: SymbolSpec
    position: PositionSnapshot = field(default_factory=PositionSnapshot)
    metrics: AccountMetrics = field(default_factory=AccountMetrics)
    points: PointsState = field(default_factory=PointsState)
    week_start: datetime = field(default_factory=lambda: current_week_start())
    last_account_refresh: float = 0.0
    last_time_integrated: float = field(default_factory=lambda: time.time())
    last_trade_id: Optional[int] = None
    active_order_id: Optional[int] = None
    last_quote_update: float = 0.0
    active_order_price: float = 0.0
    active_order_qty: float = 0.0
    active_order_ts: float = 0.0
    misaligned_since: float = 0.0
    orders_placed: int = 0
    orders_filled: int = 0
    last_fill_check: float = 0.0

    def reset_for_new_week(self, new_week: datetime) -> None:
        logger.info("[%s] Resetting weekly stats", self.config.identifier)
        self.points = PointsState()
        self.week_start = new_week

    def needs_refresh(self) -> bool:
        return (time.time() - self.last_account_refresh) >= ACCOUNT_REFRESH_INTERVAL
def current_week_start(ts: Optional[datetime] = None) -> datetime:
    """Return Monday 00:00 UTC for week containing ts."""
    if ts is None:
        ts = datetime.now(UTC)
    else:
        ts = ts.astimezone(UTC)
    weekday = ts.weekday()
    start = datetime(ts.year, ts.month, ts.day, tzinfo=UTC) - timedelta(days=weekday)
    return start.replace(hour=0, minute=0, second=0, microsecond=0)


def utc_now() -> datetime:
    return datetime.now(UTC)


async def async_sleep(seconds: float) -> None:
    try:
        await asyncio.sleep(seconds)
    except asyncio.CancelledError:
        raise
def _round_val(value: Optional[float], digits: int) -> Optional[float]:
    if value is None:
        return None
    try:
        return round(float(value), digits)
    except Exception:
        return float(value)


def load_aster_credentials_from_env(identifier: str) -> Tuple[str, str]:
    """Load API credentials from environment variables.

    Simple and reliable - no database dependencies, no complex column guessing.
    """
    # Load .env file
    load_dotenv()

    # Map identifiers to environment variable suffixes
    env_suffix = identifier.upper()  # A -> A, B -> B

    api_key = os.getenv(f"ASTER_{env_suffix}_KEY")
    api_secret = os.getenv(f"ASTER_{env_suffix}_PWD")

    if not api_key or not api_secret:
        raise RuntimeError(f"API credentials not found in environment for account '{identifier}'. "
                         f"Set ASTER_{env_suffix}_KEY and ASTER_{env_suffix}_PWD in .env file")

    return api_key.strip(), api_secret.strip()


async def load_aster_credentials(identifier: str) -> Tuple[str, str]:
    """Load API credentials - now uses environment variables instead of database."""
    return load_aster_credentials_from_env(identifier)
class AsterFuturesClient:
    def __init__(self, api_key: str, api_secret: str, session: aiohttp.ClientSession, name: str):
        self.api_key = api_key
        self.api_secret = api_secret.encode()
        self.session = session
        self.name = name
        self.recv_window = 5000
        self.time_offset = 0.0
        self.last_time_sync = 0.0
        self._rate_limit_sleep = 0.1
        self._symbol_spec: Optional[SymbolSpec] = None

    async def sync_time(self) -> None:
        now = time.time()
        if now - self.last_time_sync < TIME_SYNC_INTERVAL:
            return
        try:
            data = await self._public_request("GET", "/fapi/v1/time")
            server_time = int(data.get("serverTime", 0)) / 1000.0
            if server_time:
                self.time_offset = server_time - time.time()
                self.last_time_sync = now
                logger.debug("[%s] Time synced; offset=%.3f", self.name, self.time_offset)
        except Exception as exc:
            logger.warning("[%s] Failed to sync server time: %s", self.name, exc)

    async def _public_request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = API_BASE + path
        try:
            async with self.session.request(method, url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                text = await resp.text()
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status} {text}")
                return await resp.json()
        except Exception:
            raise

    async def _signed_request(self, method: str, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        params = params.copy() if params else {}
        await self.sync_time()
        params.setdefault("timestamp", int((time.time() + self.time_offset) * 1000))
        params.setdefault("recvWindow", self.recv_window)
        query_string = urlencode(params, doseq=True)
        signature = hmac.new(self.api_secret, query_string.encode(), hashlib.sha256).hexdigest()
        params["signature"] = signature

        headers = {"X-MBX-APIKEY": self.api_key}
        url = API_BASE + path
        try:
            if method.upper() == "GET":
                async with self.session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    text = await resp.text()
                    if resp.status != 200:
                        raise RuntimeError(f"HTTP {resp.status} {text}")
                    return await resp.json()
            elif method.upper() == "DELETE":
                async with self.session.delete(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    text = await resp.text()
                    if resp.status != 200:
                        raise RuntimeError(f"HTTP {resp.status} {text}")
                    if resp.content_type == "application/json":
                        return await resp.json()
                    return text
            else:
                async with self.session.request(method, url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    text = await resp.text()
                    if resp.status != 200:
                        raise RuntimeError(f"HTTP {resp.status} {text}")
                    if resp.content_type == "application/json":
                        return await resp.json()
                    return text
        except Exception:
            raise

    async def get_symbol_spec(self) -> SymbolSpec:
        if self._symbol_spec is None:
            data = await self._public_request("GET", "/fapi/v1/exchangeInfo")
            symbols = data.get("symbols", [])
            spec = None
            for item in symbols:
                if item.get("symbol") == SYMBOL:
                    tick_size = 0.1
                    qty_step = 0.001
                    min_qty = 0.001
                    min_notional = 5.0
                    price_precision = int(item.get("pricePrecision", 1))
                    qty_precision = int(item.get("quantityPrecision", 3))
                    for flt in item.get("filters", []):
                        ftype = flt.get("filterType")
                        if ftype == "PRICE_FILTER":
                            tick_size = float(flt.get("tickSize", tick_size))
                        elif ftype == "LOT_SIZE":
                            qty_step = float(flt.get("stepSize", qty_step))
                            min_qty = float(flt.get("minQty", min_qty))
                        elif ftype == "MIN_NOTIONAL":
                            min_notional = float(flt.get("notional", flt.get("minNotional", min_notional)))
                    spec = SymbolSpec(
                        symbol=SYMBOL,
                        tick_size=tick_size,
                        qty_step=qty_step,
                        min_qty=min_qty,
                        min_notional=min_notional,
                        price_precision=price_precision,
                        qty_precision=qty_precision,
                    )
                    break
            if spec is None:
                raise RuntimeError(f"Symbol {SYMBOL} not found in exchange info")
            self._symbol_spec = spec
        return self._symbol_spec

    async def set_margin_type(self, symbol: str, margin_type: str = "CROSSED") -> None:
        try:
            await self._signed_request("POST", "/fapi/v1/marginType", {"symbol": symbol, "marginType": margin_type})
        except RuntimeError as exc:
            # API returns error if already in desired mode; ignore specific codes
            if "-4046" in str(exc):  # no need to change margin type
                logger.debug("[%s] Margin type already %s", self.name, margin_type)
            else:
                logger.warning("[%s] Failed to set margin type: %s", self.name, exc)

    async def set_leverage(self, symbol: str, leverage: int) -> None:
        try:
            await self._signed_request("POST", "/fapi/v1/leverage", {"symbol": symbol, "leverage": leverage})
        except Exception as exc:
            logger.warning("[%s] Failed to set leverage=%s: %s", self.name, leverage, exc)

    async def get_account(self) -> Dict[str, Any]:
        return await self._signed_request("GET", "/fapi/v2/account")

    async def get_positions(self) -> List[Dict[str, Any]]:
        return await self._signed_request("GET", "/fapi/v2/positionRisk")

    async def get_open_orders(self, symbol: str) -> List[Dict[str, Any]]:
        return await self._signed_request("GET", "/fapi/v1/openOrders", {"symbol": symbol})

    async def cancel_all_orders(self, symbol: str) -> None:
        try:
            await self._signed_request("DELETE", "/fapi/v1/allOpenOrders", {"symbol": symbol})
        except Exception as exc:
            logger.warning("[%s] Failed to cancel all orders: %s", self.name, exc)

    async def cancel_order(self, symbol: str, order_id: int) -> None:
        try:
            await self._signed_request("DELETE", "/fapi/v1/order", {"symbol": symbol, "orderId": order_id})
        except Exception as exc:
            logger.warning("[%s] Failed to cancel order %s: %s", self.name, order_id, exc)

    async def place_order(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return await self._signed_request("POST", "/fapi/v1/order", params)

    async def get_order(self, symbol: str, order_id: int) -> Dict[str, Any]:
        return await self._signed_request("GET", "/fapi/v1/order", {"symbol": symbol, "orderId": order_id})

    async def get_user_trades(self, symbol: str, from_id: Optional[int] = None, limit: int = 100) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"symbol": symbol, "limit": limit}
        if from_id is not None:
            params["fromId"] = from_id
        return await self._signed_request("GET", "/fapi/v1/userTrades", params)

    async def get_mark_price(self, symbol: str) -> float:
        data = await self._public_request("GET", "/fapi/v1/premiumIndex", {"symbol": symbol})
        return float(data.get("markPrice", 0.0))

    async def get_book_ticker(self, symbol: str) -> Tuple[float, float]:
        data = await self._public_request("GET", "/fapi/v1/ticker/bookTicker", {"symbol": symbol})
        bid = float(data.get("bidPrice", 0.0))
        ask = float(data.get("askPrice", 0.0))
        return bid, ask

    async def get_depth(self, symbol: str, limit: int = 5) -> Optional[Dict[str, Any]]:
        try:
            data = await self._public_request("GET", "/fapi/v1/depth", {"symbol": symbol, "limit": limit})
            return data if isinstance(data, dict) else None
        except Exception as exc:
            logger.warning("Failed to fetch depth for %s: %s", symbol, exc)
            return None
async def refresh_account_state(state: AccountState, mark_price: float) -> None:
    await integrate_time_metrics(state)
    if not state.needs_refresh():
        return
    try:
        account_info = await state.client.get_account()
        positions = account_info.get("positions", [])
        position_snapshot = PositionSnapshot()
        for item in positions:
            if item.get("symbol") != SYMBOL:
                continue
            qty = float(item.get("positionAmt", 0.0))
            entry_price = float(item.get("entryPrice", 0.0))
            unrealized = float(item.get("unRealizedProfit", 0.0))
            position_snapshot = PositionSnapshot(
                quantity=qty,
                entry_price=entry_price,
                mark_price=mark_price,
                unrealized_pnl=unrealized,
                notional=qty * mark_price,  # Fixed: preserve sign for correct net exposure calculation
            )
            break
        state.position = position_snapshot

        wallet_balance = float(account_info.get("totalWalletBalance", 0.0))
        available_balance = float(account_info.get("availableBalance", 0.0))
        initial_margin = float(account_info.get("totalInitialMargin", 0.0))
        position_margin = float(account_info.get("totalPositionInitialMargin", 0.0))
        total_margin_balance = float(account_info.get("totalMarginBalance", wallet_balance))
        total_unrealized = float(account_info.get("totalUnrealizedProfit", 0.0))
        floating_loss = max(0.0, -total_unrealized)
        margin_ratio = floating_loss / total_margin_balance if total_margin_balance > 0 else 0.0

        collateral_values: Dict[str, float] = {}
        for asset in account_info.get("assets", []):
            asset_code = asset.get("asset")
            if not asset_code:
                continue
            margin_balance = float(asset.get("marginBalance", 0.0))
            if margin_balance <= 0:
                continue
            collateral_values[asset_code.upper()] = margin_balance

        state.metrics = AccountMetrics(
            wallet_balance=wallet_balance,
            available_balance=available_balance,
            initial_margin=initial_margin,
            position_margin=position_margin,
            margin_ratio=margin_ratio,
            collateral_values=collateral_values,
        )
        state.last_account_refresh = time.time()

        old_maker_volume = state.points.maker_volume
        await update_trades_points(state)
        new_maker_volume = state.points.maker_volume

        # Update fill statistics when new trades detected
        if new_maker_volume > old_maker_volume:
            state.orders_filled += 1
            # Reset artificially high initial fill rate
            if state.orders_filled == 1 and state.orders_placed == 0:
                state.orders_placed = 1  # Assume at least one order was placed to get this fill
            logger.debug("[%s] Fill detected! New volume: %.2f (+%.2f), fill rate: %.1f%%",
                       state.config.identifier, new_maker_volume, new_maker_volume - old_maker_volume,
                       (state.orders_filled / max(state.orders_placed, 1)) * 100)
    except Exception as exc:
        logger.error("[%s] Failed to refresh account state: %s", state.config.identifier, exc)


async def integrate_time_metrics(state: AccountState) -> None:
    now = time.time()
    delta = max(0.0, now - state.last_time_integrated)
    state.last_time_integrated = now
    if delta <= 0:
        return

    # Weekly reset check before accumulation
    maybe_reset_week(state)

    hours = delta / 3600.0
    state.points.holding_usd_hours += state.position.notional * hours

    collateral_value = 0.0
    for asset, value in state.metrics.collateral_values.items():
        if asset.upper() in state.config.prefer_collateral_assets:
            collateral_value += value
    state.points.collateral_usd_hours += collateral_value * hours


def maybe_reset_week(state: AccountState) -> None:
    current = current_week_start()
    if current > state.week_start:
        state.reset_for_new_week(current)


async def update_trades_points(state: AccountState) -> None:
    maybe_reset_week(state)
    from_id = state.last_trade_id + 1 if state.last_trade_id is not None else None
    try:
        trades = await state.client.get_user_trades(SYMBOL, from_id=from_id, limit=200)
    except Exception as exc:
        logger.warning("[%s] Failed to fetch user trades: %s", state.config.identifier, exc)
        return

    if not trades:
        return

    trades_sorted = sorted(trades, key=lambda x: int(x.get("id", 0)))
    state.last_trade_id = int(trades_sorted[-1].get("id", state.last_trade_id or 0))

    for trade in trades_sorted:
        try:
            quote_qty = float(trade.get("quoteQty") or trade.get("cumQuote", 0.0))
            maker_flag = trade.get("maker")
            if isinstance(maker_flag, str):
                maker_flag = maker_flag.lower() == "true"
            maker_flag = bool(maker_flag)
            if maker_flag:
                state.points.maker_volume += quote_qty
            else:
                state.points.taker_volume += quote_qty
            realized = float(trade.get("realizedPnl", 0.0) or 0.0)
            state.points.realized_pnl += realized
        except Exception as exc:
            logger.debug("[%s] Skipped trade parsing error: %s", state.config.identifier, exc)


def compute_points(state: AccountState) -> Dict[str, float]:
    weekly_volume = state.points.weekly_volume()
    holding_points = min(state.points.holding_usd_hours * HOLDING_RATE_PER_USD_HOUR, 2 * weekly_volume)
    collateral_points = min(state.points.collateral_usd_hours * COLLATERAL_RATE_PER_USD_HOUR, 2 * weekly_volume)
    maker_points = state.points.maker_volume
    taker_points = state.points.taker_volume * 2
    pnl_points = state.points.realized_pnl * PNL_POINTS_RATE
    total_raw = maker_points + taker_points + holding_points + collateral_points + pnl_points
    total = total_raw * state.config.team_boost
    return {
        "maker_points": maker_points,
        "taker_points": taker_points,
        "holding_points": holding_points,
        "collateral_points": collateral_points,
        "pnl_points": pnl_points,
        "weekly_volume": weekly_volume,
        "team_boost": state.config.team_boost,
        "total": total,
    }
def quantize_price(spec: SymbolSpec, price: float) -> float:
    price = max(spec.tick_size, round(price / spec.tick_size) * spec.tick_size)
    return round(price, spec.price_precision)


def quantize_qty(spec: SymbolSpec, qty: float) -> float:
    # Use proper rounding instead of floor - this was killing our order sizes!
    steps = round(qty / spec.qty_step)
    qty = steps * spec.qty_step
    if qty < spec.min_qty:
        qty = spec.min_qty
    return round(qty, spec.qty_precision)


def qty_from_notional(spec: SymbolSpec, notional: float, price: float) -> float:
    if price <= 0:
        raise ValueError("Price must be positive to compute quantity")
    qty = notional / price
    qty = quantize_qty(spec, qty)
    if price * qty < spec.min_notional:
        qty = quantize_qty(spec, spec.min_notional / price + spec.qty_step)
    return qty


def has_margin_buffer(state: AccountState, additional_notional: float) -> bool:
    wallet = state.metrics.wallet_balance
    available = state.metrics.available_balance
    required_margin = additional_notional / max(1, state.config.leverage)
    projected_available = available - required_margin
    threshold = wallet * state.config.margin_buffer
    return projected_available >= threshold
class QuoteSource:
    def __init__(self, name: str) -> None:
        self.name = name
        self.bid = 0.0
        self.ask = 0.0
        self.bid_qty = 0.0
        self.ask_qty = 0.0
        self.ts = 0.0
        self.summary = ''

    def update(self, bid: float, ask: float, bid_qty: float, ask_qty: float, ts: float, summary: str) -> None:
        self.bid = bid
        self.ask = ask
        self.bid_qty = bid_qty
        self.ask_qty = ask_qty
        self.ts = ts
        self.summary = summary

class TradeCoordinator:
    def __init__(
        self,
        accounts: List[AccountConfig],
        loop_interval: float = DEFAULT_LOOP_INTERVAL,
    ):
        self.accounts_config = accounts
        self.loop_interval = loop_interval
        self.session: Optional[aiohttp.ClientSession] = None
        self.accounts: List[AccountState] = []
        self.symbol_spec: Optional[SymbolSpec] = None
        self.stop_event = asyncio.Event()
        self.iteration = 0
        self._log_points_interval = 10
        self.last_snapshot_time = 0.0
        self.snapshot_error_logged = False
        self.quote_sources = {
            "ws": QuoteSource("ws"),
            "depth": QuoteSource("depth"),
            "rest": QuoteSource("rest"),
        }
        self.aster_rest_task: Optional[asyncio.Task] = None

    def _estimate_max_notional(self, state: AccountState) -> float:
        # Use the same conservative calculation as _calculate_risk_capacity
        return _calculate_risk_capacity(state)

    def adjust_targets_for_current_state(self) -> None:
        try:
            buy_state = self.get_account_by_role("buy")
            sell_state = self.get_account_by_role("sell")
        except RuntimeError as exc:
            logger.warning("Target adjustment skipped: %s", exc)
            return

        max_buy_notional = self._estimate_max_notional(buy_state)
        max_sell_notional = self._estimate_max_notional(sell_state)

        adjusted = False

        if max_buy_notional <= 0:
            buy_state.config.base_target_notional = 0.0
            buy_state.config.apply_role_sign()
            adjusted = True
            buy_state.config.max_notional = 0.0
        elif buy_state.config.base_target_notional > max_buy_notional:
            buy_state.config.base_target_notional = max_buy_notional
            buy_state.config.apply_role_sign()
            adjusted = True
            buy_state.config.max_notional = max_buy_notional
        else:
            buy_state.config.max_notional = max_buy_notional

        if max_sell_notional <= 0:
            sell_state.config.base_target_notional = 0.0
            sell_state.config.apply_role_sign()
            adjusted = True
            sell_state.config.max_notional = 0.0
        elif sell_state.config.base_target_notional > max_sell_notional:
            sell_state.config.base_target_notional = max_sell_notional
            sell_state.config.apply_role_sign()
            adjusted = True
            sell_state.config.max_notional = max_sell_notional
        else:
            sell_state.config.max_notional = max_sell_notional

        # Remove the logic that increases targets to match current positions
        # This was causing imbalanced targets when one side had large existing positions
        # Instead, let the balanced targets from calculate_optimal_targets() stand

        # Only log current position status for monitoring
        buy_notional = abs(buy_state.position.notional)
        sell_notional = abs(sell_state.position.notional)

        if buy_notional > buy_state.config.base_target_notional * 1.5:
            logger.info("[%s] Position %.2f significantly exceeds balanced target %.2f",
                       buy_state.config.identifier, buy_notional, buy_state.config.base_target_notional)

        if sell_notional > sell_state.config.base_target_notional * 1.5:
            logger.info("[%s] Position %.2f significantly exceeds balanced target %.2f",
                       sell_state.config.identifier, sell_notional, sell_state.config.base_target_notional)

        if adjusted:
            logger.info(
                "Adjusted targets | %s -> %.2f, %s -> %.2f",
                buy_state.config.identifier,
                buy_state.config.target_hold_notional,
                sell_state.config.identifier,
                sell_state.config.target_hold_notional,
            )

    async def initialize(self) -> None:
        if self.accounts:
            return
        if self.session is None:
            timeout = aiohttp.ClientTimeout(total=10)
            self.session = aiohttp.ClientSession(timeout=timeout)

        # Redis no longer needed - simplified architecture

        for cfg in self.accounts_config:
            api_key, api_secret = await load_aster_credentials(cfg.identifier)
            client = AsterFuturesClient(api_key, api_secret, self.session, cfg.identifier)
            if self.symbol_spec is None:
                self.symbol_spec = await client.get_symbol_spec()
            await client.set_margin_type(SYMBOL)
            await client.set_leverage(SYMBOL, cfg.leverage)
            state = AccountState(config=cfg, client=client, symbol_spec=self.symbol_spec)
            self.accounts.append(state)

        if not any(asset in TRADING_CONFIG.collateral_preferred_assets for acc in self.accounts for asset in acc.config.prefer_collateral_assets):
            logger.warning("No account configured with preferred collateral assets; update configs if needed")

        if self.aster_rest_task is None:
            self.aster_rest_task = asyncio.create_task(self._aster_rest_loop())

    async def shutdown(self) -> None:
        if self.aster_rest_task:
            self.aster_rest_task.cancel()
            try:
                await self.aster_rest_task
            except asyncio.CancelledError:
                pass
            self.aster_rest_task = None
        for state in self.accounts:
            try:
                await state.client.cancel_all_orders(SYMBOL)
            except Exception as exc:
                logger.warning("[%s] Failed to cancel open orders during shutdown: %s", state.config.identifier, exc)
        if self.session:
            await self.session.close()
            self.session = None

    async def run(self) -> None:
        if not self.accounts:
            await self.initialize()
        self.install_signal_handlers()
        try:
            while not self.stop_event.is_set():
                tick_start = time.time()
                self.iteration += 1

                mark_price = await self.accounts[0].client.get_mark_price(SYMBOL)

                quote = self.quote_sources["ws"]  # Now updated by REST polling
                now_ts = time.time()

                # Check if REST polling data is fresh
                if now_ts - quote.ts > 5.0 or quote.bid <= 0 or quote.ask <= 0:
                    logger.warning("Stale quote data, age=%.1fs", now_ts - quote.ts)
                    await async_sleep(1.0)
                    continue

                bid, ask = quote.bid, quote.ask

                for state in self.accounts:
                    await refresh_account_state(state, mark_price)

                # Update all accounts simultaneously - no pausing for alignment
                for state in self.accounts:
                    await self.update_account_quote(state, mark_price, bid, ask)

                now = time.time()
                if now - self.last_snapshot_time >= SNAPSHOT_INTERVAL:
                    await self.capture_snapshot(datetime.now(UTC), mark_price, bid, ask)
                    self.last_snapshot_time = now

                    # Periodically recalculate optimal targets based on current account states
                    if self.iteration % 20 == 0:  # Every 20th snapshot (~every 5 minutes)
                        calculate_optimal_targets(self.accounts)
                        self.adjust_targets_for_current_state()

                    # Log portfolio balance status
                    portfolio_balance = self.check_portfolio_balance(mark_price)
                    if portfolio_balance["needs_rebalance"]:
                        logger.info("Portfolio imbalance detected: long=%.2f short=%.2f net=%.2f ratio=%.1f%%",
                                  portfolio_balance["long_notional"],
                                  portfolio_balance["short_notional"],
                                  portfolio_balance["net_notional"],
                                  portfolio_balance["balance_ratio"] * 100)

                if self.iteration % self._log_points_interval == 0:
                    self.log_points_summary()

                elapsed = time.time() - tick_start
                # Add randomness to sleep time to avoid mechanical timing patterns
                import random
                base_sleep = max(0.0, self.loop_interval - elapsed)
                random_variance = random.uniform(-0.2, 0.3)  # -20% to +30% variance
                sleep_time = max(0.1, base_sleep + random_variance)  # Minimum 0.1s sleep
                await async_sleep(sleep_time)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.error("Trade loop terminated due to error: %s", exc)
        finally:
            await self.shutdown()

    def install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.stop_event.set)
            except NotImplementedError:
                signal.signal(sig, lambda *_: self.stop_event.set())

    def log_points_summary(self) -> None:
        summaries = []
        total_points = 0.0
        total_volume = 0.0
        for state in self.accounts:
            summary = compute_points(state)
            total_points += summary['total']
            total_volume += summary['weekly_volume']

            # Calculate efficiency metrics
            points_per_1k_volume = (summary['total'] / max(summary['weekly_volume'], 1)) * 1000
            maker_efficiency = (summary['maker_points'] / max(state.points.maker_volume, 1)) * 100

            summaries.append(
                f"{state.config.identifier}: total={summary['total']:.1f}, volume={summary['weekly_volume']:.1f}, "
                f"maker={summary['maker_points']:.1f}({maker_efficiency:.1f}%), taker={summary['taker_points']:.1f}, "
                f"hold={summary['holding_points']:.1f}, collat={summary['collateral_points']:.1f}, "
                f"eff={points_per_1k_volume:.1f}pts/k"
            )

        # Calculate combined metrics
        combined_efficiency = (total_points / max(total_volume, 1)) * 1000
        logger.info("Weekly points snapshot | %s | COMBINED: total=%.1f, volume=%.1f, eff=%.1f pts/k",
                   " | ".join(summaries), total_points, total_volume, combined_efficiency)

    def get_account_by_role(self, role: str) -> AccountState:
        for state in self.accounts:
            if state.config.role.lower() == role.lower():
                return state
        raise RuntimeError(f"Account with role {role} not configured")
    def check_portfolio_balance(self, mark_price: float) -> Dict[str, float]:
        """Check overall portfolio balance and suggest rebalancing"""
        total_long_notional = 0.0
        total_short_notional = 0.0

        for state in self.accounts:
            if state.position.quantity > 0:
                total_long_notional += abs(state.position.notional)
            elif state.position.quantity < 0:
                total_short_notional += abs(state.position.notional)

        net_notional = total_long_notional - total_short_notional
        total_notional = total_long_notional + total_short_notional

        balance_ratio = abs(net_notional) / max(total_notional, 1.0)

        return {
            "long_notional": total_long_notional,
            "short_notional": total_short_notional,
            "net_notional": net_notional,
            "balance_ratio": balance_ratio,
            "needs_rebalance": balance_ratio > 0.2  # >20% imbalance
        }

    async def _aster_rest_loop(self) -> None:
        logger.info("Starting Aster REST depth polling (2s interval)")

        while not self.stop_event.is_set():
            try:
                # Use first account's client to fetch depth
                depth = await self.accounts[0].client.get_depth(SYMBOL, limit=5)
                if isinstance(depth, dict):
                    try:
                        bids = depth.get("bids", [])
                        asks = depth.get("asks", [])

                        if bids and asks:
                            bid = float(bids[0][0])
                            bid_qty = float(bids[0][1])
                            ask = float(asks[0][0])
                            ask_qty = float(asks[0][1])

                            # Data validation
                            spread_check = ask - bid
                            if spread_check < 10.0:  # Tighter validation for REST
                                self.quote_sources["ws"].update(
                                    bid=bid,
                                    ask=ask,
                                    bid_qty=bid_qty,
                                    ask_qty=ask_qty,
                                    ts=time.time(),
                                    summary='REST poll'
                                )
                            else:
                                logger.warning("REST depth rejected: spread=%.2f USDT", spread_check)
                    except (IndexError, ValueError, TypeError) as exc:
                        logger.warning("Failed to parse REST depth: %s", exc)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("Aster REST loop error: %s", exc)

            await async_sleep(2.0)  # 2-second polling interval

    async def capture_snapshot(
        self,
        captured_at: datetime,
        mark_price: float,
        best_bid: float,
        best_ask: float,
    ) -> None:

        entries = []
        for state in self.accounts:
            metrics = state.metrics
            position = state.position
            target_qty = state.config.target_hold_notional / max(mark_price, 1e-8)
            diff_qty = target_qty - position.quantity
            entries.append(
                (
                    state.config.identifier,
                    state.config.role.lower(),
                    _round_val(state.config.target_hold_notional, 2),
                    _round_val(position.notional, 2),
                    _round_val(diff_qty * mark_price, 2),
                    _round_val(metrics.wallet_balance, 4),
                    _round_val(metrics.available_balance, 4),
                    state.config.max_notional,
                )
            )

        ws_quote = self.quote_sources["ws"]
        snapshot_msg = [
            f"aster_bid={_round_val(best_bid, 4)}",
            f"aster_ask={_round_val(best_ask, 4)}",
            f"aster_bid_qty={_round_val(ws_quote.bid_qty, 4)}",
            f"aster_ask_qty={_round_val(ws_quote.ask_qty, 4)}",
            f"aster_ts={_round_val(ws_quote.ts, 4)}",
            f"spread= { _round_val((best_ask - best_bid) if best_bid else 0, 6)}",
        ]
        for entry in entries:
            identifier, role, target_notional, current_notional, diff_notional, wallet, available, max_notional = entry
            snapshot_msg.append(
                f"{identifier}({role}) target={target_notional} current={current_notional} diff={diff_notional} "
                f"wallet={wallet} avail={available} max={_round_val(max_notional, 2)}"
            )

        logger.info("Snapshot %s", " | ".join(str(part) for part in snapshot_msg))


    # ========== Linus-style function decomposition ==========
    # Break down the 355-line monster into single-purpose functions

    def _should_skip_update(self, state: AccountState, now: float) -> bool:
        """Simple rate limiting check."""
        return now - state.last_quote_update < 1.0

    def _calculate_position_metrics(self, state: AccountState, mark_price: float) -> Dict[str, float]:
        """Calculate all position-related metrics in one place."""
        target_qty = state.config.target_hold_notional / max(mark_price, 1e-8)
        current_qty = state.position.quantity
        tolerance_qty = max(state.symbol_spec.min_qty, abs(target_qty) * 0.02)
        diff_qty = target_qty - current_qty

        return {
            'target_qty': target_qty,
            'current_qty': current_qty,
            'tolerance_qty': tolerance_qty,
            'diff_qty': diff_qty
        }

    def _calculate_trading_decision(self, state: AccountState, position_metrics: Dict, portfolio_balance: Dict) -> str:
        """Decide what type of trading to do - eliminates complex conditionals."""
        individual_balanced = abs(position_metrics['diff_qty']) < position_metrics['tolerance_qty']
        portfolio_imbalanced = portfolio_balance["needs_rebalance"]

        if individual_balanced and not portfolio_imbalanced and TRADING_CONFIG.continuous_trading_mode:
            return "ping_pong"
        elif individual_balanced and portfolio_imbalanced:
            return "rebalance"
        elif not individual_balanced:
            return "normal"
        else:
            return "skip"

    def _calculate_order_size(self, state: AccountState, decision_type: str, available_after_buffer: float, leverage: int) -> float:
        """Calculate order size based on decision type - no more duplicated logic."""
        config = TRADING_CONFIG

        if decision_type == "ping_pong":
            base_notional = available_after_buffer * leverage * config.ping_pong_percentage
            return max(base_notional, config.min_ping_pong_notional)
        elif decision_type in ["normal", "rebalance"]:
            base_notional = available_after_buffer * leverage * config.order_size_percentage
            return max(base_notional, config.min_order_notional)
        else:
            return 0.0

    async def update_account_quote(
        self,
        state: AccountState,
        mark_price: float,
        aster_bid: float,
        aster_ask: float,
    ) -> None:
        """Simplified main trading logic - uses decomposed functions."""
        now = time.time()

        # Early exit for rate limiting
        if self._should_skip_update(state, now):
            return

        # Calculate all position metrics
        position_metrics = self._calculate_position_metrics(state, mark_price)

        # Check portfolio balance
        portfolio_balance = self.check_portfolio_balance(mark_price)

        # Decide what type of trading to do
        decision_type = self._calculate_trading_decision(state, position_metrics, portfolio_balance)

        if decision_type == "skip":
            return

        # Calculate margin constraints
        leverage = max(1, state.config.leverage)
        buffer_threshold = state.metrics.wallet_balance * state.config.margin_buffer
        available_after_buffer = max(0.0, state.metrics.available_balance - buffer_threshold)

        # Calculate order size based on decision
        order_notional = self._calculate_order_size(state, decision_type, available_after_buffer, leverage)

        if order_notional <= 0:
            return

        # The rest of the original logic continues here...

        spec = state.symbol_spec
        best_bid = aster_bid if aster_bid > 0 else mark_price
        best_ask = aster_ask if aster_ask > 0 else mark_price

        target_qty = state.config.target_hold_notional / max(mark_price, 1e-8)
        current_qty = state.position.quantity

        tolerance_qty = max(spec.min_qty, abs(target_qty) * 0.02)
        diff_qty = target_qty - current_qty

        # Calculate margin constraints early for use in all scenarios
        leverage = max(1, state.config.leverage)
        buffer_threshold = state.metrics.wallet_balance * state.config.margin_buffer
        available_after_buffer = max(0.0, state.metrics.available_balance - buffer_threshold)

        # Check portfolio balance to enable active rebalancing
        portfolio_balance = self.check_portfolio_balance(mark_price)

        # If within individual tolerance but portfolio imbalanced, allow rebalancing trades
        individual_balanced = abs(diff_qty) < tolerance_qty
        portfolio_imbalanced = portfolio_balance["needs_rebalance"]

        if individual_balanced and not portfolio_imbalanced and CONTINUOUS_TRADING_MODE:
            # Continue trading for volume even when balanced
            if now - state.last_quote_update > 10.0:  # Ping-pong every 10 seconds
                # Clean ping-pong sizing: use 3% of available margin for steady volume
                # This gives more impactful trades while leaving capacity for normal operations
                ping_pong_notional = available_after_buffer * leverage * 0.03

                # Ensure ping-pong orders are at least $200 for meaningful impact, if affordable
                # $200 gives us a chance to reach 0.002 BTC at current prices
                min_ping_pong_notional = 200.0
                max_affordable_ping_pong = available_after_buffer * leverage * 0.6  # 60% for ping-pong

                if max_affordable_ping_pong >= min_ping_pong_notional:
                    effective_ping_pong_notional = max(ping_pong_notional, min_ping_pong_notional)
                else:
                    # Small account: use percentage-based sizing
                    effective_ping_pong_notional = ping_pong_notional

                raw_ping_pong_qty = effective_ping_pong_notional / max(mark_price, spec.tick_size)
                ping_pong_size = quantize_qty(spec, raw_ping_pong_qty)

                logger.debug("[%s] Ping-pong calc: available_buffer=%.2f, leverage=%.1f, base_notional=%.2f, min_notional=%.2f, effective_notional=%.2f, raw_qty=%.6f, final_qty=%.6f, can_afford_min=%s",
                           state.config.identifier, available_after_buffer, leverage, ping_pong_notional,
                           min_ping_pong_notional, effective_ping_pong_notional, raw_ping_pong_qty, ping_pong_size,
                           max_affordable_ping_pong >= min_ping_pong_notional)

                # Simple alternating pattern based on time
                cycle = int(now / 20) % 2  # 20-second cycles
                direction_multiplier = 1 if cycle == 0 else -1

                if state.config.role.lower() == "buy":
                    diff_qty = ping_pong_size * direction_multiplier
                else:
                    diff_qty = ping_pong_size * -direction_multiplier

                logger.debug(
                    "[%s] Continuous volume trade: ping_pong=%.6f, direction=%d | Portfolio balanced",
                    state.config.identifier,
                    ping_pong_size,
                    direction_multiplier
                )
            else:
                # Too recent, skip this update
                return
        elif individual_balanced and not portfolio_imbalanced:
            # Original balanced behavior - stop trading
            logger.debug(
                "[%s] Within tolerance; target_qty=%.6f current_qty=%.6f tolerance=%.6f | Portfolio balanced",
                state.config.identifier,
                target_qty,
                current_qty,
                tolerance_qty,
            )
            await self.cancel_active_orders(state)
            state.last_quote_update = now
            return
        elif individual_balanced and portfolio_imbalanced:
            # Portfolio needs rebalancing - more aggressive rebalancing
            net_notional = portfolio_balance["net_notional"]
            if (net_notional > 0 and state.config.role.lower() == "sell") or \
               (net_notional < 0 and state.config.role.lower() == "buy"):
                # More aggressive rebalancing for faster convergence
                rebalance_qty = min(tolerance_qty * 5, abs(net_notional) / (1.5 * mark_price))  # Less conservative
                diff_qty = rebalance_qty if state.config.role.lower() == "sell" else -rebalance_qty
                logger.debug(
                    "[%s] Aggressive portfolio rebalancing: net_notional=%.2f, rebalance_qty=%.6f",
                    state.config.identifier,
                    net_notional,
                    rebalance_qty
                )
            else:
                await self.cancel_active_orders(state)
                state.last_quote_update = now
                return

        order_side = "BUY" if diff_qty > 0 else "SELL"

        # Determine if this is a reducing trade
        expected_sign = 1 if target_qty > 0 else -1 if target_qty < 0 else 0
        actual_sign = 1 if current_qty > 0 else -1 if current_qty < 0 else 0
        is_misaligned = expected_sign != actual_sign and expected_sign != 0

        # Check if this order reduces position towards target
        is_reducing = ((current_qty > target_qty and order_side == "SELL") or
                      (current_qty < target_qty and order_side == "BUY"))

        # Calculate maximum possible order size based on risk constraints
        leverage = max(1, state.config.leverage)
        buffer_threshold = state.metrics.wallet_balance * state.config.margin_buffer
        available_after_buffer = max(0.0, state.metrics.available_balance - buffer_threshold)
        max_affordable_notional = available_after_buffer * leverage

        # Use target difference but cap at maximum affordable
        target_notional = abs(diff_qty * mark_price)
        if not is_reducing:
            # For position building: use aggressive sizing for faster accumulation
            max_order_notional = min(target_notional, max_affordable_notional * 0.8)  # 80% for faster building
        else:
            # For position reducing: can be more aggressive
            max_order_notional = min(target_notional, max_affordable_notional * 0.9)

        # Simple and clean: order size based on risk capacity, no special cases
        estimated_price = (best_bid + best_ask) / 2 if best_bid > 0 and best_ask > 0 else mark_price

        # Order size based on available margin - with capital-aware limits
        base_order_notional = available_after_buffer * leverage * TRADING_CONFIG.order_size_percentage

        # Apply maximum order cap to prevent market impact with large accounts
        config_max_order = getattr(TRADING_CONFIG, 'max_order_notional', 1500.0)
        capped_base_notional = min(base_order_notional, config_max_order)
        risk_adjusted_notional = min(max_order_notional, capped_base_notional)

        # Calculate order quantity ensuring meaningful size
        raw_qty = risk_adjusted_notional / max(estimated_price, spec.tick_size)

        # Use updated minimum order size for better queue position
        min_viable_notional = TRADING_CONFIG.min_order_notional  # $300 for better impact
        max_affordable_base = available_after_buffer * leverage * 0.8  # 80% safety margin

        if max_affordable_base >= min_viable_notional:
            min_viable_qty = min_viable_notional / max(estimated_price, spec.tick_size)
            pre_quantized_qty = max(raw_qty, min_viable_qty)
        else:
            # Small account: just use what we calculated based on percentage
            pre_quantized_qty = raw_qty
        order_qty = quantize_qty(spec, pre_quantized_qty)

        logger.debug("[%s] Order calc: available_buffer=%.2f, leverage=%.1f, base_notional=%.2f, risk_adj_notional=%.2f, raw_qty=%.6f, pre_quant=%.6f, final_qty=%.6f, can_afford_min=%s",
                   state.config.identifier, available_after_buffer, leverage, base_order_notional,
                   risk_adjusted_notional, raw_qty, pre_quantized_qty, order_qty,
                   max_affordable_base >= min_viable_notional)

        logger.debug("[%s] Max order sizing: target_notional=%.2f, max_affordable=%.2f, order_notional=%.2f, order_qty=%.6f",
                   state.config.identifier, target_notional, max_affordable_notional,
                   order_qty * estimated_price, order_qty)

        if is_misaligned:
            if state.misaligned_since == 0.0:
                state.misaligned_since = now
        else:
            state.misaligned_since = 0.0

        increasing_exposure = abs(target_qty) > abs(current_qty) and not is_misaligned
        is_reducing = not increasing_exposure

        urgency_ticks = 0
        if is_misaligned and state.misaligned_since > 0.0:
            urgency_ticks = min(5, int((now - state.misaligned_since) // REPRICE_SECONDS) + 1)

        base_offset_ticks = max(1, state.config.price_offset_ticks or 1)

        # Dynamic adjustment based on fill rate
        fill_rate = state.orders_filled / max(state.orders_placed, 1)
        aggression_adjustment = 0
        if state.orders_placed >= 10:  # Only adjust after sufficient sample size
            if fill_rate < FILL_RATE_TARGET:
                # If fill rate too low, be more aggressive (closer to market)
                aggression_adjustment = -int(PRICE_AGGRESSION_ADJUSTMENT * (FILL_RATE_TARGET - fill_rate) / FILL_RATE_TARGET)
                logger.debug("[%s] Fill rate %.1f%% below target, increasing aggression by %d ticks",
                           state.config.identifier, fill_rate * 100, -aggression_adjustment)

        offset_ticks = max(0, base_offset_ticks + aggression_adjustment)

        # Check for extreme queue situation
        extreme_queue = False
        ws_quote = self.quote_sources["ws"]
        if order_side == "BUY":
            queue_qty = ws_quote.bid_qty
            queue_ratio = queue_qty / max(order_qty, spec.min_qty) if ws_quote.bid_qty > 0 else 0
            extreme_queue = queue_ratio > EXTREME_QUEUE_THRESHOLD
        else:
            queue_qty = ws_quote.ask_qty
            queue_ratio = queue_qty / max(order_qty, spec.min_qty) if ws_quote.ask_qty > 0 else 0
            extreme_queue = queue_ratio > EXTREME_QUEUE_THRESHOLD

        # More aggressive extreme queue handling for better fills
        if extreme_queue and not is_reducing:
            # Much more aggressive pricing to ensure fills
            logger.debug("[%s] Extreme queue detected (ratio=%.1f), using very aggressive pricing",
                       state.config.identifier, queue_ratio)
            if order_side == "BUY":
                # Bid much closer to ask price - willing to pay more spread
                aggressive_ticks = min(offset_ticks + 5, int((best_ask - best_bid) / spec.tick_size) - 1)
                price = best_bid + aggressive_ticks * spec.tick_size
                # Even more aggressive: go up to 80% of spread
                max_aggressive_price = best_bid + 0.8 * (best_ask - best_bid)
                price = min(price, max_aggressive_price, best_ask - spec.tick_size)
            else:
                # Ask much closer to bid price
                aggressive_ticks = min(offset_ticks + 5, int((best_ask - best_bid) / spec.tick_size) - 1)
                price = best_ask - aggressive_ticks * spec.tick_size
                # Go down to 20% of spread
                min_aggressive_price = best_ask - 0.8 * (best_ask - best_bid)
                price = max(price, min_aggressive_price, best_bid + spec.tick_size)
        else:
            # Normal pricing
            if order_side == "BUY":
                ticks = offset_ticks + (urgency_ticks if is_reducing else 0)
                price = best_bid + ticks * spec.tick_size
                price = min(price, best_ask - spec.tick_size)
            else:
                ticks = offset_ticks + (urgency_ticks if is_reducing else 0)
                price = best_ask - ticks * spec.tick_size
                price = max(price, best_bid + spec.tick_size)

        # Price validation against mark price (simple sanity check)
        if best_bid > 0 and best_ask > 0:
            deviation = abs(price - mark_price) / mark_price
            if deviation > 0.10:  # 10% extreme deviation from mark price
                logger.warning("[%s] Extreme price deviation %.2f%% from mark price, using mark price",
                             state.config.identifier, deviation * 100)
                price = mark_price

        reference_price = mark_price if mark_price > 0 else (best_bid + best_ask) / 2
        price = max(spec.tick_size, price if price > 0 else reference_price)
        if order_side == "BUY" and price >= best_ask:
            price = best_ask - spec.tick_size

        if order_side == "SELL" and price <= best_bid:
            price = best_bid + spec.tick_size

        price = quantize_price(spec, price)

        if order_qty < spec.min_qty or price <= 0:
            await self.cancel_active_orders(state)
            state.last_quote_update = now
            return

        if (
            state.active_order_id
            and abs(state.active_order_price - price) <= spec.tick_size / 2
            and abs(state.active_order_qty - order_qty) <= spec.qty_step / 2
        ):
            if not (is_reducing and state.active_order_ts and now - state.active_order_ts >= REPRICE_SECONDS):
                return

        if not is_reducing:
            ws_quote = self.quote_sources["ws"]
            queue_qty = ws_quote.bid_qty if order_side == "BUY" else ws_quote.ask_qty
            if queue_qty > 0:
                ratio = queue_qty / max(order_qty, spec.min_qty)
                queue_notional = queue_qty * price
                max_notional_cap = state.config.max_notional if state.config.max_notional > 0 else QUEUE_NOTIONAL_THRESHOLD

                # Dynamic queue threshold: reduce threshold when fill rate is low
                dynamic_ratio_threshold = QUEUE_DOMINANCE_RATIO
                fill_rate = state.orders_filled / max(state.orders_placed, 1)
                if state.orders_placed >= 5 and fill_rate < FILL_RATE_TARGET:
                    # Be more aggressive when fill rate is low
                    dynamic_ratio_threshold *= (1 + (FILL_RATE_TARGET - fill_rate) * 2)
                    logger.debug("[%s] Low fill rate %.1f%%, increasing queue tolerance to %.1f",
                               state.config.identifier, fill_rate * 100, dynamic_ratio_threshold)

                # Skip queue check if using extreme pricing (will pay spread anyway)
                if ratio <= EXTREME_QUEUE_THRESHOLD and (ratio >= dynamic_ratio_threshold or queue_notional >= max(QUEUE_NOTIONAL_THRESHOLD, max_notional_cap)):
                    logger.debug(
                        "[%s] Skip quote; queue heavy (qty=%.6f ratio=%.2f/%.1f notional=%.2f) fill_rate=%.1f%%",
                        state.config.identifier,
                        queue_qty,
                        ratio,
                        dynamic_ratio_threshold,
                        queue_notional,
                        fill_rate * 100
                    )
                    await self.cancel_active_orders(state)
                    state.last_quote_update = now
                    return
                elif ratio > EXTREME_QUEUE_THRESHOLD:
                    logger.debug(
                        "[%s] Extreme queue (ratio=%.1f), will use aggressive pricing to skip",
                        state.config.identifier,
                        ratio
                    )

        # Final validation: ensure order is valid and affordable
        order_notional = order_qty * estimated_price
        if order_qty < spec.min_qty:
            logger.debug("[%s] Order qty %.6f below minimum %.6f, canceling",
                       state.config.identifier, order_qty, spec.min_qty)
            await self.cancel_active_orders(state)
            state.last_quote_update = now
            return
        elif order_notional > available_after_buffer * leverage * 0.9:  # 90% safety check
            logger.warning("[%s] Order notional %.2f exceeds safe margin %.2f, reducing order size",
                         state.config.identifier, order_notional,
                         available_after_buffer * leverage * 0.9)
            # Reduce to safe size while maintaining meaningful volume
            safe_notional = available_after_buffer * leverage * 0.5
            min_safe_notional = 200.0  # Still ensure $200 minimum if affordable

            if available_after_buffer * leverage * 0.6 >= min_safe_notional:
                effective_safe_notional = max(safe_notional, min_safe_notional)
            else:
                # Small account: use what we can afford
                effective_safe_notional = safe_notional

            raw_safe_qty = effective_safe_notional / max(estimated_price, spec.tick_size)
            order_qty = quantize_qty(spec, raw_safe_qty)

            logger.debug("[%s] Safe size calc: safe_notional=%.2f, min_safe=%.2f, effective=%.2f, raw_qty=%.6f, final_qty=%.6f",
                       state.config.identifier, safe_notional, min_safe_notional,
                       effective_safe_notional, raw_safe_qty, order_qty)
            if order_qty < spec.min_qty:
                logger.debug("[%s] Safe order qty %.6f still below minimum, canceling",
                           state.config.identifier, order_qty)
                await self.cancel_active_orders(state)
                state.last_quote_update = now
                return

        # For reducing positions, cap at current position size
        if is_reducing:
            max_reduce_qty = quantize_qty(spec, abs(current_qty))
            if order_qty > max_reduce_qty:
                order_qty = max_reduce_qty
                logger.debug("[%s] Capping reduce order to position size: %.6f",
                           state.config.identifier, order_qty)

        state.last_quote_update = now

        await self.ensure_maker_order(state, order_side, price, order_qty)
    async def ensure_maker_order(self, state: AccountState, side: str, price: float, quantity: float) -> None:
        spec = state.symbol_spec
        desired_price = quantize_price(spec, price)
        desired_qty = quantize_qty(spec, quantity)
        desired_price_str = f"{desired_price:.{spec.price_precision}f}"
        desired_qty_str = f"{desired_qty:.{spec.qty_precision}f}"
        try:
            open_orders = await state.client.get_open_orders(SYMBOL)
        except Exception as exc:
            logger.warning("[%s] Failed to fetch open orders: %s", state.config.identifier, exc)
            open_orders = []

        keep_order: Optional[Dict[str, Any]] = None
        for order in open_orders:
            if order.get("side") != side:
                await self._safe_cancel(state, int(order.get("orderId", 0)))
                continue
            order_price = float(order.get("price", 0.0))
            order_qty = float(order.get("origQty", order.get("quantity", 0.0)))
            if (
                abs(order_price - desired_price) <= spec.tick_size / 2
                and abs(order_qty - desired_qty) <= spec.qty_step / 2
            ):
                keep_order = order
            else:
                await self._safe_cancel(state, int(order.get("orderId", 0)))

        if keep_order:
            state.active_order_id = int(keep_order.get("orderId", 0))
            state.active_order_price = desired_price
            state.active_order_qty = desired_qty
            if state.active_order_ts == 0.0:
                state.active_order_ts = time.time()
            return

        params = {
            "symbol": SYMBOL,
            "side": side,
            "type": "LIMIT",
            "timeInForce": "GTX",
            "quantity": desired_qty_str,
            "price": desired_price_str,
            "newOrderRespType": "RESULT",
        }
        try:
            order = await state.client.place_order(params)
            state.active_order_id = int(order.get("orderId", 0))
            state.active_order_price = desired_price
            state.active_order_qty = desired_qty
            state.active_order_ts = time.time()
            state.orders_placed += 1
            order_notional = desired_price * desired_qty
            logger.debug(
                "[%s] Placed %s order price=%.4f qty=%.6f notional=%.2f | ACTIVE BALANCE | placed=%d, filled=%d, rate=%.1f%%",
                state.config.identifier,
                side,
                desired_price,
                desired_qty,
                order_notional,
                state.orders_placed,
                state.orders_filled,
                (state.orders_filled / max(state.orders_placed, 1)) * 100
            )
        except Exception as exc:
            logger.warning("[%s] Failed to place maker order: %s", state.config.identifier, exc)

    async def _safe_cancel(self, state: AccountState, order_id: int) -> None:
        if not order_id:
            return
        try:
            await state.client.cancel_order(SYMBOL, order_id)
        except Exception as exc:
            logger.debug("[%s] Cancel order %s failed: %s", state.config.identifier, order_id, exc)

    async def cancel_active_orders(self, state: AccountState) -> None:
        try:
            open_orders = await state.client.get_open_orders(SYMBOL)
            for order in open_orders:
                await self._safe_cancel(state, int(order.get("orderId", 0)))
        except Exception as exc:
            logger.warning("[%s] Failed to cancel active orders: %s", state.config.identifier, exc)
        state.active_order_id = None
        state.active_order_price = 0.0
        state.active_order_qty = 0.0
        state.active_order_ts = 0.0
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aster BTCUSDT Genesis Stage 2 trading bot")
    parser.add_argument("--maker-account", default="A", help="Identifier for BUY-side account (places bids)")
    parser.add_argument("--taker-account", default="B", help="Identifier for SELL-side account (places asks)")
    # Removed hard-coded targets and order notional - both calculated dynamically based on account capacity
    parser.add_argument("--price-offset", type=int, default=PRICE_OFFSET_TICKS, help="Tick offset applied when leaning quotes")
    parser.add_argument("--loop-interval", type=float, default=DEFAULT_LOOP_INTERVAL, help="Seconds between trading loop iterations")
    parser.add_argument("--holding-rate", type=float, default=HOLDING_RATE_PER_USD_HOUR, help="Points per USD*hour of holding exposure")
    parser.add_argument("--collateral-rate", type=float, default=COLLATERAL_RATE_PER_USD_HOUR, help="Points per USD*hour of preferred collateral")
    parser.add_argument("--pnl-rate", type=float, default=PNL_POINTS_RATE, help="Points per USD realized PnL")
    parser.add_argument("--points-log-interval", type=int, default=10, help="Loop iterations between points summary logs")
    parser.add_argument("--binance-tolerance-bps", type=float, default=TRADING_CONFIG.binance_price_tolerance * 10_000,
                        help="(Legacy parameter - no longer used after Redis removal)")
    return parser.parse_args()


def apply_runtime_overrides(args: argparse.Namespace) -> None:
    global HOLDING_RATE_PER_USD_HOUR, COLLATERAL_RATE_PER_USD_HOUR, PNL_POINTS_RATE, BINANCE_PRICE_TOLERANCE
    HOLDING_RATE_PER_USD_HOUR = args.holding_rate
    COLLATERAL_RATE_PER_USD_HOUR = args.collateral_rate
    PNL_POINTS_RATE = args.pnl_rate
    BINANCE_PRICE_TOLERANCE = max(0.0, args.binance_tolerance_bps / 10_000)


def build_account_configs(args: argparse.Namespace) -> List[AccountConfig]:
    # Create initial configs with zero targets - will be calculated dynamically
    maker_cfg = AccountConfig(
        identifier=args.maker_account,
        role="buy",
        target_hold_notional=0.0,  # Will be calculated based on account capacity
        maker_order_notional=50.0,  # Use default, but order size will be calculated dynamically
        price_offset_ticks=args.price_offset,
    )
    taker_cfg = AccountConfig(
        identifier=args.taker_account,
        role="sell",
        target_hold_notional=0.0,  # Will be calculated based on account capacity
        maker_order_notional=50.0,  # Use default, but order size will be calculated dynamically
        price_offset_ticks=args.price_offset,
    )
    return [maker_cfg, taker_cfg]


def calculate_optimal_targets(states: List[AccountState]) -> None:
    """
    Scientifically calculate optimal target positions based on account capacity.

    Philosophy:
    - Each account's target should be proportional to its risk capacity
    - Risk capacity = available_balance * leverage * safety_factor
    - Maintain slight net-long bias for BTC (justified by long-term trend)
    - Ensure both accounts have meaningful but sustainable positions
    """
    if len(states) != 2:
        logger.warning("Target calculation requires exactly 2 accounts")
        return

    buy_state = sell_state = None
    for state in states:
        if state.config.role.lower() == "buy":
            buy_state = state
        elif state.config.role.lower() == "sell":
            sell_state = state

    if not buy_state or not sell_state:
        logger.warning("Need both buy and sell accounts for target calculation")
        return

    # Calculate risk capacity for each account
    buy_capacity = _calculate_risk_capacity(buy_state)
    sell_capacity = _calculate_risk_capacity(sell_state)

    # Balanced target allocation with natural variance to avoid detection:
    # 1. Use 70% of minimum capacity to ensure both accounts can sustain positions
    # 2. Add slight randomness to avoid perfect balance (reduces bot detection risk)
    # 3. Allow 5-15% target difference between accounts (more human-like)
    import random

    safety_factor = 0.7
    min_capacity = min(buy_capacity, sell_capacity)
    base_target = min_capacity * safety_factor

    # Add variance to avoid perfect balance (anti-detection measure)
    variance = random.uniform(0.05, 0.15)  # 5-15% difference
    if random.choice([True, False]):  # Randomly choose which account gets higher target
        buy_target = base_target * (1 + variance)
        sell_target = base_target * (1 - variance * 0.5)  # Smaller reduction to maintain overall balance
    else:
        buy_target = base_target * (1 - variance * 0.5)
        sell_target = base_target * (1 + variance)

    # Apply targets with proper signs
    buy_state.config.base_target_notional = buy_target
    buy_state.config.apply_role_sign()  # Makes it positive

    sell_state.config.base_target_notional = sell_target
    sell_state.config.apply_role_sign()  # Makes it negative

    logger.info("🧮 Calculated balanced targets | Buy: $%.2f | Sell: $%.2f | Net: $%.2f | Min capacity: $%.2f (buy: $%.2f, sell: $%.2f)",
               buy_target, sell_target, buy_target - sell_target, min_capacity, buy_capacity, sell_capacity)


def _calculate_risk_capacity(state: AccountState) -> float:
    """Calculate maximum sustainable position size for an account based on total margin balance."""
    wallet = state.metrics.wallet_balance
    available = state.metrics.available_balance
    initial_margin = state.metrics.initial_margin
    position_margin = state.metrics.position_margin
    leverage = max(1, state.config.leverage)

    # Calculate total margin balance: the real account equity
    # Total margin = available + all used margins (initial + position)
    total_margin_balance = available + initial_margin + position_margin

    # For accounts with positions, use total margin balance as the true account size
    # This represents the actual capital in the account
    account_equity = max(total_margin_balance, available)

    # Conservative approach: base on account equity with proper buffer
    buffer_threshold = account_equity * state.config.margin_buffer
    equity_after_buffer = max(0.0, account_equity - buffer_threshold)

    # Proper leverage calculation for futures trading
    # With 5x leverage, $5000 account can hold up to ~$25000 notional position
    # Use conservative 60% of full leverage capacity for safety
    raw_capacity = equity_after_buffer * leverage * 0.6  # 60% of leverage capacity

    # Reasonable maximum: don't use more than 80% of account equity for positions
    max_equity_utilization = account_equity * leverage * 0.8
    risk_capacity = min(raw_capacity, max_equity_utilization)

    logger.debug("[%s] Risk calc: wallet=%.2f, available=%.2f, initial_margin=%.2f, position_margin=%.2f, total_margin=%.2f, account_equity=%.2f, buffer=%.2f, raw_capacity=%.2f, max_equity_util=%.2f, final=%.2f",
                state.config.identifier, wallet, available, initial_margin, position_margin, total_margin_balance, account_equity, buffer_threshold, raw_capacity, max_equity_utilization, risk_capacity)

    return max(500.0, risk_capacity)  # Minimum $500 position for large accounts


def ensure_collateral_presence(states: List[AccountState]) -> None:
    satisfied = False
    for state in states:
        prefer = state.config.prefer_collateral_assets
        balances = state.metrics.collateral_values
        for asset in prefer:
            if balances.get(asset.upper(), 0.0) > 0:
                satisfied = True
                break
        if satisfied:
            break
    if not satisfied:
        logger.warning(
            "Preferred collateral assets (%s) not detected on any account; collateral points may not accrue",
            ", ".join(TRADING_CONFIG.collateral_preferred_assets),
        )


async def main() -> None:
    args = parse_args()
    apply_runtime_overrides(args)
    configs = build_account_configs(args)
    coordinator = TradeCoordinator(
        configs,
        loop_interval=args.loop_interval,
    )
    coordinator._log_points_interval = max(1, args.points_log_interval)
    await coordinator.initialize()

    mark_price = await coordinator.accounts[0].client.get_mark_price(SYMBOL)
    for state in coordinator.accounts:
        await refresh_account_state(state, mark_price)

    # Calculate optimal targets based on scientific risk assessment
    calculate_optimal_targets(coordinator.accounts)
    # Then adjust for current market constraints
    coordinator.adjust_targets_for_current_state()

    ensure_collateral_presence(coordinator.accounts)

    # Continue running loop until stopped
    await coordinator.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Received interrupt, shutting down")
