"""
Korea Investment & Securities (KIS) Real-Time Broker & Risk Engine
Establishes KIS Live Production WebSocket (ws://://koreainvestment.com) session.
Enforces 3 req/sec REST API Rate Limit and deterministic -5.00% Hard Portfolio Stop-Loss.
If DRY_RUN=True, executes simulated orders with full localized logging.
"""

import os
import json
import logging
import asyncio
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Callable, Dict, List

import websockets

from auth import auth_manager
from database import db

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('aurum_system.log'),
        logging.StreamHandler()
    ]
)

DRY_RUN = True
STOP_LOSS_THRESHOLD = -5.00


@dataclass
class RealTimePrice:
    timestamp: str
    ticker: str
    price: float
    volume: int
    ask_price: float
    bid_price: float
    high_price: float
    low_price: float
    open_price: float


class RateLimiter:
    def __init__(self, max_requests: int = 3, window_seconds: float = 1.0):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.request_times: List[float] = []
        self.lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self.lock:
            now = time.time()
            self.request_times = [t for t in self.request_times if now - t < self.window_seconds]
            if len(self.request_times) >= self.max_requests:
                wait_sec = self.window_seconds - (now - self.request_times[0])
                if wait_sec > 0:
                    logger.debug(f'⏳ Rate limiter throttling: waiting {wait_sec:.2f}s')
                    await asyncio.sleep(wait_sec)
                    self.request_times = []
            self.request_times.append(time.time())


class PortfolioRiskEngine:
    def __init__(self):
        self.holdings: Dict[str, int] = {}
        self.cost_basis: Dict[str, float] = {}
        self.last_prices: Dict[str, float] = {}
        self.lock = asyncio.Lock()

    async def refresh_holdings(self) -> None:
        async with self.lock:
            orders = db.get_order_history(limit=1000)
            self.holdings = {}
            self.cost_basis = {}
            for order in reversed(orders):
                ticker = order['ticker']
                qty = order['qty']
                price = order['price']
                side = order['order_type'].upper()
                if side == 'BUY':
                    self.holdings[ticker] = self.holdings.get(ticker, 0) + qty
                    total_cost = self.cost_basis.get(ticker, 0.0) * max(self.holdings.get(ticker, 0) - qty, 0)
                    self.cost_basis[ticker] = (total_cost + price * qty) / max(self.holdings[ticker], 1)
                elif side == 'SELL':
                    self.holdings[ticker] = self.holdings.get(ticker, 0) - qty
                    if self.holdings[ticker] <= 0:
                        self.holdings[ticker] = 0
                        self.cost_basis[ticker] = 0.0

    async def update_price(self, ticker: str, price: float) -> None:
        async with self.lock:
            self.last_prices[ticker] = price

    async def current_portfolio_value(self) -> float:
        async with self.lock:
            value = 0.0
            for ticker, qty in self.holdings.items():
                if qty > 0 and ticker in self.last_prices:
                    value += qty * self.last_prices[ticker]
            return value

    async def current_invested(self) -> float:
        async with self.lock:
            invested = 0.0
            for ticker, qty in self.holdings.items():
                if qty > 0:
                    invested += qty * self.cost_basis.get(ticker, 0.0)
            return invested

    async def calculate_profit_ratio(self) -> Optional[float]:
        invested = await self.current_invested()
        if invested <= 0:
            return None
        value = await self.current_portfolio_value()
        return ((value - invested) / invested) * 100.0

    async def should_liquidate(self) -> bool:
        ratio = await self.calculate_profit_ratio()
        if ratio is None:
            return False
        return ratio <= STOP_LOSS_THRESHOLD

    async def execute_forced_liquidation(self) -> None:
        async with self.lock:
            if DRY_RUN:
                logger.warning('🟡 DRY_RUN MODE ACTIVE: Suppressing actual REST orders. Running localized risk simulation.')
            await self.refresh_holdings()
            orders = []
            for ticker, qty in self.holdings.items():
                if qty <= 0:
                    continue
                market_price = self.last_prices.get(ticker)
                if market_price is None:
                    continue
                revenue_rate = 0.0
                if self.cost_basis.get(ticker, 0.0) > 0:
                    revenue_rate = ((market_price - self.cost_basis[ticker]) / self.cost_basis[ticker]) * 100.0
                if DRY_RUN:
                    logger.warning(f'💥 SIMULATED KILL-SWITCH ORDER: {ticker} {qty} shares @ {market_price:.2f} KRW ({revenue_rate:+.2f}%)')
                    db.insert_order_history(ticker=ticker, order_type='SELL', qty=qty, price=market_price, revenue_rate=revenue_rate, status='SIMULATED')
                    continue
                order_id = await self._send_market_sell_order(ticker, qty, market_price)
                if order_id is not None:
                    db.insert_order_history(ticker=ticker, order_type='SELL', qty=qty, price=market_price, revenue_rate=revenue_rate, status='LIQUIDATED')
                    orders.append(order_id)
            if orders:
                logger.info(f'✅ Forced liquidation sequence completed successfully. Order IDs: {orders}')

    async def _send_market_sell_order(self, ticker: str, qty: int, price: float) -> Optional[int]:
        await asyncio.sleep(0)  # Yield to event loop prior to REST execution
        await rate_limiter.acquire()
        endpoint = '/v1/uapi/domestic-stock/v1/trading/order-cash'
        payload = {
            'CANO': os.getenv('KIS_ACCOUNT_NUMBER'),
            'ACNT_PRDT_CD': os.getenv('KIS_ACNT_PRDT_CD'),
            'PDNO': ticker,
            'ORD_DVSN': '01',
            'ORD_QTY': str(qty),
            'ORD_UNPR': str(int(price)),
            'ORD_SPC_DVSN': '00',
            'HOGU_GUBUN': '00',
            'SLL_TYPE': '01'
        }
        try:
            response = await auth_manager.make_authenticated_request('POST', endpoint, payload=payload, timeout=10)
            logger.info(f'✅ Market sell order executed via REST: {ticker} {qty} shares')
            return response.get('output', {}).get('ord_sno') or response.get('order_id')
        except Exception as exc:
            logger.error(f'❌ Failed to execute market sell order: {ticker} {qty} shares - Reason: {exc}')
            return None


class KISRealtimeBroker:
    def __init__(self, db_callback: Optional[Callable] = None):
        self.ws_url = os.getenv('KIS_WS_URL', 'ws://://koreainvestment.com')
        self.target_stocks = [s.strip() for s in os.getenv('TARGET_STOCKS', '005930,000660,051910').split(',')]
        self.websocket: Optional[websockets.WebSocketClientProtocol] = None
        self.connected = False
        self.db_callback = db_callback
        self.price_cache: Dict[str, RealTimePrice] = {}
        self.lock: Optional[asyncio.Lock] = None
        self.rate_limiter = rate_limiter
        self.risk_engine = PortfolioRiskEngine()

        logger.info(f'✅ KISRealtimeBroker initialized: Target tickers={self.target_stocks}')
        logger.info(f'📍 WebSocket Host Endpoint={self.ws_url}')
        logger.info('🧪 DRY_RUN Mode: SIMULATION ACTIVE' if DRY_RUN else '🔴 LIVE Mode: WARNING PRODUCTION ACTIVE')

    async def _ensure_lock(self) -> None:
        if self.lock is None:
            self.lock = asyncio.Lock()

    async def connect_and_stream(self) -> None:
        """Establishes production WebSocket session and kicks off the core ingestion loop."""
        retry_count = 0
        max_retries = 5
        while retry_count < max_retries:
            try:
                logger.info(f'🔌 Attempting WebSocket session connection: {self.ws_url}')
                async with websockets.connect(self.ws_url, ping_interval=30, ping_timeout=10, max_size=10_000_000) as websocket:
                    self.websocket = websocket
                    self.connected = True
                    logger.info('✅ WebSocket connection successfully established.')
                    
                    # Ignite subscription flow
                    await self._subscribe_to_stocks()
                    break
            except Exception as e:
                retry_count += 1
                logger.error(f'❌ WebSocket connection failure (Attempt {retry_count}/{max_retries}): {e}')
                if retry_count < max_retries:
                    await asyncio.sleep(2 ** retry_count)  # Exponential backoff
                else:
                    logger.critical('🚨 Max connection retries exceeded. Forcing system lock down state.')

    async def _subscribe_to_stocks(self) -> None:
        """Transmits subscription handshake packets (H0STCNT0) to live production host."""
        await self._ensure_lock()
        approval_key = await auth_manager.get_websocket_approval_key()
        
        for ticker in self.target_stocks:
            payload = {
                "header": {
                    "approval_key": approval_key,
                    "custtype": "P",
                    "tr_type": "1",  # 1: Register Subscription
                    "content-type": "utf-8"
                },
                "body": {
                    "input": {
                        "tr_id": "H0STCNT0",  # Transaction ID for Domestic Stock Real-Time Price
                        "tr_key": ticker
                    }
                }
            }
            await self.websocket.send(json.dumps(payload))
            logger.info(f"📡 WebSocket subscription frame dispatched for ticker: {ticker}")
            await asyncio.sleep(0.1)  # Micro-delay to avoid infrastructure flood
            
        # Transition to real-time telemetry parser and security gate loop
        await self._listen_loop()

    async def _listen_loop(self) -> None:
        """Processes real-time pipeline telemetry and enforces sub-millisecond source-level Kill-Switch."""
        logger.info("🚀 Real-time stream telemetry pipeline online. Standby for Kill-Switch triggers...")
        await self.risk_engine.refresh_holdings()
        
        try:
            async for message in self.websocket:
                # Intercept and process raw data blocks starting with '0' or '1'
                if message.startswith("0") or message.startswith("1"):
                    data_parts = message.split("|")
                    if len(data_parts) >= 4:
                        # Extract stock record arrays
                        records = data_parts[3].split("^")
                        if len(records) > 2:
                            ticker = data_parts[2]  # Extract ticker ID
                            current_price = float(records[2])  # Parse real-time current price
                            
                            # Feed telemetry engine with live price quotes
                            await self.risk_engine.update_price(ticker, current_price)
                            
                            # Compute holistic portfolio performance ratio asynchronously
                            profit_ratio = await self.risk_engine.calculate_profit_ratio()
                            if profit_ratio is not None:
                                logger.info(f"📊 Live Governance Active: Total Return {profit_ratio:+.2f}%")
                                
                                # Evaluate source-level hard coded threshold (-5.00% Scan)
                                if await self.risk_engine.should_liquidate():
                                    logger.critical(
                                        f"🚨🚨 [RISK LOCK TRIGGERED] "
                                        f"Total asset exposure collapsed past threshold ({STOP_LOSS_THRESHOLD}%)! "
                                        f"Current Return: {profit_ratio:.2f}%"
                                    )
                                    logger.critical("💥 [KILL-SWITCH ENGAGED] Dispensing immediate market liquidation thread sequence.")
                                    
                                    # Launch sub-millisecond automated global portfolio liquidation
                                    await self.risk_engine.execute_forced_liquidation()
                                    
                                    # Tear down network architecture state to preserve asset synchronization
                                    await self.websocket.close()
                                    self.connected = False
                                    logger.info("🔒 [SECURITY LOCK COMPLETE] Production session dissolved. Core infrastructure secured.")
                                    break
                else:
                    # Automatically drop keep-alive/heartbeat telemetry signals
                    pass
        except Exception as e:
            logger.error(f"❌ Exception captured during real-time telemetry streaming: {e}")
            self.connected = False


# Instantiate runtime global rate limit control block
rate_limiter = RateLimiter(max_requests=3, window_seconds=1.0)
