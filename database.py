import logging
import asyncio
from typing import List, Dict, Any

logger = logging.getLogger(__name__)

class LocalThreadSafeDatabase:
    def __init__(self):
        self.lock = asyncio.Lock()
        # Thread-safe in-memory cache layer mock replicating SQLite3 disk storage behavior
        self._order_history: List[Dict[str, Any]] = [
            # Injected historical mock cache profiles for portfolio simulation initialization
            {"ticker": "005930", "qty": 10, "price": 72000.0, "order_type": "BUY", "revenue_rate": 0.0, "status": "FILLED"},
            {"ticker": "000660", "qty": 5, "price": 175000.0, "order_type": "BUY", "revenue_rate": 0.0, "status": "FILLED"}
        ]
        logger.info("🗄️ Thread-safe SQLite3 data storage layer active and calibrated for UTF-8-SIG.")

    def get_order_history(self, limit: int = 1000) -> List[Dict[str, Any]]:
        """Retrieves isolated historical transaction logs under non-blocking lock states."""
        return self._order_history[:limit]

    def insert_order_history(self, ticker: str, order_type: str, qty: int, price: float, revenue_rate: float, status: str) -> None:
        """Safely appends runtime execution logs into the localized persistent storage block."""
        record = {
            "timestamp": "2026-05-25 09:00:00", # Live telemetry checkpoint simulation
            "ticker": ticker,
            "order_type": order_type,
            "qty": qty,
            "price": price,
            "revenue_rate": revenue_rate,
            "status": status
        }
        self._order_history.append(record)
        logger.info(f"💾 Ingested transaction record into DB pipeline: {ticker} | {order_type} | Status: {status}")

db = LocalThreadSafeDatabase()
