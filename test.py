import asyncio
from pathlib import Path
from datetime import datetime, timezone

from src.cli import load_wallets
from src.monitor import TradeMonitor
from src.utils.logging import get_logger
from src.output.formatters import format_trade

from DataBaseUtils import TrackerDB

log = get_logger(__name__)

def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)

wallets = load_wallets(Path('config/wallets.txt'))
monitor = TradeMonitor()

db = TrackerDB(Path('activity_tracker.db'))

async def task():
    def on_trade(trade) -> None:
        fmt_trade = format_trade(trade)
        print("Block Hash", fmt_trade.block_number)
        print("Wallet ID", fmt_trade.wallet)
        print("Token ID", fmt_trade.token_id)
        print("Side", fmt_trade.side)
        print("Shares", fmt_trade.tokens)
        print("Price", fmt_trade.price)
        print("Size USDC", fmt_trade.total_usdc)
        print("Transaction Hash", fmt_trade. tx_hash)
        print("Timestamp", fmt_trade.timestamp)
        received_at = utc_now()

        db.insert_event(fmt_trade, received_at)

    monitor.on("transaction", on_trade)
    monitor.on("error", lambda e: log.error("Error", error=str(e)))
    monitor.on("close", lambda d: log.warning("Connection closed", details=d))
    await monitor.start(wallets)

asyncio.run(task())