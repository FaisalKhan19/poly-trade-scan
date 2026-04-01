"""Real-time trade monitor via WebSocket."""

import asyncio
from typing import Any, Callable, Optional

from src.api.polygon import PolygonClient
from src.constants import ENABLE_BACKPRESSURE_DROP, QUEUE_SIZE, RPC_TIMEOUT, WORKER_COUNT
from src.core.block_processor import BlockProcessor
from src.core.decoder import TransactionDecoder
from src.core.wallet_filter import WalletFilter
from src.utils.logging import get_logger

log = get_logger(__name__)


class TradeMonitor:
    """Main orchestrator for monitoring wallet trades."""

    def __init__(self, wss_url: Optional[str] = None) -> None:
        self.client = PolygonClient(wss_url) if wss_url else PolygonClient()
        self.decoder = TransactionDecoder()
        self.queue = asyncio.Queue(maxsize=QUEUE_SIZE)
        self.workers: list[asyncio.Task] = []
        self.blocks_processed = 0
        self.blocks_dropped = 0
        self._callbacks: dict[str, list[Callable]] = {
            "transaction": [],
            "error": [],
            "close": [],
        }
        self._running = False

    def on(self, event: str, callback: Callable) -> None:
        """Register event callback."""
        if event in self._callbacks:
            self._callbacks[event].append(callback)

    def emit(self, event: str, data: Any) -> None:
        """Emit event to all registered callbacks."""
        for callback in self._callbacks.get(event, []):
            if asyncio.iscoroutinefunction(callback):
                asyncio.create_task(callback(data))
            else:
                callback(data)

    async def start(self, target_wallets: list[str]) -> None:
        """Start monitoring for trades from target wallets."""
        self._running = True
        wallet_count = len(target_wallets) if target_wallets else 0
        log.info("Starting monitor", wallet_count=wallet_count)

        await self.client.connect()

        wallet_filter = WalletFilter(target_wallets)
        processor = BlockProcessor(self.client, self.decoder, wallet_filter)

        if wallet_filter.is_tracking_all:
            log.info("Tracking ALL Polymarket trades")
        else:
            log.info("Tracking specific wallets", count=wallet_count)

        # Start worker pool
        self.workers = [asyncio.create_task(self._worker(processor)) for _ in range(WORKER_COUNT)]

        try:
            await self.client.subscribe_blocks(
                lambda block_num: self._on_block(block_num, processor)
            )
        except Exception as e:
            log.error("Monitor error", error=str(e))
            self.emit("error", e)
            self.emit("close", {"code": -1, "reason": str(e)})
        finally:
            await self.stop()

    async def _on_block(self, block_number: int, processor: BlockProcessor) -> None:
        """Handle new block event."""
        try:
            await self.queue.put(block_number)
        except asyncio.QueueFull:
            if ENABLE_BACKPRESSURE_DROP:
                try:
                    dropped = self.queue.get_nowait()
                    log.warning("Queue full, dropped oldest block", dropped=dropped, current=block_number)
                    self.blocks_dropped += 1
                except asyncio.QueueEmpty:
                    pass
                await self.queue.put(block_number)
            else:
                log.warning("Queue full, skipping block", block=block_number)
                self.blocks_dropped += 1

    async def _worker(self, processor: BlockProcessor) -> None:
        """Worker task to process blocks from queue."""
        while self._running:
            try:
                block_number = await self.queue.get()
                await self._process_block_safe(block_number, processor)
                self.queue.task_done()
            except Exception as e:
                log.error("Worker error", error=str(e))

    async def _process_block_safe(self, block_number: int, processor: BlockProcessor) -> None:
        """Process block with timeout and error isolation."""
        try:
            trades = await asyncio.wait_for(processor.process_block(block_number), timeout=RPC_TIMEOUT)
            self.blocks_processed += 1
            for trade in trades:
                self.emit("transaction", trade)
        except asyncio.TimeoutError:
            log.warning("Block processing timeout", block=block_number)
        except Exception as e:
            log.error("Block processing error", block=block_number, error=str(e))
            self.emit("error", e)

    async def stop(self) -> None:
        """Stop monitoring."""
        self._running = False
        # Cancel workers
        for task in self.workers:
            task.cancel()
        await asyncio.gather(*self.workers, return_exceptions=True)
        # Drain queue
        try:
            while not self.queue.empty():
                self.queue.get_nowait()
                self.queue.task_done()
        except asyncio.QueueEmpty:
            pass
        await self.client.disconnect()
        log.info("Monitor stopped")
