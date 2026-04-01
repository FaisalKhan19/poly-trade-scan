You are tasked with upgrading this Polygon WebSocket trade monitoring system from a prototype to a production-grade, fault-tolerant, high-throughput service.

STRICT RULES:

* Do NOT change external behavior or output format unless explicitly specified
* Maintain backward compatibility with current tracker usage
* Focus on reliability, concurrency, and resource safety
* Avoid unnecessary refactors; prefer surgical improvements

---

# 1. WEBSOCKET RESILIENCE (CRITICAL)

## Problem

Current WebSocket connection drops with:

* ping timeout (1011)
* no reconnection logic
* monitor exits completely

## Required Changes

### 1.1 Add automatic reconnect loop in PolygonClient.subscribe_blocks

Wrap the entire subscription loop in a retry mechanism:

* On ANY exception:

  * log error
  * close existing websocket safely
  * wait (exponential backoff: 1s → 5s → 10s max)
  * reconnect
  * resubscribe

Ensure:

* No duplicate subscriptions
* No memory leaks

---

### 1.2 Improve WebSocket config

Update websockets.connect:

* ping_interval = 20
* ping_timeout = 20
* close_timeout = 5
* max_queue = None (prevent backpressure drops)

---

### 1.3 Detect silent stalls

Add watchdog:

* If no block received for > 60 seconds → force reconnect

---

# 2. NON-BLOCKING BLOCK PROCESSING (HIGH PRIORITY)

## Problem

*block processing blocks the WebSocket listener*

Current:

```
await processor.process_block(block_number)
```

This blocks incoming messages and causes ping timeout.

## Required Changes

### 2.1 Convert to async task queue

Inside TradeMonitor:

* Create asyncio.Queue (bounded, e.g., size=100)
* WebSocket handler ONLY enqueues block numbers
* Separate worker(s) process blocks

---

### 2.2 Add worker pool

* Configurable worker count (default: 2–4)
* Each worker:

  * pulls block_number from queue
  * processes independently

---

### 2.3 Handle backpressure

If queue is full:

* drop oldest OR skip new blocks (configurable)
* log warning: "block dropped due to backpressure"

---

# 3. HTTP SESSION & RESOURCE MANAGEMENT (CRITICAL)

## Problem

Unclosed aiohttp session → memory + socket leaks

## Required Changes

### 3.1 Ensure session lifecycle

* Create session ONCE
* Close session in disconnect()
* Guarantee cleanup via:

  * try/finally in monitor.start()
  * or context manager pattern

---

### 3.2 Add graceful shutdown

TradeMonitor.stop():

* stop workers
* drain queue
* close websocket
* close HTTP session

---

# 4. ERROR ISOLATION (IMPORTANT)

## Problem

Single failure can kill entire pipeline

## Required Changes

### 4.1 Isolate block processing errors

Inside worker:

* wrap process_block in try/except
* log error
* continue (DO NOT crash worker)

---

### 4.2 Timeout protection

Add timeout:

```
await asyncio.wait_for(process_block(...), timeout=10)
```

If timeout:

* skip block
* log warning

---

# 5. PERFORMANCE OPTIMIZATIONS

## 5.1 Reduce redundant RPC calls

If not already implemented:

* Prefer `eth_getBlockReceipts` over per-tx receipt calls
* Cache recent blocks (LRU cache, size ~100)

---

## 5.2 Avoid repeated decoding work

* Cache decoded contract selectors if applicable

---

# 6. CONFIGURABILITY

Create a config module or env-based config:

* WORKER_COUNT (default 2)
* QUEUE_SIZE (default 100)
* RPC_TIMEOUT (default 10s)
* RECONNECT_MAX_DELAY (default 10s)
* ENABLE_BACKPRESSURE_DROP (true/false)

---

# 7. LOGGING IMPROVEMENTS

* Add structured logs for:

  * reconnect attempts
  * queue size
  * worker lag
  * dropped blocks

Example:

```
log.warning("Queue full", size=queue.qsize())
```

---

# 8. METRICS (OPTIONAL BUT RECOMMENDED)

Expose counters:

* blocks_processed
* blocks_dropped
* reconnect_count
* avg_processing_time

---

# 9. TRACKER COMPATIBILITY

Ensure:

* Existing tracker_wss.py works without modification
* Event callbacks ("transaction", "error", "close") unchanged

---

# 10. TESTING CHECKLIST

After implementation, validate:

1. Run for 30+ minutes without disconnect crash
2. Simulate slow processing → confirm no websocket drop
3. Kill network → confirm reconnect
4. High TPS blocks (~500 txs) → no backlog explosion
5. No "Unclosed client session" warnings

---

# DELIVERABLE

* Updated PolygonClient
* Updated TradeMonitor
* Any minimal helper modules (queue/worker/config)
* No breaking API changes

---

# PRIORITY ORDER

1. Non-blocking processing (Queue + Workers)
2. Reconnection logic
3. Resource cleanup
4. Timeouts
5. Performance improvements

---

Be precise. Avoid overengineering. Focus on stability under continuous load.
