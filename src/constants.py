"""Application constants."""
import os
from pathlib import Path

from dotenv import load_dotenv

# Project root directory (parent of src/)
PROJECT_ROOT = Path(__file__).parent.parent

# Configuration directory
CONFIG_DIR = PROJECT_ROOT / "config"

# Load environment variables from .env in project root
load_dotenv(PROJECT_ROOT / ".env")

# Polygon WebSocket endpoint - configurable via POLYGON_WSS_URL env var
POLYGON_WSS_URL = os.environ["POLYGON_WSS_URL"]

# Default path for wallets file
DEFAULT_WALLETS_FILE = CONFIG_DIR / "wallets.txt"

# RPC retry settings
RPC_MAX_RETRIES = 3
RPC_RETRY_DELAY_SECONDS = 1.0
RPC_TIMEOUT_SECONDS = 2

# Production-grade settings
WORKER_COUNT = int(os.getenv("WORKER_COUNT", 2))
QUEUE_SIZE = int(os.getenv("QUEUE_SIZE", 100))
RPC_TIMEOUT = int(os.getenv("RPC_TIMEOUT", 10))
RECONNECT_MAX_DELAY = int(os.getenv("RECONNECT_MAX_DELAY", 10))
ENABLE_BACKPRESSURE_DROP = os.getenv("ENABLE_BACKPRESSURE_DROP", "true").lower() == "true"
