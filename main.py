import asyncio
import aiohttp
from aiohttp import web
import random
import websockets
import json
import urllib.parse
import time
import hashlib
import csv
import logging
import os
import uuid
import inspect


def load_dotenv_file(env_path: str = ".env"):
    """Load KEY=VALUE pairs from .env into os.environ without overriding existing env vars."""
    try:
        if not os.path.exists(env_path):
            return
        with open(env_path, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                if not key:
                    continue
                # Remove optional wrapping quotes.
                if len(value) >= 2 and ((value[0] == '"' and value[-1] == '"') or (value[0] == "'" and value[-1] == "'")):
                    value = value[1:-1]
                os.environ.setdefault(key, value)
    except Exception:
        # Keep startup resilient even if .env has malformed lines.
        pass


load_dotenv_file(os.path.join(os.getcwd(), ".env"))

# Configuration
# Require MARKET_API_KEY from environment. Do NOT keep a hardcoded API key here.
API_KEY = os.getenv("MARKET_API_KEY")  # must be set in environment
DEBUG_MODE = os.getenv("MARKET_DEBUG", "1") in ("1", "true", "True")
# You can target either by name_id (if using names.json / WS events) or by market_hash_name
TARGET_NAME_ID = None  # Example: "12345" or None
# Support multiple targets (comma-separated env var) e.g. "Sealed Genesis Terminal,Recoil Case"
_targets_env = os.getenv("TARGET_MARKET_HASH_NAME", "Sealed Genesis Terminal")
TARGET_MARKET_HASHES = [t.strip() for t in _targets_env.split(",") if t.strip()]
# Default threshold from env (USD) -> units where 1 USD = 1000.
DEFAULT_THRESHOLD_USD = float(os.getenv("DEFAULT_THRESHOLD_USD", "0.29"))
THRESHOLD = int(round(DEFAULT_THRESHOLD_USD * 1000))
# Hard safety cap from env (USD). If 0, cap is disabled.
HARD_MAX_BUY_PRICE_USD = float(os.getenv("HARD_MAX_BUY_PRICE_USD", "0"))
HARD_MAX_BUY_UNITS = int(round(HARD_MAX_BUY_PRICE_USD * 1000)) if HARD_MAX_BUY_PRICE_USD > 0 else 0
# Startup safety controls.
REQUIRE_EXPLICIT_THRESHOLD = os.getenv("REQUIRE_EXPLICIT_THRESHOLD", "1") in ("1", "true", "True", "yes", "on")
RESET_THRESHOLDS_ON_START = os.getenv("RESET_THRESHOLDS_ON_START", "1") in ("1", "true", "True", "yes", "on")
RESET_MODE_ON_START = os.getenv("RESET_MODE_ON_START", "1") in ("1", "true", "True", "yes", "on")
SAFE_START_THRESHOLD_USD = float(os.getenv("SAFE_START_THRESHOLD_USD", "0"))
SAFE_START_THRESHOLD_UNITS = int(round(SAFE_START_THRESHOLD_USD * 1000)) if SAFE_START_THRESHOLD_USD > 0 else 0
# How to apply ignore: 'id' = prefer listing id (default), 'name' = ignore by market_hash_name
IGNORE_BY = os.getenv("IGNORE_BY", "id")  # 'id' or 'name'

# Telegram settings (optional). Read only from environment/.env.
TELEGRAM_BOT_TOKEN = os.getenv("MARKET_TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("MARKET_TELEGRAM_CHAT_ID")
WS_URL = "wss://wsprice.csgo.com/connection/websocket"
REST_PRICES_URL = "https://market.csgo.com/api/v2/prices/USD.json"
GET_WS_TOKEN_URL = "https://market.csgo.com/api/v2/get-ws-token"
WS_ORIGIN = os.getenv("WS_ORIGIN", "https://market.csgo.com")
WS_USER_AGENT = os.getenv("WS_USER_AGENT", "market-bot/1.0")
# Optional proxy configuration removed — proxies disabled by default
# (If you later want proxies, set HTTP_PROXY or WS_PROXY and re-enable support.)
# Fallback polling interval in seconds (default 10). Lower -> more REST requests.
# You can override via environment variable `POLL_INTERVAL` on Render.
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "2"))  # Updated to 2 seconds for safe API usage
# Dedupe bucket size in seconds (used to group similar alerts). Default 120 (2 minutes).
# Lowering this reduces grouping window so similar-priced alerts are treated sooner.
DEDUPE_BUCKET_SEC = int(os.getenv("DEDUPE_BUCKET_SEC", "60"))
# Debounce window (seconds) to aggregate multiple near-simultaneous offers for same item.
# Lower -> attempt buys faster for single events. Default reduced to 1.
DEBOUNCE_SEC = float(os.getenv("DEBOUNCE_SEC", "0.3"))  # Updated to 0.3 seconds for safe API usage
# Minimum interval between AUTO triggers for the same normalized item name.
# Keeps AUTO responsive (seconds) without minute-level throttling.
AUTO_RECHECK_INTERVAL_SEC = float(os.getenv("AUTO_RECHECK_INTERVAL_SEC", "2"))
# Maximum number of lots to buy in one aggregated AUTO window (0 = disabled).
# Set >0 to allow small batch purchases when many offers appear simultaneously.
MAX_BATCH_BUY = int(os.getenv("MAX_BATCH_BUY", "0"))
# Purchase log and spend limits
PURCHASE_LOG = os.path.join(os.getcwd(), "purchase_history.csv")
DAILY_SPEND_LIMIT_USD = float(os.getenv("DAILY_SPEND_LIMIT_USD", "0"))  # 0 = disabled
SESSION_SPEND_LIMIT_USD = float(os.getenv("SESSION_SPEND_LIMIT_USD", "0"))  # 0 = disabled
# Market API hard safety limit (<5 req/s per provider rules)
MARKET_MAX_RPS = float(os.getenv("MARKET_MAX_RPS", "4.5"))
MARKET_MIN_INTERVAL_SEC = 1.0 / MARKET_MAX_RPS if MARKET_MAX_RPS > 0 else 0.0
HISTORY_ALL_LOOKBACK_DAYS = int(os.getenv("HISTORY_ALL_LOOKBACK_DAYS", "3650"))
HISTORY_SPLIT_THRESHOLD = int(os.getenv("HISTORY_SPLIT_THRESHOLD", "190"))
HISTORY_MIN_SPLIT_SEC = int(os.getenv("HISTORY_MIN_SPLIT_SEC", "21600"))
HISTORY_MAX_SPLIT_DEPTH = int(os.getenv("HISTORY_MAX_SPLIT_DEPTH", "16"))
HISTORY_MAX_RANGE_SEC = int(os.getenv("HISTORY_MAX_RANGE_SEC", "2592000"))
HISTORY_FETCH_RETRIES = int(os.getenv("HISTORY_FETCH_RETRIES", "3"))
HISTORY_RETRY_BASE_SEC = float(os.getenv("HISTORY_RETRY_BASE_SEC", "1.0"))

# Logging setup
logging.basicConfig(level=logging.DEBUG if DEBUG_MODE else logging.INFO)
logger = logging.getLogger("market")

# Log whether Telegram is configured (without printing the token)
if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
    logger.info("Telegram configured: yes")
else:
    logger.info("Telegram configured: no (messages will be printed)")

# Fail fast when API key is not provided — enforce env var usage so deployments
# (e.g., Render) must supply `MARKET_API_KEY` and no secret remains in code.
if not API_KEY:
    logger.error("No MARKET_API_KEY provided. Set environment variable MARKET_API_KEY before starting the service.")
    raise SystemExit(1)

# Persistent state for confirmed/ignored items
STATE_FILE = os.path.join(os.getcwd(), "state.json")
state = {"ignored": [], "confirmed": []}

# Last state save timestamp for debouncing
_last_state_save_ts = 0
# Rate-limit repetitive upstream error logs.
_last_balance_error_log_ts = 0
_last_known_balance_usd = None
_last_known_balance_ts = 0
_market_last_request_ts = 0.0
_market_rate_lock = asyncio.Lock()

# Telegram anti-flood controls.
TELEGRAM_MIN_SEND_INTERVAL_SEC = float(os.getenv("TELEGRAM_MIN_SEND_INTERVAL_SEC", "1.2"))
TELEGRAM_AUTO_DUPLICATE_WINDOW_SEC = int(os.getenv("TELEGRAM_AUTO_DUPLICATE_WINDOW_SEC", "45"))
_tg_send_lock = asyncio.Lock()
_tg_last_send_ts = 0.0
_tg_block_until_ts = 0.0
_tg_auto_recent_msgs = {}


def detect_ws_headers_kwarg() -> str:
    """Detect correct websockets.connect header kwarg across library versions."""
    try:
        params = inspect.signature(websockets.connect).parameters
        if "additional_headers" in params:
            return "additional_headers"
        if "extra_headers" in params:
            return "extra_headers"
    except Exception:
        pass
    # Default to newer name; fallback logic in caller still handles failures.
    return "additional_headers"


WS_HEADERS_KWARG = detect_ws_headers_kwarg()


async def market_rate_limit_wait():
    """Global rate limiter for market.csgo.com requests."""
    global _market_last_request_ts
    if MARKET_MIN_INTERVAL_SEC <= 0:
        return
    async with _market_rate_lock:
        now = time.monotonic()
        wait_for = MARKET_MIN_INTERVAL_SEC - (now - _market_last_request_ts)
        if wait_for > 0:
            await asyncio.sleep(wait_for)
        _market_last_request_ts = time.monotonic()


async def market_get_text(session_obj, url: str, *, params=None, headers=None, timeout=20):
    """Rate-limited GET to market API. Returns (status, text, content_type)."""
    await market_rate_limit_wait()
    async with session_obj.get(url, params=params, headers=headers, timeout=timeout) as response:
        status = response.status
        text = await response.text()
        content_type = response.headers.get("Content-Type", "")
        return status, text, content_type

# Telemetry settings and in-memory counters.
METRICS_WINDOW_SEC = int(os.getenv("METRICS_WINDOW_SEC", "300"))
METRICS_REPORT_SEC = int(os.getenv("METRICS_REPORT_SEC", "300"))
metrics_state = {
    "mode_events": [(time.time(), "unknown")],
    "counts": {
        "signal_ws": [],
        "signal_rest": [],
        "skip_filtered_ws": [],
        "skip_filtered_rest": [],
        "skip_dedupe_ws": [],
        "skip_dedupe_rest": [],
    },
    "attempt_delays": [],
}


def metrics_set_mode(mode: str):
    """Record mode transitions: ws/fallback/unknown."""
    try:
        now = time.time()
        events = metrics_state.setdefault("mode_events", [])
        if events and events[-1][1] == mode:
            return
        events.append((now, mode))
    except Exception:
        pass


def metrics_inc(counter: str):
    """Increment timestamped counter for rolling-window reports."""
    try:
        metrics_state.setdefault("counts", {}).setdefault(counter, []).append(time.time())
    except Exception:
        pass


def metrics_add_attempt_delay(delay_sec: float):
    """Store signal->attempt delay samples in seconds."""
    try:
        if delay_sec is None:
            return
        metrics_state.setdefault("attempt_delays", []).append((time.time(), float(delay_sec)))
    except Exception:
        pass


def metrics_prune(now_ts: float):
    """Prune telemetry buffers to rolling window."""
    try:
        keep_from = now_ts - METRICS_WINDOW_SEC
        for key, arr in metrics_state.setdefault("counts", {}).items():
            metrics_state["counts"][key] = [t for t in arr if t >= keep_from]

        delays = metrics_state.setdefault("attempt_delays", [])
        metrics_state["attempt_delays"] = [(t, d) for (t, d) in delays if t >= keep_from]

        events = metrics_state.setdefault("mode_events", [])
        # Keep at most one event just before the window start to reconstruct durations.
        if not events:
            metrics_state["mode_events"] = [(now_ts, "unknown")]
            return
        idx = 0
        for i, (ts, _m) in enumerate(events):
            if ts <= keep_from:
                idx = i
            else:
                break
        metrics_state["mode_events"] = events[max(0, idx):]
    except Exception:
        pass


def metrics_mode_durations(now_ts: float):
    """Compute mode durations over rolling window."""
    keep_from = now_ts - METRICS_WINDOW_SEC
    events = metrics_state.get("mode_events", []) or [(keep_from, "unknown")]

    # Determine starting mode at window boundary.
    start_mode = "unknown"
    for ts, mode in events:
        if ts <= keep_from:
            start_mode = mode
        else:
            break

    durations = {"ws": 0.0, "fallback": 0.0, "unknown": 0.0}
    prev_ts = keep_from
    prev_mode = start_mode

    for ts, mode in events:
        if ts < keep_from:
            continue
        if ts > now_ts:
            break
        durations[prev_mode] = durations.get(prev_mode, 0.0) + max(0.0, ts - prev_ts)
        prev_ts = ts
        prev_mode = mode

    durations[prev_mode] = durations.get(prev_mode, 0.0) + max(0.0, now_ts - prev_ts)
    return durations


async def metrics_reporter_loop():
    """Emit periodic 5-minute reliability stats to logs."""
    while True:
        try:
            await asyncio.sleep(max(10, METRICS_REPORT_SEC))
            now_ts = time.time()
            metrics_prune(now_ts)
            durations = metrics_mode_durations(now_ts)
            window = float(METRICS_WINDOW_SEC)

            ws_pct = (durations.get("ws", 0.0) / window) * 100.0
            fb_pct = (durations.get("fallback", 0.0) / window) * 100.0
            unk_pct = (durations.get("unknown", 0.0) / window) * 100.0

            counts = metrics_state.get("counts", {})
            signals_ws = len(counts.get("signal_ws", []))
            signals_rest = len(counts.get("signal_rest", []))
            skip_filtered = len(counts.get("skip_filtered_ws", [])) + len(counts.get("skip_filtered_rest", []))
            skip_dedupe = len(counts.get("skip_dedupe_ws", [])) + len(counts.get("skip_dedupe_rest", []))

            delays = [d for (_t, d) in metrics_state.get("attempt_delays", [])]
            avg_delay = (sum(delays) / len(delays)) if delays else 0.0

            logger.info(
                "Metrics(5m): mode_ws=%.1f%% mode_fallback=%.1f%% mode_unknown=%.1f%% "
                "signals_ws=%d signals_rest=%d skipped_filtered=%d skipped_dedupe=%d avg_signal_to_attempt=%.3fs",
                ws_pct,
                fb_pct,
                unk_pct,
                signals_ws,
                signals_rest,
                skip_filtered,
                skip_dedupe,
                avg_delay,
            )
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("metrics_reporter_loop failed")


def build_stats_text() -> str:
    """Build human-readable telemetry snapshot for Telegram /stats command."""
    now_ts = time.time()
    metrics_prune(now_ts)
    durations = metrics_mode_durations(now_ts)
    window = float(METRICS_WINDOW_SEC)

    ws_pct = (durations.get("ws", 0.0) / window) * 100.0
    fb_pct = (durations.get("fallback", 0.0) / window) * 100.0
    unk_pct = (durations.get("unknown", 0.0) / window) * 100.0

    counts = metrics_state.get("counts", {})
    signals_ws = len(counts.get("signal_ws", []))
    signals_rest = len(counts.get("signal_rest", []))
    skip_filtered_ws = len(counts.get("skip_filtered_ws", []))
    skip_filtered_rest = len(counts.get("skip_filtered_rest", []))
    skip_dedupe_ws = len(counts.get("skip_dedupe_ws", []))
    skip_dedupe_rest = len(counts.get("skip_dedupe_rest", []))

    delays = [d for (_t, d) in metrics_state.get("attempt_delays", [])]
    avg_delay = (sum(delays) / len(delays)) if delays else 0.0

    mode_now = state.get("mode", "MANUAL")
    target_thr = []
    for t in TARGET_MARKET_HASHES:
        nk = normalize_name(t)
        tv = state.get("thresholds", {}).get(nk)
        if tv is None:
            target_thr.append(f"{t}: not set")
        else:
            target_thr.append(f"{t}: {int(tv)} units (${int(tv)/1000:.3f})")

    lines = [
        f"Stats window: {METRICS_WINDOW_SEC}s",
        f"Mode now: {mode_now}",
        f"Source share: WS {ws_pct:.1f}%, fallback {fb_pct:.1f}%, unknown {unk_pct:.1f}%",
        f"Signals: ws={signals_ws}, rest={signals_rest}",
        f"Skipped filtered: ws={skip_filtered_ws}, rest={skip_filtered_rest}",
        f"Skipped dedupe: ws={skip_dedupe_ws}, rest={skip_dedupe_rest}",
        f"Avg signal->attempt delay (AUTO): {avg_delay:.3f}s",
        f"Thresholds: {'; '.join(target_thr) if target_thr else '(no targets)'}",
    ]
    return "\n".join(lines)

# In-memory pending custom threshold requests: chat_id -> hash_name
awaiting_custom = {}
# Recent observed prices per item for floating threshold calculations
# structure: {name: [price_units, ...]}
state.setdefault("recent_prices", {})

# in-memory pending custom global threshold requests removed (no global manual)
awaiting_global = set()

# Persistent processed offer IDs (for dedupe and ignore). Keys are strings of numeric ids -> timestamp
PROCESSED_FILE = os.path.join(os.getcwd(), "processed_ids.json")
processed_ids = {}  # dict[str->int]
# TTL for processed ids (seconds)
PROCESSED_TTL = int(os.getenv("PROCESSED_TTL", str(10 * 60)))  # default 10 minutes


# Добавлена функция ensure_storage_files для проверки и создания необходимых файлов
def ensure_storage_files():
    """Ensure that state.json exists with default content."""
    try:
        if not os.path.exists(STATE_FILE):
            with open(STATE_FILE, "w", encoding="utf-8") as f:
                json.dump({
                    "mode": "MANUAL",
                    "thresholds": {},
                    "processed_alerts": {}
                }, f, ensure_ascii=False, indent=2)
            logger.info(f"Created missing state file: {STATE_FILE}")
        else:
            # Если файл существует, удалить поле "ignored", если оно есть
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                current_state = json.load(f)
            if "ignored" in current_state:
                del current_state["ignored"]
                with open(STATE_FILE, "w", encoding="utf-8") as f:
                    json.dump(current_state, f, ensure_ascii=False, indent=2)
                logger.info(f"Removed deprecated 'ignored' field from state file: {STATE_FILE}")
            else:
                logger.info(f"State file exists: {STATE_FILE}")
    except Exception:
        logger.exception("Failed to ensure state file")


def load_state():
    global state
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                state = json.load(f)
                # ensure keys
                state.setdefault("ignored", [])
                state.setdefault("confirmed", [])
    except Exception:
        logger.exception("Failed to load state.json")


def save_state(force: bool = False):
    """Save state.json to disk. Debounced by `STATE_SAVE_INTERVAL` unless `force=True`.

    Use `force=True` when immediate persistence is required (e.g., after recording a purchase).
    """
    try:
        global _last_state_save_ts
        now = int(time.time())
        if not force and _last_state_save_ts and (now - _last_state_save_ts) < int(os.getenv("STATE_SAVE_INTERVAL", "5")):
            # skip frequent saves
            return

        temp_file = f"{STATE_FILE}.tmp"
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        # Atomic replace
        os.replace(temp_file, STATE_FILE)
        _last_state_save_ts = now
        try:
            logger.info(f"State saved. thresholds keys={list(state.get('thresholds', {}).items())}")
            try:
                mtime = os.path.getmtime(STATE_FILE)
                logger.info(f"State file mtime: {mtime}")
            except Exception:
                pass
        except Exception:
            logger.info("State saved (failed to list thresholds)")

        # Write a small debug dump to inspect what was saved by this process
        try:
            dbg_file = f"{STATE_FILE}.debug"
            with open(dbg_file, "w", encoding="utf-8") as df:
                json.dump({"thresholds": state.get("thresholds", {}), "saved_at": now}, df, ensure_ascii=False, indent=2)
        except Exception:
            logger.exception("Failed to write debug state dump")
    except Exception:
        logger.exception("Failed to save state.json")


def load_processed_ids():
    global processed_ids
    try:
        if not os.path.exists(PROCESSED_FILE):
            processed_ids = {}
            save_processed_ids()  # Создать файл, если его нет
        else:
            with open(PROCESSED_FILE, "r", encoding="utf-8") as f:
                processed_ids = json.load(f)
                # ensure keys are strings and values are ints
                processed_ids = {str(k): int(v) for k, v in processed_ids.items()}
    except Exception:
        logger.exception("Failed to load processed_ids.json")


def save_processed_ids():
    try:
        temp_file = f"{PROCESSED_FILE}.tmp"
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(processed_ids, f, ensure_ascii=False, indent=2)
        os.replace(temp_file, PROCESSED_FILE)  # Атомарная замена
    except Exception:
        logger.exception("Failed to save processed_ids.json")


def cleanup_processed_ids(ttl: int = PROCESSED_TTL):
    """Remove entries older than ttl seconds."""
    try:
        now = int(time.time())
        removed = []
        for k, v in list(processed_ids.items()):
            if now - int(v) > ttl:
                removed.append(k)
                processed_ids.pop(k, None)
        if removed:
            logger.debug(f"Cleaned processed_ids: removed {len(removed)} entries")
            save_processed_ids()
    except Exception:
        logger.exception("Failed during cleanup of processed_ids")


def is_processed_offer(offer_id) -> bool:
    try:
        if offer_id is None:
            return False
        key = str(int(offer_id))
        ts = processed_ids.get(key)
        if not ts:
            return False
        # check TTL
        if int(time.time()) - int(ts) > PROCESSED_TTL:
            # expired
            processed_ids.pop(key, None)
            save_processed_ids()
            return False
        return True
    except Exception:
        return False


def add_processed_offer(offer_id):
    try:
        key = str(int(offer_id))
        processed_ids[key] = int(time.time())
        save_processed_ids()
    except Exception:
        logger.exception("Failed to add processed offer")


def make_item_key_from_raw(raw: dict) -> str:
    # Prefer stable ids if present
    if not raw:
        return ""
    for k in ("id", "name_id", "market_id"):
        v = raw.get(k)
        if v:
            return str(v)
    # fallback to market_hash_name
    name = raw.get("market_hash_name") or raw.get("name")
    if name:
        return name.strip().lower()
    return ""


def normalize_name(name: str) -> str:
    """Normalize item names for use as keys in state (case-insensitive)."""
    if not name:
        return ""
    try:
        return str(name).strip().lower()
    except Exception:
        return str(name).strip()


def make_item_key(data: dict, prefer: str = "id") -> str:
    # data may contain 'raw' (REST) or just 'name'/'price'
    raw = data.get("raw") if isinstance(data, dict) else None
    name = data.get("name") or data.get("market_hash_name") if isinstance(data, dict) else None

    # prefer by id: use id fields if available, else fallback to name
    if prefer == "id":
        if raw and isinstance(raw, dict):
            # prefer stable id fields
            for k in ("id", "name_id", "market_id"):
                v = raw.get(k)
                if v:
                    return str(v)
        # if no id available, return empty key so ignores apply strictly by ID
        # (do NOT fallback to name-based keys when IGNORE_BY='id')
        return ""

    # prefer by name: use market_hash_name if available, else ids
    if prefer == "name":
        if name:
            return name.strip().lower()
        if raw and isinstance(raw, dict):
            by_raw = make_item_key_from_raw(raw)
            if by_raw:
                return by_raw

    # last resort: hash of repr
    try:
        return hashlib.sha1(repr(data).encode()).hexdigest()[:12]
    except Exception:
        return ""


def extract_offer_id(raw) -> int | None:
    """Extract numeric offer_id from raw dict if present. Returns int or None."""
    if not raw or not isinstance(raw, dict):
        return None
    for k in ("offer_id", "id", "offerId", "listing_id", "item_id"):
        v = raw.get(k)
        if v is None:
            continue
        try:
            return int(v)
        except Exception:
            # try to extract digits
            s = str(v)
            digits = "".join(ch for ch in s if ch.isdigit())
            if digits:
                try:
                    return int(digits)
                except Exception:
                    continue
    return None


def _today_str():
    return time.strftime("%Y-%m-%d")


def can_spend(amount_usd):
    """Check daily and session spend limits. Returns (True, '') if allowed, else (False, reason)."""
    try:
        if amount_usd is None:
            return False, "invalid amount"
        today = _today_str()
        spent_today = float(state.setdefault("spent", {}).get(today, 0.0) or 0.0)
        session_spent = float(state.get("session_spent", 0.0) or 0.0)
        if DAILY_SPEND_LIMIT_USD and (spent_today + amount_usd) > DAILY_SPEND_LIMIT_USD:
            return False, "daily limit exceeded"
        if SESSION_SPEND_LIMIT_USD and (session_spent + amount_usd) > SESSION_SPEND_LIMIT_USD:
            return False, "session limit exceeded"
        return True, ""
    except Exception:
        logger.exception("can_spend failed")
        return False, "error"


def record_purchase(hash_name, price_units, offer_id=None, raw=None, source="manual"):
    """Append successful purchase to CSV and update spend counters in state."""
    try:
        price_usd = float(price_units) / 1000.0 if price_units is not None else 0.0
        ts = int(time.time())
        today = _today_str()
        header = ["timestamp", "iso", "name", "price_units", "price_usd", "offer_id", "source", "raw_json"]
        iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))
        row = [ts, iso, hash_name, price_units, f"{price_usd:.3f}", offer_id or "", source, json.dumps(raw or {}, ensure_ascii=False)]
        write_header = not os.path.exists(PURCHASE_LOG)
        with open(PURCHASE_LOG, "a", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            if write_header:
                writer.writerow(header)
            writer.writerow(row)

        # update state counters
        state["session_spent"] = float(state.get("session_spent", 0.0) or 0.0) + price_usd
        state.setdefault("spent", {})
        state["spent"][today] = float(state["spent"].get(today, 0.0) or 0.0) + price_usd
        # Force immediate save after recording a purchase
        save_state(force=True)
        logger.info(f"Recorded purchase: {hash_name} ${price_usd:.3f} to {PURCHASE_LOG}")
    except Exception:
        logger.exception("Failed to record purchase")

# Helpers for history parsing: price extraction, normalization, and purchase detection
def extract_price_raw(item):
    """Try to extract a raw price value from a history item dict.
    Returns the raw value (int/float/str/dict) or None.
    """
    try:
        if not item or not isinstance(item, dict):
            return None
        # common flat keys
        for k in ("price", "value", "cost", "sum", "amount", "price_usd", "money"):
            if k in item:
                v = item.get(k)
                if v is not None:
                    return v
        # nested containers
        for container in ("data", "raw", "offer", "item"):
            v = item.get(container)
            if isinstance(v, dict):
                for k in ("price", "value", "cost", "amount", "price_usd"):
                    if k in v:
                        return v.get(k)
        return None
    except Exception:
        return None


def normalize_price(raw_p):
    """Normalize extracted raw price into USD float when possible.
    Heuristics:
    - numeric values >1000 are treated as "units" and divided by 1000
    - numeric values <=1000 are treated as USD if fractional or plausible
    - strings are parsed to float after removing $/USD
    - dicts are inspected for common keys
    Returns float (USD) or None.
    """
    try:
        if raw_p is None:
            return None
        if isinstance(raw_p, (int, float)):
            num = float(raw_p)
            if num > 1000:
                return num / 1000.0
            return float(num)
        if isinstance(raw_p, str):
            s = raw_p.strip().replace(',', '.')
            s = s.replace('$', '').replace('USD', '').strip()
            try:
                num = float(s)
            except Exception:
                return None
            if num > 1000:
                return num / 1000.0
            return num
        if isinstance(raw_p, dict):
            for k in ("amount", "price", "value", "price_usd"):
                if k in raw_p:
                    return normalize_price(raw_p.get(k))
        return None
    except Exception:
        return None


def is_purchase_entry(item) -> bool:
    """Heuristic: determine whether a history entry represents a purchase."""
    try:
        if not item or not isinstance(item, dict):
            return False
        # explicit string indicators
        for k in ("type", "action", "side", "direction", "event", "status"):
            v = item.get(k)
            if isinstance(v, str) and "buy" in v.lower():
                return True
        # buyer presence or buyer id fields
        for k in ("buyer", "buyer_id", "user", "user_id"):
            if k in item and item.get(k):
                return True
        # fallback: presence of price + offer/listing id
        if any(k in item for k in ("price", "value")) and any(k in item for k in ("offer_id", "id", "listing_id", "item_id")):
            return True
        return False
    except Exception:
        return False

# Вызов ensure_storage_files перед загрузкой состояния
ensure_storage_files()

# Логирование путей к файлам
logger.info(f"State file path: {STATE_FILE}, exists: {os.path.exists(STATE_FILE)}")
logger.info(f"Processed IDs file path: {PROCESSED_FILE}, exists: {os.path.exists(PROCESSED_FILE)}")

# Загрузка состояния
load_state()

# Ensure some default keys exist in `state` to avoid KeyError and to use global THRESHOLD
state.setdefault("processed_alerts", {})
state.setdefault("mode", "MANUAL")
state.setdefault("thresholds", {})
state.setdefault("global_threshold", THRESHOLD)
state.setdefault("session_spent", 0.0)
state.setdefault("spent", {})

# Safe startup mode: clear per-item thresholds and force MANUAL mode unless disabled via env.
startup_changed = False
if RESET_THRESHOLDS_ON_START:
    if state.get("thresholds"):
        logger.info("RESET_THRESHOLDS_ON_START enabled: clearing saved per-item thresholds")
    state["thresholds"] = {}
    state["global_threshold"] = SAFE_START_THRESHOLD_UNITS
    startup_changed = True

if RESET_MODE_ON_START and state.get("mode") != "MANUAL":
    logger.info("RESET_MODE_ON_START enabled: forcing MANUAL mode on startup")
    state["mode"] = "MANUAL"
    startup_changed = True

if startup_changed:
    save_state(force=True)

# Normalize any existing threshold keys so lookups by normalized name work
try:
    thr = state.get("thresholds", {}) or {}
    new_thr = {}
    changed = False
    for k, v in thr.items():
        nk = normalize_name(k)
        # prefer existing value if collision
        if nk in new_thr and new_thr[nk] != v:
            logger.warning(f"Threshold key collision when normalizing '{k}' -> '{nk}'; keeping existing value {new_thr[nk]}")
            continue
        new_thr[nk] = v
        if nk != k:
            changed = True
    if changed:
        state["thresholds"] = new_thr
        save_state()
        logger.info("Normalized threshold keys in state.json to lowercase keys for consistent lookup")
except Exception:
    logger.exception("Failed to normalize threshold keys on startup")

async def get_ws_token(session):
    """Fetch WebSocket token using REST API."""
    try:
        status, text, _ct = await market_get_text(session, GET_WS_TOKEN_URL, params={"key": API_KEY}, timeout=15)
    except asyncio.CancelledError:
        # Allow cancellation to propagate gracefully during shutdown
        return None
    except Exception:
        logger.exception("get_ws_token request failed")
        return None

    # Log response details for debugging transient token errors.
    if DEBUG_MODE:
        logger.debug(f"get-ws-token status={status}, body={text[:500]}")

    if status != 200:
        logger.error(f"get-ws-token returned HTTP {status}")
        return None

    try:
        data = json.loads(text)
    except Exception:
        logger.error("get-ws-token returned non-JSON response")
        return None

    # API may return token under different keys.
    token = data.get("ws_token") or data.get("token")
    if token:
        return token

    logger.error(f"Failed to get WS token: {data}")
    return None

# Define a global aiohttp.ClientSession
session = None

async def init_session():
    global session
    if session is None:
        # Create a regular ClientSession (no proxy by default)
        session = aiohttp.ClientSession()

async def close_session():
    global session
    if session:
        await session.close()
        session = None

async def websocket_listener():
    """Connect to WebSocket and listen for market events."""
    await init_session()  # Ensure session is initialized

    # Start background poller for Telegram callback queries if configured
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        logger.info("Starting Telegram poller task (poll_telegram_updates)")
        asyncio.create_task(poll_telegram_updates())

        # Send a small startup notification to ensure bot/token/chat work
        async def _startup_ping():
            try:
                await send_simple_message(session, TELEGRAM_CHAT_ID, "Bot started and poller scheduled.")
                logger.info("Startup ping sent to Telegram chat")
            except Exception:
                logger.exception("Failed to send startup ping")

        asyncio.create_task(_startup_ping())

        # Also send the interactive menu at startup so user can enable AUTO/thresholds quickly
        async def _startup_menu():
            try:
                await handle_menu_command(session, TELEGRAM_CHAT_ID)
                logger.info("Startup menu sent to Telegram chat")
            except Exception:
                logger.exception("Failed to send startup menu")

        asyncio.create_task(_startup_menu())

    # Attempt to maintain a persistent WS connection with reconnects
    fallback_task = None
    retry_delay = 1
    load_processed_ids()
    cleanup_processed_ids()
    asyncio.create_task(periodic_processed_cleanup())

    while True:
        try:
            ws_token = await get_ws_token(session)
            if not ws_token:
                raise Exception("No ws_token received")

            # Add optional headers (User-Agent/Origin). websockets>=15 uses additional_headers.
            ws_headers = [("User-Agent", WS_USER_AGENT), ("Origin", WS_ORIGIN)]
            connect_kwargs = {"ping_interval": 20, "open_timeout": 10, WS_HEADERS_KWARG: ws_headers}
            ws_ctx = websockets.connect(f"{WS_URL}?token={ws_token}", **connect_kwargs)
            async with ws_ctx as websocket:
                # Stop fallback only after WS handshake succeeds.
                # This avoids blind gaps during token/handshake failures and backoff sleeps.
                if fallback_task and not fallback_task.done():
                    fallback_task.cancel()
                metrics_set_mode("ws")
                subscription_message = json.dumps({"type": "subscribe", "data": "public:items:730:usd"})
                await websocket.send(subscription_message)
                if DEBUG_MODE:
                    logger.debug(f"Sent subscription message: {subscription_message}")

                retry_delay = 1
                while True:
                    message = await websocket.recv()
                    if DEBUG_MODE:
                        try:
                            raw = message if isinstance(message, str) else message.decode(errors='ignore')
                        except Exception:
                            raw = str(message)
                        logger.debug(f"Received raw message: {raw[:500]}")

                    raw_msg = message if isinstance(message, str) else None
                    if raw_msg is None:
                        logger.debug("Received non-text WS frame")
                        continue

                    if raw_msg.startswith("a"):
                        payload = raw_msg[1:]
                        try:
                            parsed = json.loads(payload)
                            # SockJS "a[...]" can contain a batch of JSON strings/events.
                            if isinstance(parsed, list):
                                for event_entry in parsed:
                                    event_obj = event_entry
                                    if isinstance(event_entry, str):
                                        try:
                                            event_obj = json.loads(event_entry)
                                        except Exception:
                                            logger.debug(f"Failed to decode nested WS event: {event_entry[:200]}")
                                            continue
                                    process_event(event_obj)
                            else:
                                process_event(parsed)
                        except json.JSONDecodeError:
                            logger.error(f"Failed to decode JSON payload: {payload[:200]}")
                    elif raw_msg.startswith("o"):
                        logger.debug("SockJS open frame received.")
                    elif raw_msg.startswith("h"):
                        logger.debug("SockJS heartbeat frame received.")
                    else:
                        # Unknown frame format — log first to help debugging
                        logger.debug(f"Unknown WS frame: {raw_msg[:200]}")

        except Exception as e:
            logger.error(f"WebSocket error: {e}")
            # For token-fetch failures, retry faster to reduce blind time.
            err_text = str(e).lower()
            if "ws_token" in err_text:
                retry_delay = min(max(retry_delay, 1) + 1, 6)
            else:
                # jittered exponential backoff with cap for transport/protocol errors
                retry_delay = min(retry_delay * 2, 30)
            sleep_for = retry_delay + random.uniform(0, 1.5)
            logger.info(f"WS reconnect sleeping {sleep_for:.2f}s before retry")
            await asyncio.sleep(sleep_for)

        # Start fallback task if WebSocket fails
        if not fallback_task or fallback_task.done():
            fallback_task = asyncio.create_task(fallback_polling())

async def fallback_polling():
    """Fallback to REST API polling if WebSocket fails."""
    # Reuse global session to avoid creating/destroying client sessions frequently
    await init_session()
    global session
    metrics_set_mode("fallback")
    while True:
        try:
            status, text, _ct = await market_get_text(session, REST_PRICES_URL, params={"key": API_KEY}, timeout=20)
            if status != 200:
                logger.error(f"Fallback prices endpoint HTTP {status}")
                await asyncio.sleep(POLL_INTERVAL)
                continue
            try:
                data = json.loads(text)
            except Exception:
                logger.error(f"Fallback prices endpoint returned non-JSON body: {text[:200]}")
                await asyncio.sleep(POLL_INTERVAL)
                continue

            if data.get("success"):
                items = data.get("items", [])
                logger.info(f"Fallback: fetched {len(items)} items")
                if DEBUG_MODE and items:
                    try:
                        sample = items[:3]
                        logger.debug(f"Fallback sample items: {json.dumps(sample)[:1000]}")
                    except Exception:
                        logger.debug(f"Fallback sample (repr): {repr(items[:3])}")

                # Only process items matching any target in TARGET_MARKET_HASHES to reduce noise
                for item in items:
                    # extract offer id and skip if already processed
                    offer_id = extract_offer_id(item)
                    if offer_id and is_processed_offer(offer_id):
                        continue

                    name = (item.get("market_hash_name") or item.get("name") or "")
                    # filter by name_id or market_hash_name targets
                    if TARGET_NAME_ID:
                        if str(item.get("name_id")) != str(TARGET_NAME_ID):
                            continue
                    else:
                        if not any(t.lower() in name.lower() for t in TARGET_MARKET_HASHES):
                            continue

                    process_item(item)
            else:
                logger.error(f"Failed to fetch prices: {data}")
        except Exception as e:
            logger.error(f"Polling error: {e}")

        await asyncio.sleep(POLL_INTERVAL)

# Temporary storage for pending offers
pending_offers = {}
# Last AUTO trigger timestamps by normalized item name
last_auto_trigger_ts = {}

# Helper: enqueue offers for debounced AUTO buying
def enqueue_pending_auto(name: str, price_units: int, threshold_units: int = None, raw: dict | None = None):
    try:
        norm = normalize_name(name)
        bucket = int(time.time() // DEDUPE_BUCKET_SEC)
        key = f"{norm}|{bucket}"
        entry = pending_offers.setdefault(key, {"name": name, "bucket": bucket, "offers": [], "task": None, "threshold_units": threshold_units})
        # keep the lowest threshold if multiple enqueue calls provide different thresholds
        if threshold_units is not None:
            existing = entry.get("threshold_units")
            if existing is None or threshold_units < existing:
                entry["threshold_units"] = threshold_units
        entry["offers"].append({"price_units": int(price_units), "raw": raw, "signal_ts": time.time()})
        # schedule debounced task once
        if not entry.get("task"):
            entry["task"] = asyncio.create_task(debounced_auto_buy(key))
        logger.debug(f"Enqueued pending auto offer: key={key} price={price_units} total_offers={len(entry['offers'])}")
    except Exception:
        logger.exception("Failed to enqueue pending auto offer")


async def debounced_auto_buy(key: str):
    """Wait DEBOUNCE_SEC seconds and attempt a single purchase for aggregated offers keyed by name|bucket."""
    start_time = time.time()
    logger.info(f"Debounced auto-buy started for key={key} at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}")
    try:
        await asyncio.sleep(DEBOUNCE_SEC)
        entry = pending_offers.pop(key, None)
        if not entry:
            return
        name = entry.get("name")
        bucket = entry.get("bucket")
        offers = entry.get("offers", [])
        if not offers:
            return
        # choose cheapest offer
        cheapest = min(offers, key=lambda o: o.get("price_units", 10**12))
        price_units = int(cheapest.get("price_units"))
        logger.info(f"Debounced AUTO buy: attempting single purchase for '{name}' cheapest={price_units/1000.0:.3f} USD from {len(offers)} offers")
        try:
            earliest_signal_ts = min(float(o.get("signal_ts", start_time)) for o in offers)
            metrics_add_attempt_delay(max(0.0, time.time() - earliest_signal_ts))
        except Exception:
            pass

        # final safety check: only proceed if still in AUTO mode
        if state.get("mode") != "AUTO":
            logger.warning(f"Debounced AUTO buy aborted: current mode={state.get('mode')}")
            return

        # Reuse global session for notifications to avoid creating many ClientSession objects
        try:
            await init_session()
            s = session
            if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
                await send_simple_message(s, TELEGRAM_CHAT_ID, f"AUTO: aggregated {len(offers)} offers for {name}, buying cheapest ${price_units/1000.0:.3f}")
        except Exception:
            logger.exception("Failed to send debounced AUTO attempt message")

        # Determine threshold to use for fetching current offers (prefer stored threshold)
        threshold_units = entry.get("threshold_units") or price_units
        try:
            offers_now = await fetch_available_offers(name, threshold_units)
        except Exception:
            offers_now = []

        # Decide how many to attempt: if MAX_BATCH_BUY <= 0 -> buy all available; otherwise cap
        available = len(offers_now)
        if available == 0:
            to_buy = 1
        elif MAX_BATCH_BUY <= 0:
            to_buy = available
        else:
            to_buy = min(available, MAX_BATCH_BUY)

        bought_count = 0
        last_err = None
        purchases_made = []
        for i in range(to_buy):
            if state.get("mode") != "AUTO":
                logger.warning("Aborting batch buy: mode switched from AUTO")
                break
            target_offer = offers_now[i] if i < len(offers_now) else cheapest
            tgt_price = int(target_offer.get("price_units", price_units))

            # Re-check real balance before attempting each purchase to avoid "Not money" races
            try:
                current_balance = await check_balance()
            except Exception:
                current_balance = None
            if current_balance is not None and current_balance < (tgt_price / 1000.0):
                logger.warning(f"Batch buy stopped: insufficient balance for next lot. needed={tgt_price/1000.0:.3f}, balance={current_balance:.3f}")
                last_err = f"insufficient_balance:{current_balance}"
                break
            if current_balance is None:
                logger.warning("Balance check unavailable before buy; proceeding to buy endpoint")

            # Check spend limits stored in state
            allowed, reason = can_spend(tgt_price / 1000.0)
            if not allowed:
                logger.warning(f"Batch buy stopped by spend limits: {reason}")
                last_err = reason
                break

            # Attempt purchase
            ok, res = await buy_item(name, tgt_price, source="auto", raw=target_offer.get("raw") if isinstance(target_offer, dict) else None)
            if ok:
                bought_count += 1
                try:
                    purchases_made.append({"price_units": tgt_price, "id": res, "ts": int(time.time())})
                except Exception:
                    purchases_made.append({"price_units": tgt_price, "id": res})
                # Small delay between successive buys to reduce race with other buyers/provider throttling
                try:
                    await asyncio.sleep(0.4)
                except Exception:
                    pass
            else:
                last_err = res
                logger.warning(f"Batch buy attempt failed for {name} at {tgt_price/1000.0:.3f}: {res}")
                err_text = str(res).lower()
                is_server_7 = (
                    err_text.strip() == "7"
                    or "server 7" in err_text
                    or "error 7" in err_text
                    or "ошибка сервера 7" in err_text
                )
                if is_server_7:
                    logger.info(
                        "Skipping invalid cheapest lot due to server error 7; trying next offer in batch"
                    )
                    continue
                break

        if bought_count:
            logger.info(f"Debounced AUTO buys completed for {name}: bought {bought_count}/{to_buy} lots")
            try:
                now_ts = int(time.time())
                for o in offers:
                    a_key = f"{name}|{o.get('price_units')}|{bucket}"
                    state.setdefault("processed_alerts", {})[a_key] = now_ts
                save_state()
            except Exception:
                logger.exception("Failed to mark aggregated offers processed")
            # Send detailed report to Telegram (one line per purchase)
            try:
                await init_session()
                s = session
                lines = [f"AUTO batch report: {bought_count} bought for {name}"]
                total = 0.0
                for idx, p in enumerate(purchases_made, start=1):
                    pu = int(p.get("price_units", 0))
                    total += (pu / 1000.0)
                    pid = p.get("id") or "-"
                    tstr = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(p.get("ts", int(time.time()))))
                    lines.append(f"{idx}. ${pu/1000.0:.3f} id={pid} at {tstr}")
                lines.append(f"Total: ${total:.3f}")
                text = "\n".join(lines)
                # If too long, send as document
                if len(text) > 3500:
                    # write temp file
                    fp = os.path.join(os.getcwd(), f"auto_report_{int(time.time())}.txt")
                    with open(fp, "w", encoding="utf-8") as tf:
                        tf.write(text)
                    await send_document(s, TELEGRAM_CHAT_ID, fp, filename=os.path.basename(fp))
                    try:
                        os.remove(fp)
                    except Exception:
                        pass
                else:
                    await send_simple_message(s, TELEGRAM_CHAT_ID, text)
            except Exception:
                logger.exception("Failed to send detailed AUTO batch report")
        else:
            logger.warning(f"Debounced AUTO buy failed for {name}: {last_err}")
            try:
                async with aiohttp.ClientSession() as s:
                    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
                        await send_simple_message(s, TELEGRAM_CHAT_ID, f"AUTO: failed to buy {name}: {last_err}")
            except Exception:
                logger.exception("Failed to send debounced AUTO failure message")
    except Exception:
        logger.exception("Error in debounced_auto_buy")
    finally:
        end_time = time.time()
        logger.info(f"Debounced auto-buy ended for key={key} at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}")
        logger.info(f"Debounced auto-buy duration: {end_time - start_time:.2f} seconds")


async def fetch_available_offers(name: str, max_price_units: int):
    """Fetch current REST listings and return list of offers for `name` with price_units <= max_price_units.
    Returns list of dicts with keys including 'price_units' and 'raw'."""
    start_time = time.time()
    try:
        await init_session()
        s = session
        status, text, _ct = await market_get_text(s, REST_PRICES_URL, params={"key": API_KEY}, timeout=20)
        if status != 200:
            return []
        try:
            data = json.loads(text)
        except Exception:
            return []

        if not data.get("success"):
            return []
        items = data.get("items") or []
        matches = []
        norm = normalize_name(name)
        for it in items:
            try:
                it_name = it.get("market_hash_name") or it.get("name") or ""
                if normalize_name(it_name) != norm:
                    continue
                raw_price = it.get("price") or it.get("value")
                if raw_price is None:
                    continue
                p = float(raw_price)
                pu = int(p * 1000)
                if pu <= int(max_price_units):
                    offer_id = extract_offer_id(it)
                    if offer_id and is_processed_offer(offer_id):
                        continue
                    matches.append({"price_units": pu, "raw": it, "offer_id": offer_id})
            except Exception:
                continue
        matches.sort(key=lambda x: x.get("price_units", 10**12))
        return matches
    except Exception:
        logger.exception("Failed to fetch available offers")
        return []
    finally:
        end_time = time.time()
        logger.info(f"fetch_available_offers duration: {end_time - start_time:.2f} seconds")

# Update process_event to handle the cheapest lot logic
def process_event(event):
    """Process a WebSocket event."""
    try:
        if not isinstance(event, dict):
            metrics_inc("skip_filtered_ws")
            return

        name = event.get("market_hash_name") or event.get("name") or ""
        raw_price = event.get("price") or event.get("value")
        if raw_price is None:
            metrics_inc("skip_filtered_ws")
            return

        try:
            price_float = float(raw_price)
            price_units = int(price_float * 1000)
        except Exception:
            metrics_inc("skip_filtered_ws")
            return

        # Use normalized name for recent prices and thresholds
        norm = normalize_name(name)
        recent = state.setdefault("recent_prices", {}).setdefault(norm, [])
        recent.append(price_units)
        # keep last 20 prices
        if len(recent) > 20:
            recent[:] = recent[-20:]
        # compute baseline as median of recent prices
        try:
            sorted_recent = sorted(recent)
            mid = len(sorted_recent) // 2
            if len(sorted_recent) % 2 == 1:
                baseline = sorted_recent[mid]
            else:
                baseline = (sorted_recent[mid - 1] + sorted_recent[mid]) // 2
        except Exception:
            baseline = THRESHOLD

        # floating margin percent (default 10%) — notify if price is below baseline * (1 - margin)
        floating_margin = float(state.get("floating_margin_pct", 0.10))

        cfg_threshold = state.get("thresholds", {}).get(norm)
        if REQUIRE_EXPLICIT_THRESHOLD and cfg_threshold is None:
            metrics_inc("skip_filtered_ws")
            logger.debug(f"WS skip for '{name}': explicit per-item threshold required")
            return
        try:
            if cfg_threshold is not None:
                effective_threshold_units = int(cfg_threshold)
            else:
                effective_threshold_units = int(baseline * (1.0 - floating_margin))
        except Exception:
            effective_threshold_units = THRESHOLD

        # Apply hard safety cap from environment so restart cannot expand buy limit unexpectedly.
        if HARD_MAX_BUY_UNITS > 0:
            effective_threshold_units = min(effective_threshold_units, HARD_MAX_BUY_UNITS)
        # persist recent prices periodically
        save_state()

        # If price (in units) is higher than the effective threshold, skip
        # Log decision details for debugging
        logger.debug(f"WS check for '{name}': price_units={price_units}, effective_threshold_units={effective_threshold_units}, cfg_threshold={cfg_threshold}")
        if price_units > effective_threshold_units:
            metrics_inc("skip_filtered_ws")
            logger.debug(f"Skipping WS item '{name}': price {price_units} > threshold {effective_threshold_units}")
            return

        # Защита от спама
        timestamp_bucket = int(time.time() // DEDUPE_BUCKET_SEC)
        alert_key = f"{name}|{price_units}|{timestamp_bucket}"

        if state.get("mode") == "AUTO":
            now_ts = time.time()
            prev_ts = float(last_auto_trigger_ts.get(norm, 0.0) or 0.0)
            if AUTO_RECHECK_INTERVAL_SEC > 0 and (now_ts - prev_ts) < AUTO_RECHECK_INTERVAL_SEC:
                metrics_inc("skip_dedupe_ws")
                return
            last_auto_trigger_ts[norm] = now_ts
            metrics_inc("signal_ws")
            # Aggregate near-simultaneous offers and debounce actual buy
            enqueue_pending_auto(name, price_units, threshold_units=effective_threshold_units, raw=event)
        else:
            if alert_key in state["processed_alerts"]:
                metrics_inc("skip_dedupe_ws")
                return
            state["processed_alerts"][alert_key] = int(time.time())
            metrics_inc("signal_ws")
            save_state()
            # Notify asynchronously
            asyncio.create_task(notify_telegram({
                "source": "ws",
                "name": name,
                "price": price_float,
                "lot_key": alert_key,
                "raw": event
            }))
    except Exception:
        logger.exception("Error processing WS event")

# Логика AUTO-режима
async def handle_auto_mode(name, price_units):
    """Handle automatic purchase logic."""
    try:
        async with aiohttp.ClientSession() as session:
            # Получаем текущий баланс пользователя
            async with session.get(f"https://market.csgo.com/api/v2/get-money", params={"key": API_KEY}) as response:
                data = await response.json()
                if data.get("success"):
                    balance_usd = float(data.get("money", 0))
                    # Проверяем, хватает ли баланса для покупки
                    if balance_usd >= (price_units / 1000):
                        # Логируем попытку покупки и уведомляем кратко (без кнопок)
                        logger.info(f"AUTO found: {name} for {price_units / 1000:.2f} USD. Attempting purchase...")
                        try:
                            if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
                                await send_simple_message(session, TELEGRAM_CHAT_ID, f"AUTO: attempting to buy {name} for ${price_units/1000:.3f}")
                            else:
                                print(f"AUTO: attempting to buy {name} for ${price_units/1000:.3f}")
                        except Exception:
                            logger.exception("Failed to send AUTO attempt message")

                        # Check spend limits before attempting
                        allowed, reason = can_spend(price_units / 1000.0)
                        if not allowed:
                            logger.warning(f"AUTO purchase blocked by limits: {reason}")
                            try:
                                if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
                                    await send_simple_message(session, TELEGRAM_CHAT_ID, f"AUTO: blocked purchase of {name} for ${price_units/1000:.3f}: {reason}")
                                else:
                                    print(f"AUTO blocked: {reason}")
                            except Exception:
                                logger.exception("Failed to send AUTO block message")
                            return

                        # Perform purchase via centralized function
                        ok, res = await buy_item(name, price_units, source="auto", raw={"price_units": price_units})
                        if ok:
                            try:
                                if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
                                    await send_simple_message(session, TELEGRAM_CHAT_ID, f"AUTO: bought {name} for ${price_units/1000:.3f}")
                                else:
                                    print(f"AUTO: bought {name} for ${price_units/1000:.3f}")
                            except Exception:
                                logger.exception("Failed to send AUTO success message")
                            return
                        else:
                            logger.warning(f"Не удалось выполнить покупку: {res}")
                            try:
                                if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
                                    await send_simple_message(session, TELEGRAM_CHAT_ID, f"AUTO: failed to buy {name}: {res}")
                                else:
                                    print(f"AUTO: failed to buy {name}: {res}")
                            except Exception:
                                logger.exception("Failed to send AUTO failure message")
                else:
                    logger.warning("Не удалось получить баланс пользователя")
    except Exception as e:
        logger.exception(f"Ошибка в логике автозакупки: {e}")


async def handle_telegram_callback(data_cd, cq_id, session, cq=None):
    """Handle Telegram callback actions."""
    try:
        # determine chat id from callback_query message if available
        chat_id = str(TELEGRAM_CHAT_ID)
        try:
            if isinstance(cq, dict) and cq.get("message") and cq["message"].get("chat"):
                chat_id = str(cq["message"]["chat"].get("id", chat_id))
        except Exception:
            pass

        logger.debug(f"handle_telegram_callback called: data_cd={data_cd} cq_id={cq_id} chat_id={chat_id}")

        if data_cd.startswith("set_mode:"):
            mode = data_cd.split(":", 1)[1]
            state["mode"] = mode
            save_state()
            await answer_callback(session, cq_id, f"Mode set to {mode}")
            await handle_menu_command(session, chat_id)  # Обновить меню
            return
        if data_cd == "toggle_mode":
            # Toggle between AUTO and MANUAL
            current = state.get("mode", "MANUAL")
            new = "AUTO" if current != "AUTO" else "MANUAL"
            state["mode"] = new
            save_state()
            await answer_callback(session, cq_id, f"Mode set to {new}")
            await handle_menu_command(session, chat_id)
            return
        # accept both 'set_custom|' and 'set_custom:' formats
        if data_cd.startswith("set_custom:") or data_cd.startswith("set_custom|"):
            # user clicked Custom -> ask them to send a price in chat
            try:
                parts = data_cd.replace(":", "|").split("|", 2)
                hash_enc = parts[1]
            except Exception:
                await answer_callback(session, cq_id, "Invalid custom request.")
                return
            hash_name = urllib.parse.unquote_plus(hash_enc)
            # remember that this chat expects a custom price for this hash_name
            awaiting_custom[chat_id] = hash_name
            # Debug: log awaiting_custom after setting
            try:
                logger.debug(f"awaiting_custom set for chat {chat_id}: hash_name={hash_name}. keys={list(awaiting_custom.keys())}")
            except Exception:
                logger.debug("awaiting_custom updated (debug failed to format keys)")
            await answer_callback(session, cq_id, f"Send custom price (USD) for {hash_name} in chat now.")
            # Send a ForceReply so Telegram clients prompt the user to reply and update will contain the message
            try:
                fr_payload = {"chat_id": chat_id, "text": f"Please reply with a price in USD for '{hash_name}', e.g. 0.29", "reply_markup": json.dumps({"force_reply": True, "selective": True})}
                send_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
                async with session.post(send_url, json=fr_payload) as fr_resp:
                    fr_text = await fr_resp.text()
                    if fr_resp.status != 200:
                        logger.error(f"Failed to send force-reply prompt: {fr_resp.status} {fr_text}")
                    else:
                        logger.debug(f"Force-reply prompt sent: {fr_text[:200]}")
            except Exception:
                logger.exception("Failed to send force-reply prompt")
            return

        # No global manual threshold: ignore any set_global* callbacks
        if data_cd.startswith("set_global"):
            await answer_callback(session, cq_id, "Global manual threshold disabled. Use per-item Custom buttons.")
            return

        parts = data_cd.split("|", 2)
        action = parts[0]
        hash_enc = parts[1] if len(parts) > 1 else ""
        limit = parts[2] if len(parts) > 2 else "0"
        hash_name = urllib.parse.unquote_plus(hash_enc) if hash_enc else ""
        try:
            limit_units = int(limit)
        except Exception:
            limit_units = 0
        logger.debug(f"Parsed callback parts: action={action}, hash_name={hash_name}, limit_units={limit_units}")

        if action == "confirm_buy":
            async with session.get(f"https://market.csgo.com/api/v2/get-money", params={"key": API_KEY}) as response:
                data = await response.json()
                logger.debug(f"get-money response for confirm_buy: {data}")
                if data.get("success"):
                            # API returns money as USD float (e.g. 18.191)
                            balance_usd = float(data.get("money", 0))
                            logger.debug(f"Balance (USD): {balance_usd}, limit_units: {limit_units}, limit_USD: {limit_units/1000:.3f}")
                            if balance_usd >= (limit_units / 1000):
                                # Check spend limits
                                allowed, reason = can_spend(limit_units / 1000.0)
                                if not allowed:
                                    await answer_callback(session, cq_id, "Blocked by spend limits")
                                    await send_simple_message(session, chat_id, f"Purchase blocked: {reason}")
                                else:
                                    ok, res = await buy_item(hash_name, limit_units, source="manual_confirm")
                                    logger.debug(f"buy response: {res}")
                                    if ok:
                                        await answer_callback(session, cq_id, "Purchase successful.")
                                        await send_simple_message(session, chat_id, f"Successfully bought {hash_name} for {limit_units / 1000:.2f} USD.")
                                    else:
                                        await answer_callback(session, cq_id, "Purchase failed.")
                                        await send_simple_message(session, chat_id, f"Failed to buy {hash_name}: {res}")
                            else:
                                await answer_callback(session, cq_id, "Insufficient funds.")
                                await send_simple_message(session, chat_id, "Insufficient funds for purchase.")
                else:
                            await answer_callback(session, cq_id, "Failed to fetch balance.")
        elif action == "set_threshold":
            # Set per-item threshold (limit is in units) using normalized key
            state.setdefault("thresholds", {})[normalize_name(hash_name)] = limit_units
            save_state()
            await answer_callback(session, cq_id, f"Threshold for {hash_name} set to {limit_units/1000:.3f}$")
            await handle_menu_command(session, TELEGRAM_CHAT_ID)
        elif action == "clear_threshold":
            if "thresholds" in state and normalize_name(hash_name) in state["thresholds"]:
                state["thresholds"].pop(normalize_name(hash_name), None)
                save_state()
                await answer_callback(session, cq_id, f"Threshold for {hash_name} cleared.")
            else:
                await answer_callback(session, cq_id, "No threshold was set for this item.")
            await handle_menu_command(session, TELEGRAM_CHAT_ID)
        elif action == "ignore":
            await answer_callback(session, cq_id, "Ignored.")
    except Exception:
        logger.exception("Error handling callback")

# Обновлён poll_telegram_updates для упрощения фильтра чата
async def poll_telegram_updates():
    """Long-poll Telegram getUpdates to handle callback_query actions."""
    logger.info("poll_telegram_updates starting")
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        logger.warning("Telegram token or chat id not configured; poll_telegram_updates exiting")
        return

    # Check webhook info first — if a webhook is set, getUpdates will not return updates.
    try:
        async with aiohttp.ClientSession() as _s:
            wh_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getWebhookInfo"
            async with _s.get(wh_url, timeout=5) as wh_resp:
                wh = await wh_resp.json()
                if wh and wh.get("ok") and wh.get("result") and wh["result"].get("url"):
                    logger.warning("Telegram webhook is set — getUpdates may not receive callbacks. Attempting to delete webhook.")
                    # Try to delete the webhook so getUpdates will work
                    try:
                        del_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook"
                        async with _s.get(del_url, timeout=5) as del_resp:
                            del_res = await del_resp.json()
                            logger.info(f"deleteWebhook response: {del_res}")
                    except Exception as e:
                        logger.exception(f"Failed to delete webhook: {e}")
                else:
                    # Even if no webhook URL is set, the bot's allowed_updates may be restricted
                    # (e.g., only ['callback_query']). Ensure Telegram will deliver 'message' updates
                    # by calling setWebhook with empty URL and explicit allowed_updates.
                    try:
                        set_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/setWebhook"
                        payload = {"url": "", "allowed_updates": ["message", "callback_query"]}
                        async with _s.post(set_url, json=payload, timeout=5) as set_resp:
                            set_res = await set_resp.json()
                            logger.info(f"setWebhook(empty) response: {set_res}")
                    except Exception:
                        logger.exception("Failed to call setWebhook to ensure allowed_updates include messages")
                # Send short diagnostic summary to Telegram to confirm poller startup
                try:
                    info = wh.get("result", {}) if isinstance(wh, dict) else {}
                    url_str = info.get("url") if isinstance(info, dict) else None
                    pending = info.get("pending_update_count") if isinstance(info, dict) else None
                    diag = f"WebhookInfo: url={url_str}, pending_updates={pending}"
                    await send_simple_message(_s, TELEGRAM_CHAT_ID, diag)
                except Exception:
                    logger.exception("Failed to send webhook diag message")

                # Also perform a single getUpdates call and report result count
                try:
                    gu_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
                    async with _s.get(gu_url, params={"timeout": 1}, timeout=5) as gu_resp:
                        gu = await gu_resp.json()
                        cnt = len(gu.get("result", [])) if isinstance(gu, dict) else 0
                        await send_simple_message(_s, TELEGRAM_CHAT_ID, f"getUpdates initial: ok={gu.get('ok')}, count={cnt}")
                except Exception:
                    logger.exception("Failed to fetch/send initial getUpdates diag")
    except Exception:
        logger.debug("Failed to fetch webhook info; proceeding with getUpdates")

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    offset = None
    async with aiohttp.ClientSession() as tg_session:
        logger.debug(f"poll_telegram_updates: created tg_session id={id(tg_session)} closed={getattr(tg_session,'closed',None)}")
        while True:
            try:
                params = {"timeout": 20}
                logger.debug(f"poll_telegram_updates: using tg_session id={id(tg_session)} closed={getattr(tg_session,'closed',None)} offset={offset}")
                if offset:
                    params["offset"] = offset
                async with tg_session.get(url, params=params, timeout=30) as resp:
                    data = await resp.json()
                    # Debug: log full getUpdates response and current offset
                    try:
                        logger.debug(f"getUpdates response offset={offset} raw={json.dumps(data)[:4000]}")
                    except Exception:
                        logger.debug("getUpdates response (failed to json.dumps)")
                    if not data.get("ok"):
                        await asyncio.sleep(1)
                        continue
                    for upd in data.get("result", []):
                        offset = upd.get("update_id") + 1
                        logger.debug(f"Telegram update received: {json.dumps(upd)[:1000]}")
                        cq = upd.get("callback_query")
                        if cq:
                            cq_id = cq.get("id")
                            data_cd = cq.get("data")
                            await handle_telegram_callback(data_cd, cq_id, tg_session, cq)
                            continue

                        msg = upd.get("message")
                        if not msg:
                            continue
                        chat = msg.get("chat", {})
                        chat_id_msg = str(chat.get("id"))
                        # Log incoming message text for debugging
                        text_msg = (msg.get("text") or "").strip()
                        logger.debug(f"Message from chat {chat_id_msg}: {text_msg}")
                        # Debug: log incoming chat id and awaiting_custom keys
                        try:
                            logger.debug(f"Incoming message chat={chat_id_msg}, text='{text_msg}' awaiting_custom_keys={list(awaiting_custom.keys())}")
                        except Exception:
                            logger.debug(f"Incoming message chat={chat_id_msg}, text present, awaiting_custom debug failed")

                        # If this chat was awaiting a custom price (ForceReply), handle it first
                        try:
                            if chat_id_msg in awaiting_custom and text_msg:
                                hash_name = awaiting_custom.pop(chat_id_msg)
                                # parse price like 0.29 or 0,29
                                try:
                                    price_val = float(text_msg.replace(',', '.'))
                                    units = int(round(price_val * 1000))
                                    nk = normalize_name(hash_name)
                                    state.setdefault("thresholds", {})[nk] = units
                                    save_state()
                                    await send_simple_message(tg_session, chat_id_msg, f"Порог для '{hash_name}' установлен: {price_val:.3f}$ ({units} units)")
                                except Exception:
                                    await send_simple_message(tg_session, chat_id_msg, "Неверный формат цены. Отправь, например, 0.29")
                                continue
                        except Exception:
                            logger.exception("Error handling awaiting_custom reply")

                        if text_msg.startswith("/menu"):
                            logger.info(f"Invoking handle_menu_command for chat {chat_id_msg}")
                            await handle_menu_command(tg_session, chat_id_msg)
                        elif text_msg.startswith("/purchases"):
                            # /purchases [N] - send whole CSV or last N records as text
                            try:
                                parts = text_msg.split()
                                n = None
                                if len(parts) > 1:
                                    try:
                                        n = int(parts[1])
                                    except Exception:
                                        n = None
                                if not os.path.exists(PURCHASE_LOG):
                                    await send_simple_message(tg_session, chat_id_msg, "No purchases recorded yet.")
                                elif n is None:
                                    await send_document(tg_session, chat_id_msg, PURCHASE_LOG, filename=os.path.basename(PURCHASE_LOG))
                                else:
                                    # read last N rows
                                    rows = []
                                    try:
                                        with open(PURCHASE_LOG, "r", encoding="utf-8") as f:
                                            reader = list(csv.reader(f))
                                            if len(reader) <= 1:
                                                await send_simple_message(tg_session, chat_id_msg, "No purchases recorded yet.")
                                            else:
                                                header = reader[0]
                                                data_rows = reader[1:]
                                                tail = data_rows[-n:]
                                                text = ",".join(header) + "\n"
                                                for r in tail:
                                                    text += ",".join(str(x) for x in r) + "\n"
                                                # Telegram messages have size limits; truncate if too long
                                                if len(text) > 3500:
                                                    text = text[-3500:]
                                                await send_simple_message(tg_session, chat_id_msg, text)
                                    except Exception:
                                        logger.exception("Failed to read purchases CSV")
                                        await send_simple_message(tg_session, chat_id_msg, "Failed to read purchases file.")
                            except Exception:
                                logger.exception("Failed to handle /purchases command")
                                await send_simple_message(tg_session, chat_id_msg, "Failed to send purchases file.")
                        elif text_msg.startswith("/balance"):
                            logger.info(f"Invoking handle_balance_command for chat {chat_id_msg}")
                            await handle_balance_command(tg_session, chat_id_msg)
                        elif text_msg.startswith("/set"):
                            logger.info(f"Processing /set raw: {text_msg}")
                            # Remote set: /set "Item Name" 0.29  OR  /set Item Name 0.29 (name may contain spaces)
                            import re
                            # Try quoted name or single-word name first
                            m = re.match(r'/set\s+(?:"([^"]+)"|([^\s"]+))\s+([0-9]+(?:[\.,][0-9]+)?)', text_msg)
                            if m:
                                item = m.group(1) or m.group(2)
                                num = m.group(3).replace(',', '.')
                            else:
                                # Fallback: accept anything until last whitespace+number as item name
                                m2 = re.match(r'/set\s+(.+)\s+([0-9]+(?:[\.,][0-9]+)?)\s*$', text_msg)
                                if not m2:
                                    await send_simple_message(tg_session, chat_id_msg, "Usage: /set \"Item Name\" 0.29  OR /set Item Name 0.29")
                                    continue
                                item = m2.group(1).strip()
                                num = m2.group(2).replace(',', '.')

                            logger.info(f"/set parsed: item='{item}' num='{num}'")
                            try:
                                price_val = float(num)
                                units = int(round(price_val * 1000))
                                nk = normalize_name(item)
                                old = state.get("thresholds", {}).get(nk)
                                state.setdefault("thresholds", {})[nk] = units
                                logger.info(f"/set command: chat={chat_id_msg} item='{item}' nk='{nk}' old={old} new={units}")
                                save_state()
                                # get latest price if any
                                latest = None
                                try:
                                    recent = state.get("recent_prices", {}).get(nk, [])
                                    if recent:
                                        latest = recent[-1] / 1000.0
                                except Exception:
                                    latest = None
                                if latest is None:
                                    status = "Нет данных о последней цене."
                                else:
                                    status = (f"Последняя цена ${latest:.3f} ≤ порог; уведомления будут срабатывать." if latest <= price_val
                                              else f"Последняя цена ${latest:.3f} > порог; уведомления не будут до снижения цены.")
                                await send_simple_message(tg_session, chat_id_msg, f"Порог для '{item}' установлен: {price_val:.3f}$ ({units} units). {status}\nStored key: '{nk}'")
                            except Exception:
                                logger.exception("Exception while processing /set")
                                await send_simple_message(tg_session, chat_id_msg, "Не удалось установить порог. Укажите число, например 0.29")
                        elif text_msg.startswith("/margin"):
                            # Set floating margin percent: /margin 0.05  (5%) or /margin 5 (means 5%)
                            try:
                                parts = text_msg.split()
                                if len(parts) < 2:
                                    raise ValueError()
                                raw = parts[1].replace(',', '.')
                                val = float(raw)
                                if val > 1:
                                    val = val / 100.0
                                state["floating_margin_pct"] = float(val)
                                save_state()
                                await send_simple_message(tg_session, chat_id_msg, f"Плавающая маржа установлена: {state['floating_margin_pct']:.4f} ({state['floating_margin_pct']*100:.2f}%)")
                            except Exception:
                                await send_simple_message(tg_session, chat_id_msg, "Usage: /margin 0.05  OR /margin 5 for 5%")
                        elif text_msg.startswith("/show"):
                            # Show current thresholds and margin
                            try:
                                thr = state.get("thresholds", {})
                                lines = [f"{k}: {v/1000:.3f}$ ({v} units)" for k, v in thr.items()]
                                margin = state.get("floating_margin_pct", 0.10)
                                header = f"Floating margin: {margin} ({margin*100:.2f}%)\n"
                                body = "\n".join(lines) if lines else "(no thresholds set)"
                                await send_simple_message(tg_session, chat_id_msg, header + body)
                            except Exception:
                                await send_simple_message(tg_session, chat_id_msg, "Failed to read thresholds")
                        elif text_msg.startswith("/stats"):
                            try:
                                await send_simple_message(tg_session, chat_id_msg, build_stats_text())
                            except Exception:
                                logger.exception("Failed to handle /stats")
                                await send_simple_message(tg_session, chat_id_msg, "Failed to build stats report")
                        elif text_msg.startswith("/history_terminal_success") or text_msg.startswith("/history_day") or text_msg.startswith("/history_all") or text_msg.startswith("/history_from") or text_msg.startswith("/history"):
                            # Supported commands:
                            # /history_terminal_success [DD.MM.YYYY] [DD.MM.YYYY]
                            # /history_day
                            # /history_all
                            # /history_from DD.MM.YYYY [DD.MM.YYYY]
                            # /history day|all
                            try:
                                cmd = (text_msg or "").strip().lower()
                                scope = None
                                if cmd.startswith("/history_terminal_success"):
                                    parts_raw = (text_msg or "").strip().split()
                                    if len(parts_raw) == 1:
                                        lt = time.localtime(int(time.time()))
                                        start_ts = int(time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, 0, 0, 0, lt.tm_wday, lt.tm_yday, lt.tm_isdst)))
                                        end_ts = int(time.time())
                                        label = "terminal success today"
                                    else:
                                        try:
                                            start_ts = parse_history_date_to_ts(parts_raw[1])
                                            end_ts = int(time.time())
                                            if len(parts_raw) >= 3:
                                                end_ts = parse_history_date_to_ts(parts_raw[2]) + 86399
                                            if end_ts < start_ts:
                                                await send_simple_message(tg_session, chat_id_msg, "End date must be >= start date")
                                                continue
                                            label = f"terminal success from {parts_raw[1]}" + (f" to {parts_raw[2]}" if len(parts_raw) >= 3 else " to now")
                                        except Exception:
                                            await send_simple_message(tg_session, chat_id_msg, "Usage: /history_terminal_success [DD.MM.YYYY] [DD.MM.YYYY]")
                                            continue

                                    await send_simple_message(tg_session, chat_id_msg, f"Preparing history export ({label})...")
                                    await export_operation_history_to_csv(
                                        tg_session,
                                        chat_id_msg,
                                        scope="custom",
                                        start_ts=start_ts,
                                        end_ts=end_ts,
                                        scope_label=label,
                                        success_buys_only=True,
                                        target_only=True,
                                        pretty=True,
                                    )
                                    continue
                                elif cmd.startswith("/history_day"):
                                    scope = "day"
                                elif cmd.startswith("/history_all"):
                                    scope = "all"
                                elif cmd.startswith("/history_from"):
                                    parts_raw = (text_msg or "").strip().split()
                                    if len(parts_raw) < 2:
                                        await send_simple_message(tg_session, chat_id_msg, "Usage: /history_from DD.MM.YYYY [DD.MM.YYYY]")
                                        continue
                                    try:
                                        start_ts = parse_history_date_to_ts(parts_raw[1])
                                        end_ts = int(time.time())
                                        if len(parts_raw) >= 3:
                                            end_ts = parse_history_date_to_ts(parts_raw[2]) + 86399
                                        if end_ts < start_ts:
                                            await send_simple_message(tg_session, chat_id_msg, "End date must be >= start date")
                                            continue
                                        label = f"from {parts_raw[1]} to {parts_raw[2]}" if len(parts_raw) >= 3 else f"from {parts_raw[1]} to now"
                                        await send_simple_message(tg_session, chat_id_msg, f"Preparing history export ({label})...")
                                        await export_operation_history_to_csv(
                                            tg_session,
                                            chat_id_msg,
                                            scope="custom",
                                            start_ts=start_ts,
                                            end_ts=end_ts,
                                            scope_label=label,
                                        )
                                    except Exception:
                                        await send_simple_message(tg_session, chat_id_msg, "Invalid date format. Use DD.MM.YYYY, DD-MM-YYYY, or UNIX timestamp")
                                    continue
                                else:
                                    parts = cmd.split()
                                    if len(parts) >= 2 and parts[1] in ("day", "today"):
                                        scope = "day"
                                    elif len(parts) >= 2 and parts[1] in ("all", "full"):
                                        scope = "all"
                                    else:
                                        scope = "day"

                                await send_simple_message(tg_session, chat_id_msg, f"Preparing history export ({scope})...")
                                await export_operation_history_to_csv(tg_session, chat_id_msg, scope=scope)
                            except Exception as e:
                                logger.exception("Ошибка при обработке команды /history")
                                try:
                                    await send_simple_message(tg_session, chat_id_msg, f"Ошибка при получении истории: {str(e)[:220]}")
                                except Exception:
                                    logger.exception("Failed to notify user about /history error")
            except Exception as e:
                logger.exception("poll_telegram_updates exception")
                await asyncio.sleep(1)


async def answer_callback(session, callback_query_id, text):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery"
    try:
        async with session.post(url, json={"callback_query_id": callback_query_id, "text": text, "show_alert": False}) as r:
            return await r.json()
    except Exception as e:
        logger.error(f"Failed to answer callback: {e}")


async def periodic_processed_cleanup():
    """Background task to periodically cleanup processed_ids."""
    while True:
        try:
            cleanup_processed_ids()
        except Exception:
            logger.exception("Error in periodic_processed_cleanup")
        await asyncio.sleep(PROCESSED_TTL // 2)


async def send_simple_message(session, chat_id, text):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        global _tg_last_send_ts, _tg_block_until_ts
        try:
            sess_id = id(session)
            sess_closed = getattr(session, 'closed', None)
        except Exception:
            sess_id = None
            sess_closed = None

        now = time.time()
        # Drop repeated AUTO status messages in a short window.
        # This prevents floods when market repeatedly returns the same failure.
        if isinstance(text, str) and text.startswith("AUTO:"):
            key = f"{chat_id}|{text}"
            last_ts = float(_tg_auto_recent_msgs.get(key, 0.0) or 0.0)
            if TELEGRAM_AUTO_DUPLICATE_WINDOW_SEC > 0 and (now - last_ts) < TELEGRAM_AUTO_DUPLICATE_WINDOW_SEC:
                logger.debug(
                    "send_simple_message skipped duplicate AUTO message (%.1fs window): %s",
                    float(TELEGRAM_AUTO_DUPLICATE_WINDOW_SEC),
                    text[:200],
                )
                return {"ok": True, "skipped": "duplicate-auto-message"}
            _tg_auto_recent_msgs[key] = now
            # Keep memory bounded.
            if len(_tg_auto_recent_msgs) > 500:
                cutoff = now - max(float(TELEGRAM_AUTO_DUPLICATE_WINDOW_SEC), 60.0)
                _tg_auto_recent_msgs_keys = list(_tg_auto_recent_msgs.keys())
                for k in _tg_auto_recent_msgs_keys:
                    if float(_tg_auto_recent_msgs.get(k, 0.0) or 0.0) < cutoff:
                        _tg_auto_recent_msgs.pop(k, None)

        payload = {"chat_id": chat_id, "text": text}
        masked = (TELEGRAM_BOT_TOKEN[:6] + "..." + TELEGRAM_BOT_TOKEN[-6:]) if TELEGRAM_BOT_TOKEN else "<no-token>"
        logger.debug(f"send_simple_message -> URL={url} token={masked} payload_keys={list(payload.keys())} session_id={sess_id} session_closed={sess_closed}")
        async with _tg_send_lock:
            now = time.time()
            if _tg_block_until_ts > now:
                remain = _tg_block_until_ts - now
                logger.warning("Telegram send is temporarily blocked by previous 429; skipping message for %.1fs", remain)
                return {"ok": False, "error": "telegram-429-cooldown", "retry_after": remain}

            if TELEGRAM_MIN_SEND_INTERVAL_SEC > 0:
                wait_for = TELEGRAM_MIN_SEND_INTERVAL_SEC - (now - _tg_last_send_ts)
                if wait_for > 0:
                    await asyncio.sleep(wait_for)

            async with session.post(url, json=payload) as r:
                body = await r.text()
                _tg_last_send_ts = time.time()
                if r.status != 200:
                    logger.error(f"send_simple_message failed: {r.status} {body}")
                    if r.status == 429:
                        retry_after = None
                        try:
                            parsed = json.loads(body)
                            retry_after = float(parsed.get("parameters", {}).get("retry_after", 0) or 0)
                        except Exception:
                            retry_after = 0
                        if retry_after and retry_after > 0:
                            _tg_block_until_ts = max(_tg_block_until_ts, time.time() + retry_after)
                            logger.warning("Telegram 429 received; pausing sends for %.1fs", retry_after)
                else:
                    logger.debug(f"send_simple_message ok: {body[:200]}")
                try:
                    return json.loads(body)
                except Exception:
                    return {"ok": False, "raw": body}
    except Exception as e:
        logger.error(f"Failed to send simple message: {e}")
        return {"ok": False, "error": str(e)}


async def send_document(session, chat_id, file_path, filename=None):
    """Send a file to Telegram chat via sendDocument."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    try:
        fname = filename or os.path.basename(file_path)
        with open(file_path, "rb") as f:
            data = aiohttp.FormData()
            data.add_field("chat_id", str(chat_id))
            data.add_field("document", f, filename=fname, content_type="application/octet-stream")
            async with session.post(url, data=data, timeout=60) as resp:
                body = await resp.text()
                if resp.status != 200:
                    logger.error(f"send_document failed: {resp.status} {body}")
                else:
                    logger.debug(f"send_document ok: {body[:200]}")
                try:
                    return json.loads(body)
                except Exception:
                    return {"ok": False, "raw": body}
    except Exception:
        logger.exception("Failed to send document")
        return {"ok": False, "error": "exception"}


def _history_event_name(entry):
    try:
        return str((entry or {}).get("event") or "unknown").lower()
    except Exception:
        return "unknown"


def _history_entry_time(entry):
    try:
        val = (entry or {}).get("time")
        if val is None:
            val = (entry or {}).get("settlement")
        return int(float(val)) if val is not None else 0
    except Exception:
        return 0


def _history_entries_from_payload(payload):
    if isinstance(payload, dict):
        return payload.get("data") or payload.get("items") or payload.get("result") or []
    if isinstance(payload, list):
        return payload
    return []


def _history_dedupe_key(entry):
    try:
        if not isinstance(entry, dict):
            return json.dumps(entry, ensure_ascii=False, sort_keys=True)
        return "|".join([
            str(entry.get("time") or ""),
            str(entry.get("event") or ""),
            str(entry.get("item_id") or entry.get("id") or ""),
            str(entry.get("market_hash_name") or ""),
            str(entry.get("paid") or entry.get("received") or entry.get("amount") or ""),
        ])
    except Exception:
        return str(entry)


def _safe_int(v, default=0):
    try:
        if v is None or v == "":
            return default
        return int(float(v))
    except Exception:
        return default


def _stage_label(stage_val):
    s = str(stage_val)
    if s == "1":
        return "new"
    if s == "2":
        return "item_given"
    if s == "5":
        return "timed_out"
    return s or "unknown"


def _format_ts(ts_val):
    ts = _safe_int(ts_val, 0)
    if ts <= 0:
        return ""
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
    except Exception:
        return str(ts)


def _is_target_name(name: str):
    norm = normalize_name(name or "")
    if not norm:
        return False
    for t in TARGET_MARKET_HASHES:
        tn = normalize_name(t)
        if not tn:
            continue
        if tn == norm or tn in norm or norm in tn:
            return True
    return False


def _entry_is_success_buy(item, target_only: bool = False):
    if not isinstance(item, dict):
        return False
    ev = str(item.get("event") or "").lower()
    if ev != "buy":
        return False
    stage = str(item.get("stage") or "")
    if stage != "2":
        return False
    if target_only:
        name = item.get("market_hash_name") or item.get("name") or ""
        return _is_target_name(name)
    return True


def parse_history_date_to_ts(raw: str):
    """Parse date string to UNIX timestamp.

    Supported formats:
    - DD.MM.YYYY
    - DD-MM-YYYY
    - UNIX timestamp (seconds)
    """
    s = (raw or "").strip()
    if not s:
        raise ValueError("empty date")

    if s.isdigit():
        # Accept unix timestamp directly.
        return int(s)

    for fmt in ("%d.%m.%Y", "%d-%m-%Y"):
        try:
            tm = time.strptime(s, fmt)
            return int(time.mktime((tm.tm_year, tm.tm_mon, tm.tm_mday, 0, 0, 0, tm.tm_wday, tm.tm_yday, tm.tm_isdst)))
        except Exception:
            continue

    raise ValueError("unsupported date format")


async def fetch_history_range(session_obj, endpoint: str, start_ts: int, end_ts: int, depth: int = 0):
    """Fetch history endpoint for [start_ts, end_ts] with adaptive splitting."""
    if end_ts < start_ts:
        return [], 0, 0

    req_count = 0
    payload = None
    ok = False
    attempts = max(1, HISTORY_FETCH_RETRIES)
    for attempt in range(1, attempts + 1):
        status, text, _ct = await market_get_text(
            session_obj,
            endpoint,
            params={"key": API_KEY, "date": int(start_ts), "date_end": int(end_ts)},
            timeout=25,
        )
        req_count += 1

        if status != 200:
            logger.warning(f"History endpoint HTTP {status} for range {start_ts}-{end_ts} (attempt {attempt}/{attempts}): {text[:200]}")
            if attempt < attempts:
                await asyncio.sleep(HISTORY_RETRY_BASE_SEC * attempt)
            continue

        try:
            payload = json.loads(text)
        except Exception:
            logger.warning(f"History endpoint non-JSON for range {start_ts}-{end_ts} (attempt {attempt}/{attempts}): {text[:200]}")
            if attempt < attempts:
                await asyncio.sleep(HISTORY_RETRY_BASE_SEC * attempt)
            continue

        if isinstance(payload, dict) and payload.get("success") is False:
            logger.warning(f"History endpoint returned success=false for range {start_ts}-{end_ts} (attempt {attempt}/{attempts}): {str(payload)[:300]}")
            if attempt < attempts:
                await asyncio.sleep(HISTORY_RETRY_BASE_SEC * attempt)
            continue

        ok = True
        break

    if not ok or payload is None:
        # Mark range as failed so caller can report partial result.
        return [], req_count, 1

    entries = _history_entries_from_payload(payload)
    if not isinstance(entries, list):
        entries = []

    span = int(end_ts) - int(start_ts)
    should_split_by_span = (
        HISTORY_MAX_RANGE_SEC > 0
        and span > HISTORY_MAX_RANGE_SEC
        and depth < HISTORY_MAX_SPLIT_DEPTH
    )
    should_split_by_density = (
        len(entries) >= HISTORY_SPLIT_THRESHOLD
        and span > HISTORY_MIN_SPLIT_SEC
        and depth < HISTORY_MAX_SPLIT_DEPTH
    )

    if should_split_by_span or should_split_by_density:
        mid = int(start_ts + (span // 2))
        left, left_req, left_failed = await fetch_history_range(session_obj, endpoint, int(start_ts), mid, depth + 1)
        right, right_req, right_failed = await fetch_history_range(session_obj, endpoint, mid + 1, int(end_ts), depth + 1)
        req_count += left_req + right_req

        merged = []
        seen = set()
        for item in left + right:
            k = _history_dedupe_key(item)
            if k in seen:
                continue
            seen.add(k)
            merged.append(item)
        return merged, req_count, (left_failed + right_failed)

    return entries, req_count, 0


async def fetch_operation_history_range(session_obj, start_ts: int, end_ts: int, depth: int = 0):
    return await fetch_history_range(
        session_obj,
        "https://market.csgo.com/api/v2/operation-history",
        start_ts,
        end_ts,
        depth,
    )


async def fetch_trade_history_range(session_obj, start_ts: int, end_ts: int, depth: int = 0):
    return await fetch_history_range(
        session_obj,
        "https://market.csgo.com/api/v2/history",
        start_ts,
        end_ts,
        depth,
    )


async def export_operation_history_to_csv(
    tg_session,
    chat_id: str,
    scope: str,
    start_ts: int | None = None,
    end_ts: int | None = None,
    scope_label: str | None = None,
    success_buys_only: bool = False,
    target_only: bool = False,
    pretty: bool = False,
):
    """Export operation-history by scope: day|all|custom."""
    now_ts = int(time.time())
    if start_ts is None or end_ts is None:
        if scope == "day":
            lt = time.localtime(now_ts)
            start_ts = int(time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, 0, 0, 0, lt.tm_wday, lt.tm_yday, lt.tm_isdst)))
            end_ts = now_ts
        else:
            start_ts = int(now_ts - (HISTORY_ALL_LOOKBACK_DAYS * 86400))
            end_ts = now_ts

    if end_ts < start_ts:
        raise ValueError("end date is earlier than start date")

    scope_text = scope_label or scope

    await init_session()
    s = session

    source_endpoint = "operation-history"
    entries, req_count, failed_ranges = await fetch_operation_history_range(s, start_ts, end_ts)
    # Fallback for providers/accounts where operation-history can be sparse on wide ranges.
    if not entries and (scope == "all" or scope == "custom"):
        try:
            entries_alt, req_alt, failed_alt = await fetch_trade_history_range(s, start_ts, end_ts)
            req_count += req_alt
            failed_ranges += failed_alt
            if entries_alt:
                entries = entries_alt
                source_endpoint = "history"
        except Exception:
            logger.exception("Fallback /history request failed")

    # For accuracy in filtered target-success mode, merge both sources and dedupe.
    if success_buys_only and target_only and (scope == "all" or scope == "custom"):
        try:
            entries_alt2, req_alt2, failed_alt2 = await fetch_trade_history_range(s, start_ts, end_ts)
            req_count += req_alt2
            failed_ranges += failed_alt2
            if entries_alt2:
                merged = []
                seen = set()
                for it in entries + entries_alt2:
                    k = _history_dedupe_key(it)
                    if k in seen:
                        continue
                    seen.add(k)
                    merged.append(it)
                entries = merged
                source_endpoint = "operation-history+history"
        except Exception:
            logger.exception("Merge fallback /history request failed")

    if not entries:
        await send_simple_message(
            tg_session,
            chat_id,
            f"История пуста для диапазона {scope_text}. Попробуй уменьшить окно (например /history_day) или указать конкретную дату через /history_from DD.MM.YYYY.",
        )
        return

    if success_buys_only:
        filtered = [e for e in entries if _entry_is_success_buy(e, target_only=target_only)]
        entries = filtered
        if not entries:
            what = "успешные покупки" + (" по целевым предметам" if target_only else "")
            await send_simple_message(tg_session, chat_id, f"За диапазон {scope_text} не найдено: {what}.")
            return

    entries = sorted(entries, key=_history_entry_time, reverse=True)
    buys = sum(1 for e in entries if _history_event_name(e) == "buy")
    sells = sum(1 for e in entries if _history_event_name(e) == "sell")
    checkouts = sum(1 for e in entries if _history_event_name(e) == "checkout")
    total_paid_units = sum(_safe_int((e or {}).get("paid") or (e or {}).get("price"), 0) for e in entries if isinstance(e, dict))

    ts = int(time.time())
    suffix = ""
    if success_buys_only and target_only:
        suffix = "_terminal_success"
    elif success_buys_only:
        suffix = "_success_buys"
    filename = f"operation_history_{scope}{suffix}_{ts}.csv"
    filepath = os.path.join(os.getcwd(), filename)
    with open(filepath, "w", encoding="utf-8", newline="") as csvf:
        writer = csv.writer(csvf)
        if pretty:
            writer.writerow([
                "datetime",
                "item",
                "paid_usd",
                "stage",
                "app",
                "item_id",
                "settlement_datetime",
            ])
        else:
            writer.writerow([
                "time",
                "event",
                "market_hash_name",
                "item_id",
                "app",
                "stage",
                "settlement",
                "paid",
                "received",
                "amount",
                "currency",
                "causer",
                "refund_seller",
                "refund_market",
                "raw_json",
            ])
        for item in entries:
            if not isinstance(item, dict):
                if pretty:
                    writer.writerow(["", "", "", "", "", "", ""])
                else:
                    writer.writerow(["", "", "", "", "", "", "", "", "", "", "", "", "", "", json.dumps(item, ensure_ascii=False)])
                continue
            refund = item.get("refund") or {}
            refund_seller = None
            refund_market = None
            try:
                if isinstance(refund, dict):
                    refund_seller = (refund.get("seller") or {}).get("amount")
                    refund_market = (refund.get("market") or {}).get("amount")
            except Exception:
                pass
            if pretty:
                paid_units = _safe_int(item.get("paid") or item.get("price"), 0)
                writer.writerow([
                    _format_ts(item.get("time")),
                    item.get("market_hash_name") or item.get("name") or "",
                    f"{paid_units/1000.0:.3f}",
                    _stage_label(item.get("stage")),
                    item.get("app"),
                    item.get("item_id") or item.get("id"),
                    _format_ts(item.get("settlement")),
                ])
            else:
                writer.writerow([
                    item.get("time"),
                    item.get("event"),
                    item.get("market_hash_name"),
                    item.get("item_id") or item.get("id"),
                    item.get("app"),
                    item.get("stage"),
                    item.get("settlement"),
                    item.get("paid") or item.get("price"),
                    item.get("received"),
                    item.get("amount"),
                    item.get("currency"),
                    item.get("causer"),
                    refund_seller,
                    refund_market,
                    json.dumps(item, ensure_ascii=False),
                ])

    mode_label = "success-buys" if success_buys_only else "all-events"
    if success_buys_only and target_only:
        mode_label = "terminal-success"
    summary = (
        f"History scope={scope_text}. Mode={mode_label}. Entries: {len(entries)}. "
        f"buy={buys}, sell={sells}, checkout={checkouts}. "
        f"total_paid=${total_paid_units/1000.0:.3f}. source={source_endpoint}. API requests: {req_count}."
    )
    if failed_ranges > 0:
        summary += f" WARNING: partial result, failed_ranges={failed_ranges}."
    await send_simple_message(tg_session, chat_id, summary)
    await send_document(tg_session, chat_id, filepath, filename=filename)

    try:
        os.remove(filepath)
    except Exception:
        pass

async def get_balance():
    """Fetch balance from the API."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://market.csgo.com/api/v2/get-money", params={"key": API_KEY}) as response:
                data = await response.json()
                if data.get("success"):
                    # API returns money as USD float (e.g. 18.191)
                    balance = float(data.get("money", 0))
                    logger.info(f"Current balance: ${balance}")
                    return balance
                else:
                    logger.error(f"Failed to fetch balance: {data}")
                    return 0
    except Exception as e:
        logger.exception(f"Error fetching balance: {e}")
        return 0

# Функция для переключения режима и обработки команды /menu
async def handle_menu_command(session, chat_id):
    """Send menu with mode switching buttons."""
    try:
        mode = state.get("mode", "MANUAL")
        # Mode toggle buttons (also provide a single toggle button)
        mode_buttons = [
            {"text": f"Mode: AUTO {'✅' if mode == 'AUTO' else ''}", "callback_data": "set_mode:AUTO"},
            {"text": f"Mode: MANUAL {'✅' if mode == 'MANUAL' else ''}", "callback_data": "set_mode:MANUAL"}
        ]
        toggle_text = "Enable AUTO" if mode != "AUTO" else "Disable AUTO"
        toggle_button = [{"text": toggle_text, "callback_data": "toggle_mode"}]

        # Per-target: do not show preset price buttons or threshold labels; only provide Custom and Clear
        keyboard = [mode_buttons, toggle_button]
        for t in TARGET_MARKET_HASHES:
            row = []
            # Provide Clear and Custom input button only
            row.append({"text": "Clear", "callback_data": f"clear_threshold|{urllib.parse.quote_plus(t)}|0"})
            row.append({"text": "Custom", "callback_data": f"set_custom|{urllib.parse.quote_plus(t)}|0"})
            keyboard.append(row)

        reply_markup = {"inline_keyboard": keyboard}
        # Build payload that includes a descriptive text plus reply_markup
        payload = {"chat_id": chat_id, "text": "Select mode and per-item thresholds:", "reply_markup": json.dumps(reply_markup)}
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        async with session.post(url, json=payload) as resp:
            if resp.status != 200:
                logger.error(f"Failed to send menu: {resp.status} {await resp.text()}")
    except Exception:
        logger.exception("Failed to handle /menu command")

# Обновлённая функция для обработки команды /balance
async def handle_balance_command(session, chat_id):
    """Fetch and send balance information."""
    try:
        # Request detailed balance info
        async with aiohttp.ClientSession() as s:
            async with s.get("https://market.csgo.com/api/v2/get-money", params={"key": API_KEY}) as resp:
                data = await resp.json()
                logger.debug(f"get-money response for /balance: {data}")
                if data.get("success"):
                    money = float(data.get("money", 0))
                    money_settlement = data.get("money_settlement")
                    text = f"Balance: ${money:.3f} USD"
                    if money_settlement is not None:
                        try:
                            text += f"\nSettlement: ${float(money_settlement):.3f} USD"
                        except Exception:
                            text += f"\nSettlement: {money_settlement}"
                else:
                    text = "Failed to fetch balance."
        await send_simple_message(session, chat_id, text)
    except Exception:
        logger.exception("Error fetching balance")
        await send_simple_message(session, chat_id, "Error fetching balance.")

# Функция для проверки баланса
async def check_balance():
    """Check the current balance, fetching from the API if necessary."""
    global _last_balance_error_log_ts, _last_known_balance_usd, _last_known_balance_ts
    try:
        await init_session()
        s = session
        status, text, _ct = await market_get_text(
            s,
            "https://market.csgo.com/api/v2/get-money",
            params={"key": API_KEY},
            timeout=12,
        )

        if status != 200:
            now = int(time.time())
            if now - _last_balance_error_log_ts >= 30:
                logger.warning(f"Balance endpoint HTTP {status}; body={text[:200]}")
                _last_balance_error_log_ts = now
            return None

        try:
            data = json.loads(text)
        except Exception:
            now = int(time.time())
            if now - _last_balance_error_log_ts >= 30:
                logger.warning(f"Balance endpoint returned non-JSON body: {text[:200]}")
                _last_balance_error_log_ts = now
            return None

        if data.get("success"):
            # API returns money as USD float
            bal = float(data.get("money", 0))
            _last_known_balance_usd = bal
            _last_known_balance_ts = int(time.time())
            return bal

        now = int(time.time())
        if now - _last_balance_error_log_ts >= 30:
            logger.warning(f"Failed to fetch balance: {data}")
            _last_balance_error_log_ts = now
        return None
    except Exception as e:
        now = int(time.time())
        if now - _last_balance_error_log_ts >= 30:
            logger.warning(f"Error fetching balance: {e}")
            _last_balance_error_log_ts = now
        return None

# Функция для выполнения покупки с проверкой баланса
async def buy_item_with_balance_check(hash_name: str, price: int):
    """Check balance before attempting to buy an item."""
    try:
        balance = await check_balance()
        if balance is None:
            logger.warning("Balance check unavailable; continuing with buy attempt")
            balance = float("inf")
        if balance >= price / 1000:
            # Check spend limits
            allowed, reason = can_spend(price / 1000.0)
            if not allowed:
                logger.warning(f"Purchase blocked by limits: {reason}")
                return
            success, result = await buy_item(hash_name, price, source="balance_check")
            if success:
                logger.info(f"Purchase successful: {result}")
            else:
                logger.error(f"Purchase failed: {result}")
        else:
            logger.warning("Insufficient balance for purchase.")
    except Exception:
        logger.exception("Error during purchase with balance check.")

# Функция для выполнения покупки
async def buy_cheapest_by_hash_name(hash_name, threshold_units):
    """Attempt to buy the cheapest lot by hash_name and price limit."""
    try:
        # reuse buy_item which handles logging and recording
        ok, res = await buy_item(hash_name, threshold_units, source="auto")
        if ok:
            return True, "Purchase successful."
        return False, res
    except Exception:
        logger.exception("Error during purchase")
        return False, "Error during purchase."

async def buy_item(hash_name: str, price: int, source: str = "manual", raw=None):
    """Покупка предмета через API market.csgo.com."""
    # Safety: do not allow automatic-source purchases when mode is not AUTO
    try:
        current_mode = state.get("mode", "MANUAL")
    except Exception:
        current_mode = "MANUAL"
    logger.debug(f"buy_item called: name={hash_name} price={price} source={source} mode={current_mode}")
    if source == "auto" and current_mode != "AUTO":
        logger.warning(f"Blocked auto purchase for {hash_name} because mode={current_mode}")
        return False, "blocked: manual mode"
    url = f"https://market.csgo.com/api/v2/buy"
    params = {"key": API_KEY, "hash_name": hash_name, "price": price}
    try:
        # Reuse global session to reduce overhead and keep cookies/headers consistent
        await init_session()
        s = session
        headers = {"User-Agent": WS_USER_AGENT, "Origin": WS_ORIGIN}
        status, text, content_type = await market_get_text(s, url, params=params, headers=headers, timeout=15)
        if status != 200:
            logger.error(f"Buy endpoint HTTP {status}; body={text[:300]}")
            return False, f"http-{status}"

        # If server returned HTML (e.g., rate-limit page or captcha), log full text for debugging
        if "application/json" not in content_type:
            logger.error(f"Buy endpoint returned non-JSON content-type: {content_type}; body={text[:1000]}")
            # Try to surface useful message
            return False, f"unexpected content-type {content_type}: {text[:200]}"

        try:
            data = json.loads(text)
        except Exception:
            logger.error(f"Failed to parse JSON from buy response: {text[:1000]}")
            return False, "invalid-json-response"

        if data.get("success"):
            logger.info(f"Successfully bought {hash_name} for {price / 1000:.2f} USD")
            try:
                record_purchase(hash_name, price, offer_id=data.get("id"), raw=raw, source=source)
                try:
                    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
                        await send_simple_message(s, TELEGRAM_CHAT_ID, f"Purchase recorded: {hash_name} for ${price/1000:.3f} (id: {data.get('id')})")
                except Exception:
                    logger.exception("Failed to send purchase summary message")
            except Exception:
                logger.exception("Failed recording purchase after buy")
            return True, data.get("id")

        # Not success
        err = data.get("error") or data
        logger.error(f"Failed to buy {hash_name}: {err}")
        # If provider says Not money, re-check balance to log real balance and avoid blind retries
        if isinstance(err, str) and "Not money" in err:
            try:
                bal = await check_balance()
                if bal is None:
                    logger.warning("Buy failed due to Not money; balance check unavailable")
                    return False, "Not money; balance=unknown"
                logger.warning(f"Buy failed due to Not money; balance reported: {bal}")
                return False, f"Not money; balance={bal}"
            except Exception:
                return False, "Not money"

        return False, err
    except Exception as e:
        logger.exception("Error during purchase")
        return False, str(e)

# Добавлено определение функции process_item

def process_item(item):
    """Process an item from REST API response."""
    try:
        name = item.get("market_hash_name") or item.get("name")
        if not name:
            metrics_inc("skip_filtered_rest")
            return

        if TARGET_NAME_ID:
            if item.get("name_id") != TARGET_NAME_ID:
                metrics_inc("skip_filtered_rest")
                return
        else:
            if not any(t.lower() in name.lower() for t in TARGET_MARKET_HASHES):
                metrics_inc("skip_filtered_rest")
                return

        raw_price = item.get("price") or item.get("value")
        if raw_price is None:
            metrics_inc("skip_filtered_rest")
            return
        try:
            price_float = float(raw_price)
        except Exception:
            metrics_inc("skip_filtered_rest")
            return

        # Determine effective threshold per-item (units) using recent prices (floating)
        price_units = int(price_float * 1000)
        norm = normalize_name(name)
        recent = state.setdefault("recent_prices", {}).setdefault(norm, [])
        recent.append(price_units)
        if len(recent) > 20:
            recent[:] = recent[-20:]
        try:
            sorted_recent = sorted(recent)
            mid = len(sorted_recent) // 2
            if len(sorted_recent) % 2 == 1:
                baseline = sorted_recent[mid]
            else:
                baseline = (sorted_recent[mid - 1] + sorted_recent[mid]) // 2
        except Exception:
            baseline = THRESHOLD
        floating_margin = float(state.get("floating_margin_pct", 0.10))
        cfg_threshold = state.get("thresholds", {}).get(norm)
        if REQUIRE_EXPLICIT_THRESHOLD and cfg_threshold is None:
            metrics_inc("skip_filtered_rest")
            logger.debug(f"REST skip for '{name}': explicit per-item threshold required")
            save_state()
            return
        try:
            if cfg_threshold is not None:
                effective_threshold_units = int(cfg_threshold)
            else:
                effective_threshold_units = int(baseline * (1.0 - floating_margin))
        except Exception:
            effective_threshold_units = THRESHOLD

        # Apply hard safety cap from environment so restart cannot expand buy limit unexpectedly.
        if HARD_MAX_BUY_UNITS > 0:
            effective_threshold_units = min(effective_threshold_units, HARD_MAX_BUY_UNITS)
        logger.debug(f"REST check for '{name}': price_units={price_units}, effective_threshold_units={effective_threshold_units}, cfg_threshold={cfg_threshold}, baseline={baseline}, floating_margin={floating_margin}, norm={norm}")
        if price_units > effective_threshold_units:
            metrics_inc("skip_filtered_rest")
            logger.debug(f"Skipping REST item '{name}': price {price_units} > threshold {effective_threshold_units}")
            save_state()
            return

        # Dedupe key used for MANUAL notifications.
        timestamp_bucket = int(time.time() // DEDUPE_BUCKET_SEC)
        alert_key = f"{name}|{price_units}|{timestamp_bucket}"

        # If AUTO mode is enabled, aggregate offers for debounced buy; otherwise notify
        if state.get("mode") == "AUTO":
            now_ts = time.time()
            prev_ts = float(last_auto_trigger_ts.get(norm, 0.0) or 0.0)
            if AUTO_RECHECK_INTERVAL_SEC > 0 and (now_ts - prev_ts) < AUTO_RECHECK_INTERVAL_SEC:
                metrics_inc("skip_dedupe_rest")
                return
            last_auto_trigger_ts[norm] = now_ts
            metrics_inc("signal_rest")
            enqueue_pending_auto(name, price_units, threshold_units=effective_threshold_units, raw=item)
        else:
            if alert_key in state["processed_alerts"]:
                metrics_inc("skip_dedupe_rest")
                return
            state["processed_alerts"][alert_key] = int(time.time())
            metrics_inc("signal_rest")
            save_state()
            asyncio.create_task(notify_telegram({"source": "rest", "name": name, "price": price_float, "lot_key": alert_key, "raw": item}))
    except Exception:
        logger.exception("Error processing REST item")

# Добавлено определение функции notify_telegram

async def notify_telegram(data):
    """Schedule Telegram notification (non-blocking)."""
    name = data.get("name") or data.get("market_hash_name") or "<unknown>"
    price = data.get("price")
    source = data.get("source", "unknown")

    # Generate alert_id
    alert_id = data.get("lot_key") or str(uuid.uuid4())

    # Determine recommendation without extra API calls to avoid noisy errors during upstream outages.
    try:
        price_units = int(float(price) * 1000)
        norm = normalize_name(name)
        cfg_threshold = state.get("thresholds", {}).get(norm)
        if cfg_threshold is not None:
            effective_threshold_units = int(cfg_threshold)
        else:
            effective_threshold_units = THRESHOLD
        if HARD_MAX_BUY_UNITS > 0:
            effective_threshold_units = min(effective_threshold_units, HARD_MAX_BUY_UNITS)
        recommendation = "BUY" if price_units <= effective_threshold_units else "DO NOT BUY"
    except Exception:
        price_units = None
        recommendation = "unknown"

    text = f"{name}\nprice: ${price} ({price_units or 'N/A'} units)\nrecommendation: {recommendation}\nsource: {source}"

    logger.info(f"[Telegram] Item matched: name={name}, price={price}, recommendation={recommendation}")

    # Include alert_id in the message
    text = f"{text}\nalert_id: {alert_id}\nfilter_by_name_id: {bool(TARGET_NAME_ID)}"

    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        include_buttons = False if source == "auto" else True
        asyncio.create_task(send_telegram(text, name, alert_id, price_units, include_buttons=include_buttons))
    else:
        # If Telegram not configured, just print the message
        print(text)

# Добавлено определение функции send_telegram
async def send_telegram(text: str, name: str, alert_id: str, price_units: int, include_buttons: bool = True):
    """Send message via Telegram bot API (async) with optional inline buttons.

    When `include_buttons` is False (e.g. for AUTO notifications) only the "Open on Market"
    button will be sent to avoid presenting Confirm/Ignore actions.
    """
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    market_url = build_market_search_url(name)
    if include_buttons:
        reply_markup = {
            "inline_keyboard": [
                [
                    {"text": "Open on Market", "url": market_url}
                ],
                [
                    {"text": "Confirm Buy", "callback_data": f"confirm_buy|{urllib.parse.quote_plus(name)}|{price_units}"},
                    {"text": "Ignore", "callback_data": f"ignore|{urllib.parse.quote_plus(name)}|{price_units}"}
                ]
            ]
        }
    else:
        reply_markup = {
            "inline_keyboard": [
                [
                    {"text": "Open on Market", "url": market_url}
                ]
            ]
        }
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "reply_markup": json.dumps(reply_markup)}
    try:
        masked = (TELEGRAM_BOT_TOKEN[:6] + "..." + TELEGRAM_BOT_TOKEN[-6:]) if TELEGRAM_BOT_TOKEN else "<no-token>"
        logger.debug(f"send_telegram -> URL={url} token={masked} chat_id={TELEGRAM_CHAT_ID} payload_keys={list(payload.keys())}")
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=10) as resp:
                body = await resp.text()
                if resp.status != 200:
                    logger.error(f"Telegram send failed: {resp.status} {body}")
                else:
                    if DEBUG_MODE:
                        logger.debug(f"Telegram message sent: {body[:200]}")
    except Exception as e:
        logger.exception(f"Failed to send Telegram message: {e}")

def build_market_search_url(name: str) -> str:
    """Generate a URL to search the market for a given item name."""
    base_url = "https://market.csgo.com/"
    query = urllib.parse.quote_plus(name)
    return f"{base_url}search?q={query}"

if __name__ == "__main__":
    # Run an aiohttp web server with /ping for health checks and run websocket_listener in background
    async def _on_startup(app):
        logger.info("Starting background websocket listener task")
        app['ws_task'] = asyncio.create_task(websocket_listener())
        logger.info("Starting metrics reporter task")
        app['metrics_task'] = asyncio.create_task(metrics_reporter_loop())

    async def _on_cleanup(app):
        # Cancel background task
        task = app.get('ws_task')
        if task:
            try:
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=5)
                except asyncio.CancelledError:
                    pass
                except asyncio.TimeoutError:
                    logger.warning("ws_task did not exit within timeout during cleanup")
            except Exception:
                logger.exception("Error cancelling ws_task during cleanup")

        mt = app.get('metrics_task')
        if mt:
            try:
                mt.cancel()
                try:
                    await asyncio.wait_for(mt, timeout=5)
                except asyncio.CancelledError:
                    pass
                except asyncio.TimeoutError:
                    logger.warning("metrics_task did not exit within timeout during cleanup")
            except Exception:
                logger.exception("Error cancelling metrics_task during cleanup")
        # Close global aiohttp session if any
        await close_session()

    app = web.Application()
    app.router.add_get('/ping', lambda request: web.Response(text='pong'))
    app.on_startup.append(_on_startup)
    app.on_cleanup.append(_on_cleanup)

    port = int(os.getenv('PORT', '8000'))
    logger.info(f"Starting webserver on 0.0.0.0:{port}, /ping endpoint available")
    try:
        web.run_app(app, host='0.0.0.0', port=port)
    except KeyboardInterrupt:
        logger.info("Program terminated by user.")