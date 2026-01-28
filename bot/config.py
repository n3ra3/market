# Конфигурация для API ключей и токенов

# Do NOT store real secrets here. Use environment variables instead.
# Example: set MARKET_API_KEY, MARKET_TELEGRAM_TOKEN, MARKET_TELEGRAM_CHAT_ID in your deployment.
API_KEY = ""  # removed hardcoded key
MARKET_API_KEY = API_KEY  # kept for compatibility if needed, but empty by default
TELEGRAM_TOKEN = ""  # set via env MARKET_TELEGRAM_TOKEN
CHAT_ID = ""  # set via env MARKET_TELEGRAM_CHAT_ID
POLL_INTERVAL = 10  # seconds between HTTP poll cycles
PRICE_THRESHOLD = 10000  # default price threshold (same unit as API)