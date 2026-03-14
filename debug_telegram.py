import json
import sys
import os


def load_dotenv_file(env_path):
    try:
        if not os.path.exists(env_path):
            return
        with open(env_path, 'r', encoding='utf-8') as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip()
                if not key:
                    continue
                if len(value) >= 2 and ((value[0] == '"' and value[-1] == '"') or (value[0] == "'" and value[-1] == "'")):
                    value = value[1:-1]
                os.environ.setdefault(key, value)
    except Exception as e:
        print('Failed to load .env:', e)


load_dotenv_file(os.path.join(os.getcwd(), '.env'))
TELEGRAM_TOKEN = os.getenv('MARKET_TELEGRAM_TOKEN')
CHAT_ID = os.getenv('MARKET_TELEGRAM_CHAT_ID')
if not TELEGRAM_TOKEN or not CHAT_ID:
    print('Missing MARKET_TELEGRAM_TOKEN or MARKET_TELEGRAM_CHAT_ID in env/.env')
    sys.exit(1)
try:
    import requests
    _HAS_REQUESTS = True
except Exception:
    _HAS_REQUESTS = False

token = TELEGRAM_TOKEN
chat = CHAT_ID
print('Using token:', (token[:6] + '...' + token[-6:]) if token else '<no-token>')
print('Using chat_id:', chat)

base = f'https://api.telegram.org/bot{token}'

def _print_resp(prefix, status, text):
    print(f"{prefix} status: {status}")
    try:
        print(text)
    except Exception:
        print(repr(text))

if _HAS_REQUESTS:
    try:
        r = requests.get(base + '/getWebhookInfo', timeout=10)
        _print_resp('getWebhookInfo', r.status_code, r.text)
    except Exception as e:
        print('getWebhookInfo error:', e)

    try:
        r = requests.get(base + '/getUpdates', timeout=10)
        _print_resp('getUpdates', r.status_code, r.text[:4000])
    except Exception as e:
        print('getUpdates error:', e)

    try:
        payload = {'chat_id': chat, 'text': 'TEST MESSAGE FROM debug_telegram.py'}
        r = requests.post(base + '/sendMessage', json=payload, timeout=10)
        _print_resp('sendMessage', r.status_code, r.text)
    except Exception as e:
        print('sendMessage error:', e)
else:
    # Fallback to urllib if requests is not installed
    import urllib.request
    import json as _json

    try:
        with urllib.request.urlopen(base + '/getWebhookInfo', timeout=10) as r:
            txt = r.read().decode()
            _print_resp('getWebhookInfo', r.status, txt)
    except Exception as e:
        print('getWebhookInfo error (urllib):', e)

    try:
        with urllib.request.urlopen(base + '/getUpdates', timeout=10) as r:
            txt = r.read().decode()
            _print_resp('getUpdates', r.status, txt[:4000])
    except Exception as e:
        print('getUpdates error (urllib):', e)

    try:
        data = _json.dumps({'chat_id': chat, 'text': 'TEST MESSAGE FROM debug_telegram.py (urllib)'}).encode('utf-8')
        req = urllib.request.Request(base + '/sendMessage', data=data, headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=10) as r:
            txt = r.read().decode()
            _print_resp('sendMessage', r.status, txt)
    except Exception as e:
        print('sendMessage error (urllib):', e)
