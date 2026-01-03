import asyncio
import websockets
import aiohttp
import json
import logging
import os

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')


async def get_ws_token(api_key):
    logging.info("Fetching WebSocket token...")
    async with aiohttp.ClientSession() as session:
        async with session.get(f'https://market.csgo.com/api/v2/get-ws-token?key={api_key}') as response:
            data = await response.json()
            logging.info(f"Token response: {data}")
            # API may return token under different keys
            return data.get('ws_token') or data.get('token')


async def websocket_listener(api_key):
    logging.info("Starting WebSocket listener")
    ws_token = await get_ws_token(api_key)
    if not ws_token:
        logging.error("No ws_token received; aborting listener")
        return
    # Include ws_token as query parameter (required by API)
    uri = f"wss://wsprice.csgo.com/connection/websocket?token={ws_token}"
    logging.info(f"Attempting WebSocket connections for token")
    origin = 'https://market.csgo.com'

    # Candidate endpoints to try (original host, direct IP, port 8080 plain/ws/wss)
    candidates = [
        (f"wss://wsprice.csgo.com/connection/websocket?token={ws_token}", {'Origin': origin, 'Host': 'wsprice.csgo.com'}),
        (f"wss://104.18.8.154/connection/websocket?token={ws_token}", {'Origin': origin, 'Host': 'wsprice.csgo.com'}),
        (f"wss://104.18.8.154:8080/connection/websocket?token={ws_token}", {'Origin': origin, 'Host': 'wsprice.csgo.com'}),
        (f"ws://104.18.8.154:8080/connection/websocket?token={ws_token}", {'Origin': origin, 'Host': 'wsprice.csgo.com'})
    ]

    # Try each candidate with aiohttp (allows setting Host header explicitly)
    for uri_candidate, headers in candidates:
        logging.info(f"Trying candidate: {uri_candidate} with headers {headers}")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(uri_candidate, headers=headers) as ws:
                    logging.info(f"Connected via aiohttp to {uri_candidate}")
                    await ws.send_json({"op": "subscribe", "args": ["public:items:730:usd"]})
                    logging.info("Subscribed, listening for messages...")
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            logging.debug(f"Raw message: {msg.data}")
                            try:
                                data = json.loads(msg.data)
                            except Exception:
                                logging.exception("Failed to parse message JSON")
                                continue
                            if "data" in data:
                                for item in data["data"]:
                                    await process_item(item)
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            logging.error("WebSocket error message received; closing candidate")
                            break
                    return
        except Exception:
            logging.exception(f"Candidate {uri_candidate} failed")

    logging.error("All WebSocket candidates failed")
    # Start HTTP polling fallback
    logging.info("Starting HTTP polling fallback")
    await polling_loop(api_key)


async def process_item(item):
    logging.info(f"Processing item: {item.get('name_id') or item.get('id')}")
    try:
        name_id = item.get('name_id')
        price = item.get('price')
    except Exception:
        logging.exception("Malformed item data")
        return
    if price is None:
        # Try alternate price keys
        price = item.get('value') or item.get('price_value') or item.get('amount')
    if price is None:
        logging.warning("Item has no price; skipping")
        return
    try:
        price_num = float(price)
    except Exception:
        logging.exception("Price is not a number; skipping")
        return
    from bot.config import PRICE_THRESHOLD
    if price_num < PRICE_THRESHOLD:  # Пример пороговой цены
        logging.info(f"Item matches criteria: {name_id}, {price}")
        # Попытка отправить уведомление через существующий модуль
        try:
            from bot.telegram_module import send_telegram_notification as _send
        except Exception:
            _send = None
        if _send:
            try:
                await _send(name_id, price, item.get('id'))
            except Exception:
                logging.exception("Failed to call send_telegram_notification")
        else:
            logging.info(f"(stub) would notify: {name_id} {price} {item.get('id')}")


    async def polling_loop(api_key, interval=10):
        """Periodic HTTP polling fallback. Tries several common endpoints and
        processes items returned under common keys.
        """
        endpoints = [
            f'https://market.csgo.com/api/v2/items?key={api_key}',
            f'https://market.csgo.com/api/v2/prices?key={api_key}',
            f'https://market.csgo.com/api/v2/search?key={api_key}',
            f'https://market.csgo.com/api/v2/search-item-by-hash-name?key={api_key}'
        ]
        async with aiohttp.ClientSession() as session:
            while True:
                for url in endpoints:
                    logging.info(f"Polling {url}")
                    try:
                        async with session.get(url, timeout=20) as resp:
                            if resp.status != 200:
                                logging.debug(f"Non-200 from {url}: {resp.status}")
                                continue
                            data = await resp.json()
                    except Exception:
                        logging.exception(f"Request to {url} failed")
                        continue

                    # Normalize possible response shapes
                    items = None
                    if isinstance(data, dict):
                        if 'data' in data and isinstance(data['data'], list):
                            items = data['data']
                        elif 'items' in data and isinstance(data['items'], list):
                            items = data['items']
                        elif 'prices' in data and isinstance(data['prices'], list):
                            items = data['prices']
                        else:
                            # If the top-level dict looks like an item mapping, try values
                            for v in data.values():
                                if isinstance(v, list):
                                    items = v
                                    break
                    elif isinstance(data, list):
                        items = data

                    if not items:
                        logging.debug(f"No items found at {url}")
                        continue

                    for item in items:
                        try:
                            # Attempt to adapt item shapes to process_item expectations
                            if isinstance(item, dict):
                                # ensure keys exist
                                await process_item(item)
                            else:
                                logging.debug("Skipping non-dict item")
                        except Exception:
                            logging.exception("Error processing polled item")

                logging.info(f"Polling cycle complete — sleeping {interval}s")
                await asyncio.sleep(interval)


async def polling_loop(api_key, interval=10):
    """Periodic HTTP polling fallback. Tries several common endpoints and
    processes items returned under common keys.
    """
    endpoints = [
        f'https://market.csgo.com/api/v2/items?key={api_key}',
        f'https://market.csgo.com/api/v2/prices?key={api_key}',
        f'https://market.csgo.com/api/v2/search?key={api_key}',
        f'https://market.csgo.com/api/v2/search-item-by-hash-name?key={api_key}'
    ]
    async with aiohttp.ClientSession() as session:
        while True:
            for url in endpoints:
                logging.info(f"Polling {url}")
                try:
                    async with session.get(url, timeout=20) as resp:
                        if resp.status != 200:
                            logging.debug(f"Non-200 from {url}: {resp.status}")
                            continue
                        data = await resp.json()
                except Exception:
                    logging.exception(f"Request to {url} failed")
                    continue

                # Normalize possible response shapes
                items = None
                if isinstance(data, dict):
                    if 'data' in data and isinstance(data['data'], list):
                        items = data['data']
                    elif 'items' in data and isinstance(data['items'], list):
                        items = data['items']
                    elif 'prices' in data and isinstance(data['prices'], list):
                        items = data['prices']
                    else:
                        for v in data.values():
                            if isinstance(v, list):
                                items = v
                                break
                elif isinstance(data, list):
                    items = data

                if not items:
                    logging.debug(f"No items found at {url}")
                    continue

                for item in items:
                    try:
                        if isinstance(item, dict):
                            await process_item(item)
                        else:
                            logging.debug("Skipping non-dict item")
                    except Exception:
                        logging.exception("Error processing polled item")

            logging.info(f"Polling cycle complete — sleeping {interval}s")
            await asyncio.sleep(interval)


if __name__ == '__main__':
    # Попробовать взять ключ из переменных окружения или из config
    try:
        from bot.config import MARKET_API_KEY as _cfg_key
    except Exception:
        logging.exception("Failed importing bot.config; trying local import")
        try:
            from bot.config import MARKET_API_KEY as _cfg_key
        except Exception:
            logging.exception("Failed importing config from local module")
            _cfg_key = None
    api_key = os.getenv('MARKET_API_KEY') or _cfg_key
    if not api_key:
        logging.error('No MARKET_API_KEY provided. Set env or bot.config.MARKET_API_KEY')
    else:
        try:
            asyncio.run(websocket_listener(api_key))
        except KeyboardInterrupt:
            logging.info('Interrupted by user')
        except Exception:
            logging.exception('Unhandled error in listener')