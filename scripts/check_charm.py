import urllib.request, json
URL = 'https://market.csgo.com/api/v2/prices/USD.json'
print('Fetching', URL)
with urllib.request.urlopen(URL, timeout=30) as r:
    data = json.loads(r.read().decode())
items = data.get('items', {}) or []
# items might be dict keyed by market_hash_name or list depending on API
matches = []
if isinstance(items, dict):
    for k, v in items.items():
        name = k
        # v may be dict with 'price' or list
        price = None
        if isinstance(v, dict):
            price = v.get('price') or v.get('value')
        matches.append((name, price))
else:
    for it in items:
        name = it.get('market_hash_name') or it.get('name')
        price = it.get('price') or it.get('value')
        matches.append((name, price))

# Filter for target
targets = ['Charm | Hungry Eyes']
found = [m for m in matches if m[0] and any(t.lower() in m[0].lower() for t in targets)]
print('Total matches:', len(found))
for name, price in found[:50]:
    print(name, price)

# show sample of keys if items is dict
if isinstance(items, dict):
    keys = list(items.keys())[:20]
    print('\nSample keys:')
    for k in keys:
        print(k)
else:
    print('\nSample items count:', len(items))
print('\nDone')
