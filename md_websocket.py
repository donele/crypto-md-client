import websocket
import base64
import hashlib
import hmac
import json
import os
import threading
import argparse
import time
import urllib.request


def _fetch_kucoin_ws_url(venue):
    if venue == 'KUCOIN_SPOT':
        bullet_url = 'https://api.kucoin.com/api/v1/bullet-public'
    elif venue == 'KUCOIN_FUTURES':
        bullet_url = 'https://api-futures.kucoin.com/api/v1/bullet-public'
    else:
        return ''

    req = urllib.request.Request(bullet_url, method='POST')
    with urllib.request.urlopen(req, timeout=10) as resp:
        payload = json.loads(resp.read().decode('utf-8'))

    if payload.get('code') != '200000':
        raise RuntimeError(f"KuCoin bullet-public failed: {payload}")

    data = payload.get('data') or {}
    token = data.get('token')
    servers = data.get('instanceServers') or []
    endpoint = servers[0].get('endpoint') if servers else ''
    if not token or not endpoint:
        raise RuntimeError(f"KuCoin bullet-public missing token/endpoint: {payload}")
    return f"{endpoint}?token={token}&connectId=md_websocket"


def get_url(venue):
    url = ''
    if venue == 'BINANCEUS_SPOT':
        url = 'wss://stream.binance.us:9443/ws' # success
    elif venue == 'BINANCE_SPOT':
        url = 'wss://stream.binance.com:9443/ws' # US blocked
    elif venue == 'BINANCE_SPOT_TESTNET':
        url = 'wss://ws-api.testnet.binance.vision:443/ws-api/v3' # 443 or 9443, US blocked
    elif venue == 'BINANCE_FUTURES':
        url = 'wss://fstream.binance.com/ws' # US blocked
    elif venue.startswith('OKX') and 'DEMO' not in venue:
        url = 'wss://ws.okx.com:8443/ws/v5/public' # success
    elif venue.startswith('OKX') and 'DEMO' in venue:
        url = 'wss://wspap.okx.com:8443/ws/v5/public' # success
    elif venue == 'HYPERLIQUID':
        url = 'wss://api.hyperliquid.xyz/ws' # success
    elif venue == 'HYPERLIQUID_TESTNET':
        url = 'wss://api.hyperliquid-testnet.xyz/ws' # success
    elif venue == 'DYDX':
        url = 'wss://indexer.dydx.trade/v4/ws' # success
    elif venue == 'DYDX_TESTNET':
        url = 'wss://indexer.v4testnet.dydx.exchange/v4/ws' # success
    elif venue == 'BYBIT_SPOT_TESTNET':
        url = 'wss://stream-testnet.bybit.com/v5/public' # geoblocked
    elif venue == 'BYBIT_SPOT':
        url = 'wss://stream.bybit.com/v5/public/spot' # success
    elif venue == 'BYBIT_FUTURES':
        url = 'wss://stream.bybit.com/v5/public/linear' # success
    elif venue == 'BYBIT_FUTURES_TESTNET':
        url = 'wss://stream-testnet.bybit.com/v5/public/linear' # geoblocked
    elif venue.startswith('BYBIT'):
        url = 'wss://stream.bybit.com/v5/public' # US blocked
    elif venue == 'GATE_SPOT_TESTNET':
        url = 'wss://ws-testnet.gate.com/v4/ws/spot' # success
    elif venue == 'GATE_SPOT':
        url = 'wss://api.gateio.ws/ws/v4' # 404
    elif venue == 'GATE_FUTURES_BTC':
        url = 'wss://fx-ws.gateio.ws/v4/ws/btc' # unknown currency pair
    elif venue == 'GATE_FUTURES_BTC_TESTNET':
        url = 'wss://fx-ws-testnet.gateio.ws/v4/ws/btc' # unknown currency pair
    elif venue == 'GATE_FUTURES_USDT':
        url = 'wss://fx-ws.gateio.ws/v4/ws/usdt' # success
    elif venue == 'GATE_FUTURES_USDT_TESTNET':
        url = 'wss://ws-testnet.gate.com/v4/ws/futures/usdt' # success
    elif venue == 'KRAKEN':
        url = 'wss://ws.kraken.com/v2' # success
    elif venue == 'KRAKEN_FUTURES_DEMO':
        url = 'wss://demo-futures.kraken.com/ws/v1' # success
    elif venue == 'COINBASE_SANDBOX':
        url = 'wss://ws-feed-public.sandbox.exchange.coinbase.com'
    elif venue == 'COINBASE':
        url = 'wss://ws-feed.exchange.coinbase.com' # ticker: success, level2: needs auth
    elif venue == 'BITGET_SPOT':
        url = 'wss://ws.bitget.com/v2/ws/public' # success
    elif venue == 'BITGET_FUTURES':
        url = 'wss://ws.bitget.com/v2/ws/public' # success
    elif venue.startswith('KUCOIN'):
        url = _fetch_kucoin_ws_url(venue) # requires bullet-public token
    elif venue == 'DERIBIT':
        url = 'wss://www.deribit.com/ws/api/v2' # success
    elif venue == 'DERIBIT_TESTNET':
        url = 'wss://test.deribit.com/ws/api/v2' # success
    return url


def _redact_message(msg):
    if isinstance(msg, dict):
        return {
            key: 'REDACTED' if key in {'key', 'passphrase', 'signature', 'secret_key'} else _redact_message(value)
            for key, value in msg.items()
        }
    if isinstance(msg, list):
        return [_redact_message(value) for value in msg]
    return msg


def _coinbase_auth_fields():
    api_key = os.environ.get('COINBASE_API_KEY')
    passphrase = os.environ.get('COINBASE_PASSPHRASE')
    secret_key = os.environ.get('COINBASE_SECRET_KEY')
    missing = [
        name for name, value in [
            ('COINBASE_API_KEY', api_key),
            ('COINBASE_PASSPHRASE', passphrase),
            ('COINBASE_SECRET_KEY', secret_key),
        ] if not value
    ]
    if missing:
        raise RuntimeError(f"Missing Coinbase credential env vars: {', '.join(missing)}")

    timestamp = f"{time.time():.6f}"
    payload = f"{timestamp}GET/users/self/verify".encode('utf-8')
    decoded_secret = base64.b64decode(secret_key)
    signature = base64.b64encode(hmac.new(decoded_secret, payload, hashlib.sha256).digest()).decode('utf-8')
    return {
        "signature": signature,
        "key": api_key,
        "passphrase": passphrase,
        "timestamp": timestamp,
    }


def _coinbase_message(product, channels, auth):
    msg = {
        "type": "subscribe",
        "product_ids": [product],
        "channels": channels,
    }
    if auth:
        msg.update(_coinbase_auth_fields())
    return msg


def get_message(venue, product='BTC-USD', channel='ticker', auth=False):
    msg = ''
    if venue in ['BINANCEUS_SPOT', 'BINANCE_SPOT', 'BINANCE_SPOT_TESTNET']:
        msg = {
            "method": "SUBSCRIBE",
            "params": ["btcusdt@ticker"],
            "id": 1
        }
    elif venue.startswith('OKX') and 'SPOT' in venue:
        msg = {
            "op": "subscribe",
            "args": [{
                "channel": "tickers",
                "instId": "BTC-USDT"
            }]
        }
    elif venue.startswith('OKX') and 'FUTURES' in venue:
        msg = {
            "op": "subscribe",
            "args": [{
                "channel": "tickers",
                "instId": "BTC-USDT-SWAP"
            }]
        }
    elif venue.startswith('HYPERLIQUID'):
        msg = {
            "method": "subscribe",
            "subscription": {
                "type": "l2Book",
                "coin": "BTC"
            }
        }
    elif venue.startswith('DYDX'):
        msg = {
            "type": "subscribe",
            "channel": "v4_orderbook",
            "id": "BTC-USD",
            "includeOffsets": True,
            "batched": False
        }
    elif venue.startswith('BYBIT_SPOT'):
        msg = {
            "op": "subscribe",
            "args": [
                "orderbook.50.BTCUSDT",
                "publicTrade.BTCUSDT"
            ]
        }
    elif venue.startswith('BYBIT_FUTURES'):
        msg = {
            "op": "subscribe",
            "args": [
                "orderbook.50.BTCUSDT",
                "publicTrade.BTCUSDT"
            ]
        }
    elif venue.startswith('GATE_SPOT'):
        msg = {
            "time": 0,
            "channel": "spot.order_book_update",
            "event": "subscribe",
            "payload": [
                "BTC_USDT",
                "100ms"
            ]
        }
    elif venue.startswith('GATE_FUTURES_USDT'):
        msg = {
            "time": 0,
            "channel": "futures.tickers",
            "event": "subscribe",
            "payload": [
                "BTC_USDT"
            ]
        }
    elif venue == 'KRAKEN':
        msg = {
            "method": "subscribe",
            "params": {
                "channel": "book",
                "symbol": [
                    "BTC/USD"
                ]
            }
        }
    elif venue == 'KRAKEN_FUTURES_DEMO':
        msg = {
            "event": "subscribe",
            "feed": "ticker",
            "product_ids": [
                "PI_XBTUSD"
            ]
        }
    elif venue.startswith('COINBASE'):
        channels = [channel]
        msg = _coinbase_message(product, channels, auth)
    elif venue == 'BITGET_SPOT':
        msg = {
            "op": "subscribe",
            "args": [
                {
                    "instType": "SPOT",
                    "channel": "ticker",
                    "instId": "BTCUSDT"
                }
            ]
        }
    elif venue == 'BITGET_FUTURES':
        msg = {
            "op": "subscribe",
            "args": [
                {
                    "instType": "USDT-FUTURES",
                    "channel": "ticker",
                    "instId": "BTCUSDT"
                }
            ]
        }
    elif venue == 'KUCOIN_SPOT':
        msg = {
            "id": 1,
            "type": "subscribe",
            "topic": "/market/ticker:BTC-USDT",
            "privateChannel": False,
            "response": True
        }
    elif venue == 'KUCOIN_FUTURES':
        msg = {
            "id": 1,
            "type": "subscribe",
            "topic": "/contractMarket/tickerV2:XBTUSDTM",
            "privateChannel": False,
            "response": True
        }
    elif venue in ['DERIBIT', 'DERIBIT_TESTNET']:
        msg = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "public/subscribe",
            "params": {
                "channels": ["ticker.BTC-PERPETUAL.raw"]
            }
        }
    return msg


def on_message(ws, message):
    data = json.loads(message)
    if isinstance(data, dict) and 'stream' in data:
        # you can pipeline this data to your function, analysis or backtesting
        print(f"Symbol: {data['data']['s']}, Price: {data['data']['c']}, Time: {data['data']['E']}")
    elif isinstance(data, dict) and data.get("type") == "ticker" and data.get("product_id") and data.get("price"):
        print(f"Symbol: {data['product_id']}, Price: {data['price']}, Time: {data.get('time')}")
    else:
        print(f"Received message: {message}")


def on_error(ws, error):
    err = str(error)
    lower = err.lower()
    if "handshake status 451" in lower or "service unavailable from a restricted location" in lower:
        print("Error: endpoint blocked for this location (HTTP 451 restricted region).")
        return
    if "handshake status 403" in lower and ("cloudfront" in lower or "block access from your country" in lower):
        print("Error: region blocked by CloudFront (HTTP 403). Endpoint not accessible from this network/location.")
        return
    print(f"Error: {error}")


def on_close(ws, close_status_code, close_msg):
    print(f"WebSocket connection closed: {close_status_code} - {close_msg}")


def on_open(ws):
    print("WebSocket connection opened")
    # Subscribe to the ticker stream for BTCUSDT
    subscribe_message = {
        "method": "SUBSCRIBE",
        "params": ["btcusdt@ticker"],
        "id": 1
    }
    ws.send(json.dumps(subscribe_message))


class OnOpen:
    def __init__(self, msg):
        self.msgs = msg if isinstance(msg, list) else [msg]

    def __call__(self, ws):
        for index, msg in enumerate(self.msgs):
            encoded = json.dumps(msg)
            print(f"Sending message {index + 1}: {json.dumps(_redact_message(msg))}")
            ws.send(encoded)


def on_ping(ws, message):
    print(f"Received ping: {message}")
    ws.send(message, websocket.ABNF.OPCODE_PONG)
    print(f"Sent pong: {message}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--venue', type=str, required=True)
    parser.add_argument('--product', type=str, default='BTC-USD')
    parser.add_argument('--channel', type=str, default='ticker')
    parser.add_argument('--auth', action='store_true')
    parser.add_argument('--timeout-sec', type=float, default=5.0)
    parser.add_argument('--trace', action='store_true')
    a = parser.parse_args()

    url = get_url(a.venue)
    msg = get_message(a.venue, a.product, a.channel, a.auth)
    on_open_callable = OnOpen(msg)

    websocket.enableTrace(a.trace)
    ws = websocket.WebSocketApp(url,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close,
                                on_open=on_open_callable,
                                on_ping=on_ping)
    if a.timeout_sec > 0:
        threading.Timer(a.timeout_sec, ws.close).start()
    ws.run_forever()
