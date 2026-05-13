import websocket
import base64
import hashlib
import hmac
import json
import os
import secrets
import subprocess
import tempfile
import threading
import argparse
import time
import urllib.request


def _split_csv_values(value):
    return [item.strip() for item in str(value).split(',') if item.strip()]


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
    elif venue == 'COINBASE_ADVANCED':
        url = 'wss://advanced-trade-ws.coinbase.com'
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
    elif venue == 'POLYMARKET':
        url = 'wss://ws-subscriptions-clob.polymarket.com/ws/market'
    return url


def _redact_message(msg):
    if isinstance(msg, dict):
        return {
            key: 'REDACTED' if key in {'key', 'jwt', 'passphrase', 'signature', 'secret_key'} else _redact_message(value)
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


def _load_coinbase_jwt_key(key_file):
    if key_file:
        values = [line.strip() for line in open(key_file) if line.strip()]
        if len(values) != 2:
            raise RuntimeError(f"Expected api key name and EC private key in {key_file}")
        return values[0], values[1].replace('\\n', '\n')

    api_key = os.environ.get('COINBASE_API_KEY')
    private_key = os.environ.get('COINBASE_PRIVATE_KEY')
    missing = [
        name for name, value in [
            ('COINBASE_API_KEY', api_key),
            ('COINBASE_PRIVATE_KEY', private_key),
        ] if not value
    ]
    if missing:
        raise RuntimeError(f"Missing Coinbase JWT env vars: {', '.join(missing)}")
    return api_key, private_key.replace('\\n', '\n')


def _base64url(data):
    return base64.urlsafe_b64encode(data).rstrip(b'=').decode('ascii')


def _decode_ecdsa_der_signature(signature_der):
    if len(signature_der) < 8 or signature_der[0] != 0x30:
        raise RuntimeError('Invalid ECDSA DER signature')
    index = 2
    if signature_der[1] & 0x80:
        len_bytes = signature_der[1] & 0x7f
        index = 2 + len_bytes
    if signature_der[index] != 0x02:
        raise RuntimeError('Invalid ECDSA DER signature: missing r')
    r_len = signature_der[index + 1]
    r = signature_der[index + 2:index + 2 + r_len]
    index += 2 + r_len
    if signature_der[index] != 0x02:
        raise RuntimeError('Invalid ECDSA DER signature: missing s')
    s_len = signature_der[index + 1]
    s = signature_der[index + 2:index + 2 + s_len]
    return r[-32:].rjust(32, b'\0') + s[-32:].rjust(32, b'\0')


def _openssl_es256_sign(signing_input, private_key):
    with tempfile.NamedTemporaryFile('w', delete=False) as key_file:
        key_file.write(private_key)
        key_path = key_file.name
    try:
        proc = subprocess.run(
            ['openssl', 'dgst', '-sha256', '-sign', key_path],
            input=signing_input,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
    finally:
        try:
            os.unlink(key_path)
        except OSError:
            pass
    return _decode_ecdsa_der_signature(proc.stdout)


def _coinbase_jwt(key_file=None):
    api_key, private_key = _load_coinbase_jwt_key(key_file)
    now = int(time.time())
    header = {
        'alg': 'ES256',
        'kid': api_key,
        'nonce': secrets.token_hex(16),
        'typ': 'JWT',
    }
    payload = {
        'sub': api_key,
        'iss': 'cdp',
        'nbf': now,
        'exp': now + 120,
    }
    signing_input = (
        _base64url(json.dumps(header, separators=(',', ':')).encode('utf-8'))
        + '.'
        + _base64url(json.dumps(payload, separators=(',', ':')).encode('utf-8'))
    ).encode('ascii')
    signature = _openssl_es256_sign(signing_input, private_key)
    return signing_input.decode('ascii') + '.' + _base64url(signature)


def _coinbase_exchange_message(product, channels, auth, auth_mode, key_file):
    msg = {
        "type": "subscribe",
        "product_ids": [product],
        "channels": channels,
    }
    if auth:
        if auth_mode == 'hmac':
            msg.update(_coinbase_auth_fields())
        elif auth_mode == 'jwt':
            msg['jwt'] = _coinbase_jwt(key_file)
        else:
            raise RuntimeError(f"Unsupported Coinbase auth mode: {auth_mode}")
    return msg


def _coinbase_advanced_message(product, channel, auth, auth_mode, key_file):
    msg = {
        "type": "subscribe",
        "product_ids": [product],
        "channel": channel,
    }
    if auth:
        if auth_mode != 'jwt':
            raise RuntimeError('COINBASE_ADVANCED requires --auth-mode jwt')
        msg['jwt'] = _coinbase_jwt(key_file)
    return msg


def _polymarket_message(product):
    asset_ids = _split_csv_values(product)
    if not asset_ids or asset_ids == ['BTC-USD']:
        raise RuntimeError(
            'POLYMARKET requires --product set to one or more asset IDs '
            '(comma-separated for multiple assets).'
        )
    return {
        "assets_ids": asset_ids,
        "type": "market",
        "custom_feature_enabled": True,
    }


def get_message(venue, product='BTC-USD', channel='ticker', auth=False, auth_mode='hmac', key_file=None):
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
    elif venue == 'COINBASE_ADVANCED':
        msg = _coinbase_advanced_message(product, channel, auth, auth_mode, key_file)
    elif venue.startswith('COINBASE'):
        channels = [channel]
        msg = _coinbase_exchange_message(product, channels, auth, auth_mode, key_file)
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
    elif venue == 'POLYMARKET':
        msg = _polymarket_message(product)
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
    parser.add_argument('--auth-mode', choices=['hmac', 'jwt'], default='hmac')
    parser.add_argument('--coinbase-key-file', type=str, default='')
    parser.add_argument('--timeout-sec', type=float, default=5.0)
    parser.add_argument('--trace', action='store_true')
    a = parser.parse_args()

    url = get_url(a.venue)
    msg = get_message(a.venue, a.product, a.channel, a.auth, a.auth_mode, a.coinbase_key_file)
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
