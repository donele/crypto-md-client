import websocket
import json
import threading
import argparse

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
    #elif venue == 'OKX':
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
    elif venue == 'BYBIT_TESTNET':
        url = 'wss://stream-testnet.bybit.com/v5/public' # US blocked
    elif venue == 'BYBIT_SPOT':
        url = 'wss://stream.bybit.com/v5/public/spot' # success
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
    return url

def get_message(venue):
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
    elif venue == 'BYBIT_SPOT':
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
    return msg

def on_message(ws, message):
    data = json.loads(message)
    if 'stream' in data:
        # you can pipeline this data to your function, analysis or backtesting
        print(f"Symbol: {data['data']['s']}, Price: {data['data']['c']}, Time: {data['data']['E']}")
    else:
        print(f"Received message: {message}")

def on_error(ws, error):
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
        self.msg = msg
    def __call__(self, ws):
        ws.send(json.dumps(self.msg))

def on_ping(ws, message):
    print(f"Received ping: {message}")
    ws.send(message, websocket.ABNF.OPCODE_PONG)
    print(f"Sent pong: {message}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--venue', type=str, required=True)
    a = parser.parse_args()

    url = get_url(a.venue)
    msg = get_message(a.venue)
    on_open_callable = OnOpen(msg)

    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(url,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close,
                                on_open=on_open_callable,
                                on_ping=on_ping)
    ws.run_forever()
