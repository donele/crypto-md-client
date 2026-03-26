# Simple Crypto Market Data Subscriber

## What this does

This repository contains a small Python WebSocket client (`md_websocket.py`)
that connects to a selected crypto exchange venue, sends a subscription message,
and prints incoming market data messages to stdout.

It is useful for quickly validating exchange connectivity and inspecting live
market data payloads without a larger framework.

## Requirements

- Python 3.8+
- `websocket-client` Python package

Install dependency:

```bash
pip install websocket-client
```

## Usage

Run the script with a required venue argument:

```bash
python md_websocket.py --venue <VENUE>
```

Short option:

```bash
python md_websocket.py -v <VENUE>
```

## Supported venues

The script includes URL/subscription mappings for these venue strings:

- `BINANCEUS_SPOT`
- `BINANCE_SPOT`
- `BINANCE_SPOT_TESTNET`
- `BINANCE_FUTURES`
- `OKX`
- `OKX_DEMO`
- `HYPERLIQUID`
- `HYPERLIQUID_TESTNET`
- `DYDX`
- `DYDX_TESTNET`
- `BYBIT_TESTNET`
- `BYBIT_SPOT` (message mapping exists)
- `GATE_SPOT_TESTNET`
- `GATE_SPOT`
- `GATE_FUTURES_BTC`
- `GATE_FUTURES_BTC_TESTNET`
- `GATE_FUTURES_USDT`
- `GATE_FUTURES_USDT_TESTNET`
- `KRAKEN`
- `KRAKEN_FUTURES_DEMO`

## Examples

```bash
python md_websocket.py -v BINANCEUS_SPOT
python md_websocket.py -v HYPERLIQUID
python md_websocket.py -v DYDX_TESTNET
python md_websocket.py -v KRAKEN
```

## Output behavior

- For Binance-style payloads containing `stream`, it prints:
  symbol, price, and exchange event time.
- For all other payload formats, it prints the full raw message.

## Notes

- Some endpoints in the script may be region-restricted (for example from US IPs)
  or may have changed behavior over time.
- `websocket.enableTrace(True)` is enabled, so low-level WebSocket logs are printed.
- Venue strings are case-sensitive.

## Troubleshooting

- If you get immediate disconnects or handshake errors, verify:
  - the venue name is valid,
  - the endpoint is reachable from your region/network,
  - the exchange still supports that URL and channel.
- If import fails, install dependency:

```bash
pip install websocket-client
```
