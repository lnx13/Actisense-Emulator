# YDWG → Actisense Emulator

Small Python tool that reads the Yacht Devices YDWG raw TCP feed and exposes it as an Actisense-style TCP server that currently speaks the ASCII (`A…`) protocol. It also accepts Actisense commands (e.g. `N2K_MSG_SEND`) and injects them back onto the YDWG network, so tools like OpenCPN can both read and write via YDWG.

## Features

- Connects to the YDWG RAW ASCII stream (`HH:MM:SS.mmm R 1DF01100 ...`).
- Reassembles fast-packet PGNs and forwards each message as an Actisense ASCII `A…` sentence (no binary `N2K_MSG_RECEIVED` output yet).
- Accepts Actisense binary `N2K_MSG_SEND`, Actisense ASCII (`A…`) or legacy YDWG RAW commands from clients and retransmits them via YDWG.
- Built-in TCP server that multiple clients can connect to.
- Automatic reconnect if the YDWG link drops.

## Current limitations

- Only Actisense ASCII output is generated. Binary `N2K_MSG_RECEIVED` frames are not emitted, so clients must be able to consume ASCII sentences.
- ISO request / product-information responses (PGNs 59904 and 126996) are not synthesized. If your client requires TX enablement, it must trigger it itself.

## Requirements

- Python 3.9+ (only standard library used).
- Network access to the YDWG RAW stream (default port 1456).

## Quick start

```bash
python3 ydwg_actisense_emulator.py \
  --ydwg-host 192.168.4.1 \
  --ydwg-port 1456 \
  --listen-host 0.0.0.0 \
  --listen-port 10001
```

Then point your client (e.g. OpenCPN configured for Actisense ASCII sentences) at `tcp://<listen-host>:<listen-port>`, e.g.

```bash
actisense-serial tcp://127.0.0.1:10001
```

### Raw logger

Sometimes you may want to capture the traffic without any transformation. Use `ydwg_actisense_logger.py` instead of the emulator:

```bash
python3 ydwg_actisense_logger.py \
  --ydwg-host 192.168.4.1 \
  --ydwg-port 1456 \
  --listen-host 0.0.0.0 \
  --listen-port 10002 \
  --log-direction tx   # log only client -> YDWG
```

It simply proxies the raw YDWG stream to any Actisense-aware client and prints a readable hex+ASCII dump for debugging.

## Command-line options

| Flag | Description | Default |
| --- | --- | --- |
| `--ydwg-host` | YDWG IP/hostname | `127.0.0.1` |
| `--ydwg-port` | YDWG RAW port | `1456` |
| `--listen-host` | Local address for the Actisense server | `127.0.0.1` |
| `--listen-port` | Local port for the Actisense server | `50001` |
| `--fast-timeout` | Seconds before dropping an incomplete fast packet | `2.0` |
| `--reconnect-delay` | Seconds to wait before reconnecting to YDWG | `3.0` |
| `--tx-source` | Source address used when sending binary `N2K_MSG_SEND` commands | `0x00` |
| `--log-direction` | Limit debug output to `both`, `tx`, or `rx` | `both` |
| `--log-level` | Logging level (`INFO`, `DEBUG`, …) | `INFO` |

Stop the emulator with `Ctrl+C`.

## How it works

1. `YdwgClient` reads raw YDWG ASCII (`HH:MM:SS.mmm R/T …`) from the gateway.
2. `ActisenseBridge` converts those lines into Actisense ASCII sentences and pushes them to any Actisense-aware TCP client.
3. When clients send Actisense ASCII commands back, the bridge simply reformats them to YDWG RAW (without touching the payload), so the gateway performs the actual fast-packet fragmentation.
4. Legacy N2K_MSG_SEND (binary) commands are still supported; in that case the bridge fabricates the necessary RAW lines using the configured `--tx-source`.
