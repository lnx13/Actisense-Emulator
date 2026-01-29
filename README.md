# YDWG → Actisense Emulator

Small Python tool that reads the Yacht Devices YDWG raw TCP feed and exposes it as an Actisense NGT‑1–style TCP server. This lets programs such as CANboat `actisense-serial` or OpenCPN talk to a YDWG as if it were a real Actisense interface.

## Features

- Connects to the YDWG RAW ASCII stream (`HH:MM:SS.mmm R 1DF01100 ...`).
- Reassembles fast-packet PGNs and wraps each message as `N2K_MSG_RECEIVED`.
- Built-in TCP server that multiple clients can connect to.
- Automatic reconnect if the YDWG link drops.

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

Then point your client at `tcp://<listen-host>:<listen-port>`, e.g.

```bash
actisense-serial tcp://127.0.0.1:10001
```

## Command-line options

| Flag | Description | Default |
| --- | --- | --- |
| `--ydwg-host` | YDWG IP/hostname | `127.0.0.1` |
| `--ydwg-port` | YDWG RAW port | `1456` |
| `--listen-host` | Local address for the Actisense server | `127.0.0.1` |
| `--listen-port` | Local port for the Actisense server | `50001` |
| `--fast-timeout` | Seconds before dropping an incomplete fast packet | `2.0` |
| `--reconnect-delay` | Seconds to wait before reconnecting to YDWG | `3.0` |
| `--log-level` | Logging level (`INFO`, `DEBUG`, …) | `INFO` |

Stop the emulator with `Ctrl+C`.

## How it works

1. `YdwgClient` reads each ASCII line from the YDWG TCP socket.
2. `ActisenseBridge` parses the CAN ID, rebuilds fast packets, and produces binary Actisense frames.
3. `ActisenseServer` sends those frames to every connected client.

Incoming data from clients is ignored (most software simply listens). Extend `ActisenseClient` if you need to handle commands.***
