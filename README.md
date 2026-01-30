# YDWG → Actisense Emulator
[![Buy Me A Coffee](https://img.shields.io/badge/☕-Buy%20Me%20a%20Coffee-yellow?style=for-the-badge)](https://buymeacoffee.com/lnx13)
![Stars](https://img.shields.io/github/stars/lnx13/Actisense-Emulator?color=yellow&style=flat)
![Issues](https://img.shields.io/github/issues/lnx13/Actisense-Emulator?color=orange&style=flat)
![Last Commit](https://img.shields.io/github/last-commit/lnx13/Actisense-Emulator?color=blue&style=flat)
[![Stand With Ukraine](https://raw.githubusercontent.com/vshymanskyy/StandWithUkraine/main/badges/StandWithUkraine.svg)](https://stand-with-ukraine.pp.ua)
---

Small Python tool that reads the Yacht Devices YDWG raw TCP feed and exposes it as an Actisense-style TCP server. By default it emits standard Actisense binary `N2K_MSG_RECEIVED` frames (so Actisense Reader, CANboat `actisense-serial`, etc. behave as if a real NGT‑1 were attached) while mirroring each PGN as an Actisense ASCII (`A…`) sentence for any client that switches into text mode. You can force either output via `--actisense-output {binary,ascii}` when needed. It also accepts Actisense commands (e.g. `N2K_MSG_SEND`) and injects them back onto the YDWG network, so tools like OpenCPN can both read and write via YDWG.

## Features

- Connects to the YDWG RAW ASCII stream (`HH:MM:SS.mmm R 1DF01100 ...`).
- Reassembles fast-packet PGNs and forwards each message as Actisense binary `N2K_MSG_RECEIVED` frames, plus optional ASCII `A…` mirrors for clients that send printable commands (telnet, netcat, etc.). Use `--actisense-output` to force either format globally.
- Accepts Actisense binary `N2K_MSG_SEND`, Actisense ASCII (`A…`) or legacy YDWG RAW commands from clients and retransmits them via YDWG.
- Basic Actisense control channel support: answers hardware-info (0x10) and operating-mode (0x11) requests so Actisense Reader can determine the link state.
- Built-in TCP server that multiple clients can connect to.
- Automatic reconnect if the YDWG link drops.

## Current limitations

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

Binary Actisense clients (Actisense Reader, CANboat `actisense-serial`, etc.) will immediately receive `N2K_MSG_RECEIVED` frames. Text-mode clients only start receiving the mirrored ASCII `A…` stream after they send printable data (for example, pressing Enter in a telnet session or issuing an `A...` command), which signals the bridge to switch that connection into ASCII mode. If you want to force ASCII from the start (e.g. for OpenCPN), add `--actisense-output ascii`:

```bash
python3 ydwg_actisense_emulator.py \
  --ydwg-host 192.168.4.1 \
  --ydwg-port 1456 \
  --listen-host 0.0.0.0 \
  --listen-port 10001 \
  --actisense-output ascii
```

Then point your client (OpenCPN, Actisense Reader, CANboat tools, etc.) at `tcp://<listen-host>:<listen-port>`, e.g.

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

It simply proxies the raw YDWG stream and prints a readable hex+ASCII dump for debugging.

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
| `--actisense-output` | Emit `auto`, `binary`, or `ascii` Actisense frames | `auto` |
| `--log-level` | Logging level (`INFO`, `DEBUG`, …) | `INFO` |

Stop the emulator with `Ctrl+C`.

## How it works

1. `YdwgClient` reads raw YDWG ASCII (`HH:MM:SS.mmm R/T …`) from the gateway.
2. `ActisenseBridge` converts those lines into Actisense binary frames (plus optional Actisense ASCII sentences, depending on `--actisense-output`) and pushes them to any Actisense-aware TCP client.
3. When clients send Actisense ASCII commands back, the bridge simply reformats them to YDWG RAW (without touching the payload), so the gateway performs the actual fast-packet fragmentation.
4. Legacy N2K_MSG_SEND (binary) commands are still supported; in that case the bridge fabricates the necessary RAW lines using the configured `--tx-source`.
