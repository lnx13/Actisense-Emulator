#!/usr/bin/env python3
"""Bridge a Yacht Devices YDWG TCP feed into an Actisense-compatible TCP server."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import time
from dataclasses import dataclass
from typing import List, Optional, Sequence

DLE = 0x10
STX = 0x02
ETX = 0x03
N2K_MSG_RECEIVED = 0x93

# Taken from OpenCPN's FastMessage PGN list. These PGNs need reassembly.
FAST_MESSAGE_PGNS = {
    65240,
    126208,
    126464,
    126720,
    126996,
    126998,
    127233,
    127237,
    127489,
    127496,
    127506,
    128275,
    129029,
    129038,
    129039,
    129040,
    129041,
    129284,
    129285,
    129540,
    129793,
    129794,
    129795,
    129797,
    129798,
    129801,
    129802,
    129808,
    129809,
    129810,
    130065,
    130074,
    130323,
    130577,
    130820,
    130822,
    130824,
}


def parse_time_to_seconds(value: str) -> float:
    """Return seconds since midnight for HH:MM:SS.mmm strings."""
    h_part, m_part, s_part = value.split(":")
    if "." in s_part:
        s_str, ms_str = s_part.split(".", 1)
        ms = int(ms_str.ljust(3, "0")[:3])
    else:
        s_str = s_part
        ms = 0
    return int(h_part) * 3600 + int(m_part) * 60 + int(s_str) + ms / 1000.0


@dataclass
class RawCanFrame:
    """Single CAN frame parsed from the YDWG raw ASCII feed."""

    seconds_since_midnight: Optional[float]
    can_id: int
    data: bytes
    direction: str = "R"

    def timestamp_ms(self) -> int:
        """Return a 32-bit timestamp in milliseconds for the Actisense payload."""
        if self.seconds_since_midnight is not None:
            base = self.seconds_since_midnight
        else:
            base = time.time()
        return int(base * 1000) & 0xFFFFFFFF


@dataclass(frozen=True)
class CanHeader:
    """Parsed information from a 29-bit CAN identifier."""

    pgn: int
    source: int
    destination: int
    priority: int


def parse_can_header(can_id: int) -> CanHeader:
    """Decode priority, PGN, destination, and source from a 29-bit CAN identifier."""
    source = can_id & 0xFF
    ps = (can_id >> 8) & 0xFF
    pf = (can_id >> 16) & 0xFF
    dp = (can_id >> 24) & 0x01
    priority = (can_id >> 26) & 0x07

    if pf < 240:
        destination = ps
        pgn = (dp << 16) | (pf << 8)
    else:
        destination = 0xFF
        pgn = (dp << 16) | (pf << 8) | ps

    return CanHeader(pgn=pgn, source=source, destination=destination, priority=priority)


class FastPacketAssembler:
    """Reassemble NMEA2000 fast packets from individual CAN frames."""

    @dataclass
    class _Entry:
        sid: int
        expected_length: int
        data: bytearray
        last_seen: float

    def __init__(self, timeout: float = 2.0) -> None:
        self._timeout = timeout
        self._entries: dict[tuple[int, int, int, int], FastPacketAssembler._Entry] = {}

    def _expire_old_entries(self) -> None:
        now = time.monotonic()
        stale = [
            key
            for key, entry in self._entries.items()
            if now - entry.last_seen > self._timeout
        ]
        for key in stale:
            del self._entries[key]

    def feed(self, header: CanHeader, data: bytes) -> Optional[bytes]:
        """Consume a CAN frame and return complete payload bytes when done."""
        if not data:
            return None
        self._expire_old_entries()

        sid = data[0]
        key = (header.pgn, header.source, header.destination, sid & 0xE0)
        frame_index = sid & 0x1F
        now = time.monotonic()

        if frame_index == 0:
            if len(data) < 2:
                return None
            expected = data[1]
            payload = bytearray(data[2:])
            if expected <= len(payload):
                return bytes(payload[:expected])
            entry = FastPacketAssembler._Entry(
                sid=sid, expected_length=expected, data=payload, last_seen=now
            )
            self._entries[key] = entry
            return None

        entry = self._entries.get(key)
        if not entry:
            return None
        if ((entry.sid + 1) & 0xFF) != sid:
            del self._entries[key]
            return None

        entry.sid = sid
        entry.last_seen = now
        entry.data.extend(data[1:])

        if len(entry.data) >= entry.expected_length:
            payload = bytes(entry.data[: entry.expected_length])
            del self._entries[key]
            return payload
        return None


def encode_actisense_n2k(
    header: CanHeader, payload: bytes, timestamp_ms: int
) -> bytes:
    """Wrap PGN data in an Actisense N2K_MSG_RECEIVED frame."""
    body = bytearray()
    body.append(header.priority & 0xFF)
    body.extend(header.pgn.to_bytes(3, byteorder="little", signed=False))
    body.append(header.destination & 0xFF)
    body.append(header.source & 0xFF)
    body.extend(timestamp_ms.to_bytes(4, byteorder="little", signed=False))
    body.append(len(payload) & 0xFF)
    body.extend(payload)
    return wrap_actisense_payload(N2K_MSG_RECEIVED, body)


def wrap_actisense_payload(command: int, payload: Sequence[int]) -> bytes:
    """Wrap a payload using the Actisense binary framing."""
    frame = bytearray()
    frame.extend((DLE, STX))
    frame.append(command & 0xFF)
    crc = command & 0xFF

    payload_len = len(payload)
    frame.append(payload_len & 0xFF)
    if payload_len == DLE:
        frame.append(DLE)
    crc = (crc + payload_len) & 0xFF

    for value in payload:
        value &= 0xFF
        if value == DLE:
            frame.append(DLE)
        frame.append(value)
        crc = (crc + value) & 0xFF

    checksum = (-crc) & 0xFF
    if checksum == DLE:
        frame.append(DLE)
    frame.append(checksum)
    frame.extend((DLE, ETX))
    return bytes(frame)


class ActisenseClient:
    """Single TCP client connected to the emulator."""

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        on_disconnect,
    ) -> None:
        self.reader = reader
        self.writer = writer
        self._on_disconnect = on_disconnect
        self._closed = False
        self._task: Optional[asyncio.Task[None]] = None
        self.peername = writer.get_extra_info("peername")

    def start(self) -> None:
        self._task = asyncio.create_task(self._consume())

    async def send(self, data: bytes) -> None:
        if self._closed:
            raise ConnectionError("Client already closed")
        self.writer.write(data)
        await self.writer.drain()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._task and self._task is not asyncio.current_task():
            self._task.cancel()
        self.writer.close()
        try:
            await self.writer.wait_closed()
        except Exception:
            pass
        self._on_disconnect(self)

    async def _consume(self) -> None:
        try:
            while True:
                data = await self.reader.read(1024)
                if not data:
                    break
                logging.debug("Ignoring %d bytes from client %s", len(data), self.peername)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logging.warning("Client %s read error: %s", self.peername, exc)
        finally:
            await self.close()


class ActisenseServer:
    """Tiny TCP server that pretends to be an Actisense NGT-1 device."""

    def __init__(self, host: str, port: int) -> None:
        self._host = host
        self._port = port
        self._server: Optional[asyncio.base_events.Server] = None
        self._clients: set[ActisenseClient] = set()

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._on_connection, self._host, self._port
        )
        socknames = ", ".join(str(sock.getsockname()) for sock in self._server.sockets)
        logging.info("Actisense emulator listening on %s", socknames)

    async def broadcast(self, payload: bytes) -> None:
        if not self._clients:
            return
        dead: List[ActisenseClient] = []
        for client in list(self._clients):
            try:
                await client.send(payload)
            except Exception as exc:
                logging.warning("Failed to send to %s: %s", client.peername, exc)
                dead.append(client)
        for client in dead:
            await client.close()

    async def close(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        for client in list(self._clients):
            await client.close()

    def _on_disconnect(self, client: ActisenseClient) -> None:
        self._clients.discard(client)
        logging.info("Client %s disconnected", client.peername)

    async def _on_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        client = ActisenseClient(reader, writer, self._on_disconnect)
        self._clients.add(client)
        logging.info("Client %s connected", client.peername)
        client.start()


def parse_ydwg_line(line: str) -> Optional[RawCanFrame]:
    """Parse a single YD RAW ASCII line."""
    striped = line.strip()
    if not striped or striped.startswith("#"):
        return None
    parts = striped.split()
    if len(parts) < 4:
        return None
    time_str, direction, can_id_str = parts[:3]
    try:
        seconds = parse_time_to_seconds(time_str)
    except ValueError:
        seconds = None
    try:
        can_id = int(can_id_str, 16)
        data = bytes(int(value, 16) for value in parts[3:])
    except ValueError:
        logging.debug("Skipping malformed line: %s", line.rstrip())
        return None
    return RawCanFrame(seconds_since_midnight=seconds, can_id=can_id, data=data, direction=direction)


class ActisenseBridge:
    """Convert YDWG frames into Actisense messages and forward them."""

    def __init__(self, server: ActisenseServer, fast_timeout: float = 2.0) -> None:
        self._server = server
        self._assembler = FastPacketAssembler(timeout=fast_timeout)

    async def handle_frame(self, frame: RawCanFrame) -> None:
        if not frame.data:
            return
        header = parse_can_header(frame.can_id)
        if header.pgn in FAST_MESSAGE_PGNS:
            payload = self._assembler.feed(header, frame.data)
            if payload is None:
                return
        else:
            payload = bytes(frame.data)

        message = encode_actisense_n2k(header, payload, frame.timestamp_ms())
        await self._server.broadcast(message)


class YdwgClient:
    """TCP client for YDWG raw ASCII streams."""

    def __init__(
        self, host: str, port: int, reconnect_delay: float = 3.0
    ) -> None:
        self._host = host
        self._port = port
        self._reconnect_delay = reconnect_delay

    async def run(self, handler) -> None:
        while True:
            reader: Optional[asyncio.StreamReader] = None
            writer: Optional[asyncio.StreamWriter] = None
            try:
                reader, writer = await asyncio.open_connection(self._host, self._port)
                logging.info("Connected to YDWG at %s:%s", self._host, self._port)
                await self._pump(reader, handler)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logging.warning("YDWG connection error: %s", exc)
            finally:
                if writer:
                    writer.close()
                    try:
                        await writer.wait_closed()
                    except Exception:
                        pass
                logging.info(
                    "Reconnecting to YDWG in %.1f seconds", self._reconnect_delay
                )
                await asyncio.sleep(self._reconnect_delay)

    async def _pump(self, reader: asyncio.StreamReader, handler) -> None:
        while True:
            line = await reader.readline()
            if not line:
                raise ConnectionError("YDWG stream closed")
            try:
                text = line.decode("ascii", errors="ignore")
            except UnicodeDecodeError:
                continue
            frame = parse_ydwg_line(text)
            if not frame:
                continue
            await handler(frame)


async def run_emulator(args) -> None:
    server = ActisenseServer(args.listen_host, args.listen_port)
    await server.start()

    bridge = ActisenseBridge(server, fast_timeout=args.fast_timeout)
    ydwg = YdwgClient(args.ydwg_host, args.ydwg_port, args.reconnect_delay)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            # Windows may not support signal handlers for subprocesses.
            pass

    ydwg_task = asyncio.create_task(ydwg.run(bridge.handle_frame))
    await stop_event.wait()
    ydwg_task.cancel()
    try:
        await ydwg_task
    except asyncio.CancelledError:
        pass
    await server.close()


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Emulate an Actisense NGT-1 device using a YDWG raw TCP stream."
    )
    parser.add_argument("--ydwg-host", default="127.0.0.1", help="YDWG host/IP (default: %(default)s)")
    parser.add_argument("--ydwg-port", type=int, default=1456, help="YDWG TCP port (default: %(default)s)")
    parser.add_argument("--listen-host", default="127.0.0.1", help="Local host/IP for the Actisense server")
    parser.add_argument("--listen-port", type=int, default=50001, help="Local TCP port for the Actisense server")
    parser.add_argument("--fast-timeout", type=float, default=2.0, help="Seconds before abandoning an incomplete fast packet")
    parser.add_argument("--reconnect-delay", type=float, default=3.0, help="Seconds to wait before reconnecting to YDWG")
    parser.add_argument("--log-level", default="INFO", help="Logging level (default: %(default)s)")
    return parser.parse_args(argv)


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)
    configure_logging(args.log_level)
    try:
        asyncio.run(run_emulator(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
