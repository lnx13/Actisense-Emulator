#!/usr/bin/env python3
"""Bridge a Yacht Devices YDWG TCP feed into an Actisense-compatible TCP server."""

# pylint: disable=too-many-lines

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import signal
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, List, Optional, Sequence, Tuple

DLE = 0x10
STX = 0x02
ETX = 0x03
N2K_MSG_RECEIVED = 0x93
N2K_MSG_SEND = 0x94
ISO_REQUEST_PGN = 59904
PRODUCT_INFO_PGN = 126996
ACTISENSE_COMMAND_SEND = 0xA1
ACTISENSE_COMMAND_RECV = 0xA0
ACTISENSE_CMD_HARDWARE_INFO = 0x10
ACTISENSE_CMD_OPERATING_MODE = 0x11

OPERATING_MODE_RX_ALL = 0x0002
DEFAULT_MODEL_ID = 0x0101
DEFAULT_SERIAL_ID = 0x0000_0001

# Taken from Canboat's FastMessage PGN list. These PGNs need reassembly.
FAST_MESSAGE_PGNS = {
    126208,
    126464,
    126720,
    126983,
    126984,
    126985,
    126986,
    126987,
    126988,
    126996,
    126998,
    127233,
    127237,
    127489,
    127490,
    127491,
    127494,
    127495,
    127496,
    127497,
    127498,
    127503,
    127504,
    127506,
    127507,
    127509,
    127510,
    127511,
    127512,
    127513,
    127514,
    128275,
    128520,
    128538,
    129029,
    129038,
    129039,
    129040,
    129041,
    129044,
    129045,
    129284,
    129285,
    129301,
    129302,
    129538,
    129540,
    129541,
    129542,
    129545,
    129547,
    129549,
    129551,
    129556,
    129792,
    129793,
    129794,
    129795,
    129796,
    129797,
    129798,
    129799,
    129800,
    129801,
    129802,
    129803,
    129804,
    129805,
    129806,
    129807,
    129808,
    129809,
    129810,
    130052,
    130053,
    130054,
    130060,
    130061,
    130064,
    130065,
    130066,
    130067,
    130068,
    130069,
    130070,
    130071,
    130072,
    130073,
    130074,
    130320,
    130321,
    130322,
    130323,
    130324,
    130330,
    130561,
    130562,
    130563,
    130564,
    130565,
    130566,
    130567,
    130569,
    130570,
    130571,
    130572,
    130573,
    130574,
    130577,
    130578,
    130580,
    130581,
    130583,
    130584,
    130586,
    130816,
    130817,
    130818,
    130819,
    130820,
    130821,
    130822,
    130823,
    130824,
    130825,
    130827,
    130828,
    130831,
    130832,
    130833,
    130834,
    130835,
    130836,
    130837,
    130838,
    130839,
    130840,
    130842,
    130843,
    130845,
    130846,
    130847,
    130848,
    130850,
    130851,
    130856,
    130860,
    130880,
    130881,
    130918,
    130944,
}

COLOR_RESET = "\033[0m"
COLOR_YDWG_OC = "\033[96m"
COLOR_OC_EMU = "\033[92m"
COLOR_EMU_YDWG = "\033[93m"
COLOR_SYSTEM = "\033[95m"

# Highlight colors for key fields inside the log messages
COLOR_PGN_VALUE = "\033[94m"
COLOR_LEN_VALUE = "\033[91m"
COLOR_PAYLOAD_VALUE = "\033[90m"
COLOR_CAN_VALUE = "\033[95m"
COLOR_SID_VALUE = "\033[96m"
COLOR_DIRECTION_VALUE = "\033[92m"
COLOR_TIME_VALUE = "\033[90m"
LEVEL_COLORS = {
    logging.DEBUG: "\033[94m",
    logging.INFO: "\033[92m",
    logging.WARNING: "\033[93m",
    logging.ERROR: "\033[91m",
    logging.CRITICAL: "\033[41m",
}

TAG_YDWG_OC = f"{COLOR_YDWG_OC}[YDWG->OC]{COLOR_RESET}"
TAG_OC_EMU = f"{COLOR_OC_EMU}[OC->EMU]{COLOR_RESET}"
TAG_EMU_YDWG = f"{COLOR_EMU_YDWG}[EMU->YDWG]{COLOR_RESET}"
TAG_SYSTEM = f"{COLOR_SYSTEM}[EMU]{COLOR_RESET}"


async def _cancel_background_task(task: Optional[asyncio.Task[object]]) -> None:
    """Cancel and await a background task if it exists."""
    if task is None:
        return
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


async def _close_stream_writer(writer: Optional[asyncio.StreamWriter]) -> None:
    """Close an asyncio stream writer while ignoring platform errors."""
    if writer is None:
        return
    writer.close()
    with contextlib.suppress(OSError):
        await writer.wait_closed()


def _color_value(color: str, value: str) -> str:
    """Wrap a string with ANSI color markers."""
    return f"{color}{value}{COLOR_RESET}"


def format_pgn_field(pgn: int) -> str:
    """Return a PGN label formatted for logs."""
    return f"PGN {_color_value(COLOR_PGN_VALUE, str(pgn))}"


def format_len_field(length: int) -> str:
    """Return a colored length label for logs."""
    return f"len={_color_value(COLOR_LEN_VALUE, str(length))}"


def format_payload_field(payload: Sequence[int]) -> str:
    """Return a colored payload preview for logs."""
    return f"{COLOR_PAYLOAD_VALUE}{format_hex_ascii(payload)}{COLOR_RESET}"


def colorize_raw_line(line: str) -> str:
    """Add ANSI colors to a decoded RAW ASCII line for readability."""
    tokens = line.strip().split()
    if not tokens:
        return line
    idx = 0
    colored: list[str] = []
    if tokens[idx].count(":") == 2:
        colored.append(_color_value(COLOR_TIME_VALUE, tokens[idx]))
        idx += 1
    if idx < len(tokens) and tokens[idx].upper() in ("R", "T"):
        colored.append(_color_value(COLOR_DIRECTION_VALUE, tokens[idx].upper()))
        idx += 1
    if idx < len(tokens):
        colored.append(_color_value(COLOR_CAN_VALUE, tokens[idx]))
        idx += 1
    if idx < len(tokens):
        colored.append(_color_value(COLOR_SID_VALUE, tokens[idx]))
        idx += 1
    for token in tokens[idx:]:
        colored.append(_color_value(COLOR_PAYLOAD_VALUE, token))
    return " ".join(colored)


class ColorFormatter(logging.Formatter):
    """Formatter that injects ANSI colors into log records."""

    def format(self, record: logging.LogRecord) -> str:
        """Apply temporary ANSI decorations then defer to the base formatter."""
        original_levelname = record.levelname
        original_asctime = record.__dict__.get("asctime")
        level_color = LEVEL_COLORS.get(record.levelno)
        if level_color:
            record.levelname = f"{level_color}{record.levelname}{COLOR_RESET}"
        if self.usesTime():
            time_text = self.formatTime(record, self.datefmt)
            record.asctime = f"{COLOR_TIME_VALUE}{time_text}{COLOR_RESET}"
        try:
            return super().format(record)
        finally:
            record.levelname = original_levelname
            if original_asctime is not None:
                record.asctime = original_asctime
            elif hasattr(record, "asctime"):
                delattr(record, "asctime")

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


def format_hex_ascii(data: Sequence[int]) -> str:
    """Render bytes as a combined hex/ascii string."""
    hex_part = " ".join(f"{byte:02X}" for byte in data)
    ascii_part = "".join(chr(byte) if 32 <= byte <= 126 else "." for byte in data)
    return f"{hex_part} |{ascii_part}|"


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


@dataclass
class OutboundMessage:
    """N2K message requested by an Actisense client."""

    priority: int
    pgn: int
    source: int
    destination: int
    data: bytes


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


def build_can_id(priority: int, source: int, destination: int, pgn: int) -> int:
    """Build a 29-bit CAN identifier."""
    priority &= 0x7
    source &= 0xFF
    destination &= 0xFF
    pf = (pgn >> 8) & 0xFF
    ps = pgn & 0xFF
    dp = (pgn >> 16) & 0x01

    can_id = priority << 26
    can_id |= dp << 24
    can_id |= pf << 16
    if pf < 240:
        can_id |= destination << 8
    else:
        can_id |= ps << 8
    can_id |= source
    return can_id


class FastPacketAssembler:  # pylint: disable=too-few-public-methods
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

    def feed(  # pylint: disable=too-many-return-statements
        self, header: CanHeader, data: bytes
    ) -> Optional[bytes]:
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


class FastPacketEncoder:  # pylint: disable=too-few-public-methods
    """Split payloads into ISO fast-packet CAN frames."""

    def __init__(self) -> None:
        self._sequence_id = 0

    def encode(self, payload: bytes) -> List[bytes]:
        """Return CAN frames containing the provided payload."""
        if not payload:
            return []
        seq = self._sequence_id & 0x07
        self._sequence_id = (self._sequence_id + 1) & 0x07

        frames: List[bytes] = []
        total = len(payload)
        cursor = 0

        first = bytearray(8)
        first[0] = (seq << 5) | 0
        first[1] = total & 0xFF
        chunk = payload[:6]
        first[2 : 2 + len(chunk)] = chunk
        if len(chunk) < 6:
            first[2 + len(chunk) : 8] = b"\xFF" * (6 - len(chunk))
        frames.append(bytes(first))
        cursor += len(chunk)

        frame_index = 1
        while cursor < total:
            frame = bytearray(8)
            frame[0] = (seq << 5) | (frame_index & 0x1F)
            chunk = payload[cursor : cursor + 7]
            frame[1 : 1 + len(chunk)] = chunk
            if len(chunk) < 7:
                frame[1 + len(chunk) : 8] = b"\xFF" * (7 - len(chunk))
            frames.append(bytes(frame))
            cursor += len(chunk)
            frame_index += 1

        return frames


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


def actisense_time_string(seconds: Optional[float] = None) -> str:
    """Return an Actisense timestamp string in HHMMSS.mmm format."""
    if seconds is None:
        now = time.localtime()
        millis = int((time.time() % 1) * 1000)
        return f"{now.tm_hour:02d}{now.tm_min:02d}{now.tm_sec:02d}.{millis:03d}"
    total = seconds % 86400
    hours = int(total // 3600) % 24
    minutes = int((total % 3600) // 60)
    secs = int(total % 60)
    millis = int((total - int(total)) * 1000)
    return f"{hours:02d}{minutes:02d}{secs:02d}.{millis:03d}"


def encode_actisense_ascii_line(
    header: CanHeader, payload: bytes, seconds: Optional[float] = None
) -> str:
    """Format an Actisense ASCII line using the provided payload."""
    time_str = actisense_time_string(seconds)
    sdp = f"{header.source & 0xFF:02X}{header.destination & 0xFF:02X}{header.priority & 0x0F:X}"
    spgn = f"{header.pgn & 0x1FFFF:05X}"
    data_str = "".join(f"{byte:02X}" for byte in payload)
    return f"A{time_str} {sdp} {spgn} {data_str}"


class ActisenseClient:  # pylint: disable=too-many-instance-attributes
    """Single TCP client connected to the emulator."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        on_disconnect,
        *,
        on_command: Optional[
            Callable[["ActisenseClient", int, bytes], Awaitable[None]]
        ] = None,
        on_text: Optional[
            Callable[["ActisenseClient", str], Awaitable[None]]
        ] = None,
        output_mode: str = "auto",
    ) -> None:
        self.reader = reader
        self.writer = writer
        self._on_disconnect = on_disconnect
        self._on_command = on_command
        self._on_text = on_text
        self._output_mode = output_mode
        self._closed = False
        self._task: Optional[asyncio.Task[None]] = None
        self.peername = writer.get_extra_info("peername")
        self._parser = ActisenseFrameParser(self._handle_frame)
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._logged_activity = False
        self._text_buffer = bytearray()
        self._text_mode_detected = False
        self._prefers_ascii = output_mode == "ascii"

    def start(self) -> None:
        """Start consuming reads from the client."""
        self._task = asyncio.create_task(self._consume())

    async def send(self, data: bytes) -> None:
        """Send a binary frame to the client."""
        if self._closed:
            raise ConnectionError("Client already closed")
        self.writer.write(data)
        await self.writer.drain()

    async def send_ascii_line(self, line: str) -> None:
        """Send an ASCII line terminated with CRLF."""
        if self._closed:
            return
        payload = (line + "\r\n").encode("ascii")
        self.writer.write(payload)
        await self.writer.drain()

    def wants_ascii(self) -> bool:
        """Return True if the client prefers ASCII payloads."""
        if self._output_mode == "ascii":
            return True
        if self._output_mode == "binary":
            return False
        return self._prefers_ascii

    def wants_binary(self) -> bool:
        """Return True if the client prefers binary payloads."""
        if self._output_mode == "ascii":
            return False
        if self._output_mode == "binary":
            return True
        return not self._prefers_ascii

    async def close(self) -> None:
        """Close the client connection and cancel the reader task."""
        if self._closed:
            return
        self._closed = True
        if self._task and self._task is not asyncio.current_task():
            self._task.cancel()
        await _close_stream_writer(self.writer)
        self._on_disconnect(self)

    async def _consume(self) -> None:
        """Drain data from the TCP stream and dispatch to handlers."""
        try:
            while True:
                data = await self.reader.read(1024)
                if not data:
                    break
                snippet = data[:32].hex()
                if not self._logged_activity:
                    logging.info(
                        "Client %s sent %d bytes (first chunk: %s%s)",
                        self.peername,
                        len(data),
                        snippet,
                        "…" if len(data) > 32 else "",
                    )
                    self._logged_activity = True
                else:
                    logging.debug(
                        "Client %s sent %d bytes (sample %s%s)",
                        self.peername,
                        len(data),
                        snippet,
                        "…" if len(data) > 32 else "",
                    )
                if self._on_command:
                    self._parser.feed(data)
                if self._on_text:
                    self._maybe_process_text(data)
        except asyncio.CancelledError:
            pass
        except (asyncio.IncompleteReadError, ConnectionError, OSError) as exc:
            logging.warning("Client %s read error: %s", self.peername, exc)
        finally:
            await self.close()

    def _handle_frame(self, command: int, payload: bytes) -> None:
        """Invoke the command handler for decoded frames."""
        if not self._on_command:
            return
        if self._loop is None:
            try:
                self._loop = asyncio.get_running_loop()
            except RuntimeError:
                return
        self._loop.create_task(self._on_command(self, command, payload))

    def _maybe_process_text(self, data: bytes) -> None:
        """Detect ASCII clients and forward their lines."""
        printable_chunk = all(
            0x20 <= byte <= 0x7E or byte in (0x09, 0x0A, 0x0D, 0x0B)
            for byte in data
        )
        if not printable_chunk:
            if self._text_mode_detected:
                self._text_buffer.clear()
            return
        if not self._text_mode_detected:
            logging.info("Client %s switched to RAW ASCII mode", self.peername)
            self._text_mode_detected = True
            if self._output_mode == "auto":
                self._prefers_ascii = True
        self._text_buffer.extend(data)
        while b"\n" in self._text_buffer:
            line_bytes, _, remainder = self._text_buffer.partition(b"\n")
            self._text_buffer = bytearray(remainder)
            line_bytes = line_bytes.rstrip(b"\r")
            if not line_bytes:
                continue
            try:
                line = line_bytes.decode("ascii")
            except UnicodeDecodeError:
                continue
            if self._loop is None:
                try:
                    self._loop = asyncio.get_running_loop()
                except RuntimeError:
                    return
            clean_line = line.strip()
            if not clean_line:
                continue
            self._loop.create_task(self._on_text(self, clean_line))


class ActisenseServer:
    """Tiny TCP server that pretends to be an Actisense NGT-1 device."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        host: str,
        port: int,
        *,
        output_mode: str = "auto",
        command_handler: Optional[
            Callable[[ActisenseClient, int, bytes], Awaitable[None]]
        ] = None,
        text_handler: Optional[
            Callable[[ActisenseClient, str], Awaitable[None]]
        ] = None,
    ) -> None:
        self._host = host
        self._port = port
        self._server: Optional[asyncio.base_events.Server] = None
        self._clients: set[ActisenseClient] = set()
        self._on_command = command_handler
        self._on_text = text_handler
        self._mode = output_mode

    async def start(self) -> None:
        """Start listening for Actisense client connections."""
        self._server = await asyncio.start_server(
            self._on_connection, self._host, self._port
        )
        socknames = ", ".join(str(sock.getsockname()) for sock in self._server.sockets)
        logging.info("Actisense emulator listening on %s", socknames)

    async def broadcast(self, payloads: Sequence[bytes]) -> None:
        """Broadcast binary payloads to clients that opted in."""
        if not self._clients:
            return
        dead: List[ActisenseClient] = []
        for client in list(self._clients):
            if not client.wants_binary():
                continue
            for payload in payloads:
                try:
                    await client.send(payload)
                except (ConnectionError, asyncio.CancelledError, OSError) as exc:
                    logging.warning("Failed to send to %s: %s", client.peername, exc)
                    dead.append(client)
                    break
        for client in dead:
            await client.close()

    async def broadcast_ascii(self, lines: Sequence[str]) -> None:
        """Broadcast ASCII lines to interested clients."""
        if not self._clients:
            return
        dead: List[ActisenseClient] = []
        for client in list(self._clients):
            if not client.wants_ascii():
                continue
            for line in lines:
                try:
                    await client.send_ascii_line(line)
                except (ConnectionError, asyncio.CancelledError, OSError) as exc:
                    logging.warning("Failed to send ASCII to %s: %s", client.peername, exc)
                    dead.append(client)
                    break
        for client in dead:
            await client.close()

    async def close(self) -> None:
        """Shut down the server and all connected clients."""
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        for client in list(self._clients):
            await client.close()

    def _on_disconnect(self, client: ActisenseClient) -> None:
        """Remove clients when their connections close."""
        self._clients.discard(client)
        logging.info("Client %s disconnected", client.peername)

    def set_command_handler(
        self,
        handler: Optional[
            Callable[[ActisenseClient, int, bytes], Awaitable[None]]
        ],
    ) -> None:
        """Register a callback for binary Actisense frames."""
        self._on_command = handler

    def set_text_handler(
        self,
        handler: Optional[
            Callable[[ActisenseClient, str], Awaitable[None]]
        ],
    ) -> None:
        """Register a callback for ASCII text lines."""
        self._on_text = handler

    def wants_binary_output(self) -> bool:
        """Return True if any client should receive binary output."""
        if not self._clients or self._mode == "ascii":
            return False
        if self._mode == "binary":
            return True
        return any(client.wants_binary() for client in self._clients)

    def wants_ascii_output(self) -> bool:
        """Return True if any client should receive ASCII output."""
        if not self._clients or self._mode == "binary":
            return False
        if self._mode == "ascii":
            return True
        return any(client.wants_ascii() for client in self._clients)

    async def _on_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Create a client wrapper for each inbound TCP connection."""
        client = ActisenseClient(
            reader,
            writer,
            self._on_disconnect,
            on_command=self._on_command,
            on_text=self._on_text,
            output_mode=self._mode,
        )
        self._clients.add(client)
        logging.info("Client %s connected", client.peername)
        client.start()


class ActisenseFrameParser:  # pylint: disable=too-few-public-methods
    """Decode DLE-STX framed Actisense messages from clients."""

    def __init__(self, on_frame: Callable[[int, bytes], None]) -> None:
        self._on_frame = on_frame
        self._buffer = bytearray()
        self._in_frame = False
        self._escaping = False

    def feed(self, data: bytes) -> None:
        """Process a chunk of bytes coming from a client."""
        for byte in data:
            if self._escaping:
                self._handle_escape(byte)
            elif byte == DLE:
                self._escaping = True
            elif self._in_frame:
                self._buffer.append(byte)

    def _handle_escape(self, byte: int) -> None:
        self._escaping = False
        if byte == STX:
            self._buffer.clear()
            self._in_frame = True
        elif byte == ETX:
            if self._in_frame:
                frame = bytes(self._buffer)
                self._buffer.clear()
                self._in_frame = False
                self._process_frame(frame)
        elif byte == DLE:
            if self._in_frame:
                self._buffer.append(byte)
        else:
            # Unexpected escape sequence, drop current frame
            self._buffer.clear()
            self._in_frame = False

    def _process_frame(self, frame: bytes) -> None:
        if len(frame) < 3:
            return
        command = frame[0]
        length = frame[1]
        if len(frame) < 3 + length:
            return
        payload = frame[2 : 2 + length]
        checksum = frame[2 + length]
        computed = (command + length + sum(payload) + checksum) & 0xFF
        if computed != 0:
            logging.debug("Invalid checksum for command 0x%02X", command)
            return
        self._on_frame(command, bytes(payload))


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
    return RawCanFrame(
        seconds_since_midnight=seconds,
        can_id=can_id,
        data=data,
        direction=direction,
    )


def parse_actisense_ascii_line(  # pylint: disable=too-many-return-statements
    line: str,
) -> Optional[OutboundMessage]:
    """Parse an Actisense ASCII command into an outbound message."""
    stripped = line.strip()
    if not stripped or stripped[0].upper() != "A":
        return None
    parts = stripped[1:].strip().split()
    if len(parts) < 4:
        return None
    sdp = parts[1]
    if len(sdp) < 5:
        return None
    try:
        source = int(sdp[0:2], 16)
        destination = int(sdp[2:4], 16)
        priority = int(sdp[4], 16)
    except ValueError:
        return None
    try:
        pgn = int(parts[2], 16)
    except ValueError:
        return None
    data_field = parts[3]
    if len(data_field) % 2 != 0:
        return None
    try:
        data = bytes(int(data_field[i : i + 2], 16) for i in range(0, len(data_field), 2))
    except ValueError:
        return None
    return OutboundMessage(
        priority=priority,
        pgn=pgn,
        source=source,
        destination=destination,
        data=data,
    )


class ActisenseBridge:  # pylint: disable=too-many-instance-attributes
    """Convert between YDWG frames and Actisense messages."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        server: ActisenseServer,
        ydwg: "YdwgClient",
        *,
        fast_timeout: float = 2.0,
        tx_source: int = 0x00,
        log_direction: str = "both",
    ) -> None:
        self._server = server
        self._ydwg = ydwg
        self._assembler = FastPacketAssembler(timeout=fast_timeout)
        self._fast_encoder = FastPacketEncoder()
        self._tx_source = tx_source & 0xFF
        self._log_direction = log_direction
        self._operating_mode = OPERATING_MODE_RX_ALL
        self._status_sid = 0
        self._model_id = DEFAULT_MODEL_ID
        self._serial_id = DEFAULT_SERIAL_ID
        self._error_id = 0
        self._hardware_info = "YDWG Actisense Emulator"

    async def handle_frame(self, frame: RawCanFrame) -> None:
        """Process a raw CAN frame coming from the YDWG source."""
        if not frame.data:
            return
        header = parse_can_header(frame.can_id)
        if header.pgn in FAST_MESSAGE_PGNS:
            payload = self._assembler.feed(header, frame.data)
            if payload is None:
                return
        else:
            payload = bytes(frame.data)

        self._log(
            "rx",
            f"{TAG_YDWG_OC} ASCII {format_pgn_field(header.pgn)} "
            f"{format_len_field(len(payload))} {format_payload_field(payload)}",
        )
        if self._server.wants_binary_output():
            binary_frame = encode_actisense_n2k(header, payload, frame.timestamp_ms())
            await self._server.broadcast([binary_frame])
        if self._server.wants_ascii_output():
            ascii_line = encode_actisense_ascii_line(
                header, payload, frame.seconds_since_midnight
            )
            await self._server.broadcast_ascii([ascii_line])

    async def handle_client_command(  # pylint: disable=too-many-return-statements
        self, client: ActisenseClient, command: int, payload: bytes
    ) -> None:
        """Handle binary commands received from Actisense clients."""
        if command == N2K_MSG_SEND:
            outbound = self._parse_outbound_message(payload)
            if not outbound:
                logging.debug("Malformed send request from %s", client.peername)
                return
            prio_field = _color_value(COLOR_SID_VALUE, str(outbound.priority))
            dst_field = _color_value(COLOR_SID_VALUE, str(outbound.destination))
            self._log(
                "tx",
                f"{TAG_OC_EMU} {client.peername} {format_pgn_field(outbound.pgn)} "
                f"prio={prio_field} dst={dst_field} {format_len_field(len(outbound.data))} "
                f"{format_payload_field(outbound.data)}",
            )
            frames = self._prepare_outbound_frames(outbound)
            if not frames:
                logging.debug("No frames generated for PGN %u", outbound.pgn)
                return
            for line in frames:
                self._log("tx", f"{TAG_EMU_YDWG} RAW {colorize_raw_line(line)}")
            await self._ydwg.send_frames(frames)
            return
        if command == ACTISENSE_COMMAND_SEND:
            await self._handle_actisense_command(client, payload)
            return
        logging.debug(
            "Ignoring unsupported command 0x%02X from %s",
            command,
            client.peername,
        )

    async def handle_text_line(self, client: ActisenseClient, line: str) -> None:
        """Handle ASCII-mode clients that send commands as text."""
        if line.upper().startswith("A"):
            msg = parse_actisense_ascii_line(line)
            if msg:
                can_id = build_can_id(msg.priority, msg.source, msg.destination, msg.pgn)
                raw_lines = self._format_raw_fast_packets(can_id, msg.data)
                self._log(
                    "tx",
                    f"{TAG_OC_EMU} {client.peername} ASCII {format_pgn_field(msg.pgn)} "
                    f"{format_len_field(len(msg.data))} {format_payload_field(msg.data)}",
                )
                for raw in raw_lines:
                    self._log("tx", f"{TAG_EMU_YDWG} RAW {colorize_raw_line(raw)}")
                await self._ydwg.send_frames(raw_lines)
                return
        parsed_raw = self._parse_raw_ascii_line(line)
        if parsed_raw:
            clean = line.rstrip("\r\n")
            self._log("tx", f"{TAG_EMU_YDWG} passthrough {colorize_raw_line(clean)}")
            await self._ydwg.send_frames([clean])
            return
        logging.debug("Ignoring malformed ASCII line from %s: %s", client.peername, line)

    def _parse_outbound_message(self, payload: bytes) -> Optional[OutboundMessage]:
        """Decode a binary N2K_MSG_SEND payload."""
        if len(payload) < 6:
            return None
        priority = payload[0] & 0x07
        pgn = payload[1] | (payload[2] << 8) | (payload[3] << 16)
        destination = payload[4]
        length = payload[5]
        data = payload[6 : 6 + length]
        if len(data) != length:
            return None
        return OutboundMessage(
            priority=priority,
            pgn=pgn,
            source=self._tx_source,
            destination=destination,
            data=bytes(data),
        )

    def _prepare_outbound_frames(self, msg: OutboundMessage) -> List[str]:
        """Split outbound payloads into RAW ASCII CAN frames."""
        can_id = build_can_id(msg.priority, msg.source, msg.destination, msg.pgn)
        return self._format_raw_fast_packets(can_id, msg.data)

    def _format_raw_fast_packets(self, can_id: int, payload: bytes) -> List[str]:
        """Format payload bytes as RAW ASCII FAST packet lines."""
        if len(payload) <= 8:
            return [f"{can_id:08X} " + " ".join(f"{byte:02X}" for byte in payload)]

        frames: List[str] = []
        total_len = len(payload)
        cursor = 0
        frame_index = 0
        while cursor < len(payload):
            if frame_index == 0:
                chunk = payload[cursor : cursor + 6]
                cursor += len(chunk)
                data = [frame_index & 0x1F, total_len & 0xFF]
                data.extend(chunk)
                while len(data) < 8:
                    data.append(0xFF)
            else:
                chunk = payload[cursor : cursor + 7]
                cursor += len(chunk)
                data = [frame_index & 0x1F]
                data.extend(chunk)
                while len(data) < 8:
                    data.append(0xFF)
            frames.append(f"{can_id:08X} " + " ".join(f"{b:02X}" for b in data))
            frame_index += 1
        return frames

    def _log(self, direction: str, message: str) -> None:
        """Emit directional debug logs."""
        if self._log_direction not in ("both", direction):
            return
        logging.debug(message)

    @staticmethod
    def _requested_pgn_from_iso(data: Sequence[int]) -> Optional[int]:
        """Return the PGN requested by an ISO Request payload."""
        if len(data) < 3:
            return None
        return data[0] | (data[1] << 8) | (data[2] << 16)

    @staticmethod
    def _parse_raw_ascii_line(
        line: str,
    ) -> Optional[Tuple[Optional[float], str, int, List[int]]]:
        """Parse a passthrough RAW ASCII line."""
        tokens = line.strip().split()
        if not tokens:
            return None
        idx = 0
        seconds = None
        direction = "T"
        token0 = tokens[idx]
        if token0.count(":") == 2:
            try:
                seconds = parse_time_to_seconds(token0)
                idx += 1
            except ValueError:
                seconds = None
        if idx < len(tokens) and tokens[idx].upper() in ("R", "T"):
            direction = tokens[idx].upper()
            idx += 1
        if idx >= len(tokens):
            return None
        can_str = tokens[idx]
        idx += 1
        try:
            can_id = int(can_str, 16)
        except ValueError:
            return None
        data_bytes: List[int] = []
        while idx < len(tokens):
            token = tokens[idx]
            idx += 1
            try:
                value = int(token, 16)
            except ValueError:
                return None
            data_bytes.append(value & 0xFF)
        if not data_bytes:
            return None
        return seconds, direction, can_id, data_bytes

    def _next_sid(self) -> int:
        """Return the next sequential message SID."""
        sid = self._status_sid & 0xFF
        self._status_sid = (self._status_sid + 1) & 0xFF
        return sid

    def _build_operating_mode_payload(self) -> bytes:
        """Prepare the Actisense operating mode reply payload."""
        sid = self._next_sid()
        body = bytearray()
        body.append(ACTISENSE_CMD_OPERATING_MODE)
        body.append(sid)
        body.extend(self._model_id.to_bytes(2, byteorder="little"))
        body.extend(self._serial_id.to_bytes(4, byteorder="little"))
        body.extend(self._error_id.to_bytes(4, byteorder="little"))
        body.extend(self._operating_mode.to_bytes(2, byteorder="little"))
        return bytes(body)

    async def _handle_actisense_command(
        self, client: ActisenseClient, payload: bytes
    ) -> None:
        """Respond to Actisense configuration commands."""
        if not payload:
            return
        command_id = payload[0]
        if command_id == ACTISENSE_CMD_OPERATING_MODE:
            payload = self._build_operating_mode_payload()
            await self._send_actisense_reply(client, payload)
            logging.debug(
                "%s Responded with operating mode 0x%04X to %s",
                TAG_SYSTEM,
                self._operating_mode,
                client.peername,
            )
            return
        if command_id == ACTISENSE_CMD_HARDWARE_INFO:
            body = bytearray()
            body.append(ACTISENSE_CMD_HARDWARE_INFO)
            info_bytes = self._hardware_info.encode("ascii", errors="ignore")
            body.extend(info_bytes)
            body.append(0x00)
            await self._send_actisense_reply(client, bytes(body))
            logging.debug(
                "%s Responded with hardware info '%s' to %s",
                TAG_SYSTEM,
                self._hardware_info,
                client.peername,
            )
            return
        logging.debug(
            "%s Unknown Actisense command 0x%02X from %s",
            TAG_SYSTEM,
            command_id,
            client.peername,
        )

    async def _send_actisense_reply(self, client: ActisenseClient, body: bytes) -> None:
        """Send a command reply frame to a client."""
        frame = wrap_actisense_payload(ACTISENSE_COMMAND_RECV, body)
        try:
            await client.send(frame)
        except (ConnectionError, asyncio.TimeoutError, asyncio.CancelledError, OSError) as exc:
            logging.debug(
                "%s Failed to send Actisense command response: %s",
                TAG_SYSTEM,
                exc,
            )

class YdwgClient:
    """TCP client for YDWG raw ASCII streams."""

    def __init__(
        self, host: str, port: int, reconnect_delay: float = 3.0
    ) -> None:
        self._host = host
        self._port = port
        self._reconnect_delay = reconnect_delay
        self._tx_queue: asyncio.Queue[str] = asyncio.Queue()
        self._connected = asyncio.Event()

    async def run(self, handler: Callable[[RawCanFrame], Awaitable[None]]) -> None:
        """Connect to YDWG, forwarding frames to the provided handler."""
        while True:
            reader: Optional[asyncio.StreamReader] = None
            writer: Optional[asyncio.StreamWriter] = None
            reader_task: Optional[asyncio.Task[None]] = None
            writer_task: Optional[asyncio.Task[None]] = None
            try:
                reader, writer = await asyncio.open_connection(self._host, self._port)
                self._connected.set()
                logging.info("Connected to YDWG at %s:%s", self._host, self._port)
                reader_task = asyncio.create_task(self._read_loop(reader, handler))
                writer_task = asyncio.create_task(self._write_loop(writer))
                done, pending = await asyncio.wait(
                    [reader_task, writer_task], return_when=asyncio.FIRST_COMPLETED
                )
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                for task in done:
                    try:
                        exc = task.exception()
                    except asyncio.CancelledError:
                        continue
                    if exc:
                        raise exc
            except (ConnectionError, OSError, asyncio.TimeoutError) as exc:
                logging.warning("YDWG connection error: %s", exc)
            finally:
                self._connected.clear()
                if reader_task:
                    reader_task.cancel()
                if writer_task:
                    writer_task.cancel()
                await asyncio.gather(
                    *(t for t in [reader_task, writer_task] if t),
                    return_exceptions=True,
                )
                await _close_stream_writer(writer)
                logging.info(
                    "Reconnecting to YDWG in %.1f seconds", self._reconnect_delay
                )
                await asyncio.sleep(self._reconnect_delay)

    async def send_frames(self, frames: Sequence[str]) -> None:
        """Queue text frames for transmission to YDWG."""
        if not frames:
            return
        for frame in frames:
            await self._tx_queue.put(frame)
        if not self._connected.is_set():
            logging.debug("Queued %d frames while YDWG offline", len(frames))

    async def _read_loop(
        self, reader: asyncio.StreamReader, handler: Callable[[RawCanFrame], Awaitable[None]]
    ) -> None:
        """Continuously read ASCII lines from the YDWG socket."""
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

    async def _write_loop(self, writer: asyncio.StreamWriter) -> None:
        """Transmit queued frames to the YDWG device."""
        try:
            while True:
                line = await self._tx_queue.get()
                payload = (line + "\r\n").encode("ascii")
                writer.write(payload)
                await writer.drain()
                logging.debug("%s RAW SENT %s", TAG_EMU_YDWG, colorize_raw_line(line))
        except (ConnectionError, OSError, asyncio.TimeoutError) as exc:
            logging.warning("YDWG write failed: %s", exc)
            raise


async def run_emulator(args) -> None:
    """Start the TCP bridge and keep it running until interrupted."""
    ydwg = YdwgClient(args.ydwg_host, args.ydwg_port, args.reconnect_delay)
    server = ActisenseServer(
        args.listen_host,
        args.listen_port,
        output_mode=args.actisense_output,
    )
    bridge = ActisenseBridge(
        server,
        ydwg,
        fast_timeout=args.fast_timeout,
        tx_source=args.tx_source,
        log_direction=args.log_direction,
    )
    server.set_command_handler(bridge.handle_client_command)
    server.set_text_handler(bridge.handle_text_line)
    await server.start()

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
    await _cancel_background_task(ydwg_task)
    await server.close()


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Return parsed CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Emulate an Actisense NGT-1 device using a YDWG raw TCP stream."
    )
    parser.add_argument(
        "--ydwg-host",
        default="127.0.0.1",
        help="YDWG host/IP (default: %(default)s)",
    )
    parser.add_argument(
        "--ydwg-port",
        type=int,
        default=1456,
        help="YDWG TCP port (default: %(default)s)",
    )
    parser.add_argument(
        "--listen-host",
        default="127.0.0.1",
        help="Local host/IP for the Actisense server",
    )
    parser.add_argument(
        "--listen-port",
        type=int,
        default=50001,
        help="Local TCP port for the Actisense server",
    )
    parser.add_argument(
        "--fast-timeout",
        type=float,
        default=2.0,
        help="Seconds before abandoning an incomplete fast packet",
    )
    parser.add_argument(
        "--reconnect-delay",
        type=float,
        default=3.0,
        help="Seconds to wait before reconnecting to YDWG",
    )
    parser.add_argument(
        "--tx-source",
        type=lambda value: int(value, 0),
        default=0x00,
        help="Source address for binary N2K_MSG_SEND frames",
    )
    parser.add_argument(
        "--log-direction",
        choices=["both", "tx", "rx"],
        default="both",
        help="Choose which Actisense directions appear in emulator debug logs",
    )
    parser.add_argument(
        "--actisense-output",
        choices=["auto", "binary", "ascii"],
        default="auto",
        help=(
            "Choose how clients receive PGNs: auto-detect per client, "
            "force binary, or force ASCII"
        ),
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (default: %(default)s)",
    )
    return parser.parse_args(argv)


def configure_logging(level: str) -> None:
    """Install the colored logging configuration."""
    log_level = getattr(logging, level.upper(), logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(ColorFormatter("%(asctime)s %(levelname)s %(message)s"))
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(log_level)
    root.addHandler(handler)


def main(argv: Optional[Sequence[str]] = None) -> None:
    """Entry point used by the CLI."""
    args = parse_args(argv)
    configure_logging(args.log_level)
    try:
        asyncio.run(run_emulator(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
