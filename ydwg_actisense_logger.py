#!/usr/bin/env python3
"""Transparent Actisense/YDWG logger."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
from typing import Optional, Sequence, Set

COLOR_RESET = "\033[0m"
COLOR_RX = "\033[96m"
COLOR_TX = "\033[92m"
COLOR_SYS = "\033[95m"

TAG_RX = f"{COLOR_RX}[YDWG->OC]{COLOR_RESET}"
TAG_TX = f"{COLOR_TX}[OC->YDWG]{COLOR_RESET}"
TAG_SYS = f"{COLOR_SYS}[LOGGER]{COLOR_RESET}"


async def _stop_task(task: Optional[asyncio.Task[object]]) -> None:
    """Best-effort cancellation helper for logger tasks."""
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def _close_writer(writer: Optional[asyncio.StreamWriter]) -> None:
    """Close a stream writer while ignoring OS-specific errors."""
    if writer is None:
        return
    writer.close()
    try:
        await writer.wait_closed()
    except OSError:
        pass


def format_hex_ascii(data: bytes) -> str:
    """Render bytes as a dual hex/ASCII preview string."""
    hex_part = " ".join(f"{byte:02X}" for byte in data)
    ascii_part = "".join(chr(byte) if 32 <= byte <= 126 else "." for byte in data)
    return f"{hex_part} |{ascii_part}|"


class RawLoggerBridge:  # pylint: disable=too-many-instance-attributes,too-few-public-methods
    """Simple pass-through bridge with logging."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        ydwg_host: str,
        ydwg_port: int,
        *,
        listen_host: str,
        listen_port: int,
        log_filter: str,
    ) -> None:
        self._ydwg_host = ydwg_host
        self._ydwg_port = ydwg_port
        self._listen_host = listen_host
        self._listen_port = listen_port
        self._log_filter = log_filter
        self._clients: Set[asyncio.StreamWriter] = set()
        self._tx_queue: Optional[asyncio.Queue[bytes]] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._ydwg_writer: Optional[asyncio.StreamWriter] = None

    async def run(self) -> None:
        """Run the logger until the stop event is triggered."""
        loop = asyncio.get_running_loop()
        self._stop_event = asyncio.Event()
        self._tx_queue = asyncio.Queue()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._stop_event.set)
            except NotImplementedError:
                pass

        server = await asyncio.start_server(
            self._handle_client, self._listen_host, self._listen_port
        )
        logging.info(
            "%s Listening on %s:%d", TAG_SYS, self._listen_host, self._listen_port
        )

        assert self._stop_event is not None
        ydwg_task = asyncio.create_task(self._run_ydwg())
        try:
            await self._stop_event.wait()
        finally:
            await _stop_task(ydwg_task)
            server.close()
            await server.wait_closed()
            logging.info("%s Logger stopped", TAG_SYS)

    async def _run_ydwg(self) -> None:
        """Maintain the upstream YDWG connection."""
        assert self._stop_event is not None and self._tx_queue is not None
        while not self._stop_event.is_set():
            writer_task: Optional[asyncio.Task[None]] = None
            try:
                reader, writer = await asyncio.open_connection(
                    self._ydwg_host, self._ydwg_port
                )
                self._ydwg_writer = writer
                logging.info(
                    "%s Connected to YDWG %s:%d",
                    TAG_SYS,
                    self._ydwg_host,
                    self._ydwg_port,
                )
                writer_task = asyncio.create_task(self._ydwg_writer_task(writer))
                while True:
                    data = await reader.read(4096)
                    if not data:
                        raise ConnectionError("YDWG closed connection")
                    await self._broadcast_to_clients(data)
                    self._log_packet("rx", data)
            except (ConnectionError, OSError, asyncio.TimeoutError) as exc:
                logging.warning("%s YDWG link error: %s", TAG_SYS, exc)
            finally:
                await _stop_task(writer_task)
                await _close_writer(self._ydwg_writer)
                self._ydwg_writer = None
                await self._clear_tx_queue()
                if not self._stop_event.is_set():
                    logging.info("%s Reconnecting to YDWG in 2s", TAG_SYS)
                    await asyncio.sleep(2)

    async def _ydwg_writer_task(self, writer: asyncio.StreamWriter) -> None:
        """Forward client data to the YDWG writer."""
        assert self._tx_queue is not None
        try:
            while True:
                data = await self._tx_queue.get()
                writer.write(data)
                await writer.drain()
        except (ConnectionError, OSError, asyncio.TimeoutError) as exc:
            logging.warning("%s YDWG writer error: %s", TAG_SYS, exc)

    async def _broadcast_to_clients(self, data: bytes) -> None:
        """Send data received from YDWG to every Actisense client."""
        self._log_packet("rx", data)
        dead: Set[asyncio.StreamWriter] = set()
        for client in list(self._clients):
            try:
                client.write(data)
                await client.drain()
            except (ConnectionError, OSError):
                dead.add(client)
        for client in dead:
            await self._drop_client(client)

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Handle an Actisense client connection."""
        peer = writer.get_extra_info("peername")
        logging.info("%s Client %s connected", TAG_SYS, peer)
        self._clients.add(writer)
        try:
            while True:
                data = await reader.read(4096)
                if not data:
                    break
                assert self._tx_queue is not None
                await self._tx_queue.put(data)
                self._log_packet("tx", data)
        except ConnectionResetError:
            logging.warning("%s Client %s reset connection", TAG_SYS, peer)
        finally:
            logging.info("%s Client %s disconnected", TAG_SYS, peer)
            await self._drop_client(writer)

    async def _drop_client(self, writer: asyncio.StreamWriter) -> None:
        """Remove a client and close its socket."""
        if writer in self._clients:
            self._clients.remove(writer)
        await _close_writer(writer)

    async def _clear_tx_queue(self) -> None:
        """Empty queued TX messages to avoid stale data."""
        if not self._tx_queue:
            return
        while not self._tx_queue.empty():
            try:
                self._tx_queue.get_nowait()
                self._tx_queue.task_done()
            except asyncio.QueueEmpty:
                break

    def _log_packet(self, direction: str, data: bytes) -> None:
        """Emit a formatted log entry when traffic matches the filter."""
        if self._log_filter == "tx" and direction != "tx":
            return
        if self._log_filter == "rx" and direction != "rx":
            return
        tag = TAG_TX if direction == "tx" else TAG_RX
        logging.debug("%s %s", tag, format_hex_ascii(data))


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse CLI arguments for the logger."""
    parser = argparse.ArgumentParser(
        description="Transparent logger between OpenCPN and YDWG."
    )
    parser.add_argument(
        "--ydwg-host",
        default="127.0.0.1",
        help="YDWG host/IP",
    )
    parser.add_argument(
        "--ydwg-port",
        type=int,
        default=1456,
        help="YDWG TCP port",
    )
    parser.add_argument(
        "--listen-host",
        default="127.0.0.1",
        help="Local host for Actisense clients",
    )
    parser.add_argument(
        "--listen-port",
        type=int,
        default=50002,
        help="Local listening port",
    )
    parser.add_argument(
        "--log-direction",
        choices=["both", "tx", "rx"],
        default="both",
        help="Filter logger output to TX, RX, or both directions",
    )
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    return parser.parse_args(argv)


def configure_logging(level: str) -> None:
    """Configure the basic logger."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )


def main() -> None:
    """CLI entry point for the logger."""
    args = parse_args()
    configure_logging(args.log_level)
    bridge = RawLoggerBridge(
        args.ydwg_host,
        args.ydwg_port,
        listen_host=args.listen_host,
        listen_port=args.listen_port,
        log_filter=args.log_direction,
    )
    try:
        asyncio.run(bridge.run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
