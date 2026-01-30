#!/usr/bin/env python3
"""Transparent Actisense/YDWG logger."""

import argparse
import asyncio
import contextlib
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


def format_hex_ascii(data: bytes) -> str:
    hex_part = " ".join(f"{byte:02X}" for byte in data)
    ascii_part = "".join(chr(byte) if 32 <= byte <= 126 else "." for byte in data)
    return f"{hex_part} |{ascii_part}|"


class RawLoggerBridge:
    """Simple pass-through bridge with logging."""

    def __init__(
        self,
        ydwg_host: str,
        ydwg_port: int,
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
        assert self._stop_event is not None
        ydwg_task = asyncio.create_task(self._run_ydwg())
        await self._stop_event.wait()

        ydwg_task.cancel()
        try:
            await ydwg_task
        except asyncio.CancelledError:
            pass

        server.close()
        await server.wait_closed()
        logging.info("%s Logger stopped", TAG_SYS)

    async def _run_ydwg(self) -> None:
        assert self._stop_event is not None and self._tx_queue is not None
        while not self._stop_event.is_set():
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
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logging.warning("%s YDWG link error: %s", TAG_SYS, exc)
            finally:
                if self._ydwg_writer:
                    self._ydwg_writer.close()
                    try:
                        await self._ydwg_writer.wait_closed()
                    except Exception:
                        pass
                    self._ydwg_writer = None
                await self._clear_tx_queue()
                if "writer_task" in locals():
                    writer_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await writer_task
                logging.info("%s Reconnecting to YDWG in 2s", TAG_SYS)
                await asyncio.sleep(2)

    async def _ydwg_writer_task(self, writer: asyncio.StreamWriter) -> None:
        assert self._tx_queue is not None
        try:
            while True:
                data = await self._tx_queue.get()
                writer.write(data)
                await writer.drain()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logging.warning("%s YDWG writer error: %s", TAG_SYS, exc)

    async def _broadcast_to_clients(self, data: bytes) -> None:
        self._log_packet("rx", data)
        dead: Set[asyncio.StreamWriter] = set()
        for client in list(self._clients):
            try:
                client.write(data)
                await client.drain()
            except Exception:
                dead.add(client)
        for client in dead:
            await self._drop_client(client)

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
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
        except asyncio.CancelledError:
            raise
        except ConnectionResetError:
            logging.warning("%s Client %s reset connection", TAG_SYS, peer)
        finally:
            logging.info("%s Client %s disconnected", TAG_SYS, peer)
            await self._drop_client(writer)

    async def _drop_client(self, writer: asyncio.StreamWriter) -> None:
        if writer in self._clients:
            self._clients.remove(writer)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

    async def _clear_tx_queue(self) -> None:
        if not self._tx_queue:
            return
        while not self._tx_queue.empty():
            try:
                self._tx_queue.get_nowait()
                self._tx_queue.task_done()
            except asyncio.QueueEmpty:
                break

    def _log_packet(self, direction: str, data: bytes) -> None:
        if self._log_filter == "tx" and direction != "tx":
            return
        if self._log_filter == "rx" and direction != "rx":
            return
        tag = TAG_TX if direction == "tx" else TAG_RX
        logging.debug("%s %s", tag, format_hex_ascii(data))


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transparent logger between OpenCPN and YDWG."
    )
    parser.add_argument("--ydwg-host", default="127.0.0.1", help="YDWG host/IP")
    parser.add_argument("--ydwg-port", type=int, default=1456, help="YDWG TCP port")
    parser.add_argument(
        "--listen-host", default="127.0.0.1", help="Local host for Actisense clients"
    )
    parser.add_argument(
        "--listen-port", type=int, default=50002, help="Local listening port"
    )
    parser.add_argument(
        "--log-direction",
        choices=["both", "tx", "rx"],
        default="both",
        help="Which direction to show in debug logs",
    )
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    return parser.parse_args(argv)


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )


def main() -> None:
    args = parse_args()
    configure_logging(args.log_level)
    bridge = RawLoggerBridge(
        args.ydwg_host,
        args.ydwg_port,
        args.listen_host,
        args.listen_port,
        args.log_direction,
    )
    try:
        asyncio.run(bridge.run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
