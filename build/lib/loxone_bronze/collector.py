from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import uuid

import aiohttp

from loxwebsocket import LoxWs

from . import __version__
from .common import compact_json, env_required, raw_envelope, sha256_text, utc_now_iso
from .spool import Spool

LOG = logging.getLogger("loxone_bronze.collector")

MSG_META = 90
PAYLOAD_FORMAT = "loxone_ws_envelope_v1"


class Collector:
    def __init__(self) -> None:
        self.loxone_url = env_required("LOXONE_URL")
        self.username = env_required("LOXONE_USERNAME")
        self.password = env_required("LOXONE_PASSWORD")
        self.source_id = os.environ.get("LOXONE_SOURCE_ID", "home-miniserver").strip()
        self.spool = Spool(os.environ.get("SPOOL_DB", "/var/lib/loxone-bronze/spool.sqlite3"))
        self.loxone_version = float(os.environ.get("LOXONE_VERSION", "17.0"))
        self.structure_check_seconds = int(os.environ.get("STRUCTURE_CHECK_SECONDS", "300"))
        self.run_id = str(uuid.uuid4())
        self.stop_event = asyncio.Event()
        self.ws = LoxWs(version=self.loxone_version)

    def store_payload(self, message_type: int, envelope: dict) -> None:
        received_at = envelope.get("captured_at") or utc_now_iso()
        payload_json = compact_json(envelope)
        self.spool.insert_message(
            message_id=str(uuid.uuid4()),
            source_id=self.source_id,
            run_id=self.run_id,
            received_at=received_at,
            message_type=message_type,
            payload_format=PAYLOAD_FORMAT if message_type < MSG_META else "collector_meta_v1",
            payload_json=payload_json,
            payload_sha256=sha256_text(payload_json),
            collector_version=__version__,
        )

    def store_meta(self, event: str, **details) -> None:
        envelope = {
            "captured_at": utc_now_iso(),
            "event": event,
            "details": details,
        }
        self.store_payload(MSG_META, envelope)

    def install_capture_handlers(self) -> None:
        # The library parses type 2/3. We preserve BOTH the raw transport payload
        # and the parsed representation in Bronze. For currently unparsed types
        # 1/4/5/7 we still retain the raw payload losslessly as base64.
        original = dict(self.ws._message_handler)

        def wrap_with_parsed(handler):
            async def wrapped(message, event_dict):
                captured_at = utc_now_iso()
                parsed = await handler(message, event_dict)
                return raw_envelope(message, parsed=parsed, captured_at=captured_at)
            return wrapped

        async def raw_only(message, event_dict):
            return raw_envelope(message, parsed=None, captured_at=utc_now_iso())

        self.ws._message_handler[0] = wrap_with_parsed(original[0])
        self.ws._message_handler[1] = raw_only
        self.ws._message_handler[2] = wrap_with_parsed(original[2])
        self.ws._message_handler[3] = wrap_with_parsed(original[3])
        self.ws._message_handler[4] = raw_only
        self.ws._message_handler[5] = raw_only
        self.ws._message_handler[7] = raw_only

    def install_callbacks(self) -> None:
        async def on_message(data, message_type: int):
            try:
                self.store_payload(message_type, data)
            except Exception:
                LOG.exception("Failed to persist WebSocket message to local spool")

        # Register BEFORE connect(). This is deliberate: enablebinstatusupdate
        # sends the full initial state immediately after authentication.
        self.ws.add_message_callback(on_message, message_types=[0, 1, 2, 3, 4, 5, 7])

        async def on_connected():
            self.store_meta("connected", run_id=self.run_id)

        async def on_disconnected():
            self.store_meta("connection_closed", run_id=self.run_id)

        async def on_reconnected():
            self.store_meta("reconnected", run_id=self.run_id)
            asyncio.create_task(self.capture_structure_if_changed(force=True))

        self.ws.add_event_callback(on_connected, event_types=[self.ws.EventType.CONNECTED])
        self.ws.add_event_callback(on_disconnected, event_types=[self.ws.EventType.CONNECTION_CLOSED])
        self.ws.add_event_callback(on_reconnected, event_types=[self.ws.EventType.RECONNECTED])

    @staticmethod
    def _extract_ll_value(raw) -> object:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        parsed = json.loads(raw)
        return parsed.get("LL", {}).get("value")

    async def wait_for_listener(self, timeout_seconds: float = 10.0) -> None:
        """Wait until ws_listen owns receive() before sending commands."""
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        while asyncio.get_running_loop().time() < deadline:
            if self.ws._listener_running:
                return
            await asyncio.sleep(0.05)
        raise RuntimeError("WebSocket listener did not become ready in time")

    async def capture_structure_if_changed(self, force: bool = False) -> None:
        try:
            # LoxAPPversion3 is cheap and tells us whether the configuration
            # structure changed. send_command is only safe after ws_listen owns
            # the WebSocket receive loop.
            await self.wait_for_listener()

            version_raw = await self.ws.send_command("jdev/sps/LoxAPPversion3")
            remote_version = self._extract_ll_value(version_raw)
            remote_version_str = (
                None if remote_version is None else str(remote_version)
            )

            local_version = self.spool.latest_structure_version(self.source_id)

            if (
                not force
                and remote_version_str
                and local_version == remote_version_str
            ):
                return

            # LoxAPP3.json is a real HTTP resource. Do not request it through
            # send_command(), which only handles jdev-style command responses.
            url = f"{self.loxone_url.rstrip('/')}/data/LoxAPP3.json"

            timeout = aiohttp.ClientTimeout(total=20)
            auth = aiohttp.BasicAuth(self.username, self.password)

            async with aiohttp.ClientSession(
                auth=auth,
                timeout=timeout
            ) as session:
                async with session.get(
                    url,
                    allow_redirects=True,
                    ssl=False
                ) as response:
                    if response.status != 200:
                        raise RuntimeError(
                            f"LoxAPP3 HTTP status {response.status}"
                        )

                    structure_text = await response.text()

            # Defensive validation: never poison Bronze with a small LL error
            # response or otherwise invalid structure.
            if len(structure_text) < 1000:
                raise RuntimeError(
                    f"LoxAPP3 response suspiciously small: "
                    f"{len(structure_text)} bytes"
                )

            structure_obj = json.loads(structure_text)

            controls = structure_obj.get("controls")
            if not isinstance(controls, dict) or not controls:
                raise RuntimeError(
                    "LoxAPP3 JSON contains no valid controls object"
                )

            last_modified = structure_obj.get("lastModified")
            last_modified_str = (
                None if last_modified is None else str(last_modified)
            )

            structure_sha = sha256_text(structure_text)

            self.spool.insert_structure(
                structure_id=f"{self.source_id}:{structure_sha}",
                source_id=self.source_id,
                captured_at=utc_now_iso(),
                last_modified=last_modified_str or remote_version_str,
                payload_json=structure_text,
                payload_sha256=structure_sha,
                collector_version=__version__,
            )

            LOG.info(
                "Stored valid LoxAPP3 structure version %s "
                "(%d controls, %d bytes)",
                last_modified_str or remote_version_str,
                len(controls),
                len(structure_text),
            )

        except Exception:
            LOG.exception("Could not capture/check LoxAPP3 structure")

    async def structure_watch_loop(self) -> None:
        while not self.stop_event.is_set():
            await asyncio.sleep(self.structure_check_seconds)
            if self.stop_event.is_set():
                break
            await self.capture_structure_if_changed(force=False)

    async def run(self) -> None:
        self.install_capture_handlers()
        self.install_callbacks()

        self.store_meta("collector_starting", loxone_url=self.loxone_url, run_id=self.run_id)

        # 0 means unlimited reconnect attempts in loxwebsocket.
        await self.ws.connect(
            user=self.username,
            password=self.password,
            loxone_url=self.loxone_url,
            receive_updates=True,
            max_reconnect_attempts=0,
        )

        # Capture the structure after the listener owns the socket. send_command
        # is serialized safely by the client library.
        await self.capture_structure_if_changed(force=True)
        watcher = asyncio.create_task(self.structure_watch_loop(), name="structure-watcher")

        try:
            await self.stop_event.wait()
        finally:
            watcher.cancel()
            try:
                await watcher
            except asyncio.CancelledError:
                pass
            self.store_meta("collector_stopping", run_id=self.run_id)
            await self.ws.stop()

    def request_stop(self) -> None:
        self.stop_event.set()


async def async_main() -> None:
    collector = Collector()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, collector.request_stop)
        except NotImplementedError:
            pass
    await collector.run()


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
