"""Count Discord user messages and rotate Hermes sessions."""

from __future__ import annotations

import dataclasses
import inspect
import json
import logging
import os
import threading
from pathlib import Path
from typing import Any

logger = logging.getLogger("hermes.plugins.session_counter")

LIMIT = 15
STATE_FILE = (
    Path(os.environ.get("HERMES_HOME", str(Path.home() / ".hermes")))
    / "plugin-data"
    / "hermes-session-counter.json"
)

_LOCK = threading.Lock()


def _read_state() -> dict:
    try:
        if STATE_FILE.exists():
            data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
    except Exception:
        logger.warning("Could not read counter state", exc_info=True)
    return {}


def _write_state(data: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    temp = STATE_FILE.with_suffix(".tmp")
    temp.write_text(
        json.dumps(data, ensure_ascii=False),
        encoding="utf-8",
    )
    os.replace(temp, STATE_FILE)


def _is_slash_command(event: Any) -> bool:
    get_command = getattr(event, "get_command", None)
    if callable(get_command):
        try:
            return bool(get_command())
        except Exception:
            pass

    text = getattr(event, "text", "")
    return isinstance(text, str) and text.lstrip().startswith("/")


def _get_session_key(gateway: Any, source: Any) -> str | None:
    helper = getattr(gateway, "_session_key_for_source", None)
    if not callable(helper):
        return None

    try:
        key = helper(source)
        return str(key) if key else None
    except Exception:
        logger.warning("Could not resolve session key", exc_info=True)
        return None


def _lookup_entry(session_store: Any, key: str) -> Any:
    try:
        return session_store.lookup_by_session_key(key)
    except Exception:
        return None


class _CommandFreeEvent:
    __slots__ = ("source",)

    def __init__(self, source: Any):
        self.source = source

    def get_command_args(self) -> str:
        return ""


def _make_reset_event(event: Any) -> Any:
    try:
        return dataclasses.replace(event, text="")
    except Exception:
        return _CommandFreeEvent(getattr(event, "source", None))


async def _reset_session(
    gateway: Any,
    event: Any,
) -> bool:
    reset = getattr(gateway, "_handle_reset_command", None)

    if not callable(reset):
        logger.error("Gateway reset handler is unavailable")
        return False

    try:
        result = reset(_make_reset_event(event))
        if inspect.isawaitable(result):
            await result
        return True
    except Exception:
        logger.exception("Session reset failed")
        return False


async def _on_message(
    event: Any,
    gateway: Any,
    session_store: Any,
    **kwargs: Any,
) -> None:
    source = getattr(event, "source", None)
    if source is None:
        return

    platform = getattr(source, "platform", "")
    platform = str(getattr(platform, "value", platform)).lower()

    # Only count Discord messages.
    if platform != "discord":
        return

    # Commands are not conversation messages.
    if _is_slash_command(event):
        return

    key = _get_session_key(gateway, source)
    if not key:
        return

    message_id = str(getattr(event, "message_id", "") or "")
    entry = _lookup_entry(session_store, key)

    with _LOCK:
        state = _read_state()
        item = state.get(key, {})

        count = int(item.get("count", 0))
        last_id = str(item.get("last_message_id", ""))

        # Avoid counting the same delivery twice.
        if message_id and message_id == last_id:
            return

        # Fifteen messages have already been processed.
        # The next message starts a fresh session.
        if count >= LIMIT and entry is not None:
            if getattr(entry, "active_turn_token", None):
                count += 1
                state[key] = {
                    "count": count,
                    "last_message_id": message_id,
                }
                _write_state(state)
                return

            # Reserve this message while the reset is performed.
            state[key] = {
                "count": count,
                "last_message_id": message_id,
                "reset_pending": True,
            }
            _write_state(state)
        else:
            count += 1
            state[key] = {
                "count": count,
                "last_message_id": message_id,
            }
            _write_state(state)
            return

    # Do not hold the file lock while awaiting the gateway.
    success = await _reset_session(gateway, event)

    with _LOCK:
        state = _read_state()
        if success:
            state[key] = {
                "count": 1,
                "last_message_id": message_id,
            }
        else:
            state[key] = {
                "count": count + 1,
                "last_message_id": message_id,
            }
        _write_state(state)


def register(ctx: Any) -> None:
    ctx.register_hook("pre_gateway_dispatch", _on_message)
    logger.info("hermes-session-counter enabled (limit=%s)", LIMIT)


__all__ = ["register"]
