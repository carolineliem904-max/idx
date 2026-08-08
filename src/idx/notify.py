"""Notifier interface. Adding Telegram later is one class and zero rework
in the callers — jobs/daily.py and jobs/dead_mans_switch.py only ever call
`notifier.notify(...)`, never anything console-specific.
"""
from __future__ import annotations

import datetime as dt
import os
from abc import ABC, abstractmethod

import structlog

log = structlog.get_logger()

LEVELS = ("info", "warning", "error")


class Notifier(ABC):
    @abstractmethod
    def notify(self, subject: str, body: str, level: str = "info") -> None: ...


class ConsoleNotifier(Notifier):
    """Default/dev implementation. Prints to stdout AND logs structurally,
    so alerts show up both in an interactive run and in launchd's redirected
    log files (spec §8-style local-first, see README "Local scheduling")."""

    _ICONS = {"info": "i", "warning": "!", "error": "X"}

    def notify(self, subject: str, body: str, level: str = "info") -> None:
        if level not in LEVELS:
            level = "info"
        icon = self._ICONS[level]
        timestamp = dt.datetime.now(dt.timezone.utc).isoformat()
        print(f"\n[{icon}] {level.upper()} — {subject}  ({timestamp})\n{body}\n")
        log.info("alert_sent", channel="console", level=level, subject=subject)


def get_notifier() -> Notifier:
    """Selects the notifier implementation. IDX_NOTIFIER env var, default
    'console' — this is the one place a future Telegram implementation
    gets registered, not scattered through call sites."""
    backend = os.environ.get("IDX_NOTIFIER", "console")
    if backend == "console":
        return ConsoleNotifier()
    raise ValueError(f"Unknown IDX_NOTIFIER backend: {backend!r} (only 'console' exists so far)")
