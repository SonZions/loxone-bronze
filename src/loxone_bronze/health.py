from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

from .spool import Spool


def parse_dt(value):
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def env_flag(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized not in {"1", "true", "yes", "on", "0", "false", "no", "off"}:
        raise ValueError(f"{name} must be a boolean")
    return normalized in {"1", "true", "yes", "on"}


def threshold(name: str, default: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except ValueError:
        raise ValueError(f"{name} must be a non-negative integer") from None
    if value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def health_problems(status: dict, now: datetime | None = None) -> list[str]:
    now = now or datetime.now(timezone.utc)
    max_event_age = threshold("HEALTH_MAX_EVENT_AGE_MINUTES", 10)
    max_pending = threshold("HEALTH_MAX_PENDING_MESSAGES", 100000)
    max_pending_age = threshold("HEALTH_MAX_PENDING_AGE_MINUTES", 30)
    check_backlog = env_flag("HEALTH_CHECK_BACKLOG", True)
    messages = status["messages"]
    latest = parse_dt(messages["latest"])
    problems = []
    if latest is None:
        problems.append("collector_stale: no WebSocket messages recorded yet")
    else:
        age = (now - latest).total_seconds() / 60
        if age > max_event_age:
            problems.append(f"collector_stale: latest event is {age:.1f} minutes old (limit {max_event_age})")
    if check_backlog:
        if messages["pending"] > max_pending:
            problems.append(f"upload_backlog: {messages['pending']} messages (limit {max_pending})")
        oldest = parse_dt(messages["oldest_pending"])
        if oldest is not None:
            age = (now - oldest).total_seconds() / 60
            if age > max_pending_age:
                problems.append(f"upload_backlog: oldest pending event is {age:.1f} minutes old (limit {max_pending_age})")
    return problems


def main() -> None:
    spool = Spool(os.environ.get("SPOOL_DB", "/var/lib/loxone-bronze/spool.sqlite3"))
    status = spool.status(include_totals=False)
    print(json.dumps(status, indent=2, ensure_ascii=False))
    try:
        problems = health_problems(status)
    except ValueError:
        print("UNHEALTHY: invalid health configuration", file=sys.stderr)
        raise SystemExit(2) from None
    for problem in problems:
        print(f"UNHEALTHY: {problem}", file=sys.stderr)
    raise SystemExit(1 if problems else 0)


if __name__ == "__main__":
    main()
