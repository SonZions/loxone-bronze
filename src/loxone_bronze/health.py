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
    return value.strip().lower() in {"1", "true", "yes", "on"}


def main() -> None:
    spool = Spool(os.environ.get("SPOOL_DB", "/var/lib/loxone-bronze/spool.sqlite3"))
    status = spool.status()
    print(json.dumps(status, indent=2, ensure_ascii=False))

    latest = parse_dt(status["messages"]["latest"])
    pending = status["messages"]["pending"] or 0
    max_age_minutes = int(os.environ.get("HEALTH_MAX_EVENT_AGE_MINUTES", "10"))
    check_backlog = env_flag("HEALTH_CHECK_BACKLOG", True)

    unhealthy = False
    if latest is None:
        print("UNHEALTHY: no WebSocket messages recorded yet", file=sys.stderr)
        unhealthy = True
    else:
        age = (datetime.now(timezone.utc) - latest.astimezone(timezone.utc)).total_seconds() / 60
        if age > max_age_minutes:
            print(f"UNHEALTHY: latest event is {age:.1f} minutes old", file=sys.stderr)
            unhealthy = True

    if check_backlog and pending > 100000:
        print(f"UNHEALTHY: upload backlog is {pending} messages", file=sys.stderr)
        unhealthy = True

    raise SystemExit(1 if unhealthy else 0)


if __name__ == "__main__":
    main()
