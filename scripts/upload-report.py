#!/usr/bin/env python3
"""Read-only wall-clock upload throughput/ETA report. No environment secrets."""
from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path


def report(path: str, window_minutes: int, ingress_per_day: int) -> dict:
    now = datetime.now(timezone.utc)
    cutoff = (now - timedelta(minutes=window_minutes)).isoformat(timespec="microseconds")
    end = now.isoformat(timespec="microseconds")
    con = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True, timeout=10)
    try:
        con.execute("BEGIN")
        pending = con.execute("SELECT COUNT(*) FROM ws_messages WHERE uploaded_at IS NULL").fetchone()[0]
        oldest = con.execute("SELECT received_at FROM ws_messages WHERE uploaded_at IS NULL ORDER BY received_at LIMIT 1").fetchone()
        latest = con.execute("SELECT MAX(received_at) FROM ws_messages").fetchone()[0]
        latest_uploaded = con.execute("SELECT MAX(uploaded_at) FROM ws_messages").fetchone()[0]
        uploaded = con.execute("SELECT COUNT(*) FROM ws_messages WHERE uploaded_at >= ? AND uploaded_at <= ?", (cutoff, end)).fetchone()[0]
        incoming = con.execute("SELECT COUNT(*) FROM ws_messages WHERE received_at >= ? AND received_at <= ?", (cutoff, end)).fetchone()[0]
    finally:
        con.close()
    seconds = window_minutes * 60
    rate = uploaded / seconds
    measured_incoming = incoming / seconds
    assumed_incoming = max(measured_incoming, ingress_per_day / 86400)
    net = rate - assumed_incoming
    oldest = oldest[0] if oldest else None
    return {
        "measured_at": end, "window_minutes": window_minutes,
        "pending": pending, "oldest_pending": oldest,
        "oldest_pending_age_hours": (now - datetime.fromisoformat(oldest)).total_seconds() / 3600 if oldest else None,
        "latest_local": latest, "latest_local_ack": latest_uploaded,
        "uploaded_in_window": uploaded, "received_in_window": incoming,
        "wall_clock_upload_messages_per_second": round(rate, 3),
        "measured_incoming_messages_per_second": round(measured_incoming, 3),
        "eta_assumed_ingress_per_day": round(assumed_incoming * 86400),
        "net_drain_messages_per_second": round(net, 3),
        "eta_hours": 0 if pending == 0 else (round(pending / net / 3600, 2) if net > 0 else None),
        "required_upload_per_second_for_12h": round(assumed_incoming + pending / 43200, 3),
        "catches_up_within_12h": pending == 0 or (net > 0 and pending / net <= 43200),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spool", default="/var/lib/loxone-bronze/spool.sqlite3")
    parser.add_argument("--window-minutes", type=int, default=60)
    parser.add_argument("--ingress-per-day", type=int, default=323000)
    args = parser.parse_args()
    if args.window_minutes <= 0 or args.ingress_per_day < 0:
        parser.error("window must be positive and ingress non-negative")
    print(json.dumps(report(args.spool, args.window_minutes, args.ingress_per_day), indent=2))


if __name__ == "__main__":
    main()
