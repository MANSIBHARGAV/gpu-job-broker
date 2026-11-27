"""Replay the supplied synthetic submission trace against broker policies."""

from __future__ import annotations

import argparse
import csv
import json
import math
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Any

from broker import Broker, format_time, parse_time


def load_trace(
    path: str | Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    jobs = []
    rejected = []
    normalized = []
    with Path(path).open(newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source)
        expected = {
            "submitted_at",
            "team",
            "gpus",
            "estimated_minutes",
            "actual_minutes",
            "priority",
            "outcome",
        }
        if reader.fieldnames is None or not expected.issubset(reader.fieldnames):
            raise ValueError(f"Trace must contain columns: {', '.join(sorted(expected))}")
        for line, row in enumerate(reader, start=2):
            try:
                submitted_at = parse_time(row["submitted_at"])
                team = row["team"].strip()
                gpus = int(row["gpus"])
                estimate = float(row["estimated_minutes"])
                actual = float(row["actual_minutes"])
                priority = row["priority"].strip().lower()
                outcome = row["outcome"].strip().lower()
            except (TypeError, ValueError) as exc:
                rejected.append({"line": line, "reason": f"invalid value: {exc}"})
                continue
            if (
                not team
                or not math.isfinite(actual)
                or not math.isfinite(estimate)
                or actual <= 0
                or estimate <= 0
            ):
                rejected.append({"line": line, "reason": "team and durations must be positive"})
                continue
            if gpus not in {1, 2, 4, 8}:
                rounded = next((supported for supported in (1, 2, 4, 8) if supported >= gpus), None)
                if rounded is None or gpus < 1:
                    rejected.append(
                        {"line": line, "reason": f"unsupported GPU request: {gpus}"}
                    )
                    continue
                normalized.append({"line": line, "from_gpus": gpus, "to_gpus": rounded})
                gpus = rounded
            if outcome not in {"succeeded", "failed", "cancelled"}:
                rejected.append({"line": line, "reason": f"unsupported outcome: {outcome}"})
                continue
            if priority not in {"low", "normal", "high"}:
                rejected.append({"line": line, "reason": f"unsupported priority: {priority}"})
                continue
            jobs.append(
                {
                    "submitted_at": submitted_at,
                    "team": team,
                    "gpus": gpus,
                    "estimated_minutes": estimate,
                    "actual_minutes": actual,
                    "priority": priority,
                    "outcome": outcome,
                    "client_key": f"trace-line-{line}",
                }
            )
    return sorted(jobs, key=lambda job: job["submitted_at"]), rejected, normalized


def percentile(values: list[float], percentage: int) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, math.ceil(percentage / 100 * len(ordered)) - 1)
    return round(ordered[index], 2)


def run_replay(
    trace_path: str | Path,
    *,
    policy: str,
    idle_timeout_seconds: int = 300,
    database_path: str | Path | None = None,
) -> dict[str, Any]:
    jobs, rejected, normalized = load_trace(trace_path)
    if not jobs:
        raise ValueError("Trace contains no jobs")
    start = jobs[0]["submitted_at"]
    owns_temp = database_path is None
    temp_context = tempfile.TemporaryDirectory(prefix="gpu-broker-replay-") if owns_temp else None
    db_path = (
        Path(temp_context.name) / "replay.sqlite3"
        if temp_context is not None
        else Path(database_path)
    )
    broker = Broker(
        db_path,
        start_time=start,
        idle_timeout_seconds=idle_timeout_seconds,
        policy=policy,
    )
    try:
        for job in jobs:
            broker.advance(job["submitted_at"])
            broker.submit_job(
                team=job["team"],
                gpus=job["gpus"],
                estimated_minutes=job["estimated_minutes"],
                priority=job["priority"],
                client_key=job["client_key"],
                runtime_seconds=max(1, round(job["actual_minutes"] * 60)),
                planned_status=job["outcome"],
            )

        # This upper bound allows queued work, startup, transient retries, and reaping to finish.
        total_runtime = sum(round(job["actual_minutes"] * 60) for job in jobs)
        end = jobs[-1]["submitted_at"] + total_runtime + len(jobs) * 90 + 1800
        broker.advance(end)

        waits: dict[str, list[float]] = defaultdict(list)
        for job in broker.list_jobs():
            submitted = parse_time(job["submitted_at"])
            terminal_time = parse_time(job["finished_at"]) if job["finished_at"] else end
            wait_end = parse_time(job["started_at"]) if job["started_at"] else terminal_time
            waits[job["team"]].append(max(0.0, wait_end - submitted) / 60)
        report = broker.cost_report(start, end)
        return {
            "policy": policy,
            "input_rows": len(jobs) + len(rejected),
            "jobs": len(jobs),
            "rejected_rows": rejected,
            "normalized_rows": normalized,
            "simulated_from": format_time(start),
            "simulated_to": format_time(end),
            "wait_minutes_by_team": {
                team: {
                    "count": len(values),
                    "p50": percentile(values, 50),
                    "p95": percentile(values, 95),
                    "p99": percentile(values, 99),
                }
                for team, values in sorted(waits.items())
            },
            "total_gpu_hours": report["total_gpu_hours"],
            "total_cost": report["total_cost"],
            "cost_by_team": report["teams"],
            "final_status_counts": {
                status: sum(1 for job in broker.list_jobs() if job["status"] == status)
                for status in ("succeeded", "failed", "cancelled", "queued", "running")
            },
        }
    finally:
        broker.close()
        if temp_context is not None:
            temp_context.cleanup()


def compare(trace_path: str | Path, idle_timeout_seconds: int = 300) -> dict[str, Any]:
    return {
        "comparison": "Priority ordering with round-robin across teams vs submission-order FIFO",
        "policies": {
            policy: run_replay(
                trace_path,
                policy=policy,
                idle_timeout_seconds=idle_timeout_seconds,
            )
            for policy in ("fair", "fifo")
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "trace",
        nargs="?",
        default=str(Path(__file__).with_name("01a_trace.csv")),
        help="path to the synthetic trace CSV",
    )
    parser.add_argument("--idle-timeout-minutes", type=int, default=5)
    parser.add_argument("--policy", choices=("fair", "fifo", "both"), default="both")
    parser.add_argument("--database", help="persist one run to this database file")
    args = parser.parse_args()
    idle_timeout = args.idle_timeout_minutes * 60
    if args.policy == "both":
        result = compare(args.trace, idle_timeout)
    else:
        result = run_replay(
            args.trace,
            policy=args.policy,
            idle_timeout_seconds=idle_timeout,
            database_path=args.database,
        )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
