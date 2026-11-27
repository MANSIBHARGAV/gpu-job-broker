"""Persistent, simulated GPU job broker."""

from __future__ import annotations

import hashlib
import math
import sqlite3
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


NODE_TYPES = {
    "gpu-s": {"gpus": 1, "hourly_rate": 1.00},
    "gpu-m": {"gpus": 4, "hourly_rate": 4.50},
    "gpu-l": {"gpus": 8, "hourly_rate": 8.50},
}
VALID_GPUS = {1, 2, 4, 8}
PRIORITY_ORDER = {"high": 0, "normal": 1, "low": 2}
MINIMUM_BILLING_SECONDS = 600
STARTUP_FAILURE_SECONDS = 900
MAX_NODES = 10


class BrokerError(Exception):
    """Base class for expected broker errors."""


class NotFound(BrokerError):
    """The requested job does not exist."""


class InvalidRequest(BrokerError):
    """The request violates a broker invariant."""


class CapacityUnavailable(BrokerError):
    """The fake provider cannot currently launch a node."""


@dataclass(frozen=True)
class Launch:
    node_id: str
    ready_at: float
    fails_at: float | None


class FakeProvider:
    """Deterministic provider that models capacity bursts and startup failures."""

    def launch_node(self, node_type: str, now: float, attempt: int) -> Launch:
        if node_type not in NODE_TYPES:
            raise InvalidRequest(f"unknown node type: {node_type}")
        # Capacity outages arrive in two-attempt bursts; retries happen later.
        if attempt % 17 in (5, 6):
            raise CapacityUnavailable("Provider capacity is temporarily unavailable")

        node_id = str(uuid.uuid4())
        digest = hashlib.sha256(f"gpu-job-broker:{attempt}".encode()).digest()
        startup_seconds = 240 + int.from_bytes(digest[:2], "big") % 241
        # A stable 3% failure rate avoids nondeterministic replay results.
        fails = int.from_bytes(digest[2:4], "big") % 100 < 3
        return Launch(
            node_id=node_id,
            ready_at=now + startup_seconds,
            fails_at=now + STARTUP_FAILURE_SECONDS if fails else None,
        )

    def terminate_node(self, node_id: str) -> None:
        # Node lifecycle is held in SQLite; there is no external resource to call.
        return None


def parse_time(value: str | datetime | float | int) -> float:
    if isinstance(value, bool):
        raise InvalidRequest("time must be a timestamp or ISO-8601 datetime")
    if isinstance(value, (float, int)):
        result = float(value)
    elif isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, (float, int)):
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        result = parsed.timestamp()
    if not math.isfinite(result):
        raise InvalidRequest("time must be finite")
    return result


def format_time(value: float | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat().replace("+00:00", "Z")


class Broker:
    def __init__(
        self,
        db_path: str | Path = "broker.sqlite3",
        *,
        start_time: str | datetime | float | None = None,
        idle_timeout_seconds: int = 300,
        provider: FakeProvider | None = None,
        policy: str = "fair",
    ) -> None:
        if idle_timeout_seconds < 0:
            raise ValueError("idle_timeout_seconds must be non-negative")
        if policy not in {"fair", "fifo"}:
            raise ValueError("policy must be 'fair' or 'fifo'")
        self.db_path = str(db_path)
        self.idle_timeout_seconds = idle_timeout_seconds
        self.provider = provider or FakeProvider()
        self.policy = policy
        self._lock = threading.RLock()
        self._db = sqlite3.connect(self.db_path, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA foreign_keys = ON")
        self._initialize(parse_time(start_time) if start_time is not None else datetime.now(timezone.utc).timestamp())
        with self._lock, self._db:
            self._process_at_current_time()

    def close(self) -> None:
        with self._lock:
            self._db.close()

    def _initialize(self, start: float) -> None:
        with self._db:
            self._db.executescript(
                """
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    client_key TEXT NOT NULL UNIQUE,
                    team TEXT NOT NULL,
                    gpus INTEGER NOT NULL,
                    estimate_seconds INTEGER NOT NULL,
                    runtime_seconds INTEGER NOT NULL,
                    priority TEXT NOT NULL,
                    planned_status TEXT NOT NULL,
                    status TEXT NOT NULL,
                    submitted_at REAL NOT NULL,
                    started_at REAL,
                    finished_at REAL,
                    finish_at REAL,
                    node_id TEXT,
                    FOREIGN KEY(node_id) REFERENCES nodes(id)
                );
                CREATE INDEX IF NOT EXISTS jobs_status_submitted
                    ON jobs(status, submitted_at);
                CREATE TABLE IF NOT EXISTS nodes (
                    id TEXT PRIMARY KEY,
                    node_type TEXT NOT NULL,
                    gpus INTEGER NOT NULL,
                    hourly_rate REAL NOT NULL,
                    team TEXT NOT NULL,
                    status TEXT NOT NULL,
                    launched_at REAL NOT NULL,
                    ready_at REAL NOT NULL,
                    fails_at REAL,
                    idle_since REAL,
                    terminated_at REAL
                );
                CREATE INDEX IF NOT EXISTS nodes_status
                    ON nodes(status, team);
                """
            )
            if self._db.execute("SELECT 1 FROM metadata WHERE key='clock'").fetchone() is None:
                self._set_meta("clock", start)
                self._set_meta("launch_attempt", 0)
                self._set_meta("retry_after", 0)
                for priority in PRIORITY_ORDER:
                    self._set_meta(f"cursor_{priority}", "")

    def _get_meta(self, key: str) -> str:
        row = self._db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
        if row is None:
            raise RuntimeError(f"Missing broker metadata: {key}")
        return str(row["value"])

    def _set_meta(self, key: str, value: str | float | int) -> None:
        self._db.execute(
            "INSERT INTO metadata(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )

    @property
    def now(self) -> float:
        with self._lock:
            return float(self._get_meta("clock"))

    def submit_job(
        self,
        *,
        team: str,
        gpus: int,
        estimated_minutes: float,
        priority: str = "normal",
        client_key: str,
        runtime_seconds: int | None = None,
        planned_status: str = "succeeded",
    ) -> dict[str, Any]:
        team = team.strip() if isinstance(team, str) else ""
        if not team:
            raise InvalidRequest("team must be a non-empty string")
        if isinstance(gpus, bool) or not isinstance(gpus, int) or gpus not in VALID_GPUS:
            raise InvalidRequest("gpus must be one of 1, 2, 4, or 8")
        if (
            isinstance(estimated_minutes, bool)
            or not isinstance(estimated_minutes, (int, float))
            or not math.isfinite(estimated_minutes)
            or estimated_minutes <= 0
        ):
            raise InvalidRequest("estimated_minutes must be greater than zero")
        if priority not in PRIORITY_ORDER:
            raise InvalidRequest("priority must be low, normal, or high")
        if not isinstance(client_key, str) or not client_key.strip():
            raise InvalidRequest("client_key must be a non-empty string")
        if planned_status not in {"succeeded", "failed", "cancelled"}:
            raise InvalidRequest("planned_status must be succeeded, failed, or cancelled")
        estimate = max(1, round(estimated_minutes * 60))
        runtime = runtime_seconds if runtime_seconds is not None else estimate
        if isinstance(runtime, bool) or not isinstance(runtime, int) or runtime <= 0:
            raise InvalidRequest("runtime_seconds must be a positive integer")

        with self._lock, self._db:
            existing = self._db.execute(
                "SELECT * FROM jobs WHERE client_key=?", (client_key.strip(),)
            ).fetchone()
            if existing is not None:
                return self._job_dict(existing)
            now = float(self._get_meta("clock"))
            job_id = str(uuid.uuid4())
            self._db.execute(
                """
                INSERT INTO jobs(
                    id, client_key, team, gpus, estimate_seconds, runtime_seconds,
                    priority, planned_status, status, submitted_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?)
                """,
                (
                    job_id,
                    client_key.strip(),
                    team,
                    gpus,
                    estimate,
                    runtime,
                    priority,
                    planned_status,
                    now,
                ),
            )
            self._schedule()
            return self.get_job(job_id)

    def get_job(self, job_id: str) -> dict[str, Any]:
        with self._lock:
            row = self._db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                raise NotFound(f"Job {job_id} was not found")
            return self._job_dict(row)

    def list_jobs(self, status: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            if status is not None:
                rows = self._db.execute(
                    "SELECT * FROM jobs WHERE status=? ORDER BY submitted_at, rowid", (status,)
                ).fetchall()
            else:
                rows = self._db.execute(
                    "SELECT * FROM jobs ORDER BY submitted_at, rowid"
                ).fetchall()
            return [self._job_dict(row) for row in rows]

    def cancel_job(self, job_id: str) -> dict[str, Any]:
        with self._lock, self._db:
            row = self._db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                raise NotFound(f"Job {job_id} was not found")
            if row["status"] in {"succeeded", "failed", "cancelled"}:
                return self._job_dict(row)
            now = float(self._get_meta("clock"))
            self._db.execute(
                "UPDATE jobs SET status='cancelled', finished_at=?, finish_at=NULL WHERE id=?",
                (now, job_id),
            )
            self._refresh_idle_nodes(now)
            self._schedule()
            return self.get_job(job_id)

    def list_nodes(self, *, active_only: bool = True) -> list[dict[str, Any]]:
        with self._lock:
            query = "SELECT * FROM nodes"
            if active_only:
                query += " WHERE status IN ('starting', 'ready')"
            query += " ORDER BY launched_at, id"
            return [self._node_dict(row) for row in self._db.execute(query).fetchall()]

    def advance(self, to_time: str | datetime | float | int) -> dict[str, Any]:
        target = parse_time(to_time)
        with self._lock, self._db:
            now = float(self._get_meta("clock"))
            if target < now:
                raise InvalidRequest("simulated time cannot move backwards")
            self._process_at_current_time()
            while True:
                now = float(self._get_meta("clock"))
                event = self._next_event()
                if event is None or event > target:
                    break
                if event <= now:
                    raise RuntimeError("scheduler produced a non-forward event")
                self._set_meta("clock", event)
                self._process_at_current_time()
            self._set_meta("clock", target)
            self._process_at_current_time()
            return self.snapshot()

    def advance_seconds(self, seconds: float) -> dict[str, Any]:
        if (
            isinstance(seconds, bool)
            or not isinstance(seconds, (int, float))
            or not math.isfinite(seconds)
            or seconds < 0
        ):
            raise InvalidRequest("seconds must be a finite non-negative number")
        return self.advance(self.now + seconds)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "simulated_at": format_time(float(self._get_meta("clock"))),
                "queue": self.list_jobs("queued"),
                "running": self.list_jobs("running"),
                "nodes": self.list_nodes(),
            }

    def cost_report(
        self,
        start: str | datetime | float | None = None,
        end: str | datetime | float | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            now = float(self._get_meta("clock"))
            window_start = parse_time(start) if start is not None else float("-inf")
            window_end = min(parse_time(end), now) if end is not None else now
            if window_end < window_start:
                raise InvalidRequest("cost window end must be at or after start")
            teams: dict[str, dict[str, float]] = {}
            for row in self._db.execute("SELECT * FROM nodes").fetchall():
                lifetime_end = min(
                    float(row["terminated_at"]) if row["terminated_at"] is not None else now,
                    window_end,
                )
                lifetime_start = float(row["launched_at"])
                total_lifetime = max(
                    0.0,
                    (float(row["terminated_at"]) if row["terminated_at"] is not None else now)
                    - lifetime_start,
                )
                overlap = max(
                    0.0,
                    min(lifetime_end, window_end) - max(lifetime_start, window_start),
                )
                if overlap and total_lifetime:
                    billable = max(MINIMUM_BILLING_SECONDS, total_lifetime)
                    cost = (
                        float(row["hourly_rate"])
                        / 3600
                        * billable
                        * overlap
                        / total_lifetime
                    )
                    entry = teams.setdefault(row["team"], {"gpu_hours": 0.0, "cost": 0.0})
                    entry["cost"] += cost
            for row in self._db.execute("SELECT * FROM jobs").fetchall():
                if row["started_at"] is None:
                    continue
                execution_end = (
                    float(row["finished_at"])
                    if row["finished_at"] is not None
                    else now
                )
                overlap = max(
                    0.0,
                    min(execution_end, window_end) - max(float(row["started_at"]), window_start),
                )
                if overlap:
                    entry = teams.setdefault(row["team"], {"gpu_hours": 0.0, "cost": 0.0})
                    entry["gpu_hours"] += int(row["gpus"]) * overlap / 3600
            for value in teams.values():
                value["gpu_hours"] = round(value["gpu_hours"], 6)
                value["cost"] = round(value["cost"], 6)
            return {
                "start": format_time(window_start) if window_start != float("-inf") else None,
                "end": format_time(window_end),
                "teams": dict(sorted(teams.items())),
                "total_gpu_hours": round(sum(item["gpu_hours"] for item in teams.values()), 6),
                "total_cost": round(sum(item["cost"] for item in teams.values()), 6),
            }

    def _process_at_current_time(self) -> None:
        now = float(self._get_meta("clock"))
        starting = self._db.execute(
            "SELECT * FROM nodes WHERE status='starting' AND "
            "((fails_at IS NOT NULL AND fails_at<=?) OR (ready_at<=?))",
            (now, now),
        ).fetchall()
        for node in starting:
            if node["fails_at"] is not None and float(node["fails_at"]) <= now:
                self._db.execute(
                    "UPDATE nodes SET status='failed', terminated_at=? WHERE id=?",
                    (now, node["id"]),
                )
            else:
                self._db.execute(
                    "UPDATE nodes SET status='ready', idle_since=? WHERE id=?",
                    (float(node["ready_at"]), node["id"]),
                )

        finished = self._db.execute(
            "SELECT * FROM jobs WHERE status='running' AND finish_at<=?", (now,)
        ).fetchall()
        for job in finished:
            self._db.execute(
                "UPDATE jobs SET status=?, finished_at=finish_at, finish_at=NULL WHERE id=?",
                (job["planned_status"], job["id"]),
            )
        self._refresh_idle_nodes(now)
        self._reap_idle_nodes(now)
        self._schedule()

    def _refresh_idle_nodes(self, now: float) -> None:
        rows = self._db.execute(
            "SELECT id, status, idle_since FROM nodes WHERE status='ready'"
        ).fetchall()
        for node in rows:
            running = self._db.execute(
                "SELECT 1 FROM jobs WHERE node_id=? AND status='running' LIMIT 1", (node["id"],)
            ).fetchone()
            if running is None and node["idle_since"] is None:
                self._db.execute(
                    "UPDATE nodes SET idle_since=? WHERE id=?", (now, node["id"])
                )
            elif running is not None and node["idle_since"] is not None:
                self._db.execute(
                    "UPDATE nodes SET idle_since=NULL WHERE id=?", (node["id"],)
                )

    def _reap_idle_nodes(self, now: float) -> None:
        cutoff = now - self.idle_timeout_seconds
        expired = self._db.execute(
            """
            SELECT id FROM nodes
            WHERE status='ready' AND idle_since IS NOT NULL AND idle_since<=?
              AND NOT EXISTS (
                  SELECT 1 FROM jobs WHERE jobs.node_id=nodes.id AND jobs.status='running'
              )
            """,
            (cutoff,),
        ).fetchall()
        for node in expired:
            self.provider.terminate_node(str(node["id"]))
            self._db.execute(
                "UPDATE nodes SET status='terminated', terminated_at=? WHERE id=?",
                (now, node["id"]),
            )

    def _next_event(self) -> float | None:
        now = float(self._get_meta("clock"))
        events: list[float] = []
        retry_after = float(self._get_meta("retry_after"))
        if retry_after > now and self._db.execute(
            "SELECT 1 FROM jobs WHERE status='queued' LIMIT 1"
        ).fetchone():
            events.append(retry_after)
        rows = self._db.execute(
            "SELECT ready_at, fails_at FROM nodes WHERE status='starting'"
        ).fetchall()
        for node in rows:
            events.append(
                float(node["fails_at"])
                if node["fails_at"] is not None
                else float(node["ready_at"])
            )
        rows = self._db.execute(
            "SELECT finish_at FROM jobs WHERE status='running' AND finish_at IS NOT NULL"
        ).fetchall()
        events.extend(float(row["finish_at"]) for row in rows)
        if self.idle_timeout_seconds >= 0:
            rows = self._db.execute(
                "SELECT idle_since FROM nodes WHERE status='ready' AND idle_since IS NOT NULL"
            ).fetchall()
            events.extend(
                float(row["idle_since"]) + self.idle_timeout_seconds for row in rows
            )
        future = [event for event in events if event > now]
        return min(future) if future else None

    def _schedule(self) -> None:
        now = float(self._get_meta("clock"))
        fifo_jobs: list[sqlite3.Row] = []
        fifo_position = 0
        queued: dict[str, dict[str, list[sqlite3.Row]]] = {
            priority: {} for priority in PRIORITY_ORDER
        }
        if self.policy == "fifo":
            fifo_jobs = self._db.execute(
                "SELECT * FROM jobs WHERE status='queued' ORDER BY submitted_at, rowid"
            ).fetchall()
        else:
            for queued_job in self._db.execute(
                "SELECT * FROM jobs WHERE status='queued' "
                "ORDER BY priority, team, submitted_at, rowid"
            ).fetchall():
                priority = str(queued_job["priority"])
                queued[priority].setdefault(str(queued_job["team"]), []).append(queued_job)

            positions = {
                priority: {team: 0 for team in teams}
                for priority, teams in queued.items()
            }
        while True:
            if self.policy == "fifo":
                if fifo_position >= len(fifo_jobs):
                    return
                job = fifo_jobs[fifo_position]
                fifo_position += 1
            else:
                job = None
                for priority in PRIORITY_ORDER:
                    teams = queued[priority]
                    available = [
                        team
                        for team, jobs in teams.items()
                        if positions[priority][team] < len(jobs)
                    ]
                    if not available:
                        continue
                    available.sort()
                    cursor = self._get_meta(f"cursor_{priority}")
                    team = next((name for name in available if name > cursor), available[0])
                    job = teams[team][positions[priority][team]]
                    positions[priority][team] += 1
                    break
                if job is None:
                    return
            node = self._best_ready_node(job)
            if node is not None:
                self._db.execute(
                    "UPDATE jobs SET status='running', started_at=?, "
                    "finish_at=?, node_id=? WHERE id=?",
                    (
                        now,
                        now + int(job["runtime_seconds"]),
                        node["id"],
                        job["id"],
                    ),
                )
                self._db.execute(
                    "UPDATE nodes SET idle_since=NULL WHERE id=?", (node["id"],)
                )
                self._advance_team_cursor(job)
                continue

            pending = self._db.execute(
                "SELECT 1 FROM nodes WHERE team=? AND gpus>=? AND status='starting' LIMIT 1",
                (job["team"], job["gpus"]),
            ).fetchone()
            if pending is not None:
                continue

            active_nodes = int(
                self._db.execute(
                    "SELECT COUNT(*) FROM nodes WHERE status IN ('starting', 'ready')"
                ).fetchone()[0]
            )
            if active_nodes >= MAX_NODES:
                continue
            if now < float(self._get_meta("retry_after")):
                continue
            attempt_before = int(self._get_meta("launch_attempt"))
            self._launch_node(job, now)
            if int(self._get_meta("launch_attempt")) == attempt_before:
                return
            self._advance_team_cursor(job)

    def _advance_team_cursor(self, job: sqlite3.Row | dict[str, str]) -> None:
        if self.policy == "fair":
            self._set_meta(f"cursor_{job['priority']}", str(job["team"]))

    def _best_ready_node(self, job: sqlite3.Row) -> sqlite3.Row | None:
        return self._db.execute(
            """
            SELECT nodes.* FROM nodes
            WHERE nodes.status='ready' AND nodes.team=? AND nodes.gpus>=?
              AND nodes.gpus - COALESCE((
                  SELECT SUM(jobs.gpus) FROM jobs
                  WHERE jobs.node_id=nodes.id AND jobs.status='running'
              ), 0) >= ?
            ORDER BY nodes.gpus, nodes.launched_at
            LIMIT 1
            """,
            (job["team"], job["gpus"], job["gpus"]),
        ).fetchone()

    def _launch_node(self, job: sqlite3.Row, now: float) -> None:
        attempt = int(self._get_meta("launch_attempt")) + 1
        self._set_meta("launch_attempt", attempt)
        node_type = next(
            name for name, spec in NODE_TYPES.items() if spec["gpus"] >= job["gpus"]
        )
        try:
            launch = self.provider.launch_node(node_type, now, attempt)
        except CapacityUnavailable:
            self._set_meta("retry_after", now + 60)
            return
        spec = NODE_TYPES[node_type]
        self._db.execute(
            """
            INSERT INTO nodes(
                id, node_type, gpus, hourly_rate, team, status,
                launched_at, ready_at, fails_at, idle_since
            ) VALUES (?, ?, ?, ?, ?, 'starting', ?, ?, ?, NULL)
            """,
            (
                launch.node_id,
                node_type,
                spec["gpus"],
                spec["hourly_rate"],
                job["team"],
                now,
                launch.ready_at,
                launch.fails_at,
            ),
        )
        self._set_meta("retry_after", 0)

    def _job_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        for key in ("submitted_at", "started_at", "finished_at"):
            result[key] = format_time(result[key])
        result.pop("finish_at", None)
        return result

    def _node_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        for key in ("launched_at", "ready_at", "idle_since", "terminated_at", "fails_at"):
            result[key] = format_time(result[key])
        return result
