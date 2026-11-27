import tempfile
import json
import re
import threading
import unittest
from pathlib import Path
from urllib.error import HTTPError
from http.server import ThreadingHTTPServer
from urllib.request import Request, urlopen

from broker import Broker, CapacityUnavailable, InvalidRequest, parse_time
from server import BrokerHandler


START = "2025-01-01T00:00:00Z"


class FailFirstLaunchProvider:
    def __init__(self):
        self.calls = 0

    def launch_node(self, node_type, now, attempt):
        self.calls += 1
        if self.calls == 1:
            raise CapacityUnavailable("test capacity outage")
        from broker import FakeProvider

        return FakeProvider().launch_node(node_type, now, attempt)

    def terminate_node(self, node_id):
        return None


class FailSecondLaunchProvider:
    def __init__(self):
        from broker import FakeProvider

        self.calls = 0
        self.provider = FakeProvider()

    def launch_node(self, node_type, now, attempt):
        self.calls += 1
        if self.calls == 2:
            raise CapacityUnavailable("test capacity outage")
        return self.provider.launch_node(node_type, now, attempt)

    def terminate_node(self, node_id):
        self.provider.terminate_node(node_id)


class BrokerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "broker.sqlite3"
        self.broker = Broker(self.db, start_time=START, idle_timeout_seconds=120)

    def tearDown(self):
        self.broker.close()
        self.temp.cleanup()

    def submit(self, **overrides):
        values = {
            "team": "vision",
            "gpus": 1,
            "estimated_minutes": 2,
            "priority": "normal",
            "client_key": "client-1",
        }
        values.update(overrides)
        return self.broker.submit_job(**values)

    def test_submit_is_idempotent_for_client_key(self):
        first = self.submit()
        repeated = self.submit(team="other", gpus=8)
        self.assertEqual(first["id"], repeated["id"])
        self.assertEqual(len(self.broker.list_jobs()), 1)

    def test_job_lifecycle_idle_reaper_and_cost(self):
        job = self.submit()
        node = self.broker.list_nodes()[0]
        ready_at = parse_time(node["ready_at"])

        self.broker.advance(ready_at)
        self.assertEqual(self.broker.get_job(job["id"])["status"], "running")
        self.broker.advance(ready_at + 120)
        self.assertEqual(self.broker.get_job(job["id"])["status"], "succeeded")
        self.assertEqual(self.broker.list_nodes()[0]["status"], "ready")

        self.broker.advance(ready_at + 240)
        self.assertEqual(self.broker.list_nodes(active_only=False)[0]["status"], "terminated")
        report = self.broker.cost_report(START, ready_at + 240)
        self.assertAlmostEqual(report["teams"]["vision"]["gpu_hours"], 2 / 60, places=5)
        self.assertGreaterEqual(report["total_cost"], 1 / 6)

    def test_nodes_are_never_shared_between_teams(self):
        self.submit(client_key="vision-1", team="vision")
        self.submit(client_key="speech-1", team="speech")
        nodes = self.broker.list_nodes()
        self.assertEqual({node["team"] for node in nodes}, {"vision", "speech"})

    def test_priority_orders_queued_jobs_without_preemption(self):
        low = self.submit(client_key="low", priority="low")
        normal = self.submit(client_key="normal", priority="normal")
        high = self.submit(client_key="high", priority="high")
        ready_at = parse_time(self.broker.list_nodes()[0]["ready_at"])

        self.broker.advance(ready_at)
        self.assertEqual(self.broker.get_job(high["id"])["status"], "running")
        self.assertEqual(self.broker.get_job(low["id"])["status"], "queued")
        self.broker.advance(ready_at + 120)
        self.assertEqual(self.broker.get_job(normal["id"])["status"], "running")
        self.broker.advance(ready_at + 240)
        self.assertEqual(self.broker.get_job(low["id"])["status"], "running")

    def test_provider_capacity_failure_retries_without_losing_job(self):
        self.broker.close()
        self.broker = Broker(
            self.db,
            start_time=START,
            idle_timeout_seconds=120,
            provider=FailFirstLaunchProvider(),
        )
        job = self.submit()
        self.assertEqual(job["status"], "queued")
        self.broker.advance(parse_time(START) + 60)
        self.assertEqual(len(self.broker.list_nodes()), 1)
        self.assertEqual(self.broker.get_job(job["id"])["status"], "queued")

        node_ready = parse_time(self.broker.list_nodes()[0]["ready_at"])
        self.broker.advance(node_ready)
        self.assertEqual(self.broker.get_job(job["id"])["status"], "running")

    def test_capacity_backoff_does_not_block_jobs_that_fit_ready_nodes(self):
        self.broker.close()
        self.broker = Broker(
            self.db,
            start_time=START,
            idle_timeout_seconds=120,
            provider=FailSecondLaunchProvider(),
        )
        first = self.submit(client_key="vision-first", gpus=2, estimated_minutes=10)
        first_node = self.broker.list_nodes()[0]
        self.broker.advance(parse_time(first_node["ready_at"]))
        self.assertEqual(self.broker.get_job(first["id"])["status"], "running")

        blocked = self.submit(
            client_key="speech-high",
            team="speech",
            priority="high",
        )
        self.assertEqual(blocked["status"], "queued")

        fitting = self.submit(client_key="vision-second")
        self.assertEqual(self.broker.get_job(fitting["id"])["status"], "running")
        self.assertEqual(len(self.broker.list_nodes()), 1)

    def test_restart_preserves_jobs_nodes_and_simulated_time(self):
        job = self.submit()
        original_time = self.broker.now
        self.broker.close()

        self.broker = Broker(self.db, idle_timeout_seconds=120)
        self.assertEqual(self.broker.now, original_time)
        self.assertEqual(self.broker.get_job(job["id"])["status"], "queued")
        self.assertEqual(len(self.broker.list_nodes()), 1)

    def test_invalid_request_is_rejected(self):
        with self.assertRaises(InvalidRequest):
            self.submit(gpus=3)
        with self.assertRaises(InvalidRequest):
            self.submit(estimated_minutes=float("nan"))
        with self.assertRaises(InvalidRequest):
            self.broker.advance_seconds(float("nan"))

    def test_running_job_can_be_cancelled(self):
        job = self.submit()
        ready_at = parse_time(self.broker.list_nodes()[0]["ready_at"])
        self.broker.advance(ready_at)
        cancelled = self.broker.cancel_job(job["id"])
        self.assertEqual(cancelled["status"], "cancelled")
        self.assertIsNotNone(cancelled["finished_at"])

    def test_http_submit_state_and_cancel(self):
        handler = type(
            "TestBrokerHandler",
            (BrokerHandler,),
            {"broker": self.broker},
        )
        httpd = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        base = f"http://127.0.0.1:{httpd.server_port}"
        try:
            with urlopen(base) as response:
                page = response.read().decode()
                self.assertEqual(response.headers.get_content_type(), "text/html")
            self.assertIn("<app-root></app-root>", page)
            script_path = re.search(r'<script src="([^"]+\.js)"', page)
            self.assertIsNotNone(script_path)
            with urlopen(f"{base}/{script_path.group(1)}") as response:
                self.assertIn(
                    response.headers.get_content_type(),
                    {"text/javascript", "application/javascript"},
                )

            request = Request(
                f"{base}/api/jobs",
                data=json.dumps(
                    {
                        "team": "vision",
                        "gpus": 1,
                        "estimated_minutes": 5,
                    }
                ).encode(),
                headers={
                    "Content-Type": "application/json",
                    "Idempotency-Key": "http-key",
                },
                method="POST",
            )
            with urlopen(request) as response:
                created = json.load(response)["job"]
                self.assertEqual(response.status, 201)
            with urlopen(f"{base}/api/state") as response:
                self.assertEqual(len(json.load(response)["queue"]), 1)
            with urlopen(f"{base}/api/queue") as response:
                self.assertEqual(len(json.load(response)["jobs"]), 1)
            with urlopen(f"{base}/api/jobs?status=queued") as response:
                self.assertEqual(len(json.load(response)["jobs"]), 1)
            with urlopen(f"{base}/api/jobs/{created['id']}") as response:
                self.assertEqual(json.load(response)["job"]["id"], created["id"])
            with urlopen(f"{base}/api/nodes") as response:
                self.assertEqual(len(json.load(response)["nodes"]), 1)
            with urlopen(f"{base}/api/nodes?active=false") as response:
                self.assertEqual(len(json.load(response)["nodes"]), 1)
            with urlopen(f"{base}/api/cost") as response:
                self.assertIn("total_cost", json.load(response))

            cancel = Request(
                f"{base}/api/jobs/{created['id']}/cancel",
                data=None,
                method="POST",
            )
            with urlopen(cancel) as response:
                self.assertEqual(json.load(response)["job"]["status"], "cancelled")

            advance = Request(
                f"{base}/api/advance",
                data=json.dumps({"seconds": 30}).encode(),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urlopen(advance) as response:
                self.assertIn("simulated_at", json.load(response))

            for path in ("/api/not-found", "/api/jobs/missing-job"):
                with self.assertRaises(HTTPError) as response:
                    urlopen(f"{base}{path}")
                self.assertEqual(response.exception.code, 404)

            invalid_advance = Request(
                f"{base}/api/advance",
                data=b"{}",
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with self.assertRaises(HTTPError) as response:
                urlopen(invalid_advance)
            self.assertEqual(response.exception.code, 400)
        finally:
            httpd.shutdown()
            httpd.server_close()
            thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
