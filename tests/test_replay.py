import tempfile
import unittest
from pathlib import Path

from replay import load_trace, run_replay


TRACE = """submitted_at,team,gpus,estimated_minutes,actual_minutes,priority,outcome
2025-01-01T00:00:00Z,vision,1,1,1,normal,succeeded
2025-01-01T00:01:00Z,speech,3,2,2,high,failed
2025-01-01T00:02:00Z,other,16,1,1,low,cancelled
"""


class ReplayTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.trace = Path(self.temp.name) / "trace.csv"
        self.trace.write_text(TRACE, encoding="utf-8")

    def tearDown(self):
        self.temp.cleanup()

    def test_trace_validation_and_normalization(self):
        jobs, rejected, normalized = load_trace(self.trace)

        self.assertEqual(len(jobs), 2)
        self.assertEqual(jobs[1]["gpus"], 4)
        self.assertEqual(rejected[0]["reason"], "unsupported GPU request: 16")
        self.assertEqual(normalized[0]["from_gpus"], 3)
        self.assertEqual(normalized[0]["to_gpus"], 4)

    def test_replay_reports_wait_cost_and_terminal_outcomes(self):
        report = run_replay(self.trace, policy="fair")

        self.assertEqual(report["jobs"], 2)
        self.assertEqual(report["input_rows"], 3)
        self.assertEqual(report["final_status_counts"]["succeeded"], 1)
        self.assertEqual(report["final_status_counts"]["failed"], 1)
        self.assertEqual(report["final_status_counts"]["queued"], 0)
        self.assertGreater(report["total_gpu_hours"], 0)
        self.assertGreater(report["total_cost"], 0)
        self.assertEqual(set(report["wait_minutes_by_team"]), {"speech", "vision"})


if __name__ == "__main__":
    unittest.main()
