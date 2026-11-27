"""HTTP API and development server for the GPU job broker."""

from __future__ import annotations

import json
import mimetypes
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

from broker import Broker, BrokerError, InvalidRequest, NotFound


ROOT = Path(__file__).resolve().parent
STATIC_ROOT = ROOT / "static"
INDEX_PAGE = STATIC_ROOT / "index.html"


class BrokerHandler(BaseHTTPRequestHandler):
    broker: Broker

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        try:
            if parsed.path == "/api/state":
                self._send_json(200, self.broker.snapshot())
            elif parsed.path == "/api/jobs":
                status = params.get("status", [None])[0]
                self._send_json(200, {"jobs": self.broker.list_jobs(status)})
            elif parsed.path == "/api/queue":
                self._send_json(200, {"jobs": self.broker.list_jobs("queued")})
            elif parsed.path == "/api/nodes":
                active = params.get("active", ["true"])[0].lower() != "false"
                self._send_json(200, {"nodes": self.broker.list_nodes(active_only=active)})
            elif parsed.path.startswith("/api/jobs/"):
                job_id = parsed.path.removeprefix("/api/jobs/")
                self._send_json(200, {"job": self.broker.get_job(job_id)})
            elif parsed.path == "/api/cost":
                self._send_json(
                    200,
                    self.broker.cost_report(
                        params.get("start", [None])[0],
                        params.get("end", [None])[0],
                    ),
                )
            elif parsed.path.startswith("/api/"):
                self._send_json(404, {"error": "Not found"})
            else:
                self._send_static(parsed.path)
        except NotFound as exc:
            self._send_json(404, {"error": str(exc)})
        except InvalidRequest as exc:
            self._send_json(400, {"error": str(exc)})
        except ValueError as exc:
            self._send_json(400, {"error": str(exc)})
        except BrokerError as exc:
            self._send_json(400, {"error": str(exc)})

    def do_POST(self) -> None:
        try:
            parsed = urlparse(self.path)
            if parsed.path.startswith("/api/jobs/") and parsed.path.endswith("/cancel"):
                job_id = parsed.path.removeprefix("/api/jobs/").removesuffix("/cancel").strip("/")
                self._send_json(200, {"job": self.broker.cancel_job(job_id)})
            elif parsed.path == "/api/jobs":
                body = self._read_json()
                key = body.get("client_key") or self.headers.get("Idempotency-Key")
                if key is None:
                    raise InvalidRequest("client_key or Idempotency-Key is required")
                result = self.broker.submit_job(
                    team=body.get("team"),
                    gpus=body.get("gpus"),
                    estimated_minutes=body.get("estimated_minutes"),
                    priority=body.get("priority", "normal"),
                    client_key=key,
                )
                self._send_json(201, {"job": result})
            elif parsed.path == "/api/advance":
                body = self._read_json()
                if "to" in body:
                    state = self.broker.advance(body["to"])
                elif "seconds" in body:
                    state = self.broker.advance_seconds(body["seconds"])
                else:
                    raise InvalidRequest("provide either 'to' or 'seconds'")
                self._send_json(200, state)
            else:
                self._send_json(404, {"error": "Not found"})
        except NotFound as exc:
            self._send_json(404, {"error": str(exc)})
        except json.JSONDecodeError as exc:
            self._send_json(400, {"error": f"Invalid JSON: {exc.msg}"})
        except (InvalidRequest, TypeError, ValueError, KeyError) as exc:
            self._send_json(400, {"error": str(exc)})
        except BrokerError as exc:
            self._send_json(400, {"error": str(exc)})

    def _read_json(self) -> dict:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise InvalidRequest("Content-Length must be an integer") from exc
        if length <= 0 or length > 1_000_000:
            raise InvalidRequest("request body must be between 1 byte and 1 MB")
        payload = json.loads(self.rfile.read(length))
        if not isinstance(payload, dict):
            raise InvalidRequest("JSON request body must be an object")
        return payload

    def _send_json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload, separators=(",", ":"), allow_nan=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_static(self, request_path: str) -> None:
        if not INDEX_PAGE.is_file():
            self._send_json(503, {"error": "Frontend build is missing; see README build instructions"})
            return

        relative_path = unquote(request_path).lstrip("/")
        requested_file = (STATIC_ROOT / relative_path).resolve()
        static_root = STATIC_ROOT.resolve()
        if requested_file != static_root and static_root not in requested_file.parents:
            self._send_json(404, {"error": "Not found"})
            return
        if request_path == "/":
            requested_file = INDEX_PAGE
        elif not requested_file.is_file():
            self._send_json(404, {"error": "Not found"})
            return

        body = requested_file.read_bytes()
        content_type, _encoding = mimetypes.guess_type(requested_file.name)
        if content_type is None:
            content_type = "application/octet-stream"
        if content_type.startswith("text/") or content_type in {
            "application/javascript",
            "application/json",
            "image/svg+xml",
        }:
            content_type += "; charset=utf-8"
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args) -> None:
        print("%s - %s" % (self.address_string(), fmt % args))


def main() -> None:
    db_path = os.environ.get("BROKER_DB", str(ROOT / "broker.sqlite3"))
    idle_timeout = int(os.environ.get("BROKER_IDLE_TIMEOUT_SECONDS", "300"))
    broker = Broker(db_path, idle_timeout_seconds=idle_timeout)
    handler = type("ConfiguredBrokerHandler", (BrokerHandler,), {"broker": broker})
    server = ThreadingHTTPServer(("127.0.0.1", int(os.environ.get("PORT", "8000"))), handler)
    print(f"GPU Job Broker listening on http://127.0.0.1:{server.server_port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping server")
    finally:
        server.server_close()
        broker.close()


if __name__ == "__main__":
    main()
