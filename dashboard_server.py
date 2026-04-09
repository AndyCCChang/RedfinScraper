import json
import os
import shlex
import subprocess
import threading
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

from pipeline_context import current_run_dir


HOST = "127.0.0.1"
PORT = 8765
ROOT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = ROOT_DIR / "config.json"
ALLOWED_PREFIXES = {
    ("python3", "all_in_one.py"),
    ("python3", "generate_report.py"),
}

RUN_LOCK = threading.Lock()
ACTIVE_PROCESS: Optional[subprocess.Popen] = None
LAST_COMMAND: str = ""
LAST_RETURN_CODE: Optional[int] = None


def latest_report_path() -> str:
    latest = ROOT_DIR / "latest_report.html"
    if latest.exists():
        return "/latest_report.html"
    return "/report.html"


def read_config_payload() -> dict:
    if not CONFIG_PATH.exists():
        return {}
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def write_config_payload(config_payload: dict) -> None:
    CONFIG_PATH.write_text(json.dumps(config_payload, indent=2) + "\n", encoding="utf-8")


def status_payload() -> dict:
    global ACTIVE_PROCESS, LAST_RETURN_CODE
    with RUN_LOCK:
        active = ACTIVE_PROCESS
        if active is not None:
            return_code = active.poll()
            if return_code is not None:
                LAST_RETURN_CODE = return_code
                ACTIVE_PROCESS = None
                active = None

        run_dir = current_run_dir()
        return {
            "running": active is not None,
            "last_command": LAST_COMMAND,
            "last_return_code": LAST_RETURN_CODE,
            "current_run_dir": str(run_dir.resolve()) if run_dir else None,
            "latest_report": latest_report_path(),
        }


def validate_command(command_text: str) -> list:
    parts = shlex.split(command_text)
    if len(parts) < 2:
        raise ValueError("Command is too short.")

    prefix = tuple(parts[:2])
    if prefix not in ALLOWED_PREFIXES:
        raise ValueError("Only `python3 all_in_one.py ...` or `python3 generate_report.py` are allowed from the report.")

    return parts


class DashboardHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT_DIR), **kwargs)

    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(HTTPStatus.NO_CONTENT)
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self.send_response(HTTPStatus.FOUND)
            self.send_header("Location", latest_report_path())
            self.end_headers()
            return

        if parsed.path == "/api/status":
            payload = status_payload()
            body = json.dumps(payload).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if parsed.path == "/api/config":
            try:
                payload = {"config": read_config_payload()}
                body = json.dumps(payload).encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except (json.JSONDecodeError, OSError) as exc:
                self._send_json({"ok": False, "error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)
            return

        super().do_GET()

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path not in {"/api/run", "/api/config"}:
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return

        content_length = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(content_length) if content_length else b"{}"
        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            self._send_json({"ok": False, "error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return

        if parsed.path == "/api/config":
            try:
                config_payload = payload.get("config")
                if not isinstance(config_payload, dict):
                    raise ValueError("Config payload must be a JSON object.")
                write_config_payload(config_payload)
            except (ValueError, OSError) as exc:
                self._send_json({"ok": False, "error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
                return

            self._send_json({"ok": True, "saved": True, "config": config_payload})
            return

        try:
            command_text = str(payload.get("command", "")).strip()
            command = validate_command(command_text)
        except ValueError as exc:
            self._send_json({"ok": False, "error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return

        global ACTIVE_PROCESS, LAST_COMMAND, LAST_RETURN_CODE
        with RUN_LOCK:
            if ACTIVE_PROCESS is not None and ACTIVE_PROCESS.poll() is None:
                self._send_json({"ok": False, "error": "A pipeline run is already in progress."}, status=HTTPStatus.CONFLICT)
                return

            ACTIVE_PROCESS = subprocess.Popen(command, cwd=str(ROOT_DIR), env=os.environ.copy())
            LAST_COMMAND = command_text
            LAST_RETURN_CODE = None

        self._send_json({"ok": True, "started": True, "command": command_text, "status": status_payload()})

    def _send_json(self, payload: dict, status: int = HTTPStatus.OK):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> int:
    server = ThreadingHTTPServer((HOST, PORT), DashboardHandler)
    print(f"Dashboard server running at http://{HOST}:{PORT}")
    print(f"Open http://{HOST}:{PORT}{latest_report_path()}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
