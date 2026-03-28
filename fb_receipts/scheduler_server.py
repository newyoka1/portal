"""
Tiny local HTTP server that handles sheet button clicks.
Runs on http://localhost:5050

Endpoints:
  /sync-scheduler   — syncs Windows Task Scheduler tasks from the Google Sheet
  /run              — runs the receipt send for today
  /health           — health check
"""
import subprocess
import sys
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path

logging.basicConfig(
    filename=str(Path(__file__).parent / "receipt_automation.log"),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] scheduler_server: %(message)s",
)

PROJECT_DIR = Path(__file__).resolve().parent
PYTHON = Path(sys.executable).resolve()
PORT = 5050

RESPONSE_HTML = """<!DOCTYPE html>
<html>
<head>
  <style>
    body {{ font-family: sans-serif; display: flex; align-items: center;
            justify-content: center; height: 100vh; margin: 0; background: #f0f4ff; }}
    .box {{ text-align: center; padding: 40px; background: white;
             border-radius: 12px; box-shadow: 0 2px 16px rgba(0,0,0,0.1); }}
    h2 {{ color: #4285f4; }} p {{ color: #555; }}
  </style>
  <script>setTimeout(function(){{window.close()}}, 2000);</script>
</head>
<body>
  <div class="box">
    <h2>{title}</h2>
    <p>{message}</p>
    <p style="font-size:12px;color:#aaa">This window will close automatically.</p>
  </div>
</body>
</html>"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = self.path.split("?")[0]

        if path == "/health":
            self._respond(200, "<h2>OK</h2>")
            return

        if path == "/sync-scheduler":
            logging.info("Received /sync-scheduler request")
            subprocess.Popen(
                [str(PYTHON), str(PROJECT_DIR / "main.py"), "--sync-scheduler"],
                cwd=str(PROJECT_DIR),
            )
            html = RESPONSE_HTML.format(
                title="Syncing Scheduler...",
                message="Task Scheduler is being updated from your Google Sheet.",
            )
            self._respond(200, html)
            return

        if path == "/run":
            logging.info("Received /run request")
            subprocess.Popen(
                [str(PYTHON), str(PROJECT_DIR / "main.py"), "--since-last-send"],
                cwd=str(PROJECT_DIR),
            )
            html = RESPONSE_HTML.format(
                title="Sending Receipts...",
                message="Receipt run started. Check receipt_automation.log for progress.",
            )
            self._respond(200, html)
            return

        self._respond(404, "<h2>Not found</h2>")

    def _respond(self, code: int, html: str):
        body = html.encode()
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass  # suppress console noise


if __name__ == "__main__":
    server = HTTPServer(("localhost", PORT), Handler)
    logging.info("Scheduler server listening on http://localhost:%d", PORT)
    print(f"Scheduler server running on http://localhost:{PORT}  (Ctrl+C to stop)")
    server.serve_forever()
