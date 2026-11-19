"""Read-only transport and configuration checks for the agreed control instance."""

import argparse
import base64
import getpass
import hashlib
import http.client
import json
import os
import sys
from datetime import datetime, timezone
from urllib.parse import urlsplit


def get(base, path, headers, websocket=False):
    client = (http.client.HTTPSConnection if base.scheme == "https"
              else http.client.HTTPConnection)(base.hostname, base.port, timeout=5)
    result = {"path": path}
    try:
        client.request("GET", path, headers=headers)
        with client.getresponse() as response:
            result["http_status"] = response.status
            if websocket:
                key = headers["Sec-WebSocket-Key"] + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
                accept = base64.b64encode(hashlib.sha1(key.encode("ascii")).digest()).decode("ascii")
                result["handshake_verified"] = (
                    response.status == 101 and response.getheader("Sec-WebSocket-Accept") == accept
                    and (response.getheader("Upgrade") or "").lower() == "websocket"
                    and "upgrade" in {x.strip().lower() for x in (response.getheader("Connection") or "").split(",")}
                )
                return result, {}
            if response.status == 200:
                limit = 1024 * 1024
                raw = response.read(limit + 1)
                if len(raw) > limit:
                    result["error"] = "response_too_large"
                    return result, {}
                payload = json.loads(raw)
                data = payload.get("data", payload) if isinstance(payload, dict) else None
                if not isinstance(data, dict):
                    result["error"] = "unexpected_json_shape"
                    return result, {}
                return result, data
    except (OSError, ValueError, http.client.HTTPException) as exc:
        result["error"] = type(exc).__name__
    finally:
        client.close()
    return result, {}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("base_url", help="Agreed HTTP(S) control origin, including the port if required")
    parser.add_argument("--protected", action="store_true", help="Include authenticated configuration reads and WS handshake")
    args = parser.parse_args()
    try:
        base = urlsplit(args.base_url)
        if (base.scheme not in {"http", "https"} or not base.hostname or base.username is not None
                or base.path not in {"", "/"} or base.query or base.fragment or base.port == 0
                or any(c.isspace() or c == "\\" for c in args.base_url)):
            raise ValueError
    except ValueError:
        parser.error("Provide the agreed HTTP(S) origin only, with no credentials, path, query or fragment.")

    headers = {"Accept": "application/json"}
    if args.protected:
        token = os.environ.get("CONTROL_SERVICE_TOKEN", "")
        if not token and sys.stdin.isatty():
            token = getpass.getpass("Control service token (hidden): ")
        if not token or not token.isascii() or not token.isprintable() or any(c.isspace() for c in token):
            parser.error("Use hidden interactive input or the process environment CONTROL_SERVICE_TOKEN.")
        headers["Authorization"] = "Bearer " + token

    health, health_data = get(base, "/health", {"Accept": "application/json"})
    for key in ("started_at_ms", "point_count", "vehicle_source_reconciliation_ready", "internal_transfer_admission_ready"):
        if type(health_data.get(key)) in (int, bool):
            health[key] = health_data[key]
    # These two identifiers are shown so both sides can match the instance and history.
    for key in ("server_id", "event_history_id"):
        value = health_data.get(key)
        if isinstance(value, str) and 0 < len(value) <= 64 and all(c.isascii() and (c.isalnum() or c in "_-") for c in value):
            health[key] = value
    checks = [health]
    if args.protected:
        for path in ("/api/v1/point-business-configurations", "/api/v1/internal-vehicle-capacities"):
            result, data = get(base, path, headers)
            for key in ("configured", "revision"):
                if type(data.get(key)) in (bool, int):
                    result[key] = data[key]
            checks.append(result)
        ws_headers = dict(headers, **{
            "Connection": "Upgrade", "Upgrade": "websocket", "Sec-WebSocket-Version": "13",
            "Sec-WebSocket-Key": base64.b64encode(os.urandom(16)).decode("ascii"),
        })
        ws, _ = get(base, "/api/v1/ws", ws_headers, websocket=True)
        checks.append(ws)

    report = {
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "mode": "protected_reads_and_ws_handshake" if args.protected else "public_health_only",
        "checks": checks,
    }
    print(json.dumps(report, indent=2))
    return 0 if all("error" not in item and (item.get("handshake_verified")
                    if item["path"] == "/api/v1/ws" else item.get("http_status") == 200)
                    for item in checks) else 1


if __name__ == "__main__":
    sys.exit(main())
