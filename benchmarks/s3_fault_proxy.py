#!/usr/bin/env python3
"""
S3 fault injection proxy (HTTP-level).

Sits between Arc (port 9090) and MinIO (port 9000) as an HTTP reverse proxy.
Uses aiohttp for proper HTTP handling (chunked encoding, keep-alive, etc).

Reads a control file to decide what chaos to apply:
  - normal:    pass-through, no faults
  - latency:   add 200-800ms delay before forwarding
  - timeout:   hang for 30s then return 504
  - reset:     immediately close connection (simulates network failure)
  - error500:  return HTTP 500 on write operations (PUT/POST)
  - flaky:     50% of PUTs fail with 503, 50% succeed (triggers partial flush race)

Control file: /tmp/s3proxy_mode (default: "normal")

Usage:
  python3 benchmarks/s3_fault_proxy.py &
  echo "normal" > /tmp/s3proxy_mode     # pass-through
  echo "latency" > /tmp/s3proxy_mode    # slow S3
  echo "timeout" > /tmp/s3proxy_mode    # S3 timeout
  echo "reset" > /tmp/s3proxy_mode      # connection reset
  echo "error500" > /tmp/s3proxy_mode   # S3 internal error on writes
"""
import asyncio
import random
import time
from aiohttp import web, ClientSession, ClientTimeout

LISTEN_PORT = 9090
TARGET_HOST = "127.0.0.1"
TARGET_PORT = 9000
TARGET_BASE = f"http://{TARGET_HOST}:{TARGET_PORT}"
CONTROL_FILE = "/tmp/s3proxy_mode"

total_reqs = 0
fault_reqs = 0
start_time = time.time()


def get_mode():
    try:
        with open(CONTROL_FILE) as f:
            return f.read().strip().lower()
    except FileNotFoundError:
        return "normal"


async def proxy_handler(request: web.Request) -> web.StreamResponse:
    global total_reqs, fault_reqs
    total_reqs += 1
    mode = get_mode()

    # --- Fault injection ---
    if mode == "reset":
        fault_reqs += 1
        # Abruptly close the connection
        request.transport.close()
        raise web.HTTPServiceUnavailable()

    if mode == "timeout":
        fault_reqs += 1
        await asyncio.sleep(30)
        return web.Response(status=504, text="Gateway Timeout (fault injection)")

    if mode == "latency":
        fault_reqs += 1
        delay = random.uniform(0.2, 0.8)
        await asyncio.sleep(delay)

    if mode == "error500" and request.method in ("PUT", "POST"):
        fault_reqs += 1
        return web.Response(
            status=500,
            text='<?xml version="1.0" encoding="UTF-8"?>'
                 '<Error><Code>InternalError</Code>'
                 '<Message>Fault injection</Message></Error>',
            content_type="application/xml",
        )

    if mode == "flaky" and request.method in ("PUT", "POST"):
        # ~15% of PUTs are delayed long enough to trigger context deadline exceeded.
        # The PUT is forwarded to MinIO (data IS written) but we delay the response
        # so Arc's flush timeout fires first → Arc thinks the write FAILED.
        # This is the exact "context deadline exceeded" pattern from production.
        if random.random() < 0.15:
            fault_reqs += 1
            # Forward to MinIO FIRST (data gets written!)
            target_url = f"{TARGET_BASE}{request.path_qs}"
            fwd_headers = dict(request.headers)
            body = await request.read()
            fwd_timeout = ClientTimeout(total=120)
            try:
                async with ClientSession(timeout=fwd_timeout, auto_decompress=False) as s:
                    async with s.request(
                        method=request.method, url=target_url,
                        headers=fwd_headers, data=body,
                        allow_redirects=False,
                        skip_auto_headers={"User-Agent", "Accept", "Accept-Encoding"},
                    ) as resp:
                        await resp.read()
                        # Data is now on MinIO! But delay response so Arc times out
                        await asyncio.sleep(35)  # longer than Arc's 30s flush timeout
                        return web.Response(status=504, text="Gateway Timeout (delayed)")
            except Exception:
                await asyncio.sleep(35)
                return web.Response(status=504, text="Gateway Timeout (delayed)")

    # --- Forward to MinIO ---
    target_url = f"{TARGET_BASE}{request.path_qs}"

    # Copy all headers as-is (including Host for S3 signature compatibility)
    headers = dict(request.headers)

    body = await request.read()

    timeout = ClientTimeout(total=120)
    async with ClientSession(timeout=timeout, auto_decompress=False) as session:
        async with session.request(
            method=request.method,
            url=target_url,
            headers=headers,
            data=body,
            allow_redirects=False,
            skip_auto_headers={"User-Agent", "Accept", "Accept-Encoding"},
        ) as resp:
            # Build response with same status and headers
            excluded = {"Transfer-Encoding", "Content-Encoding", "Content-Length"}
            resp_headers = {
                k: v for k, v in resp.headers.items()
                if k not in excluded
            }
            response_body = await resp.read()
            return web.Response(
                status=resp.status,
                headers=resp_headers,
                body=response_body,
            )


async def stats_printer():
    while True:
        await asyncio.sleep(10)
        mode = get_mode()
        elapsed = int(time.time() - start_time)
        print(
            f"[proxy] reqs={total_reqs} faults={fault_reqs} mode={mode} uptime={elapsed}s",
            flush=True,
        )


async def on_startup(app):
    asyncio.create_task(stats_printer())


def main():
    with open(CONTROL_FILE, "w") as f:
        f.write("normal")

    app = web.Application(client_max_size=1024 * 1024 * 256)  # 256MB max body
    app.router.add_route("*", "/{path_info:.*}", proxy_handler)
    app.on_startup.append(on_startup)

    print(f"S3 fault proxy (HTTP) :{LISTEN_PORT} -> {TARGET_BASE}", flush=True)
    web.run_app(app, host="0.0.0.0", port=LISTEN_PORT, print=None)


if __name__ == "__main__":
    main()
