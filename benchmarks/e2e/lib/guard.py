"""Safety guard: the harness must never touch production.

Every endpoint the harness hands to Arc, DuckDB, the proxy or a client is
checked here. Only loopback endpoints are allowed, and anything that looks
like the production object store (Hetzner `*your-objectstorage.com*`) or a
production host (`*.gyoom.sa`) is refused outright. The harness never runs
kubectl.
"""
from __future__ import annotations

import ipaddress
import os
import re
import socket
from urllib.parse import urlparse

FORBIDDEN = [
    re.compile(r"your-objectstorage\.com", re.I),
    re.compile(r"(^|[./@])gyoom\.sa", re.I),
]
FORBIDDEN_COMMANDS = {"kubectl", "helm", "k9s", "argocd"}

# Environment variables Arc (viper AutomaticEnv: ARC_*) or the AWS SDK
# (AWS_*) would read. They are stripped from every child process so a
# stray production setting in the operator's shell cannot redirect Arc.
STRIPPED_ENV_PREFIXES = ("ARC_", "AWS_")


class UnsafeTarget(RuntimeError):
    pass


def check_text(text: str, what: str = "value") -> None:
    for pat in FORBIDDEN:
        if pat.search(text or ""):
            raise UnsafeTarget(f"refusing {what}: matches production pattern {pat.pattern!r}: {text!r}")


def _is_loopback(host: str) -> bool:
    if host in ("localhost",):
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        pass
    try:
        infos = socket.getaddrinfo(host, None)
    except socket.gaierror:
        return False
    return all(ipaddress.ip_address(i[4][0]).is_loopback for i in infos)


def check_endpoint(url_or_hostport: str, what: str = "endpoint") -> str:
    """Return the endpoint unchanged if it is a safe loopback target."""
    check_text(url_or_hostport, what)
    s = url_or_hostport
    if "://" not in s:
        s = "http://" + s
    host = urlparse(s).hostname or ""
    if not _is_loopback(host):
        raise UnsafeTarget(f"refusing non-loopback {what}: {url_or_hostport!r}")
    return url_or_hostport


def check_environment() -> list[str]:
    """Refuse to run if the operator's environment points at production.

    Returns the list of env vars that will be stripped from children.
    """
    stripped = []
    for k, v in os.environ.items():
        if k.startswith(STRIPPED_ENV_PREFIXES):
            check_text(v, f"environment variable {k}")
            stripped.append(k)
    return stripped


def child_env(extra: dict | None = None) -> dict:
    env = {k: v for k, v in os.environ.items() if not k.startswith(STRIPPED_ENV_PREFIXES)}
    # Never let a proxy env var reroute local traffic.
    for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
        env.pop(k, None)
    env["NO_PROXY"] = "127.0.0.1,localhost"
    if extra:
        for k, v in extra.items():
            check_text(str(v), f"child env {k}")
        env.update({k: str(v) for k, v in extra.items()})
    return env


def check_command(argv: list[str]) -> None:
    for a in argv:
        base = os.path.basename(str(a))
        if base in FORBIDDEN_COMMANDS:
            raise UnsafeTarget(f"refusing to run {base}: the harness never talks to a cluster")
        check_text(str(a), "command argument")
