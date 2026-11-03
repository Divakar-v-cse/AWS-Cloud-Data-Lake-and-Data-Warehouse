"""Hit every documented route and report status, latency and a data sample.

Reads /openapi.json so the list can never drift from the app's real surface.
"""
import json
import time
import urllib.error
import urllib.request

BASE = "http://127.0.0.1:8020"


def get(path):
    started = time.time()
    try:
        with urllib.request.urlopen(BASE + path, timeout=60) as r:
            body = r.read()
            return r.status, round((time.time() - started) * 1000), body
    except urllib.error.HTTPError as exc:
        return exc.code, round((time.time() - started) * 1000), exc.read()
    except Exception as exc:  # noqa: BLE001
        return 0, round((time.time() - started) * 1000), str(exc).encode()


def preview(payload):
    try:
        obj = json.loads(payload)
    except Exception:
        return payload[:90].decode("utf-8", "replace")
    if isinstance(obj, dict):
        bits = []
        for k, v in list(obj.items())[:4]:
            if isinstance(v, list):
                bits.append(f"{k}[{len(v)}]")
            elif isinstance(v, dict):
                bits.append(f"{k}{{{len(v)}}}")
            else:
                bits.append(f"{k}={v}")
        return ", ".join(bits)[:120]
    if isinstance(obj, list):
        return f"list[{len(obj)}] first={json.dumps(obj[0])[:90] if obj else '-'}"
    return str(obj)[:90]


with urllib.request.urlopen(BASE + "/openapi.json", timeout=30) as r:
    spec = json.load(r)

paths = sorted(p for p in spec["paths"] if p.startswith("/api") or p.startswith("/mock-api"))
print(f"routes discovered in openapi.json: {len(paths)}\n")

ok = bad = 0
for path in paths:
    if "{" in path:
        print(f"  SKIP  {path} (parameterised)")
        continue
    status, ms, body = get(path)
    good = status == 200 and len(body) > 2
    ok += bool(good)
    bad += (not good)
    flag = "OK  " if good else "FAIL"
    print(f"  {flag}  {status}  {ms:>6}ms  {path:<34} {len(body):>7}B  {preview(body)}")

print(f"\nsummary: {ok} ok, {bad} failed")

# --- SPA delivery -----------------------------------------------------------
status, ms, body = get("/")
html = body.decode("utf-8", "replace")
print(f"\nSPA  /            -> {status}  {len(body)}B  root_div={'<div id=\"root\">' in html}")
for needle in ("assets/index-", "<title>"):
    print(f"     contains {needle!r}: {needle in html}")

import re

for asset in re.findall(r'(?:src|href)="(/assets/[^"]+)"', html):
    s, ms2, b = get(asset)
    print(f"     asset {asset:<40} -> {s}  {len(b)}B  {ms2}ms")
    if asset.endswith(".js"):
        print(f"     js mentions api path: {'/api/' in b.decode('utf-8', 'replace')}")
