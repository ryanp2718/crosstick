#!/usr/bin/env python3
"""Post-`up` smoke check for the observability plane.

prometheus/grafana read their mounted config once at container creation, so a
config edit that wasn't followed by `--force-recreate` (or a bind of a file that
didn't exist at create time) leaves the stack running stale/empty config with no
error. CI config-lint catches bad *syntax*; this catches "valid config that
never actually loaded." Run it after `docker compose up -d`:

    python ops/smoke.py

Exits non-zero if Prometheus loaded no rules or an expected dashboard is missing.
Target health is reported but not fatal (the stack is often run as a subset in
dev, and the TargetDown alert already makes real outages loud). Override the
endpoints with PROM_URL / GRAFANA_URL.
"""
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

PROM = os.environ.get("PROM_URL", "http://localhost:9090").rstrip("/")
GRAFANA = os.environ.get("GRAFANA_URL", "http://localhost:3000").rstrip("/")
DASHBOARDS = Path(__file__).parent / "grafana" / "dashboards"


def get(url):
    with urllib.request.urlopen(url, timeout=10) as r:
        return r.status, json.load(r)


def check_rules():
    try:
        _, body = get(f"{PROM}/api/v1/rules")
    except (urllib.error.URLError, OSError) as e:
        return False, f"Prometheus unreachable at {PROM}: {e}"
    groups = body.get("data", {}).get("groups", [])
    n = sum(len(g.get("rules", [])) for g in groups)
    if n == 0:
        return False, "Prometheus loaded 0 alerting rules (config not picked up?)"
    return True, f"Prometheus loaded {n} rule(s) across {len(groups)} group(s)"


def check_targets():
    try:
        _, body = get(f"{PROM}/api/v1/targets?state=active")
    except (urllib.error.URLError, OSError) as e:
        return None, f"could not read targets: {e}"
    targets = body.get("data", {}).get("activeTargets", [])
    down = [t["scrapeUrl"] for t in targets if t.get("health") != "up"]
    up = len(targets) - len(down)
    msg = f"{up}/{len(targets)} scrape targets up"
    if down:
        msg += " - DOWN: " + ", ".join(down)
    return None, msg  # informational only


def check_dashboards():
    expected = {}
    for f in sorted(DASHBOARDS.glob("*.json")):
        try:
            expected[json.loads(f.read_text())["uid"]] = f.name
        except (json.JSONDecodeError, KeyError) as e:
            return False, f"{f.name}: cannot read uid ({e})"
    if not expected:
        return False, f"no dashboard JSON found under {DASHBOARDS}"
    missing = []
    for uid, name in expected.items():
        try:
            status, _ = get(f"{GRAFANA}/api/dashboards/uid/{uid}")
        except urllib.error.HTTPError as e:
            status = e.code
        except (urllib.error.URLError, OSError) as e:
            return False, f"Grafana unreachable at {GRAFANA}: {e}"
        if status != 200:
            missing.append(f"{name} (uid={uid}, HTTP {status})")
    if missing:
        return False, "dashboards not provisioned: " + ", ".join(missing)
    return True, f"all {len(expected)} dashboard(s) provisioned"


def main():
    ok = True
    for name, fn, fatal in [
        ("rules", check_rules, True),
        ("targets", check_targets, False),
        ("dashboards", check_dashboards, True),
    ]:
        passed, msg = fn()
        mark = "INFO" if passed is None else ("PASS" if passed else "FAIL")
        print(f"[{mark}] {name}: {msg}")
        if fatal and passed is False:
            ok = False
    if not ok:
        print("\nsmoke check FAILED - recreate the affected service:")
        print("  docker compose up -d --force-recreate prometheus grafana")
        sys.exit(1)
    print("\nsmoke check passed")


if __name__ == "__main__":
    main()
