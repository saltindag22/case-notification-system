#!/usr/bin/env python3
"""End-to-end smoke test against a running stack.

Uses only the Python standard library, so it runs anywhere:

    docker compose run --rm smoke                      # inside Docker (any OS, nothing to install)
    python3 scripts/smoke.py                           # from the host, API on http://localhost:8080
    API=http://localhost:8081 python3 scripts/smoke.py # API on another port
"""

import datetime
import json
import os
import sys
import time
import urllib.error
import urllib.request

API = os.environ.get("API", "http://localhost:8080").rstrip("/")
B = API + "/api/v1"
RUN_ID = "smoke-%d" % time.time()
GREEN, RED, RESET = ("\033[32m", "\033[31m", "\033[0m") if sys.stdout.isatty() else ("", "", "")
passed = 0


def request(method, url, body=None, headers=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw and resp.headers.get_content_type() == "application/json" else raw)
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw)
        except ValueError:
            return e.code, raw
    except (urllib.error.URLError, OSError) as e:  # not reachable (yet)
        return 0, str(e)


def ok(msg):
    global passed
    passed += 1
    print("  %s✓%s %s" % (GREEN, RESET, msg))


def fail(msg):
    print("  %s✗%s %s" % (RED, RESET, msg))
    sys.exit(1)


def check(cond, good, bad):
    ok(good) if cond else fail(bad)


def wait_until(fn, timeout=30.0, interval=0.5):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        done, last = fn()
        if done:
            return True, last
        time.sleep(interval)
    return False, last


def main():
    print("Smoke test against " + API)

    ready, last = wait_until(lambda: (lambda r: (r[0] == 200, r))(request("GET", API + "/health/ready")), timeout=60)
    check(ready, "readiness probe", "api not reachable/ready at %s (%s). Is the stack running, and is API set to the right port?"
          % (API, last[1] if last else "no response"))

    sms = {"recipient": "+905551234567", "channel": "sms", "content": "Your code is 123456", "priority": "high"}
    key = {"Idempotency-Key": RUN_ID + "-sms"}
    status, n = request("POST", B + "/notifications", sms, key)
    check(status == 202, "created sms %s" % n.get("id") if status == 202 else "", "create returned %s %s" % (status, n))
    nid = n["id"]

    status, _ = request("POST", B + "/notifications", sms, key)
    check(status == 200, "idempotent replay returns 200", "replay returned %s" % status)

    status, _ = request("POST", B + "/notifications", dict(sms, content="different"), key)
    check(status == 409, "key reuse with different payload returns 409", "conflict returned %s" % status)

    delivered, st = wait_until(lambda: (lambda s: (s == "sent", s))(request("GET", B + "/notifications/" + nid)[1]["status"]))
    check(delivered, "sms delivered (sent)", "sms not delivered, last status %s" % st)

    status, _ = request("POST", B + "/notifications", {"recipient": "0555", "channel": "sms", "content": ""})
    check(status == 400, "invalid request returns 400", "validation returned %s" % status)

    status, tpl = request("POST", B + "/templates", {
        "name": RUN_ID + "-welcome", "channel": "email",
        "subject": "Welcome {{.name}}", "content": "Hi {{.name}}, enjoy {{.discount}}% off.",
    })
    check(status == 201, "template created", "template create returned %s %s" % (status, tpl))
    status, e = request("POST", B + "/notifications", {
        "recipient": "user@example.com", "templateId": tpl["id"], "variables": {"name": "Ayse", "discount": 20},
    })
    check(status == 202 and e.get("subject") == "Welcome Ayse", "template rendered (subject: %s)" % e.get("subject"),
          "template rendering returned %s %s" % (status, e))

    at = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    _, s = request("POST", B + "/notifications", {"recipient": "+905551234567", "channel": "sms", "content": "later", "scheduledAt": at})
    status, c = request("POST", B + "/notifications/%s/cancel" % s["id"])
    check(status == 200 and c["status"] == "cancelled", "scheduled notification cancelled", "cancel returned %s %s" % (status, c))
    status, _ = request("POST", B + "/notifications/%s/cancel" % s["id"])
    check(status == 409, "second cancel returns 409", "second cancel returned %s" % status)

    items = [{"recipient": "+90555%07d" % i, "channel": "sms", "content": "Batch %d" % i,
              "priority": ["low", "normal", "high"][i % 3]} for i in range(200)]
    status, b = request("POST", B + "/notifications/batch", {"notifications": items}, {"Idempotency-Key": RUN_ID + "-batch"})
    check(status == 202, "batch of 200 accepted (%s)" % b.get("batchId") if status == 202 else "", "batch returned %s" % status)
    bid = b["batchId"]
    done, summary = wait_until(lambda: (lambda d: (d["completed"], d))(request("GET", B + "/batches/" + bid)[1]))
    check(done, "batch completed: %s" % (summary or {}).get("counts"), "batch did not complete: %s" % summary)

    _, page = request("GET", B + "/notifications?batchId=%s&limit=50" % bid)
    check(len(page["data"]) == 50 and page["pagination"]["hasMore"], "pagination works (50 items, hasMore)",
          "pagination returned %d items" % len(page["data"]))

    status, m = request("GET", B + "/metrics?window=5m")
    check(status == 200 and "totals" in m, "metrics endpoint: %s" % m.get("totals"), "metrics returned %s" % status)

    status, prom = request("GET", API + "/metrics")
    check(status == 200 and b"notification_api_http_requests_total" in prom, "prometheus metrics exposed", "prometheus endpoint returned %s" % status)

    print("\nAll %d checks passed." % passed)


if __name__ == "__main__":
    main()
