#!/usr/bin/env bash
# End-to-end smoke test against a running stack (`docker compose up`).
# Exercises the main API flows and waits for real asynchronous delivery.
#
#   ./scripts/smoke.sh                 # API on http://localhost:8080
#   API=http://localhost:8081 ./scripts/smoke.sh
set -euo pipefail

API="${API:-http://localhost:8080}"
B="$API/api/v1"
RUN_ID="smoke-$(date +%s)"
pass=0

json() { python3 -c "import sys,json; d=json.load(sys.stdin); print(eval(sys.argv[1], {}, {'d': d}))" "$1"; }
ok() { pass=$((pass + 1)); printf '  \033[32m✓\033[0m %s\n' "$1"; }
fail() { printf '  \033[31m✗\033[0m %s\n' "$1"; exit 1; }
post() { curl -sS -X POST "$B$1" -H 'Content-Type: application/json' "${@:3}" -d "$2"; }

wait_status() { # id wanted-status...
  local id=$1; shift
  for _ in $(seq 1 40); do
    local st; st=$(curl -sS "$B/notifications/$id" | json 'd["status"]')
    for want in "$@"; do [[ "$st" == "$want" ]] && { echo "$st"; return 0; }; done
    sleep 0.5
  done
  echo "timeout (last status: $st)"; return 1
}

echo "Smoke test against $API"

curl -sSf "$API/health/ready" >/dev/null && ok "readiness probe" || fail "api not ready"

ID=$(post /notifications '{"recipient":"+905551234567","channel":"sms","content":"Your code is 123456","priority":"high"}' \
  -H "Idempotency-Key: $RUN_ID-sms" | json 'd["id"]')
ok "created sms $ID"

REPLAY=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$B/notifications" -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $RUN_ID-sms" -d '{"recipient":"+905551234567","channel":"sms","content":"Your code is 123456","priority":"high"}')
[[ "$REPLAY" == "200" ]] && ok "idempotent replay returns 200" || fail "replay returned $REPLAY"

CONFLICT=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$B/notifications" -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $RUN_ID-sms" -d '{"recipient":"+905551234567","channel":"sms","content":"different"}')
[[ "$CONFLICT" == "409" ]] && ok "key reuse with different payload returns 409" || fail "conflict returned $CONFLICT"

ST=$(wait_status "$ID" sent) && ok "sms delivered ($ST)" || fail "sms not delivered: $ST"

BAD=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$B/notifications" -H 'Content-Type: application/json' \
  -d '{"recipient":"0555","channel":"sms","content":""}')
[[ "$BAD" == "400" ]] && ok "invalid request returns 400" || fail "validation returned $BAD"

TID=$(post /templates "{\"name\":\"$RUN_ID-welcome\",\"channel\":\"email\",\"subject\":\"Welcome {{.name}}\",\"content\":\"Hi {{.name}}, enjoy {{.discount}}% off.\"}" | json 'd["id"]')
EID=$(post /notifications "{\"recipient\":\"user@example.com\",\"templateId\":\"$TID\",\"variables\":{\"name\":\"Ayse\",\"discount\":20}}" | json 'd["id"]')
SUBJ=$(curl -sS "$B/notifications/$EID" | json 'd["subject"]')
[[ "$SUBJ" == "Welcome Ayse" ]] && ok "template rendered (subject: $SUBJ)" || fail "template subject was $SUBJ"

AT=$(python3 -c 'import datetime;print((datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ"))')
SID=$(post /notifications "{\"recipient\":\"+905551234567\",\"channel\":\"sms\",\"content\":\"later\",\"scheduledAt\":\"$AT\"}" | json 'd["id"]')
[[ $(curl -sS -X POST "$B/notifications/$SID/cancel" | json 'd["status"]') == "cancelled" ]] && ok "scheduled notification cancelled" || fail "cancel failed"
AGAIN=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$B/notifications/$SID/cancel")
[[ "$AGAIN" == "409" ]] && ok "second cancel returns 409" || fail "second cancel returned $AGAIN"

ITEMS=$(python3 -c 'import json;print(json.dumps({"notifications":[{"recipient":"+90555%07d"%i,"channel":"sms","content":"Batch %d"%i,"priority":["low","normal","high"][i%3]} for i in range(200)]}))')
BID=$(post /notifications/batch "$ITEMS" -H "Idempotency-Key: $RUN_ID-batch" | json 'd["batchId"]')
ok "batch of 200 accepted ($BID)"
for _ in $(seq 1 60); do
  DONE=$(curl -sS "$B/batches/$BID" | json 'd["completed"]')
  [[ "$DONE" == "True" ]] && break
  sleep 0.5
done
[[ "$DONE" == "True" ]] && ok "batch completed: $(curl -sS "$B/batches/$BID" | json 'd["counts"]')" || fail "batch did not complete"

PAGE=$(curl -sS "$B/notifications?batchId=$BID&limit=50" | json 'len(d["data"]), d["pagination"]["hasMore"]')
[[ "$PAGE" == "(50, True)" ]] && ok "pagination works (first page $PAGE)" || fail "pagination returned $PAGE"

curl -sS "$B/metrics?window=5m" | json 'd["totals"]' >/dev/null && ok "metrics endpoint: $(curl -sS "$B/metrics" | json 'd["totals"]')"
curl -sS "$API/metrics" | grep -q notification_api_http_requests_total && ok "prometheus metrics exposed"

echo
echo "All $pass checks passed."
