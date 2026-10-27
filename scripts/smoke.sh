#!/usr/bin/env bash
# Smoke test for the running docker compose stack: trigger the nightly job over
# GraphQL, then wait until the Kafka-fed projection reports it succeeded.
set -euo pipefail
API="${API:-http://localhost:8080/graphql}"

gql() {
  curl -sf "$API" -H 'content-type: application/json' \
    -d "$(python3 -c 'import json,sys; print(json.dumps({"query": sys.argv[1]}))' "$1")"
}

key="smoke-$(date +%s)"
resp=$(gql "mutation { triggerRun(job: \"jaffle_nightly\", idempotencyKey: \"$key\") { created run { id } } }")
echo "$resp"
run_id=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["data"]["triggerRun"]["run"]["id"])' <<<"$resp")

for _ in $(seq 1 120); do
  out=$(gql "{ run(id: \"$run_id\") { status history { status nodeExecutions nodeFailures } steps { name status } } }")
  hist=$(python3 -c 'import json,sys; h=json.load(sys.stdin)["data"]["run"]["history"]; print(h["status"] if h else "none")' <<<"$out")
  if [ "$hist" = "succeeded" ]; then
    echo "$out"
    exit 0
  fi
  if [ "$hist" = "failed" ] || [ "$hist" = "cancelled" ]; then
    echo "run ended $hist: $out" >&2
    exit 1
  fi
  sleep 1
done
echo "timed out waiting for run $run_id: $out" >&2
exit 1
