#!/usr/bin/env bash
# Docker smoke test: hot backup on graphstack/dozerdb:5.26.27.0 under concurrent writes,
# load the dump into a fresh store, start it (recovery), count assertion, then consistency check.
set -euo pipefail

cd "$(dirname "$0")/.."

IMAGE=graphstack/dozerdb:5.26.27.0
RUN=smoke-$$
SRC=nhb-src-$RUN
RESTORED_C=nhb-restored-$RUN
WORK=$(mktemp -d)
mkdir -p "$WORK/backups" "$WORK/restore"
chmod 777 "$WORK/backups" "$WORK/restore" # mktemp -d is 0700; Neo4j runs as uid 7474 on Linux

on_exit() {
  local rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "--- smoke test failed (exit $rc), diagnostics ---" >&2
    for c in "$SRC" "$RESTORED_C"; do
      if docker inspect "$c" >/dev/null 2>&1; then
        echo "--- docker logs $c (tail 40) ---" >&2
        docker logs --tail 40 "$c" >&2 2>&1 || true
        echo "--- $c debug.log lines mentioning backup ---" >&2
        docker exec "$c" sh -c 'grep -i backup /logs/debug.log | tail -40' >&2 2>&1 || true
      fi
    done
    echo "--- work dir $WORK ---" >&2
    ls -la "$WORK/backups" >&2 2>&1 || true
  fi
  if [ -n "${WRITER:-}" ] && kill -0 "$WRITER" 2>/dev/null; then kill "$WRITER" 2>/dev/null || true; fi
  docker rm -f "$SRC" "$RESTORED_C" >/dev/null 2>&1 || true
}
trap on_exit EXIT

cs() { # cs <container> <query...>
  local c=$1; shift
  docker exec "$c" cypher-shell -u neo4j -p smoketest1 --format plain "$@"
}

wait_ready() { # wait_ready <container>
  local c=$1
  for _ in $(seq 1 120); do
    if cs "$c" "RETURN 1" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  echo "FAIL: $c not ready after 120 s" >&2
  return 1
}

fail() { echo "FAIL: $*" >&2; exit 1; }

echo "== build"
./mvnw -q package -DskipTests
JAR=target/neo4j-hot-backup-5.26.27.0.jar
[ -f "$JAR" ] || fail "$JAR not built"

echo "== start $SRC"
docker run -d --name "$SRC" \
  -e NEO4J_AUTH=neo4j/smoketest1 \
  -e NEO4J_server_backup_directory=/backups \
  -e NEO4J_dbms_security_procedures_unrestricted='backup.*' \
  -e NEO4J_server_memory_heap_max__size=1g \
  -e NEO4J_db_tx__log_rotation_size=128K \
  -v "$PWD/$JAR:/plugins/$(basename "$JAR"):ro" \
  -v "$WORK/backups:/backups" \
  "$IMAGE" >/dev/null
wait_ready "$SRC"

echo "== seed 1000 nodes"
cs "$SRC" "UNWIND range(1,1000) AS i CREATE (:Smoke {i:i})" >/dev/null

echo "== start background writer"
# Each call commits 20 auto-commit transactions of 50 nodes with a 1000-char property,
# so the log grows and rotates (128K) while the backup copies it.
PAYLOAD=$(head -c 1000 /dev/zero | tr '\0' x)
(
  for _ in $(seq 1 30); do
    cs "$SRC" "UNWIND range(1,20) AS b CALL { WITH b UNWIND range(1,50) AS i CREATE (:Smoke {i:i, p:'$PAYLOAD'}) } IN TRANSACTIONS OF 1 ROWS" >/dev/null
  done
) &
WRITER=$!
sleep 2
kill -0 "$WRITER" 2>/dev/null || fail "writer finished before the backup started"

echo "== backup.database('neo4j')"
PATH_IN_CONTAINER=$(cs "$SRC" "CALL backup.database('neo4j') YIELD path RETURN path" | tail -1 | tr -d '"')
echo "backup path: $PATH_IN_CONTAINER"
[ -n "$PATH_IN_CONTAINER" ] || fail "backup.database returned no path"

echo "== backup.all()"
ALL=$(cs "$SRC" "CALL backup.all() YIELD database, status RETURN database, status")
echo "$ALL"
echo "$ALL" | grep -Eq '^"neo4j", "ok"$' || fail "backup.all() has no ok row for neo4j"
echo "$ALL" | grep -Eq '^"system", "ok"$' || fail "backup.all() has no ok row for system"

echo "== wait for writer"
wait "$WRITER" || fail "background writer failed"
WRITER=
TOTAL=$(cs "$SRC" "MATCH (n:Smoke) RETURN count(n)" | tail -1)

echo "== load dump into fresh store"
DUMP="$WORK/backups/$(basename "$PATH_IN_CONTAINER")"
[ -f "$DUMP" ] || fail "dump not found on host: $DUMP"
cp "$DUMP" "$WORK/backups/neo4j.dump"
docker run --rm -v "$WORK/restore:/data" -v "$WORK/backups:/backups" "$IMAGE" \
  neo4j-admin database load neo4j --from-path=/backups --overwrite-destination=true \
  || fail "neo4j-admin database load failed"

echo "== start $RESTORED_C"
docker run -d --name "$RESTORED_C" \
  -e NEO4J_AUTH=neo4j/smoketest1 \
  -v "$WORK/restore:/data" \
  "$IMAGE" >/dev/null
wait_ready "$RESTORED_C"
RESTORED=$(cs "$RESTORED_C" "MATCH (n:Smoke) RETURN count(n)" | tail -1)

echo "== stop $RESTORED_C, then consistency check on the recovered store"
docker stop -t 120 "$RESTORED_C" >/dev/null
docker run --rm -v "$WORK/restore:/data" "$IMAGE" neo4j-admin database check neo4j \
  || fail "neo4j-admin database check failed"

echo "restored=$RESTORED total=$TOTAL"
[[ "$RESTORED" =~ ^[0-9]+$ && "$TOTAL" =~ ^[0-9]+$ ]] || fail "counts are not numbers"
[ "$RESTORED" -ge 1000 ] || fail "restored $RESTORED < 1000 seeded before the backup"
[ "$RESTORED" -le "$TOTAL" ] || fail "restored $RESTORED > total $TOTAL"
rm -rf "$WORK" || true
echo "SMOKE OK"
