# neo4j-hot-backup

A Neo4j Community / DozerDB 5.26 plugin that backs up a running database without stopping it.
It writes a standard `.dump` file that `neo4j-admin database load` reads. Design:
[docs/superpowers/specs/2026-10-04-hot-backup-plugin-design.md](docs/superpowers/specs/2026-10-04-hot-backup-plugin-design.md).

## Install

1. Copy `neo4j-hot-backup-5.26.27.0.jar` into `/plugins`.
2. Set `server.backup.directory` in `neo4j.conf`, or `NEO4J_server_backup_directory` in Docker.
   Default: `backups` under the Neo4j home (`/var/lib/neo4j/backups` in Docker). The directory
   must exist and be writable by Neo4j.
3. Allow the procedures to use kernel internals: add `backup.*` to
   `dbms.security.procedures.unrestricted` (Docker: `NEO4J_dbms_security_procedures_unrestricted`,
   comma-separated with APOC's entries, e.g. `apoc.*,backup.*`). Without it Neo4j loads the jar but
   reports the procedures as unavailable.
4. Restart Neo4j.

## Procedures

Both are admin-only.

```
CALL backup.database(name :: STRING)
  YIELD database, path, bytes, millis, status

CALL backup.all()
  YIELD database, path, bytes, millis, status
```

- `database`: database name.
- `path`: the file written, `<dir>/<database>-<yyyyMMdd-HHmmss>.dump`.
- `bytes`: size of the file.
- `millis`: time taken.
- `status`: exactly one of `ok`, `skipped: not available`, `failed: <message>`. The last two
  have null `path`, `bytes` and `millis`.

`backup.all()` returns one row per database and never throws because one database failed or was
not started. The caller must check `status` on every row. `backup.database()` throws on a stopped,
unknown or failed database.

A second backup of the same database in the same second fails rather than overwrite the first.

## Restore

`neo4j-admin database load` looks for `<database>.dump` in `--from-path`, so rename or copy first:

```
cp backups/neo4j-20261004-120000.dump restore-in/neo4j.dump
neo4j-admin database load neo4j --from-path=restore-in --overwrite-destination=true
```

Stop the database (or the server) before loading over it.

Then start Neo4j on the loaded store. Recovery runs on that first start and replays the copied
transaction log. `neo4j-admin database check` works only after that: on a store that has not been
started it refuses with "Active logical log detected". So the order is load, start, stop, check.

## How consistency works

For each database:

1. Resolve the database and fail if it is not started.
2. Take the checkpoint mutex's store-copy lock with a checkpoint as the entry action. Every
   transaction committed so far is now in the store files, and no checkpoint or log pruning can
   run until the lock is released.
3. Archive the store files from the kernel's store-file listing, the same one Neo4j's own store
   copy uses. The listing pins the current Lucene index snapshots, so index files are not deleted
   or changed while they are copied.
4. Only after every store file is written, read the position of the last closed transaction, then
   archive the transaction log files oldest first. The live log is copied only up to that
   position, not to end of file: on Linux the log file is preallocated and its tail is zeros.
5. Release the lock, finish the archive, fsync.

Writes keep committing throughout. When the restored database first starts, recovery replays the
copied log from the checkpoint, which repairs any page that reached disk during the copy.
Transactions closed after the log position was read are not in the backup.

## Build and test

```
./mvnw test                  # unit tests and the embedded integration test
bash smoke/smoke-test.sh     # Docker: dozerdb 5.26.27.0, backup under load, load, start, count, check
```

The smoke test needs Docker and the `graphstack/dozerdb:5.26.27.0` image. It publishes no ports
and removes only the `nhb-*` containers it started.

## Versioning

The jar version is `5.26.27.0`, matching DozerDB. The plugin uses internal kernel APIs, so rebuild
and rerun the tests for every Neo4j minor version.
