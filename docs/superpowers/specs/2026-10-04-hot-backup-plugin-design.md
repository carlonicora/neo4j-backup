# Hot backup plugin for Neo4j Community / DozerDB 5.26 — design

Date: 2026-10-04
Status: draft for review

## Goal

A plugin jar, dropped into `/plugins` next to APOC, that adds admin-only procedures producing a
consistent backup of a **running** database with no downtime. The output is a standard Neo4j
`.dump` file, byte-compatible with `neo4j-admin database load`, so the existing restore flow in
`~/Development/environment/neo4j/backup/restore.sh` works unchanged.

Success criteria

- Writes keep committing while the backup runs.
- The restored copy starts cleanly, passes `neo4j-admin database check`, and contains every
  transaction committed before the procedure was called.
- Works against the image in production today: `graphstack/dozerdb:5.26.27.0`.

Out of scope (propose later, do not build now): differential backups, consistency check inside
the procedure, network transport, compression choice, encryption.

## Decisions taken with Carlo

| # | Decision |
|---|----------|
| 1 | One setting, `server.backup.directory`. The procedure writes `<dir>/<database>-<yyyyMMdd-HHmmss>.dump` and returns the path. It never writes anywhere else. |
| 2 | Two procedures: `backup.database(name)` for one database, `backup.all()` looping over every database the DBMS knows, system included. |
| 3 | Verification: JUnit integration test with an embedded Neo4j under concurrent writes, restore via the kernel `Loader`, consistency check, count assertions. Plus a Docker smoke test against `graphstack/dozerdb:5.26.27.0`. |

## Why it is consistent

Enterprise online backup is closed source, but the Community kernel ships the primitives it is
built on, and they are in the jars inside the production image:

| Class (package `org.neo4j...`) | Role |
|---|---|
| `kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex` | Read lock for store copy, write lock for checkpoints. `storeCopy(before)` runs `before` once, then blocks checkpoints until the returned `Resource` is closed. |
| `kernel.impl.transaction.log.checkpoint.CheckPointer` | `tryCheckPoint(TriggerInfo)` flushes the page cache and writes a checkpoint entry. |
| `kernel.database.Database` | `getStoreCopyCheckPointMutex()`, `getDatabaseLayout()`. Resolved from the target database's dependency resolver. |
| `kernel.impl.transaction.log.files.LogFiles` | Lists transaction log and checkpoint files. |
| `dbms.archive.StandardCompressionFormat.ZSTD` | The exact compressed stream `neo4j-admin database dump` writes. |
| `dbms.archive.Loader` | Used in tests to prove the archive loads. |

Algorithm, for one database:

1. Resolve the database through `DatabaseManagementService.database(name)`. Fail if it is not
   started.
2. `mutex.storeCopy(() -> checkPointer.tryCheckPoint(new SimpleTriggerInfo("hot backup")))`.
   This is verbatim what Neo4j 3.5's open-source store copy did. After it returns, every
   committed transaction up to the checkpoint is in the store files, and no checkpoint (and so no
   log pruning) can run until we release.
3. Archive the store files from the kernel's store-file listing (the one Neo4j's own store copy
   uses), not a directory walk. The listing pins the current Lucene index snapshots, so index
   files cannot be deleted or rewritten while they are copied.
4. **Only after** every store file is written, read the log position of the last closed
   transaction, then archive the transaction log files in ascending version order. Rotated files
   are copied whole; the live file is copied only up to that position. On Linux the live log is
   preallocated, so its end of file is a tail of zeros, not the end of the data.
5. Release the mutex, finish the archive, fsync.

Why the order matters. A page can reach disk mid-copy through page-cache eviction. Recovery on
load repairs that by replaying the log from the checkpoint, but only if every transaction whose
page might be in the copy is also in the copied log. Finishing the store copy before sizing the
log guarantees that. Transactions closed after the bound is read are simply not in the backup;
their pages cannot be in a copy that finished earlier. Bounding at the last closed transaction
means the copied log ends on a whole transaction. Recovery runs when the restored database first
starts, so `neo4j-admin database check` is only meaningful after that start.

## Why not reuse `Dumper`

`Dumper` creates each tar header from the file's size, then copies to end-of-file. On a
transaction log being appended, the file grows between those two reads and
`TarArchiveOutputStream` throws "request to write bytes exceeds size in header". It also walks
the log directory before writing the store files, which breaks the ordering rule above. So the
plugin has its own writer that reproduces `Dumper`'s on-disk format exactly:

1. `StandardCompressionFormat.ZSTD.compress(out)` (writes the `zstd` marker inside the stream,
   checksum on, multi-threaded).
2. `DataOutputStream`: `int 1`, `long fileCount`, `long byteCount` (Loader reads these for
   progress only).
3. `TarArchiveOutputStream` with `LONGFILE_POSIX` and `BIGNUMBER_POSIX`; entries named
   `./<path relative to its root>`; directory entries included.
4. For each file: take its length (the file size for store files and rotated logs, the last
   closed transaction's position for the live log), set it on the entry, copy **exactly** that
   many bytes.

`Loader` routes entries by file name (`TransactionLogFiles.DEFAULT_FILENAME_FILTER`), so log
files land in the transaction directory on load regardless of where they sat in the archive.

## Components

```
neo4j-backup/
  pom.xml                       Java 17 release, Neo4j 5.26.27 provided, shade not needed
  mvnw, .mvn/wrapper/           Apache Maven wrapper (no local mvn on this machine)
  src/main/java/com/phlow/neo4j/backup/
    BackupSettings.java         SettingsDeclaration: server.backup.directory (PATH, default <neo4j.home>/backups)
    BackupProcedures.java       @Admin, Mode.DBMS: backup.database(name), backup.all()
    HotBackup.java              Steps 1 to 5 above for one database; returns a BackupResult
    DumpArchiveWriter.java      The Dumper-compatible writer; knows nothing about Neo4j kernels
  src/test/java/com/phlow/neo4j/backup/
    DumpArchiveWriterTest.java  Round trip through Loader; appended-while-copying file case
    HotBackupIT.java            Embedded DB, concurrent writers, restore, consistency check
  smoke/
    smoke-test.sh               Docker: dozerdb 5.26.27.0 + jar, cypher-shell call, load into a
                                second container, count + neo4j-admin database check
  README.md                     Install, setting, procedures, restore
```

`DumpArchiveWriter` depends only on commons-compress and the Neo4j `StandardCompressionFormat`.
`HotBackup` depends on the kernel classes in the table. `BackupProcedures` is glue: argument
checks, result rows, logging.

### Procedure signatures

```
CALL backup.database(name :: STRING)
  YIELD database, path, bytes, millis, status
CALL backup.all()
  YIELD database, path, bytes, millis, status
```

`status` is exactly one of `ok`, `skipped: not available`, `failed: <message>`; the last two
have null `path`, `bytes` and `millis`. `backup.all()` returns one row per database and never
throws because of one database, so the caller must check `status`. `backup.database()` on a
stopped, unknown or failing database throws.

### Setting

`server.backup.directory`, type PATH, default `backups` resolved against `server.directories.neo4j_home`
(so `/var/lib/neo4j/backups` in Docker). Declared via `SettingsDeclaration` + `@ServiceProvider`,
the same mechanism APOC uses. Must exist and be writable, checked at call time, not boot.

## Error handling

- Target file opened with `CREATE_NEW`; a collision fails rather than overwrites.
- Any `IOException` during the copy: mutex released, partial archive deleted, exception
  rethrown as a procedure failure with the path in the message.
- Mutex release is in `finally`. A failed `before` action (checkpoint) propagates from
  `storeCopy` before any file is touched.
- Procedures are `@Admin`; non-admins get the standard authorization error.

## Logging

Through the Neo4j internal log (`@Context Log` on the procedure, passed down): start with
database and target path, checkpoint done with the append index, log bound (version and offset), store copy done with file
count and bytes, log copy done, total duration. One line per step, no per-file lines.

## Testing

`DumpArchiveWriterTest`

- Writes a directory tree, loads it with `Loader` into a fresh `DatabaseLayout`, asserts
  byte-equal files in the right destinations (store dir vs transaction dir by file name).
- A file that is appended by another thread while being archived: archive succeeds and contains
  exactly the bytes present at sizing time.

`HotBackupIT` (embedded `DatabaseManagementServiceBuilder`, Neo4j 5.26.27 test jars)

1. Start a DBMS with the procedure class registered and `server.backup.directory` set to a temp dir.
2. Start writer threads committing small transactions in a loop; record the count committed
   before the backup call starts.
3. `CALL backup.database('neo4j')`.
4. Stop writers; record the count committed before the call returned.
5. `Loader.load` into a fresh layout; start a second DBMS on it (recovery runs here).
6. Assert node count is between the two recorded counts.
7. `ConsistencyCheckService(layout).runFullConsistencyCheck()` is successful.
8. `backup.all()` returns a row for `neo4j` and `system` with `status = ok`.

`smoke/smoke-test.sh`

- Build the jar, run `graphstack/dozerdb:5.26.27.0` with the jar mounted in `/plugins` and
  `NEO4J_server_backup_directory=/backups`, create nodes with `cypher-shell`, call the procedure, copy
  the dump out, `neo4j-admin database load`, start a second container on it (recovery runs
  here), assert the count, stop it, then run `neo4j-admin database check`. Exit non-zero on any
  mismatch.

## Rollout into the existing deployment (not built here, for later)

- Mount `./data-backup:/var/lib/neo4j/backups` on the `neo4j` service, or set
  `NEO4J_server_backup_directory`.
- Add `backup.*` to `NEO4J_dbms_security_procedures_unrestricted` (today `gds.*,apoc.*`). The
  procedures touch kernel internals, so Neo4j refuses to load them sandboxed, exactly like APOC.
- `backup.sh` stops stopping the container: it runs
  `cypher-shell "CALL backup.all()"`, uploads each returned path as `<date>/<db>.dump`, deletes
  the local file. Any row whose `status` is not `ok` fails the job, since `backup.all()` itself
  does not throw for one database.
- `restore.sh` loads as today; the database must be started once (recovery) before any
  `neo4j-admin database check`.
- Bake the jar into `neo4j-image/Dockerfile` next to OpenGDS, for the same Coolify reason.

## Risks

- Internal kernel APIs. Each Neo4j minor needs a rebuild and a run of the IT. Version the jar
  `5.26.27.0` like DozerDB does.
- Checkpoints are blocked for the duration of the copy (minutes on 7.5 GB). Enterprise does the
  same. Transaction logs grow meanwhile; default retention is 2 days, so no pruning risk.
- Disk: the archive is written on the server, so the backup directory needs room for one
  compressed copy per database.
