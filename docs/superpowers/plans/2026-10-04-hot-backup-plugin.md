# Hot Backup Plugin Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A plugin jar adding `backup.database(name)` and `backup.all()` procedures that write a consistent, `neo4j-admin database load`-compatible `.dump` of a running Neo4j Community / DozerDB 5.26.27 database.

**Architecture:** `HotBackup` takes the kernel's `StoreCopyCheckPointMutex` with a checkpoint, streams store files then transaction logs through `DumpArchiveWriter`, which reproduces Neo4j's `Dumper` on-disk format with bounded copies. `BackupProcedures` is glue over `HotBackup`; `BackupSettings` declares the single `server.backup.directory` setting.

**Tech Stack:** Java 17 release (local JDK 21), Maven 3.9.9 via wrapper, `org.neo4j:neo4j:5.26.27` (provided), commons-compress and zstd (transitive, bundled in the server), JUnit 5, AssertJ, Docker for the smoke test.

**Spec:** `docs/superpowers/specs/2026-10-04-hot-backup-plugin-design.md`

## Global Constraints

- Neo4j version `5.26.27` everywhere; jar version `5.26.27.0`; property `neo4j.version` is the single source.
- Java `--release 17`. No dependency is shaded or bundled; everything the plugin uses is in `/var/lib/neo4j/lib` of `graphstack/dozerdb:5.26.27.0`.
- Package `com.phlow.neo4j.backup`. Procedures `backup.database`, `backup.all`. Setting `server.backup.directory`.
- Archive file name `<database>-<yyyyMMdd-HHmmss>.dump`, system default zone, opened with `CREATE_NEW`.
- Store files are fully written before the transaction log directory is listed. Never reorder.
- **No git operations.** Do not `git init`, add, or commit. Carlo tests first.
- Never kill processes by name. The smoke test removes only the containers it named.
- Tests: `./mvnw test` runs `*Test` and `*IT`. Run only the test you touched while iterating.

## Review Focus

1. Second backup of the same database within the same second collides on the file name and must fail clearly, never overwrite. Pinned in Task 3 (`HotBackupTest.secondRunInSameSecondFails`).
2. `server.backup.directory` that does not exist or is not writable must fail before the checkpoint, leave no file, and name the directory. Pinned in Task 3 (`HotBackupTest.missingDirectoryFailsBeforeTouchingDatabase`).
3. Transaction log rotation while the copy is running must still yield a recoverable dump. Pinned in Task 4 by forcing `db.tx_log.rotation.size` to its minimum under concurrent writes.
4. A file appended by another thread while being archived must produce a valid tar (exactly the sized bytes). Pinned in Task 2 (`DumpArchiveWriterTest.fileGrowingDuringCopyIsTruncatedToSizedLength`).
5. `backup.all()` on a DBMS where one database is not available must still back up the others and report `skipped: not available` for that one. Cannot be exercised in Community embedded tests (no STOP DATABASE); pinned by code review of Task 3 and by the smoke test on DozerDB in Task 5 (`backup.all()` row for `system` and `neo4j`).

---

### Task 1: Project skeleton, build, setting

**Files:**
- Create: `pom.xml`
- Create: `mvnw`, `mvnw.cmd`, `.mvn/wrapper/maven-wrapper.properties`
- Create: `.gitignore` (`target/`, `.idea/`, `*.iml`, `.DS_Store`)
- Create: `src/main/java/com/phlow/neo4j/backup/BackupSettings.java`
- Create: `src/main/resources/META-INF/services/org.neo4j.configuration.SettingsDeclaration`
- Test: `src/test/java/com/phlow/neo4j/backup/BackupSettingsTest.java`

**Interfaces:**
- Produces: `BackupSettings.backup_directory : Setting<Path>` named `server.backup.directory`, default `backups` resolved against `GraphDatabaseSettings.neo4j_home`.
- Produces: the build. `./mvnw -q package` yields `target/neo4j-hot-backup-5.26.27.0.jar`.

- [ ] **Step 1: Maven wrapper**

Download `https://repo1.maven.org/maven2/org/apache/maven/wrapper/maven-wrapper-distribution/3.3.2/maven-wrapper-distribution-3.3.2-only-script.zip`, extract `mvnw` and `mvnw.cmd` to the project root, `chmod +x mvnw`. Write `.mvn/wrapper/maven-wrapper.properties`:

```
wrapperVersion=3.3.2
distributionType=only-script
distributionUrl=https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/3.9.9/apache-maven-3.9.9-bin.zip
```

- [ ] **Step 2: `pom.xml`**

`groupId com.phlow.neo4j`, `artifactId neo4j-hot-backup`, `version 5.26.27.0`, `packaging jar`, `finalName ${project.artifactId}-${project.version}`. Properties: `neo4j.version=5.26.27`, `maven.compiler.release=17`, `project.build.sourceEncoding=UTF-8`.

Dependencies: `org.neo4j:neo4j:${neo4j.version}` scope `provided` (brings kernel, dbms, configuration, procedure-api, consistency-check, commons-compress, zstd). Test scope: `org.junit.jupiter:junit-jupiter:5.11.4`, `org.assertj:assertj-core:3.27.3`.

Plugins: `maven-compiler-plugin 3.13.0`; `maven-surefire-plugin 3.5.2` with includes `**/*Test.java` and `**/*IT.java`, and
`<argLine>--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED</argLine>` (Neo4j 5 on JDK 21 needs these).

- [ ] **Step 3: Failing test**

```java
class BackupSettingsTest {
    @Test void settingHasNameAndDefault() {
        assertThat(BackupSettings.backup_directory.name()).isEqualTo("server.backup.directory");
        Config config = Config.newBuilder()
                .set(GraphDatabaseSettings.neo4j_home, Path.of("/opt/neo4j"))
                .build();
        assertThat(config.get(BackupSettings.backup_directory)).isEqualTo(Path.of("/opt/neo4j/backups"));
    }
    @Test void settingIsDiscoveredByServiceLoader() {
        assertThat(ServiceLoader.load(SettingsDeclaration.class))
                .anyMatch(d -> d instanceof BackupSettings);
    }
}
```

- [ ] **Step 4: Run, expect compile failure**

Run: `./mvnw -q test -Dtest=BackupSettingsTest`
Expected: FAIL, `BackupSettings` cannot be resolved.

- [ ] **Step 5: Implement `BackupSettings implements SettingsDeclaration`**

```java
@Description("Directory where the backup.* procedures write .dump files")
public static final Setting<Path> backup_directory = SettingImpl.newBuilder("server.backup.directory",
        SettingValueParsers.PATH, Path.of("backups"))
        .setDependency(GraphDatabaseSettings.neo4j_home)
        .build();
```

Service file content: one line, `com.phlow.neo4j.backup.BackupSettings`. (Manual file, no annotation processor.)

- [ ] **Step 6: Run, expect pass**

Run: `./mvnw -q test -Dtest=BackupSettingsTest`
Expected: PASS. Then `./mvnw -q package -DskipTests` produces `target/neo4j-hot-backup-5.26.27.0.jar`.

---

### Task 2: DumpArchiveWriter

**Files:**
- Create: `src/main/java/com/phlow/neo4j/backup/DumpArchiveWriter.java`
- Test: `src/test/java/com/phlow/neo4j/backup/DumpArchiveWriterTest.java`

**Interfaces:**
- Consumes: Task 1 build only.
- Produces:

```java
public final class DumpArchiveWriter implements AutoCloseable {
    /** Opens archive with CREATE_NEW, wraps in StandardCompressionFormat.ZSTD.compress, writes
     *  DataOutputStream int 1, long expectedFiles, long expectedBytes, then opens a
     *  TarArchiveOutputStream with LONGFILE_POSIX and BIGNUMBER_POSIX. */
    public DumpArchiveWriter(Path archive, long expectedFiles, long expectedBytes) throws IOException;
    /** Walks root depth-first (Files.walkFileTree). Adds a directory entry for root ("./") and
     *  each sub-directory, and a file entry for each regular file not matching exclude. */
    public void addDirectoryTree(Path root, Predicate<Path> exclude) throws IOException;
    /** Adds one regular file as entry "./" + root.relativize(file). */
    public void addFile(Path root, Path file) throws IOException;
    public long filesWritten();
    public long bytesWritten();
    /** Closes tar entry stream, zstd stream, then fsyncs the archive file. */
    @Override public void close() throws IOException;
}
```

Entry rules (both methods): `TarArchiveEntry entry = new TarArchiveEntry(name)`; for files `long size = Files.size(file); entry.setSize(size); entry.setModTime(Files.getLastModifiedTime(file).toMillis())`, then copy **exactly `size` bytes** from a fresh `InputStream` (stop at `size` even if the file is longer; if the file is shorter, throw `IOException("File shrank while archiving: " + file)`). Directory names end with `/`.

- [ ] **Step 1: Failing tests**

```java
class DumpArchiveWriterTest {
    @TempDir Path tmp;

    @Test void roundTripsThroughNeo4jLoaderIntoStoreAndTransactionDirectories() throws Exception {
        // source tree: db/neostore (bytes "store"), db/schema/index/x (bytes "idx"),
        //              tx/neostore.transaction.db.0 (bytes "log0"), tx/checkpoint.0 (bytes "cp")
        // write: addDirectoryTree(db, p -> false); addFile(tx, tx/neostore.transaction.db.0); addFile(tx, tx/checkpoint.0)
        // load:  new Loader(new DefaultFileSystemAbstraction()).load(Neo4jLayout.of(tmp.resolve("home")).databaseLayout("neo4j"), archive)
        // assert layout.databaseDirectory()/neostore == "store", /schema/index/x == "idx"
        // assert layout.getTransactionLogsDirectory()/neostore.transaction.db.0 == "log0", /checkpoint.0 == "cp"
        // assert writer.filesWritten() == 4 and bytesWritten() == 5+3+4+2
    }

    @Test void excludedFilesAreNotArchived() throws Exception {
        // db/database_lock and db/neostore; exclude p -> p.getFileName().toString().equals("database_lock")
        // after Loader.load the lock file is absent, neostore present
    }

    @Test void fileGrowingDuringCopyIsTruncatedToSizedLength() throws Exception {
        // 4 MiB file; start a thread appending 64 KiB chunks in a loop until a flag flips
        // addFile(root, file); flip flag; join
        // Loader.load succeeds; loaded file length is between 4 MiB and the final source length,
        // and equals the size the writer recorded (expose via bytesWritten())
    }

    @Test void existingArchiveIsNeverOverwritten() throws Exception {
        // Files.writeString(archive, "x"); assertThatThrownBy(() -> new DumpArchiveWriter(archive, 0, 0))
        //     .isInstanceOf(FileAlreadyExistsException.class)
    }

    @Test void archiveIsReadableByZstdFormatDetection() throws Exception {
        // write an empty tree; open with DumpFormatSelector.decompress(() -> Files.newInputStream(archive))
        // assert StandardCompressionFormat.ZSTD.isFormat(stream)
    }
}
```

- [ ] **Step 2: Run, expect compile failure**

Run: `./mvnw -q test -Dtest=DumpArchiveWriterTest`
Expected: FAIL, class not found.

- [ ] **Step 3: Implement `DumpArchiveWriter`**

Bounded copy: 64 KiB buffer, `in.readNBytes` / loop until `remaining == 0`. fsync in `close()`: keep the `FileChannel` from `FileChannel.open(archive, CREATE_NEW, WRITE)` and call `force(true)` after closing the compression stream (wrap it with `Channels.newOutputStream` and do not let the zstd stream close the channel before `force`; closing order: tar `finish()`, zstd `close()`, channel `force(true)`, channel `close()`).

- [ ] **Step 4: Run, expect pass**

Run: `./mvnw -q test -Dtest=DumpArchiveWriterTest`
Expected: PASS, 5 tests.

---

### Task 3: HotBackup and BackupProcedures

**Files:**
- Create: `src/main/java/com/phlow/neo4j/backup/HotBackup.java`
- Create: `src/main/java/com/phlow/neo4j/backup/BackupProcedures.java`
- Test: `src/test/java/com/phlow/neo4j/backup/HotBackupTest.java`

**Interfaces:**
- Consumes: `DumpArchiveWriter` (Task 2 signatures), `BackupSettings.backup_directory` (Task 1).
- Produces:

```java
public final class HotBackup {
    public record Result(String database, Path path, long bytes, long millis, long checkpointTransactionId) {}
    public HotBackup(Path backupDirectory, Clock clock, InternalLog log);
    /** Steps 1 to 5 of the spec for one started database. */
    public Result run(GraphDatabaseAPI target) throws IOException;
    static String fileName(String database, Instant now, ZoneId zone); // "<db>-<yyyyMMdd-HHmmss>.dump"
}

public class BackupProcedures {
    @Context public GraphDatabaseAPI graph;
    @Context public Log log;
    public static final class Output {
        public String database; public String path; public Long bytes; public Long millis; public String status;
    }
    @Admin @Procedure(name = "backup.database", mode = Mode.DBMS)
    @Description("Consistent online backup of one database into server.backup.directory as a .dump file")
    public Stream<Output> database(@Name("name") String name);
    @Admin @Procedure(name = "backup.all", mode = Mode.DBMS)
    @Description("Consistent online backup of every database into server.backup.directory, one .dump each")
    public Stream<Output> all();
}
```

`HotBackup.run` algorithm (the order is the correctness argument, keep it):

1. `if (!Files.isDirectory(backupDirectory) || !Files.isWritable(backupDirectory)) throw new IOException("server.backup.directory is not a writable directory: " + backupDirectory)` — before touching the database.
2. `DependencyResolver deps = target.getDependencyResolver()`; resolve `Database`, `CheckPointer`, `DatabaseLayout`. `Path dbDir = layout.databaseDirectory(); Path txDir = layout.getTransactionLogsDirectory()`.
3. `Path archive = backupDirectory.resolve(fileName(layout.getDatabaseName(), clock.instant(), ZoneId.systemDefault()))`.
4. `StoreCopyCheckPointMutex mutex = database.getStoreCopyCheckPointMutex()`; `long[] txId = new long[1]`;
   `try (Resource lock = mutex.storeCopy(() -> txId[0] = checkPointer.tryCheckPoint(new SimpleTriggerInfo("hot backup")))) { ... }`.
5. Inside the lock: expected counts = files and bytes from a one-off walk of `dbDir` plus a listing of `txDir` (progress metadata only). `try (DumpArchiveWriter w = new DumpArchiveWriter(archive, files, bytes))`:
   - `w.addDirectoryTree(dbDir, exclude)` where exclude matches `layout.databaseLockFile().getFileName()`, `layout.quarantineFile().getFileName()`, and `txDir` itself when `txDir.startsWith(dbDir)`.
   - log "store files written".
   - **Then** list `txDir` (`Files.list`, regular files only), sort by `TransactionLogFilesHelper`-style version: name's trailing `.N` numeric suffix ascending, `checkpoint.*` files before `neostore.transaction.db.*` files; `w.addFile(txDir, f)` each.
6. On any exception: `Files.deleteIfExists(archive)`, rethrow. The `try-with-resources` releases the mutex regardless.
7. Return `Result(name, archive, Files.size(archive), elapsed, txId[0])`. Log one line per step as the spec lists.

`BackupProcedures`:
- `dbms = graph.getDependencyResolver().resolveDependency(DatabaseManagementService.class)`; `config = ...resolveDependency(Config.class)`; `dir = config.get(BackupSettings.backup_directory)`; `HotBackup hot = new HotBackup(dir, Clock.systemDefaultZone(), internalLog)` where `internalLog` is `graph.getDependencyResolver().resolveDependency(LogService.class).getInternalLog(BackupProcedures.class)`.
- `database(name)`: `GraphDatabaseAPI target = (GraphDatabaseAPI) dbms.database(name)` (propagates `DatabaseNotFoundException`); `if (!target.isAvailable(0)) throw new IllegalStateException("Database '" + name + "' is not available")`; map `Result` to `Output` with `status = "ok"`; wrap `IOException` in `RuntimeException(e.getMessage(), e)`.
- `all()`: for each name in `dbms.listDatabases()` (sorted), if not available emit `Output{database=name, path=null, bytes=null, millis=null, status="skipped: not available"}`, else as above. A failure in one database propagates (does not continue silently).

- [ ] **Step 1: Failing tests** (embedded DBMS, no procedure registration needed for `HotBackup`; register for the procedure tests)

```java
class HotBackupTest {
    @TempDir Path home; @TempDir Path backups;
    DatabaseManagementService dbms;
    // @BeforeEach: dbms = new DatabaseManagementServiceBuilder(home).setConfig(BackupSettings.backup_directory, backups).build();
    //              ((GraphDatabaseAPI) dbms.database("neo4j")).getDependencyResolver()
    //                  .resolveDependency(GlobalProcedures.class).registerProcedure(BackupProcedures.class);
    // @AfterEach: dbms.shutdown()

    @Test void fileNameUsesDatabaseAndTimestamp() {
        assertThat(HotBackup.fileName("neo4j", Instant.parse("2026-10-04T13:05:09Z"), ZoneOffset.UTC))
                .isEqualTo("neo4j-20261004-130509.dump");
    }

    @Test void runWritesLoadableDumpWithCommittedData() throws Exception {
        // create 100 nodes (:T); Result r = new HotBackup(backups, Clock.systemUTC(), NullLog).run(api)
        // assert r.path() exists under backups, r.bytes() > 0, r.checkpointTransactionId() > 0
        // Loader.load into Neo4jLayout.of(restoreHome).databaseLayout("neo4j"); start DBMS on restoreHome;
        // count (:T) == 100; shutdown
    }

    @Test void secondRunInSameSecondFails() throws Exception {
        // Clock fixed = Clock.fixed(Instant.parse("2026-10-04T13:05:09Z"), ZoneOffset.UTC)
        // first run ok; second run: assertThatThrownBy(...).isInstanceOf(FileAlreadyExistsException.class)
        //   .hasMessageContaining("neo4j-20261004-130509.dump")
        // only one file in backups
    }

    @Test void missingDirectoryFailsBeforeTouchingDatabase() throws Exception {
        // new HotBackup(backups.resolve("nope"), ...).run(api) -> IOException with message containing "nope"
        // backups.resolve("nope") does not exist afterwards
    }

    @Test void procedureDatabaseReturnsOkRow() {
        // CALL backup.database('neo4j') -> one row, status "ok", path starts with backups.toString(), bytes > 0
    }

    @Test void procedureDatabaseUnknownThrows() {
        // CALL backup.database('nope') -> exception whose message contains "nope"
    }

    @Test void procedureAllReturnsRowsForEveryDatabase() {
        // CALL backup.all() -> rows for "neo4j" and "system", both status "ok", two files in backups
    }
}
```

Run queries through `dbms.database("neo4j").executeTransactionally("CALL backup.database($n)", Map.of("n","neo4j"), r -> r.stream().toList())`.

- [ ] **Step 2: Run, expect compile failure**

Run: `./mvnw -q test -Dtest=HotBackupTest`

- [ ] **Step 3: Implement `HotBackup`, then `BackupProcedures`** per the algorithm above.

- [ ] **Step 4: Run, expect pass**

Run: `./mvnw -q test -Dtest=HotBackupTest`
Expected: PASS, 7 tests. `./mvnw -q test -Dtest='BackupSettingsTest,DumpArchiveWriterTest,HotBackupTest'` also green.

---

### Task 4: HotBackupIT, concurrent writes and consistency

**Files:**
- Test: `src/test/java/com/phlow/neo4j/backup/HotBackupIT.java`

**Interfaces:**
- Consumes: `BackupProcedures` registered via `GlobalProcedures.registerProcedure`; `BackupSettings.backup_directory`.

- [ ] **Step 1: Write the test**

```java
class HotBackupIT {
    @TempDir Path home; @TempDir Path backups; @TempDir Path restoreHome;

    @Test void backupUnderConcurrentWritesRestoresConsistentlyWithinBounds() throws Exception {
        // DBMS: setConfig(BackupSettings.backup_directory, backups)
        //       setConfig(GraphDatabaseSettings.logical_log_rotation_threshold, ByteUnit.kibiBytes(128))  // force rotations
        //       setConfig(GraphDatabaseSettings.check_point_interval_time, Duration.ofMillis(100))       // make the mutex matter
        // register BackupProcedures
        // 4 writer threads: loop { tx: CREATE (:W {p: <1 KiB string>}); commit; committed.incrementAndGet() } until stop flag
        // let them run 500 ms; long lower = committed.get();
        // CALL backup.database('neo4j') YIELD path
        // long upper = committed.get(); stop flag; join; dbms.shutdown()
        // Loader.load(Neo4jLayout.of(restoreHome).databaseLayout("neo4j"), Path.of(path))
        // DatabaseManagementService restored = new DatabaseManagementServiceBuilder(restoreHome).build();  // recovery runs
        // long count = MATCH (n:W) RETURN count(n); restored.shutdown()
        // assertThat(count).isBetween(lower, upper);
        // assertThat(upper).isGreaterThan(lower)  // proves writes happened during the backup
        // ConsistencyCheckService.Result cc = new ConsistencyCheckService(Neo4jLayout.of(restoreHome).databaseLayout("neo4j")).runFullConsistencyCheck();
        // assertThat(cc.isSuccessful()).withFailMessage(() -> "see " + cc.reportFile()).isTrue();
    }
}
```

If `upper == lower` on a fast machine, lengthen the writer warm-up or increase the payload until a backup takes long enough for the assertion to hold; do not weaken the assertion.

- [ ] **Step 2: Run**

Run: `./mvnw -q test -Dtest=HotBackupIT`
Expected: PASS. Note the count bounds and duration in the hand-back.

---

### Task 5: Docker smoke test and README

**Files:**
- Create: `smoke/smoke-test.sh`
- Create: `README.md`

**Interfaces:**
- Consumes: `target/neo4j-hot-backup-5.26.27.0.jar` from `./mvnw -q package -DskipTests`; procedures and setting names from Global Constraints.

- [ ] **Step 1: `smoke/smoke-test.sh`** (`set -euo pipefail`, bash)

Names: `RUN=smoke-$$`, containers `nhb-src-$RUN` and `nhb-restored-$RUN`, image `graphstack/dozerdb:5.26.27.0`, work dir `WORK=$(mktemp -d)` with `backups/` and `restore/`. `trap 'docker rm -f nhb-src-$RUN nhb-restored-$RUN >/dev/null 2>&1 || true' EXIT`. No published ports; every query via `docker exec <c> cypher-shell -u neo4j -p smoketest1 ...`.

1. `./mvnw -q package -DskipTests`; `JAR=target/neo4j-hot-backup-5.26.27.0.jar`.
2. `docker run -d --name nhb-src-$RUN -e NEO4J_AUTH=neo4j/smoketest1 -e NEO4J_server_backup_directory=/backups -e NEO4J_server_memory_heap_max__size=1g -v "$PWD/$JAR:/plugins/$(basename $JAR):ro" -v "$WORK/backups:/backups" graphstack/dozerdb:5.26.27.0`. Wait until `cypher-shell "RETURN 1"` succeeds (max 120 s).
3. `UNWIND range(1,1000) AS i CREATE (:Smoke {i:i})`.
4. Background writer: `for i in $(seq 1 200); do cypher-shell "UNWIND range(1,50) AS i CREATE (:Smoke {i:i})"; done &` and record its PID.
5. `PATH_IN_CONTAINER=$(cypher-shell --format plain "CALL backup.database('neo4j') YIELD path RETURN path" | tail -1 | tr -d '"')`; also `CALL backup.all() YIELD database, status` must list `neo4j` and `system` with `ok`.
6. Wait for the writer PID; `TOTAL=$(cypher-shell "MATCH (n:Smoke) RETURN count(n)")`.
7. `cp "$WORK/backups/$(basename $PATH_IN_CONTAINER)" "$WORK/backups/neo4j.dump"`.
8. `docker run --rm -v "$WORK/restore:/data" -v "$WORK/backups:/backups" graphstack/dozerdb:5.26.27.0 neo4j-admin database load neo4j --from-path=/backups --overwrite-destination=true`.
9. `docker run --rm -v "$WORK/restore:/data" graphstack/dozerdb:5.26.27.0 neo4j-admin database check neo4j` must exit 0.
10. `docker run -d --name nhb-restored-$RUN -e NEO4J_AUTH=neo4j/smoketest1 -v "$WORK/restore:/data" graphstack/dozerdb:5.26.27.0`; wait; `RESTORED=$(MATCH (n:Smoke) RETURN count(n))`.
11. Assert `1000 <= RESTORED <= TOTAL`; print both; exit 1 on failure.

- [ ] **Step 2: Run it**

Run: `bash smoke/smoke-test.sh`
Expected: last lines `restored=<n> total=<m>` and `SMOKE OK`; `docker ps -a --filter name=nhb-` empty afterwards.

- [ ] **Step 3: `README.md`**

Sections, in this order, short: What it is (two sentences, link to the spec); Install (copy jar to `/plugins`, set `server.backup.directory` or `NEO4J_server_backup_directory`, restart); Procedures (the two signatures and the output columns); Restore (`cp` to `<db>.dump`, `neo4j-admin database load`); How consistency works (the five steps from the spec); Build and test (`./mvnw test`, `bash smoke/smoke-test.sh`); Versioning (`5.26.27.0`, rebuild per Neo4j minor).
