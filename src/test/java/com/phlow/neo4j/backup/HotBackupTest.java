package com.phlow.neo4j.backup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.dbms.archive.DumpFormatSelector;
import org.neo4j.dbms.archive.Loader;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLog;

class HotBackupTest {
    @TempDir Path home;
    @TempDir Path backups;
    @TempDir Path restoreHome;

    DatabaseManagementService dbms;
    GraphDatabaseAPI api;

    @BeforeEach
    void start() throws Exception {
        dbms = new DatabaseManagementServiceBuilder(home)
                .setConfig(BackupSettings.backup_directory, backups)
                .build();
        api = (GraphDatabaseAPI) dbms.database("neo4j");
        api.getDependencyResolver().resolveDependency(GlobalProcedures.class).registerProcedure(BackupProcedures.class);
    }

    @AfterEach
    void stop() {
        if (dbms != null) {
            dbms.shutdown();
        }
    }

    @Test
    void fileNameUsesDatabaseAndTimestamp() {
        assertThat(HotBackup.fileName("neo4j", Instant.parse("2026-10-04T13:05:09Z"), ZoneOffset.UTC))
                .isEqualTo("neo4j-20261004-130509.dump");
    }

    @Test
    void runWritesLoadableDumpWithCommittedData() throws Exception {
        api.executeTransactionally("UNWIND range(1, 100) AS i CREATE (:T {i: i})");

        HotBackup.Result r = new HotBackup(backups, Clock.systemUTC(), NullLog.getInstance()).run(api);

        assertThat(r.path()).exists();
        assertThat(r.path().getParent()).isEqualTo(backups);
        assertThat(r.bytes()).isGreaterThan(0);
        assertThat(r.checkpointAppendIndex()).isGreaterThan(0);

        DatabaseLayout restoreLayout = Neo4jLayout.of(restoreHome).databaseLayout("neo4j");
        // neo4j-admin creates these parents before loading; the Loader itself requires them.
        Files.createDirectories(restoreLayout.databaseDirectory().getParent());
        Files.createDirectories(restoreLayout.getTransactionLogsDirectory().getParent());
        try (DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction()) {
            new Loader(fs).load(restoreLayout, r.path());
        }
        DatabaseManagementService restored = new DatabaseManagementServiceBuilder(restoreHome).build();
        try {
            long count = restored.database("neo4j")
                    .executeTransactionally("MATCH (n:T) RETURN count(n) AS c", Map.of(),
                            res -> (Long) res.next().get("c"));
            assertThat(count).isEqualTo(100);
        } finally {
            restored.shutdown();
        }
    }

    @Test
    void secondRunInSameSecondFails() throws Exception {
        Clock fixed = Clock.fixed(Instant.parse("2026-10-04T13:05:09Z"), ZoneOffset.UTC);
        HotBackup hot = new HotBackup(backups, fixed, NullLog.getInstance());
        HotBackup.Result first = hot.run(api);
        long firstSize = Files.size(first.path());

        assertThatThrownBy(() -> hot.run(api))
                .isInstanceOf(FileAlreadyExistsException.class)
                .hasMessageContaining(first.path().getFileName().toString());

        try (Stream<Path> files = Files.list(backups)) {
            assertThat(files.toList()).containsExactly(first.path());
        }
        assertThat(Files.size(first.path())).isEqualTo(firstSize);
    }

    @Test
    void missingDirectoryFailsBeforeTouchingDatabase() {
        Path missing = backups.resolve("nope");

        assertThatThrownBy(() -> new HotBackup(missing, Clock.systemUTC(), NullLog.getInstance()).run(api))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("nope");

        assertThat(missing).doesNotExist();
    }

    @Test
    void procedureDatabaseReturnsOkRow() {
        List<Map<String, Object>> rows = api.executeTransactionally(
                "CALL backup.database($n)", Map.of("n", "neo4j"), r -> r.stream().toList());

        assertThat(rows).hasSize(1);
        Map<String, Object> row = rows.get(0);
        assertThat(row.get("database")).isEqualTo("neo4j");
        assertThat(row.get("status")).isEqualTo("ok");
        assertThat((String) row.get("path")).startsWith(backups.toString());
        assertThat((Long) row.get("bytes")).isGreaterThan(0);
    }

    @Test
    void procedureDatabaseUnknownThrows() {
        assertThatThrownBy(() -> api.executeTransactionally(
                        "CALL backup.database($n)", Map.of("n", "nope"), r -> r.stream().toList()))
                .hasMessageContaining("nope");
    }

    @Test
    void procedureAllReturnsRowsForEveryDatabase() throws Exception {
        List<Map<String, Object>> rows = api.executeTransactionally(
                "CALL backup.all()", Map.of(), r -> r.stream().toList());

        assertThat(rows).extracting(row -> row.get("database")).containsExactlyInAnyOrder("neo4j", "system");
        assertThat(rows).allSatisfy(row -> assertThat(row.get("status")).isEqualTo("ok"));
        try (Stream<Path> files = Files.list(backups)) {
            assertThat(files.toList()).hasSize(2);
        }
    }

    @Test
    void backupWithFulltextIndexUnderWritesRestoresIndex() throws Exception {
        api.executeTransactionally("CREATE FULLTEXT INDEX ft FOR (n:Doc) ON EACH [n.text]");
        api.executeTransactionally("CALL db.awaitIndexes(60)");

        HotBackup.Result r;
        try (Writers writers = new Writers(2, "CREATE (:Doc {text: 'hello world ' + $i})")) {
            writers.awaitCommits(200);
            r = new HotBackup(backups, Clock.systemUTC(), NullLog.getInstance()).run(api);
        }

        DatabaseManagementService restored = new DatabaseManagementServiceBuilder(restore(r.path())).build();
        try {
            GraphDatabaseService db = restored.database("neo4j");
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            String state = null;
            while (System.nanoTime() < deadline) {
                state = db.executeTransactionally(
                        "SHOW INDEXES YIELD name, state WHERE name = 'ft' RETURN state", Map.of(),
                        res -> res.hasNext() ? (String) res.next().get("state") : null);
                if ("ONLINE".equals(state)) {
                    break;
                }
                Thread.sleep(100);
            }
            assertThat(state).isEqualTo("ONLINE");
            long hits = db.executeTransactionally(
                    "CALL db.index.fulltext.queryNodes('ft', 'hello') YIELD node RETURN count(node) AS c", Map.of(),
                    res -> (Long) res.next().get("c"));
            assertThat(hits).isGreaterThanOrEqualTo(1);
        } finally {
            restored.shutdown();
        }
    }

    @Test
    void liveLogIsBoundedByLastClosedPosition() throws Exception {
        HotBackup.Result r;
        try (Writers writers = new Writers(2, "CREATE (:W {i: $i, p: 'x'})")) {
            writers.awaitCommits(200);
            r = new HotBackup(backups, Clock.systemUTC(), NullLog.getInstance()).run(api);
        }

        String prefix = "neostore.transaction.db.";
        long highestVersion = -1;
        long highestSize = -1;
        try (InputStream in = DumpFormatSelector.decompress(() -> Files.newInputStream(r.path()))) {
            // Skip the Dumper metadata header: int format, long files, long bytes.
            new DataInputStream(in).skipNBytes(Integer.BYTES + 2L * Long.BYTES);
            TarArchiveInputStream tar = new TarArchiveInputStream(in);
            for (TarArchiveEntry e = tar.getNextEntry(); e != null; e = tar.getNextEntry()) {
                String fileName = Path.of(e.getName()).getFileName().toString();
                if (!e.isDirectory() && fileName.startsWith(prefix)) {
                    long version = Long.parseLong(fileName.substring(prefix.length()));
                    assertThat(version).isLessThanOrEqualTo(r.logBoundVersion());
                    if (version > highestVersion) {
                        highestVersion = version;
                        highestSize = e.getSize();
                    }
                }
            }
        }
        assertThat(highestVersion).isEqualTo(r.logBoundVersion());
        assertThat(highestSize).isEqualTo(r.logBoundOffset());
    }

    private Path restore(Path archive) throws Exception {
        DatabaseLayout restoreLayout = Neo4jLayout.of(restoreHome).databaseLayout("neo4j");
        // neo4j-admin creates these parents before loading; the Loader itself requires them.
        Files.createDirectories(restoreLayout.databaseDirectory().getParent());
        Files.createDirectories(restoreLayout.getTransactionLogsDirectory().getParent());
        try (DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction()) {
            new Loader(fs).load(restoreLayout, archive);
        }
        return restoreHome;
    }

    /** Threads committing one small transaction per loop until closed; close() rethrows any writer error. */
    private final class Writers implements AutoCloseable {
        private final AtomicBoolean stop = new AtomicBoolean();
        private final AtomicLong committed = new AtomicLong();
        private final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        private final List<Thread> threads = new ArrayList<>();

        Writers(int count, String cypher) {
            for (int n = 0; n < count; n++) {
                Thread t = new Thread(() -> {
                    try {
                        while (!stop.get()) {
                            api.executeTransactionally(cypher, Map.of("i", committed.get()));
                            committed.incrementAndGet();
                        }
                    } catch (Throwable e) {
                        errors.add(e);
                    }
                }, "hot-backup-test-writer-" + n);
                t.start();
                threads.add(t);
            }
        }

        void awaitCommits(long min) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (committed.get() < min && errors.isEmpty() && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertThat(errors).isEmpty();
            assertThat(committed.get()).isGreaterThanOrEqualTo(min);
        }

        @Override
        public void close() throws InterruptedException {
            stop.set(true);
            for (Thread t : threads) {
                t.join();
            }
            assertThat(errors).isEmpty();
        }
    }
}
