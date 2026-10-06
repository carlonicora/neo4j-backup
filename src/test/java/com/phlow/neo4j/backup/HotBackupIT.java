package com.phlow.neo4j.backup;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.dbms.archive.Loader;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

class HotBackupIT {

    private static final int WRITERS = 4;
    private static final long WARM_UP_MILLIS = 500;
    private static final long WARM_UP_COMMITS = 500;
    private static final String PAYLOAD = "x".repeat(1024);

    @TempDir
    Path home;

    @TempDir
    Path backups;

    @TempDir
    Path restoreHome;

    @Test
    void backupUnderConcurrentWritesRestoresConsistentlyWithinBounds() throws Exception {
        DatabaseManagementService dbms = new DatabaseManagementServiceBuilder(home)
                .setConfig(BackupSettings.backup_directory, backups)
                .setConfig(GraphDatabaseSettings.logical_log_rotation_threshold, ByteUnit.kibiBytes(128))
                .setConfig(GraphDatabaseSettings.check_point_interval_time, Duration.ofMillis(100))
                .build();

        long lower;
        long upper;
        long ceiling;
        long backupMillis;
        String path;
        List<Throwable> writerErrors = new ArrayList<>();
        try {
            GraphDatabaseService db = dbms.database("neo4j");
            ((GraphDatabaseAPI) db)
                    .getDependencyResolver()
                    .resolveDependency(GlobalProcedures.class)
                    .registerProcedure(BackupProcedures.class);

            // committed: incremented after commit() returned, so it never over-counts.
            // started: incremented just before commit(), so it never under-counts. A transaction can be
            // durable in the log before its thread reaches committed.incrementAndGet(), so the sound
            // ceiling for the restored count is started, read after the procedure returned.
            AtomicLong committed = new AtomicLong();
            AtomicLong started = new AtomicLong();
            AtomicBoolean stop = new AtomicBoolean();
            ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
            List<Thread> writers = new ArrayList<>();
            for (int i = 0; i < WRITERS; i++) {
                Thread t = new Thread(
                        () -> {
                            try {
                                while (!stop.get()) {
                                    try (Transaction tx = db.beginTx()) {
                                        tx.execute("CREATE (:W {p: $p})", Map.of("p", PAYLOAD));
                                        started.incrementAndGet();
                                        tx.commit();
                                    }
                                    committed.incrementAndGet();
                                }
                            } catch (Throwable e) {
                                errors.add(e);
                            }
                        },
                        "hot-backup-it-writer-" + i);
                t.start();
                writers.add(t);
            }

            // Warm-up: at least WARM_UP_MILLIS and at least WARM_UP_COMMITS commits, so the lower
            // bound is meaningful (the first Cypher compilation alone can outlast 500 ms).
            Thread.sleep(WARM_UP_MILLIS);
            while (committed.get() < WARM_UP_COMMITS && errors.isEmpty()) {
                Thread.sleep(10);
            }
            lower = committed.get();

            long t0 = System.nanoTime();
            path = db.executeTransactionally(
                    "CALL backup.database($n) YIELD path RETURN path",
                    Map.of("n", "neo4j"),
                    r -> (String) r.next().get("path"));
            backupMillis = (System.nanoTime() - t0) / 1_000_000;

            upper = committed.get();
            ceiling = started.get();
            stop.set(true);
            for (Thread t : writers) {
                t.join();
            }
            writerErrors.addAll(errors);
        } finally {
            dbms.shutdown();
        }
        assertThat(writerErrors).isEmpty();

        DatabaseLayout restoreLayout = Neo4jLayout.of(restoreHome).databaseLayout("neo4j");
        // Loader requires the parents of the store and transaction directories to exist,
        // as they do in a real installation.
        Files.createDirectories(restoreLayout.databaseDirectory().getParent());
        Files.createDirectories(restoreLayout.getTransactionLogsDirectory().getParent());
        new Loader(new DefaultFileSystemAbstraction()).load(restoreLayout, Path.of(path));

        DatabaseManagementService restored = new DatabaseManagementServiceBuilder(restoreHome).build(); // recovery runs
        long count;
        try {
            count = restored.database("neo4j")
                    .executeTransactionally(
                            "MATCH (n:W) RETURN count(n) AS c", Map.of(), r -> (Long) r.next().get("c"));
        } finally {
            restored.shutdown();
        }

        System.out.printf(
                "HotBackupIT: lower=%d upper=%d ceiling=%d restored=%d backupMillis=%d path=%s%n",
                lower, upper, ceiling, count, backupMillis, path);

        assertThat(count).isBetween(lower, ceiling);
        assertThat(upper).isGreaterThan(lower); // proves writes happened during the backup

        ConsistencyCheckService.Result cc = new ConsistencyCheckService(restoreLayout)
                .with(Path.of("target"))
                .runFullConsistencyCheck();
        assertThat(cc.isSuccessful()).withFailMessage(() -> "see " + cc.reportFile()).isTrue();
    }
}
