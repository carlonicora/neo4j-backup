package com.phlow.neo4j.backup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.neo4j.common.DependencyResolver;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.CheckpointFile;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.InternalLog;
import org.neo4j.storageengine.api.ClosedTransactionMetadata;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.storageengine.api.TransactionIdStore;

/** Consistent online backup of one started database into a Dumper-compatible .dump file. */
public final class HotBackup {

    /** logBoundVersion/logBoundOffset: the live transaction log was archived up to this position. */
    public record Result(
            String database,
            Path path,
            long bytes,
            long millis,
            long checkpointAppendIndex,
            long logBoundVersion,
            long logBoundOffset) {}

    private static final DateTimeFormatter STAMP = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
    private static final long CLOSE_WAIT_MILLIS = 60_000;
    private static final long CLOSE_POLL_MILLIS = 10;
    static final long PIPE_OPEN_WAIT_MILLIS = 60_000;

    private final Path backupDirectory;
    private final Clock clock;
    private final InternalLog log;
    private final long pipeOpenWaitMillis;

    public HotBackup(Path backupDirectory, Clock clock, InternalLog log) {
        this(backupDirectory, clock, log, PIPE_OPEN_WAIT_MILLIS);
    }

    /** pipeOpenWaitMillis: how long runTo waits for a reader to open a named pipe target. */
    HotBackup(Path backupDirectory, Clock clock, InternalLog log, long pipeOpenWaitMillis) {
        this.backupDirectory = backupDirectory;
        this.clock = clock;
        this.log = log;
        this.pipeOpenWaitMillis = pipeOpenWaitMillis;
    }

    static String fileName(String database, Instant now, ZoneId zone) {
        return database + "-" + STAMP.format(now.atZone(zone)) + ".dump";
    }

    /** Rejects anything but a plain file name: empty, ".", "..", or containing a slash or backslash. */
    static void validateFileName(String fileName) {
        if (fileName == null || fileName.isEmpty() || fileName.equals(".") || fileName.equals("..")
                || fileName.indexOf('/') >= 0 || fileName.indexOf('\\') >= 0) {
            throw new IllegalArgumentException(
                    "fileName must be a plain file name inside server.backup.directory, got: '" + fileName + "'");
        }
    }

    /** Steps 1 to 5 of the spec for one started database, into a new timestamped .dump file. */
    public Result run(GraphDatabaseAPI target) throws IOException {
        return run(target, null);
    }

    /**
     * Like {@link #run(GraphDatabaseAPI)}, into server.backup.directory/fileName. A missing target is
     * created as a regular file; an existing non-regular file (a named pipe) is written to and never
     * deleted; an existing regular file fails.
     */
    public Result runTo(GraphDatabaseAPI target, String fileName) throws IOException {
        validateFileName(fileName);
        return run(target, fileName);
    }

    private Result run(GraphDatabaseAPI target, String fixedFileName) throws IOException {
        // 1. Directory check, before touching the database.
        if (!Files.isDirectory(backupDirectory) || !Files.isWritable(backupDirectory)) {
            throw new IOException("server.backup.directory is not a writable directory: " + backupDirectory);
        }
        long started = System.nanoTime();

        // 2. Resolve kernel components of the target database.
        DependencyResolver deps = target.getDependencyResolver();
        Database database = deps.resolveDependency(Database.class);
        CheckPointer checkPointer = deps.resolveDependency(CheckPointer.class);
        DatabaseLayout layout = deps.resolveDependency(DatabaseLayout.class);
        TransactionIdStore txIdStore = deps.resolveDependency(TransactionIdStore.class);
        LogFiles logFiles = deps.resolveDependency(LogFiles.class);
        Path dbDir = layout.databaseDirectory();
        Path txDir = layout.getTransactionLogsDirectory();
        String name = layout.getDatabaseName();

        // 3. Target archive.
        Path archive = backupDirectory.resolve(
                fixedFileName != null ? fixedFileName : fileName(name, clock.instant(), ZoneId.systemDefault()));
        log.info("Hot backup of '%s' started, target %s", name, archive);

        // Open the output before the checkpoint and the store-copy lock: opening a pipe waits for its
        // reader, and checkpoints must never wait on that.
        DumpArchiveWriter.Target output =
                DumpArchiveWriter.openTarget(archive, fixedFileName != null, pipeOpenWaitMillis);
        // A pipe is owned by whoever created it: never delete it.
        boolean archiveCreated = output.ownsFile();

        // 4. Checkpoint under the store-copy mutex; checkpoints stay blocked until the lock is closed.
        StoreCopyCheckPointMutex mutex = database.getStoreCopyCheckPointMutex();
        long[] appendIndex = new long[1];
        LogPosition bound;
        long bytes;
        try (Resource lock = mutex.storeCopy(
                () -> appendIndex[0] = checkPointer.tryCheckPoint(new SimpleTriggerInfo("hot backup")))) {
            log.info("Hot backup of '%s': checkpoint done at append index %d", name, appendIndex[0]);

            // The kernel's listing, not a directory walk: Lucene deletes segment files at any time, and
            // keeping the iterator open pins its snapshots until the last store file is written.
            try (ResourceIterator<StoreFileMetadata> listing =
                    database.getStoreFileListing().builder().excludeLogFiles().build()) {
                List<Path> storeFiles = new ArrayList<>();
                while (listing.hasNext()) {
                    Path p = listing.next().path();
                    if (!p.startsWith(dbDir)) {
                        throw new IOException("Store file " + p + " is outside the database directory " + dbDir);
                    }
                    storeFiles.add(p);
                }

                // 5. Expected counts, progress metadata only.
                long expectedFiles = 0;
                long expectedBytes = 0;
                for (Path f : storeFiles) {
                    expectedFiles++;
                    expectedBytes += Files.size(f);
                }
                for (Path f : checkpointFiles(logFiles)) {
                    expectedFiles++;
                    expectedBytes += Files.size(f);
                }
                for (Path f : transactionLogFiles(logFiles.getLogFile())) {
                    expectedFiles++;
                    expectedBytes += Files.size(f);
                }

                DumpArchiveWriter writer = new DumpArchiveWriter(output, expectedFiles, expectedBytes);
                try (writer) {
                    writeStoreFiles(writer, dbDir, storeFiles);
                    log.info("Hot backup of '%s': store files written, %d files, %d bytes",
                            name, writer.filesWritten(), writer.bytesWritten());
                    long storeCount = writer.filesWritten();
                    long storeBytes = writer.bytesWritten();

                    // Only now bound the logs: every page in the copied store belongs to a transaction
                    // committed by now, and the closed position covers all of them.
                    ClosedTransactionMetadata lastClosed = awaitClosed(txIdStore);
                    bound = lastClosed.logPosition();
                    writeTransactionLogs(writer, txDir, logFiles, bound);
                    log.info("Hot backup of '%s': transaction logs written up to version %d offset %d "
                                    + "(last closed tx %d)",
                            name, bound.getLogVersion(), bound.getByteOffset(),
                            lastClosed.transactionId().id());
                    log.info("Hot backup of '%s': transaction logs written, %d files, %d bytes",
                            name, writer.filesWritten() - storeCount, writer.bytesWritten() - storeBytes);
                }
                bytes = writer.archiveBytes();
            }
        } catch (IOException | RuntimeException | Error e) {
            try {
                output.channel().close(); // no-op if the writer already closed it
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            if (archiveCreated) {
                try {
                    Files.deleteIfExists(archive);
                } catch (IOException suppressed) {
                    e.addSuppressed(suppressed);
                }
            }
            throw e;
        }

        long millis = (System.nanoTime() - started) / 1_000_000;
        log.info("Hot backup of '%s' finished in %d ms, %s (%d bytes)", name, millis, archive, bytes);
        return new Result(name, archive, bytes, millis, appendIndex[0], bound.getLogVersion(), bound.getByteOffset());
    }

    /** "./" once, then each parent directory between dbDir and the file (outermost first, once), then the file. */
    private static void writeStoreFiles(DumpArchiveWriter writer, Path dbDir, List<Path> storeFiles)
            throws IOException {
        Set<Path> directories = new HashSet<>();
        writer.addDirectory(dbDir, dbDir);
        directories.add(dbDir);
        for (Path p : storeFiles) {
            List<Path> parents = new ArrayList<>();
            for (Path d = p.getParent(); d != null && !d.equals(dbDir); d = d.getParent()) {
                parents.add(0, d);
            }
            for (Path d : parents) {
                if (directories.add(d)) {
                    writer.addDirectory(dbDir, d);
                }
            }
            writer.addFile(dbDir, p);
        }
    }

    /** Checkpoint files whole, then transaction logs up to the bound: whole below its version, exactly
     *  getByteOffset() bytes of its version, nothing above. */
    private static void writeTransactionLogs(DumpArchiveWriter writer, Path txDir, LogFiles logFiles, LogPosition bound)
            throws IOException {
        for (Path f : checkpointFiles(logFiles)) {
            writer.addFile(txDir, f);
        }
        LogFile logFile = logFiles.getLogFile();
        boolean boundWritten = false;
        for (Path f : transactionLogFiles(logFile)) {
            long version = logFile.getLogVersion(f);
            if (version < bound.getLogVersion()) {
                writer.addFile(txDir, f);
            } else if (version == bound.getLogVersion()) {
                writer.addFile(txDir, f, bound.getByteOffset());
                boundWritten = true;
            }
        }
        if (!boundWritten) {
            throw new IOException("Transaction log version " + bound.getLogVersion()
                    + " holding the last closed transaction is not among the log files in " + txDir);
        }
    }

    /** Waits until every transaction committed so far is closed, then returns the last closed one. */
    private static ClosedTransactionMetadata awaitClosed(TransactionIdStore txIdStore) throws IOException {
        long committed = txIdStore.getLastCommittedTransactionId();
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(CLOSE_WAIT_MILLIS);
        while (txIdStore.getLastClosedTransactionId() < committed) {
            if (System.nanoTime() > deadline) {
                throw new IOException("Timed out after " + CLOSE_WAIT_MILLIS + " ms waiting for transaction "
                        + committed + " to close; last closed is " + txIdStore.getLastClosedTransactionId());
            }
            try {
                Thread.sleep(CLOSE_POLL_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted waiting for transaction " + committed + " to close", e);
            }
        }
        return txIdStore.getLastClosedTransaction();
    }

    private static List<Path> checkpointFiles(LogFiles logFiles) throws IOException {
        CheckpointFile checkpointFile = logFiles.getCheckpointFile();
        return Arrays.stream(checkpointFile.getDetachedCheckpointFiles())
                .sorted(Comparator.comparingLong(checkpointFile::getDetachedCheckpointLogFileVersion))
                .toList();
    }

    private static List<Path> transactionLogFiles(LogFile logFile) throws IOException {
        return Arrays.stream(logFile.getMatchedFiles())
                .sorted(Comparator.comparingLong(logFile::getLogVersion))
                .toList();
    }
}
