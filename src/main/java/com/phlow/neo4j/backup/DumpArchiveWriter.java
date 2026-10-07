package com.phlow.neo4j.backup;

import java.io.DataOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.neo4j.dbms.archive.StandardCompressionFormat;

/**
 * Writes a Neo4j 5 .dump archive in the same layout as Neo4j's Dumper: ZSTD stream, size metadata
 * (int 1, long files, long bytes), then a POSIX tar of "./"-relative entries.
 */
public final class DumpArchiveWriter implements AutoCloseable {
    private static final int BUFFER_SIZE = 64 * 1024;

    private final FileChannel channel;
    private final boolean ownsFile;
    private final OutputStream compressed;
    private final TarArchiveOutputStream tar;
    private final byte[] buffer = new byte[BUFFER_SIZE];
    private long filesWritten;
    private long bytesWritten;
    private long archiveBytes;
    private boolean closed;

    /** The opened archive output: a channel, and whether this plugin created the file (and may delete it). */
    public record Target(Path path, FileChannel channel, boolean ownsFile) {}

    /** Opens archive with CREATE_NEW, wraps in StandardCompressionFormat.ZSTD.compress, writes
     *  DataOutputStream int 1, long expectedFiles, long expectedBytes, then opens a
     *  TarArchiveOutputStream with LONGFILE_POSIX and BIGNUMBER_POSIX. */
    public DumpArchiveWriter(Path archive, long expectedFiles, long expectedBytes) throws IOException {
        this(openTarget(archive, false, 0), expectedFiles, expectedBytes);
    }

    /**
     * Opens the archive output without writing anything. A missing archive is created (CREATE_NEW).
     * With allowPipe, an existing non-regular file (a named pipe) is opened for WRITE only; that open
     * blocks until a reader opens the pipe, so it runs on its own thread and fails with an IOException
     * after pipeTimeoutMillis. An existing regular file always fails.
     */
    public static Target openTarget(Path archive, boolean allowPipe, long pipeTimeoutMillis) throws IOException {
        if (allowPipe && Files.exists(archive) && !Files.isRegularFile(archive)) {
            return new Target(archive, openPipe(archive, pipeTimeoutMillis), false);
        }
        return new Target(archive,
                FileChannel.open(archive, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE), true);
    }

    private static FileChannel openPipe(Path pipe, long timeoutMillis) throws IOException {
        CompletableFuture<FileChannel> opened = new CompletableFuture<>();
        Thread opener = new Thread(() -> {
            try {
                FileChannel channel = FileChannel.open(pipe, StandardOpenOption.WRITE);
                if (!opened.complete(channel)) {
                    channel.close(); // the caller gave up: never leak the channel
                }
            } catch (Throwable e) {
                opened.completeExceptionally(e);
            }
        }, "hot-backup-pipe-open");
        opener.setDaemon(true);
        opener.start();
        boolean interrupted = false;
        try {
            return opened.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException io) {
                throw io;
            }
            throw new IOException("Opening " + pipe + " failed: " + cause, cause);
        } catch (TimeoutException | InterruptedException e) {
            interrupted = e instanceof InterruptedException;
            String message = interrupted
                    ? "Interrupted waiting for a reader to open " + pipe
                    : "no reader opened " + pipe + " within " + timeoutMillis / 1000.0 + " s";
            IOException failure = new IOException(message, e);
            if (!opened.completeExceptionally(failure)) {
                return opened.join(); // the open won the race: use it
            }
            releaseOpener(pipe);
            throw failure;
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** Unblocks the abandoned open: a read-write open of a FIFO never blocks and counts as a reader,
     *  so the opener's open returns and it closes its channel. Best effort. */
    private static void releaseOpener(Path pipe) {
        try (FileChannel ignored = FileChannel.open(pipe, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            // nothing to do
        } catch (IOException | RuntimeException ignored) {
            // the opener stays parked as a daemon thread; it closes its channel if it ever opens
        }
    }

    /** Takes ownership of target's channel and writes the dump header. On failure closes the channel
     *  and deletes the file if this plugin created it; a pipe is never deleted. */
    public DumpArchiveWriter(Target target, long expectedFiles, long expectedBytes) throws IOException {
        Path archive = target.path();
        this.ownsFile = target.ownsFile();
        this.channel = target.channel();
        try {
            // The zstd stream must not close the channel: close() forces it to disk first.
            // Counts every byte handed to the channel, i.e. the size of the compressed archive.
            OutputStream channelOut = new FilterOutputStream(Channels.newOutputStream(channel)) {
                @Override
                public void write(int b) throws IOException {
                    out.write(b);
                    archiveBytes++;
                }

                @Override
                public void write(byte[] b, int off, int len) throws IOException {
                    out.write(b, off, len);
                    archiveBytes += len;
                }

                @Override
                public void close() throws IOException {
                    flush();
                }
            };
            this.compressed = StandardCompressionFormat.ZSTD.compress(channelOut);
            DataOutputStream metadata = new DataOutputStream(compressed);
            metadata.writeInt(1);
            metadata.writeLong(expectedFiles);
            metadata.writeLong(expectedBytes);
            this.tar = new TarArchiveOutputStream(compressed);
            this.tar.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
            this.tar.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);
        } catch (IOException | RuntimeException | Error e) {
            try {
                channel.close();
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            // CREATE_NEW succeeded, so the file is ours: remove it rather than leave a broken archive.
            // A pipe belongs to whoever created it and is never deleted.
            if (ownsFile) {
                try {
                    Files.deleteIfExists(archive);
                } catch (IOException suppressed) {
                    e.addSuppressed(suppressed);
                }
            }
            throw e;
        }
    }

    /** True when this writer created the archive as a regular file (delete it on failure); false for a pipe. */
    public boolean ownsFile() {
        return ownsFile;
    }

    /** Bytes written to the archive output (the compressed stream). Final once close() returned. */
    public long archiveBytes() {
        return archiveBytes;
    }

    /** Walks root depth-first (Files.walkFileTree). Adds a directory entry for root ("./") and
     *  each sub-directory, and a file entry for each regular file not matching exclude. */
    public void addDirectoryTree(Path root, Predicate<Path> exclude) throws IOException {
        Files.walkFileTree(root, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                if (exclude.test(dir)) {
                    return FileVisitResult.SKIP_SUBTREE;
                }
                addDirectory(root, dir);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (attrs.isRegularFile() && !exclude.test(file)) {
                    addFile(root, file);
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }

    /** Adds one regular file as entry "./" + root.relativize(file), sized when this is called. */
    public void addFile(Path root, Path file) throws IOException {
        addFile(root, file, Files.size(file));
    }

    /** Adds the first {@code length} bytes of file as entry "./" + root.relativize(file).
     *  Throws IOException if the file holds fewer than {@code length} bytes. */
    public void addFile(Path root, Path file, long length) throws IOException {
        if (length < 0) {
            throw new IllegalArgumentException("Negative length " + length + " for " + file);
        }
        long size = Files.size(file);
        if (size < length) {
            throw new IOException("File " + file + " is " + size + " bytes, shorter than the " + length
                    + " bytes requested");
        }
        TarArchiveEntry entry = new TarArchiveEntry(entryName(root, file));
        entry.setSize(length);
        entry.setModTime(Files.getLastModifiedTime(file).toMillis());
        tar.putArchiveEntry(entry);
        try (InputStream in = Files.newInputStream(file)) {
            long remaining = length;
            while (remaining > 0) {
                int read = in.readNBytes(buffer, 0, (int) Math.min(buffer.length, remaining));
                if (read == 0) {
                    throw new IOException("File shrank while archiving: " + file);
                }
                tar.write(buffer, 0, read);
                remaining -= read;
            }
        }
        tar.closeArchiveEntry();
        filesWritten++;
        bytesWritten += length;
    }

    public long filesWritten() {
        return filesWritten;
    }

    public long bytesWritten() {
        return bytesWritten;
    }

    /** Finishes the tar, closes the zstd stream, fsyncs a regular file on success, always closes the channel. */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        boolean finished = false;
        try {
            tar.finish();
            finished = true;
        } finally {
            try {
                compressed.close();
                if (finished && ownsFile) {
                    channel.force(true);
                }
            } finally {
                channel.close();
            }
        }
    }

    /** Adds a single directory entry "./" + root.relativize(dir) + "/" (root itself gives "./"). */
    public void addDirectory(Path root, Path dir) throws IOException {
        String name = entryName(root, dir);
        TarArchiveEntry entry = new TarArchiveEntry(name.endsWith("/") ? name : name + "/");
        tar.putArchiveEntry(entry);
        tar.closeArchiveEntry();
    }

    private static String entryName(Path root, Path path) {
        return "./" + root.relativize(path);
    }
}
