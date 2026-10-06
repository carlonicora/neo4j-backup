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
    private final OutputStream compressed;
    private final TarArchiveOutputStream tar;
    private final byte[] buffer = new byte[BUFFER_SIZE];
    private long filesWritten;
    private long bytesWritten;
    private boolean closed;

    /** Opens archive with CREATE_NEW, wraps in StandardCompressionFormat.ZSTD.compress, writes
     *  DataOutputStream int 1, long expectedFiles, long expectedBytes, then opens a
     *  TarArchiveOutputStream with LONGFILE_POSIX and BIGNUMBER_POSIX. */
    public DumpArchiveWriter(Path archive, long expectedFiles, long expectedBytes) throws IOException {
        this.channel = FileChannel.open(archive, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        try {
            // The zstd stream must not close the channel: close() forces it to disk first.
            OutputStream channelOut = new FilterOutputStream(Channels.newOutputStream(channel)) {
                @Override
                public void write(byte[] b, int off, int len) throws IOException {
                    out.write(b, off, len);
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
            // CREATE_NEW succeeded, so the file is ours: remove it rather than leave a broken archive.
            try {
                channel.close();
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            try {
                Files.deleteIfExists(archive);
            } catch (IOException suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
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

    /** Finishes the tar, closes the zstd stream, fsyncs on success, and always closes the channel. */
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
                if (finished) {
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
