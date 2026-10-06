package com.phlow.neo4j.backup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.dbms.archive.DumpFormatSelector;
import org.neo4j.dbms.archive.Loader;
import org.neo4j.dbms.archive.StandardCompressionFormat;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;

class DumpArchiveWriterTest {
    @TempDir Path tmp;

    private DatabaseLayout load(Path archive) throws Exception {
        DatabaseLayout layout = Neo4jLayout.of(tmp.resolve("home")).databaseLayout("neo4j");
        // neo4j-admin creates these parents before loading; the Loader itself requires them.
        Files.createDirectories(layout.databaseDirectory().getParent());
        Files.createDirectories(layout.getTransactionLogsDirectory().getParent());
        try (DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction()) {
            new Loader(fs).load(layout, archive);
        }
        return layout;
    }

    private static void write(Path file, String content) throws Exception {
        Files.createDirectories(file.getParent());
        Files.writeString(file, content);
    }

    @Test void roundTripsThroughNeo4jLoaderIntoStoreAndTransactionDirectories() throws Exception {
        Path db = tmp.resolve("db");
        Path tx = tmp.resolve("tx");
        write(db.resolve("neostore"), "store");
        write(db.resolve("schema/index/x"), "idx");
        write(tx.resolve("neostore.transaction.db.0"), "log0");
        write(tx.resolve("checkpoint.0"), "cp");
        Path archive = tmp.resolve("out.dump");

        DumpArchiveWriter writer = new DumpArchiveWriter(archive, 4, 14);
        try (writer) {
            writer.addDirectoryTree(db, p -> false);
            writer.addFile(tx, tx.resolve("neostore.transaction.db.0"));
            writer.addFile(tx, tx.resolve("checkpoint.0"));
        }

        DatabaseLayout layout = load(archive);
        assertThat(Files.readString(layout.databaseDirectory().resolve("neostore"))).isEqualTo("store");
        assertThat(Files.readString(layout.databaseDirectory().resolve("schema/index/x"))).isEqualTo("idx");
        assertThat(Files.readString(layout.getTransactionLogsDirectory().resolve("neostore.transaction.db.0")))
                .isEqualTo("log0");
        assertThat(Files.readString(layout.getTransactionLogsDirectory().resolve("checkpoint.0"))).isEqualTo("cp");
        assertThat(writer.filesWritten()).isEqualTo(4);
        assertThat(writer.bytesWritten()).isEqualTo(5 + 3 + 4 + 2);
    }

    @Test void excludedFilesAreNotArchived() throws Exception {
        Path db = tmp.resolve("db");
        write(db.resolve("database_lock"), "lock");
        write(db.resolve("neostore"), "store");
        Path archive = tmp.resolve("out.dump");

        try (DumpArchiveWriter writer = new DumpArchiveWriter(archive, 1, 5)) {
            writer.addDirectoryTree(db, p -> p.getFileName().toString().equals("database_lock"));
        }

        DatabaseLayout layout = load(archive);
        assertThat(layout.databaseDirectory().resolve("database_lock")).doesNotExist();
        assertThat(Files.readString(layout.databaseDirectory().resolve("neostore"))).isEqualTo("store");
    }

    @Test void fileGrowingDuringCopyIsTruncatedToSizedLength() throws Exception {
        Path root = tmp.resolve("db");
        Path file = root.resolve("neostore");
        Files.createDirectories(root);
        byte[] initial = new byte[4 * 1024 * 1024];
        Arrays.fill(initial, (byte) 7);
        Files.write(file, initial);
        Path archive = tmp.resolve("out.dump");

        AtomicBoolean stop = new AtomicBoolean();
        Thread appender = new Thread(() -> {
            byte[] chunk = new byte[64 * 1024];
            Arrays.fill(chunk, (byte) 9);
            try (OutputStream out = Files.newOutputStream(file, StandardOpenOption.APPEND)) {
                while (!stop.get()) {
                    out.write(chunk);
                    out.flush();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        DumpArchiveWriter writer = new DumpArchiveWriter(archive, 1, initial.length);
        try (writer) {
            appender.start();
            writer.addFile(root, file);
            stop.set(true);
            appender.join();
        }

        long finalSourceLength = Files.size(file);
        DatabaseLayout layout = load(archive);
        long loaded = Files.size(layout.databaseDirectory().resolve("neostore"));
        assertThat(loaded).isBetween((long) initial.length, finalSourceLength);
        assertThat(loaded).isEqualTo(writer.bytesWritten());
    }

    @Test void addFileWithLengthCopiesExactlyThatManyBytes() throws Exception {
        Path root = tmp.resolve("db");
        Path file = root.resolve("neostore");
        Files.createDirectories(root);
        byte[] content = new byte[1024 * 1024];
        new java.util.Random(42).nextBytes(content);
        Files.write(file, content);
        Path archive = tmp.resolve("out.dump");

        DumpArchiveWriter writer = new DumpArchiveWriter(archive, 1, 300_000);
        try (writer) {
            writer.addFile(root, file, 300_000);
        }

        DatabaseLayout layout = load(archive);
        byte[] loaded = Files.readAllBytes(layout.databaseDirectory().resolve("neostore"));
        assertThat(loaded).hasSize(300_000);
        assertThat(loaded).isEqualTo(Arrays.copyOf(content, 300_000));
        assertThat(writer.bytesWritten()).isEqualTo(300_000);

        try (DumpArchiveWriter tooLong = new DumpArchiveWriter(tmp.resolve("too-long.dump"), 1, 0)) {
            assertThatThrownBy(() -> tooLong.addFile(root, file, content.length + 1L))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining(file.toString());
        }
    }

    @Test void existingArchiveIsNeverOverwritten() throws Exception {
        Path archive = tmp.resolve("out.dump");
        Files.writeString(archive, "x");
        assertThatThrownBy(() -> new DumpArchiveWriter(archive, 0, 0))
                .isInstanceOf(FileAlreadyExistsException.class);
        assertThat(Files.readString(archive)).isEqualTo("x");
    }

    @Test void archiveIsReadableByZstdFormatDetection() throws Exception {
        Path db = tmp.resolve("db");
        Files.createDirectories(db);
        Path archive = tmp.resolve("out.dump");
        try (DumpArchiveWriter writer = new DumpArchiveWriter(archive, 0, 0)) {
            writer.addDirectoryTree(db, p -> false);
        }
        try (InputStream stream = DumpFormatSelector.decompress(() -> Files.newInputStream(archive))) {
            assertThat(StandardCompressionFormat.ZSTD.isFormat(stream)).isTrue();
        }
    }
}
