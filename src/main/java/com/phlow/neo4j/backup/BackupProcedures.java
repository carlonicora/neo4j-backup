package com.phlow.neo4j.backup;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.procedure.Admin;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

/** backup.database(name) and backup.all(): thin glue over {@link HotBackup}. */
public class BackupProcedures {

    @Context
    public GraphDatabaseAPI graph;

    @Context
    public Log log;

    public static final class Output {
        public String database;
        public String path;
        public Long bytes;
        public Long millis;
        public String status;

        Output(String database, String path, Long bytes, Long millis, String status) {
            this.database = database;
            this.path = path;
            this.bytes = bytes;
            this.millis = millis;
            this.status = status;
        }
    }

    @Admin
    @Procedure(name = "backup.database", mode = Mode.DBMS)
    @Description("Consistent online backup of one database into server.backup.directory as a .dump file")
    public Stream<Output> database(@Name("name") String name) {
        DatabaseManagementService dbms = dbms();
        GraphDatabaseAPI target = (GraphDatabaseAPI) dbms.database(name);
        if (!target.isAvailable(0)) {
            throw new IllegalStateException("Database '" + name + "' is not available");
        }
        try {
            return Stream.of(backup(hotBackup(), name, target));
        } catch (IOException e) {
            log.error("Hot backup of '" + name + "' failed", e);
            throw new RuntimeException(e.getMessage(), e);
        } catch (RuntimeException e) {
            log.error("Hot backup of '" + name + "' failed", e);
            throw e;
        }
    }

    /**
     * Like backup.database, into server.backup.directory/fileName. An existing named pipe there is
     * written to (streaming, no disk) and never deleted; a missing target becomes a regular file.
     */
    @Admin
    @Procedure(name = "backup.databaseTo", mode = Mode.DBMS)
    @Description("Consistent online backup of one database into server.backup.directory/fileName; "
            + "an existing named pipe there is streamed into")
    public Stream<Output> databaseTo(@Name("name") String name, @Name("fileName") String fileName) {
        HotBackup.validateFileName(fileName);
        DatabaseManagementService dbms = dbms();
        GraphDatabaseAPI target = (GraphDatabaseAPI) dbms.database(name);
        if (!target.isAvailable(0)) {
            throw new IllegalStateException("Database '" + name + "' is not available");
        }
        try {
            HotBackup.Result r = hotBackup().runTo(target, fileName);
            log.info("Hot backup of '%s' written to %s", name, r.path());
            return Stream.of(new Output(r.database(), r.path().toString(), r.bytes(), r.millis(), "ok"));
        } catch (IOException e) {
            log.error("Hot backup of '" + name + "' failed", e);
            throw new RuntimeException(e.getMessage(), e);
        } catch (RuntimeException e) {
            log.error("Hot backup of '" + name + "' failed", e);
            throw e;
        }
    }

    /** One row per database; a failure of one database is reported in its row, never thrown. */
    @Admin
    @Procedure(name = "backup.all", mode = Mode.DBMS)
    @Description("Consistent online backup of every database into server.backup.directory, one .dump each")
    public Stream<Output> all() {
        DatabaseManagementService dbms = dbms();
        HotBackup hot = hotBackup();
        List<Output> rows = new ArrayList<>();
        for (String name : dbms.listDatabases().stream().sorted().toList()) {
            try {
                GraphDatabaseAPI target = (GraphDatabaseAPI) dbms.database(name);
                if (!target.isAvailable(0)) {
                    log.warn("Hot backup of '%s' skipped: not available", name);
                    rows.add(new Output(name, null, null, null, "skipped: not available"));
                } else {
                    rows.add(backup(hot, name, target));
                }
            } catch (IOException | RuntimeException e) {
                log.error("Hot backup of '" + name + "' failed", e);
                rows.add(new Output(name, null, null, null, "failed: " + e.getMessage()));
            }
        }
        return rows.stream();
    }

    private Output backup(HotBackup hot, String name, GraphDatabaseAPI target) throws IOException {
        HotBackup.Result r = hot.run(target);
        log.info("Hot backup of '%s' written to %s", name, r.path());
        return new Output(r.database(), r.path().toString(), r.bytes(), r.millis(), "ok");
    }

    private DatabaseManagementService dbms() {
        return graph.getDependencyResolver().resolveDependency(DatabaseManagementService.class);
    }

    private HotBackup hotBackup() {
        DependencyResolver deps = graph.getDependencyResolver();
        Path dir = deps.resolveDependency(Config.class).get(BackupSettings.backup_directory);
        return new HotBackup(dir, Clock.systemDefaultZone(),
                deps.resolveDependency(LogService.class).getInternalLog(BackupProcedures.class));
    }
}
