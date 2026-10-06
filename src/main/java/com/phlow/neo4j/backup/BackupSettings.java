package com.phlow.neo4j.backup;

import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.PATH;

import java.nio.file.Path;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.graphdb.config.Setting;

/** Settings of the hot backup plugin. Discovered by Neo4j through META-INF/services. */
public class BackupSettings implements SettingsDeclaration {

    @Description("Directory where the backup.* procedures write .dump files")
    public static final Setting<Path> backup_directory = newBuilder("server.backup.directory", PATH, Path.of("backups"))
            .setDependency(GraphDatabaseSettings.neo4j_home)
            .build();
}
