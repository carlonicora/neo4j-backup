package com.phlow.neo4j.backup;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ServiceLoader;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.SettingsDeclaration;

class BackupSettingsTest {

    @Test
    void settingHasNameAndDefault() {
        assertThat(BackupSettings.backup_directory.name()).isEqualTo("server.backup.directory");
        Config config = Config.newBuilder()
                .set(GraphDatabaseSettings.neo4j_home, Path.of("/opt/neo4j"))
                .build();
        assertThat(config.get(BackupSettings.backup_directory)).isEqualTo(Path.of("/opt/neo4j/backups"));
    }

    @Test
    void settingIsDiscoveredByServiceLoader() {
        assertThat(ServiceLoader.load(SettingsDeclaration.class)).anyMatch(d -> d instanceof BackupSettings);
    }
}
