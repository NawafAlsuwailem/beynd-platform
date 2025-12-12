package com.beynd.platform.messaging.kafka.common.startup.validation;

import com.beynd.platform.messaging.kafka.common.config.KafkaProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Slf4j
@RequiredArgsConstructor
public class DefaultKafkaStartupValidator implements KafkaStartupValidator {

    private final KafkaProperties properties;
    private final DataSource dataSource;

    @Override
    public void validate() {
        log.info("[BEYND KAFKA] Kafka is enabled. Validation phase starting (Phase 1 placeholder)...");
        validateBootstrapServers();
        validateSchemaRegistry();
        validateAggregationType();
        validateDatabaseTables();
        validateKafkaClusterConnection();
    }

    private void validateBootstrapServers() {
        String servers = properties.getBootstrapServers();

        if (servers == null || servers.isBlank()) {
            throw new IllegalStateException("[BEYND KAFKA] Missing required property: beynd.kafka.bootstrap-servers");
        }

        if (!servers.contains(":")) {
            throw new IllegalStateException("[BEYND KAFKA] Invalid bootstrap server format: " + servers);
        }

        log.info("[BEYND KAFKA] ✔ Bootstrap servers validated: {}", servers);
    }

    private void validateSchemaRegistry() {
        String url = properties.getSchemaRegistryUrl();

        if (url == null || url.isBlank()) {
            throw new IllegalStateException("[BEYND KAFKA] Schema Registry URL not provided — skipping validation.");
        }

        // simple URL validation
        if (!url.startsWith("http")) {
            throw new IllegalStateException("[BEYND KAFKA] Invalid Schema Registry URL: " + url);
        }

        log.info("[BEYND KAFKA] ✔ Schema Registry configured: {}", url);
    }

    private void validateAggregationType() {
        String aggregationType = properties.getProducer().getAggregationType();

        if (aggregationType == null || aggregationType.isBlank()) {
            throw new IllegalStateException("[BEYND KAFKA] Aggregation Type not provided — skipping validation.");
        }

        log.info("[BEYND KAFKA] ✔ Aggregation Type configured: {}", aggregationType);
    }

    private void validateDatabaseTables() {
        try (Connection conn = dataSource.getConnection()) {

            if (!tableExists(conn, "outbox_event")) {
                throw new IllegalStateException("[BEYND KAFKA] Required table 'outbox_event' is missing.");
            }

            if (!tableExists(conn, "processed_event")) {
                throw new IllegalStateException("[BEYND KAFKA] Required table 'processed_event' is missing.");
            }

            log.info("[BEYND KAFKA] ✔ Outbox and Deduplication tables validated.");

        } catch (Exception ex) {
            throw new IllegalStateException("[BEYND KAFKA] Database validation failed: " + ex.getMessage(), ex);
        }
    }

    private boolean tableExists(Connection conn, String tableName) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        ResultSet rs = meta.getTables(null, null, tableName, null);
        return rs.next();
    }

    private void validateKafkaClusterConnection() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "3000");

        try (AdminClient admin = AdminClient.create(props)) {
            admin.describeCluster().clusterId().get(3, TimeUnit.SECONDS);
            log.info("[BEYND KAFKA] ✔ Successfully connected to Kafka cluster.");
        } catch (Exception ex) {
            throw new IllegalStateException("[BEYND KAFKA] Unable to connect to Kafka cluster: " + ex.getMessage(), ex);
        }
    }
}