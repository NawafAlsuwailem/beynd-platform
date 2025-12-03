package com.beynd.platform.messaging.config;


import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "beynd.kafka")
public class BeyndKafkaProperties {

    /**
     * Enables or disables Kafka integration at the platform level.
     */
    private boolean enabled = false;

    /**
     * Kafka bootstrap servers (required only when enabled = true).
     */
    private String bootstrapServers;

    /**
     * Schema Registry URL (optional depending on serialization mode).
     */
    private String schemaRegistryUrl;

    /**
     * Allow future topic auto-creation logic.
     */
    private boolean autoCreateTopics = false;

    /**
     * Number of outbox records to publish.
     */
    private int outboxBatchSize = 50;

    /**
     * Number of outbox attempts to publish.
     */
    private int outboxMaxAttempts = 5;

    /**
     * Outbox scheduler frequency.
     */
    private long outboxPollIntervalMs = 500;

}

