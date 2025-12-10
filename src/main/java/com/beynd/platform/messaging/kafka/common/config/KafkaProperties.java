package com.beynd.platform.messaging.kafka.common.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "beynd.kafka")
public class KafkaProperties {

    private boolean enabled = false;
    private String bootstrapServers;
    private String schemaRegistryUrl;
    private Producer producer = new Producer();
    private Consumer consumer = new Consumer();

    @Getter @Setter
    public static class Producer {
        private String aggregationType;
        private Outbox outbox = new Outbox();
    }

    @Getter @Setter
    public static class Consumer {
        private int concurrency = 1;
        private int maxRetries = 5;
        private long backoffInitialIntervalMs = 2000;
        private double backoffMultiplier = 2.0;
        private long backoffMaxIntervalMs = 10000;
        private String dlqTopicSuffix = ".dlq";
        private String groupId;
        private String retryTopicSuffix = ".retry";
        private int retryMaxAttempts = 3;
    }

    @Getter @Setter
    public static class Outbox {
        private int batchSize = 50;
        private int maxAttempts = 5;
        private long pollIntervalMs = 500;
    }
}
