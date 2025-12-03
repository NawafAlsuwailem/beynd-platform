package com.beynd.platform.messaging.config;

import com.beynd.platform.messaging.outbox.OutboxEventRepository;
import com.beynd.platform.messaging.outbox.OutboxRelay;
import com.beynd.platform.messaging.publisher.DefaultEventPublisher;
import com.beynd.platform.messaging.publisher.EventPublisher;
import com.beynd.platform.messaging.serializer.AvroBinaryEventPayloadSerializer;
import com.beynd.platform.messaging.serializer.EventPayloadSerializer;
import com.beynd.platform.messaging.startup.StartupListener;
import com.beynd.platform.messaging.startup.validation.BeyndKafkaStartupValidator;
import com.beynd.platform.messaging.startup.validation.DefaultKafkaStartupValidator;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@AutoConfiguration
@EnableConfigurationProperties(BeyndKafkaProperties.class)
@ConditionalOnProperty(prefix = "beynd.kafka", name = "enabled", havingValue = "true")
public class BeyndMessagingAutoConfiguration {

    @Bean
    public BeyndKafkaStartupValidator kafkaStartupValidator(
            BeyndKafkaProperties properties,
            @Autowired(required = false) DataSource dataSource
    ) {
        return new DefaultKafkaStartupValidator(properties, dataSource);
    }


    @Bean
    public StartupListener startupListener(BeyndKafkaStartupValidator validator) {
        return new StartupListener(validator);
    }

    @Bean
    public EventPublisher eventPublisher(
            OutboxEventRepository outboxEventRepository,
            EventPayloadSerializer serializer
    ) {
        return new DefaultEventPublisher(outboxEventRepository, serializer);
    }

    @Bean
    public OutboxRelay outboxRelay(
            OutboxEventRepository outboxEventRepository,
            KafkaTemplate<String, byte[]> kafkaTemplate,
            BeyndKafkaProperties kafkaProperties
    ) {
        int batchSize = kafkaProperties.getOutboxBatchSize();     // add these to properties class
        int maxAttempts = kafkaProperties.getOutboxMaxAttempts(); // ditto

        return new OutboxRelay(outboxEventRepository, kafkaTemplate, batchSize, maxAttempts);
    }

    @Bean
    public EventPayloadSerializer eventPayloadSerializer(KafkaAvroSerializer kafkaAvroSerializer) {
        return new AvroBinaryEventPayloadSerializer(kafkaAvroSerializer);
    }

    @Bean
    public KafkaAvroSerializer kafkaAvroSerializer(BeyndKafkaProperties props) {
        KafkaAvroSerializer serializer = new KafkaAvroSerializer();
        serializer.configure(
                Map.of("schema.registry.url", props.getSchemaRegistryUrl()),
                false
        );
        return serializer;
    }

    @Bean
    public ProducerFactory<String, byte[]> producerFactory(BeyndKafkaProperties props) {

        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getBootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean public KafkaTemplate<String, byte[]> kafkaTemplate(ProducerFactory<String, byte[]> pf) { return new KafkaTemplate<>(pf); }
}

