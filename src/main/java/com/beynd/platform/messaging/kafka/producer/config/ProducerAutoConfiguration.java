package com.beynd.platform.messaging.kafka.producer.config;

import com.beynd.platform.messaging.kafka.common.config.KafkaProperties;
import com.beynd.platform.messaging.kafka.producer.outbox.OutboxEventRepository;
import com.beynd.platform.messaging.kafka.producer.outbox.OutboxRelay;
import com.beynd.platform.messaging.kafka.producer.publisher.BeyndEventPublisher;
import com.beynd.platform.messaging.kafka.producer.publisher.DefaultBeyndEventPublisher;
import com.beynd.platform.messaging.kafka.common.serializer.AvroBinaryEventPayloadSerializer;
import com.beynd.platform.messaging.kafka.common.serializer.EventPayloadSerializer;
import com.beynd.platform.messaging.kafka.common.startup.StartupListener;
import com.beynd.platform.messaging.kafka.common.startup.validation.DefaultKafkaStartupValidator;
import com.beynd.platform.messaging.kafka.common.startup.validation.KafkaStartupValidator;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@AutoConfiguration
@EnableConfigurationProperties(KafkaProperties.class)
@ConditionalOnProperty(prefix = "beynd.kafka", name = "enabled", havingValue = "true")
@RequiredArgsConstructor
public class ProducerAutoConfiguration {

    private final Environment environment;

    @Bean
    public KafkaStartupValidator kafkaStartupValidator(
            KafkaProperties properties,
            @Autowired(required = false) DataSource dataSource
    ) {
        return new DefaultKafkaStartupValidator(properties, dataSource);
    }

    @Bean
    public StartupListener startupListener(KafkaStartupValidator validator) {
        return new StartupListener(validator);
    }

    @Bean
    public ProducerFactory<String, byte[]> producerFactory(KafkaProperties properties) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, byte[]> kafkaTemplate(ProducerFactory<String, byte[]> pf) {
        return new KafkaTemplate<>(pf);
    }

    @Bean
    public KafkaAvroSerializer kafkaAvroSerializer(KafkaProperties props) {
        KafkaAvroSerializer serializer = new KafkaAvroSerializer();
        serializer.configure(Map.of("schema.registry.url", props.getSchemaRegistryUrl()), false);
        return serializer;
    }

    @Bean
    public EventPayloadSerializer eventPayloadSerializer(KafkaAvroSerializer kafkaAvroSerializer) {
        return new AvroBinaryEventPayloadSerializer(kafkaAvroSerializer);
    }

    @Bean
    public BeyndEventPublisher eventPublisher(
            OutboxEventRepository outboxEventRepository,
            EventPayloadSerializer serializer
    ) {
        return new DefaultBeyndEventPublisher(outboxEventRepository, serializer);
    }

    @Bean
    public OutboxRelay outboxRelay(
            OutboxEventRepository outboxEventRepository,
            KafkaTemplate<String, byte[]> kafkaTemplate,
            KafkaProperties kafkaProperties
    ) {
        int batchSize = kafkaProperties.getProducer().getOutbox().getBatchSize();
        int maxAttempts = kafkaProperties.getProducer().getOutbox().getMaxAttempts();
        return new OutboxRelay(outboxEventRepository, kafkaTemplate, batchSize, maxAttempts);
    }
}
