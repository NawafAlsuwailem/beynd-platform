package com.beynd.platform.messaging.kafka.consumer.config;

import com.beynd.platform.messaging.kafka.common.config.KafkaProperties;
import com.beynd.platform.messaging.kafka.common.serializer.EventPayloadSerializer;
import com.beynd.platform.messaging.kafka.consumer.idempotency.IdempotentRecordInterceptor;
import com.beynd.platform.messaging.kafka.consumer.idempotency.ProcessedEventRepository;
import com.beynd.platform.messaging.kafka.consumer.recovery.DeadLetterRecoverer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@AutoConfiguration
@EnableConfigurationProperties(KafkaProperties.class)
@ConditionalOnProperty(prefix = "beynd.kafka.consumer", name = "enabled", havingValue = "true")
@RequiredArgsConstructor
public class ConsumerAutoConfiguration {

    public static final String LISTENER_FACTORY = "beyndKafkaListenerContainerFactory";

    private final KafkaProperties properties;
    private final Environment environment;

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        cfg.put("schema.registry.url", properties.getSchemaRegistryUrl());
        cfg.put("specific.avro.reader", true);

        cfg.put(ConsumerConfig.GROUP_ID_CONFIG,
                properties.getConsumer().getGroupId() != null
                        ? properties.getConsumer().getGroupId()
                        : environment.getProperty("spring.application.name"));

        cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new DefaultKafkaConsumerFactory<>(cfg);
    }

    @Bean
    public CommonErrorHandler errorHandler(
            KafkaTemplate<String, byte[]> template,
            EventPayloadSerializer serializer) {

        KafkaProperties.Consumer c = properties.getConsumer();

        ExponentialBackOffWithMaxRetries backoff =
                new ExponentialBackOffWithMaxRetries(c.getMaxRetries());

        backoff.setInitialInterval(c.getBackoffInitialIntervalMs());
        backoff.setMultiplier(c.getBackoffMultiplier());
        backoff.setMaxInterval(c.getBackoffMaxIntervalMs());

        DefaultErrorHandler handler =
                new DefaultErrorHandler(
                        new DeadLetterRecoverer(template, properties, serializer),
                        backoff
                );

        handler.setRetryListeners((record, ex, deliveryAttempt) -> {
            if (ex != null) {
                log.error(
                        "[Kafka Consume Failure] topic={} partition={} offset={} key={} attempt={}",
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        record.key(),
                        deliveryAttempt,
                        ex
                );
            }
        });

        handler.setAckAfterHandle(true);     // commit after recovery
        handler.setCommitRecovered(true);    // commit offsets
        handler.setSeekAfterError(false);    // NEVER seek on failure

        return handler;
    }

    @Bean(name = LISTENER_FACTORY)
    public ConcurrentKafkaListenerContainerFactory<String, Object>
    kafkaListenerContainerFactory(ConsumerFactory<String, Object> cf,
                                  CommonErrorHandler errorHandler,
                                  ObjectProvider<RecordInterceptor<String, Object>> interceptor) {

        ConcurrentKafkaListenerContainerFactory<String, Object> f =
                new ConcurrentKafkaListenerContainerFactory<>();

        f.setConsumerFactory(cf);
        f.setCommonErrorHandler(errorHandler);
        f.setConcurrency(properties.getConsumer().getConcurrency());
        f.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);

        RecordInterceptor<String, Object> ri = interceptor.getIfAvailable();
        if (ri != null) {
            f.setRecordInterceptor(ri);
            log.info("[BEYND KAFKA] Idempotency RecordInterceptor attached.");
        } else {
            log.warn("[BEYND KAFKA] Idempotency RecordInterceptor not found; processed_event will not be persisted.");
        }

        return f;
    }

    @Bean
    public RecordInterceptor<String, Object> idempotentRecordInterceptor(
            ProcessedEventRepository repository) {
        return new IdempotentRecordInterceptor(repository);
    }
}

