package com.beynd.platform.messaging.kafka.consumer.config;

import com.beynd.platform.messaging.kafka.common.config.KafkaProperties;
import com.beynd.platform.messaging.kafka.consumer.recovery.DeadLetterRecoverer;
import com.beynd.platform.messaging.kafka.consumer.idempotency.IdempotentRecordInterceptor;
import com.beynd.platform.messaging.kafka.consumer.idempotency.ProcessedEventRepository;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

import java.util.HashMap;
import java.util.Map;

@AutoConfiguration
@EnableConfigurationProperties(KafkaProperties.class)
@ConditionalOnClass({KafkaTemplate.class, ConcurrentKafkaListenerContainerFactory.class})
@ConditionalOnProperty(prefix = "beynd.kafka", name = "enabled", havingValue = "true")
@RequiredArgsConstructor
public class ConsumerAutoConfiguration {

    public static final String BEYND_LISTENER_CONTAINER_FACTORY_BEAN_NAME =
            "beyndKafkaListenerContainerFactory";
    public static final String BEYND_CONSUMER_FACTORY_BEAN_NAME =
            "beyndConsumerFactory";
    private final KafkaProperties properties;
    private final Environment environment;

    @Bean
    @ConditionalOnMissingBean(name = BEYND_CONSUMER_FACTORY_BEAN_NAME)
    public ConsumerFactory<Object, Object> beyndConsumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        config.put("schema.registry.url", properties.getSchemaRegistryUrl());
        config.put("specific.avro.reader", true);

        String groupId = properties.getConsumer().getGroupId();
        if (groupId == null || groupId.isBlank()) {
            groupId = environment.getProperty("spring.application.name", "beynd-app");
        }
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    @ConditionalOnMissingBean
    public DeadLetterRecoverer beyndDeadLetterRecoverer(
            KafkaTemplate<String, byte[]> producerKafkaTemplate
    ) {
        return new DeadLetterRecoverer(producerKafkaTemplate, properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public CommonErrorHandler beyndErrorHandler(DeadLetterRecoverer recoverer) {
        var consumerProps = properties.getConsumer();

        ExponentialBackOffWithMaxRetries backOff =
                new ExponentialBackOffWithMaxRetries(consumerProps.getMaxRetries());

        backOff.setInitialInterval(consumerProps.getBackoffInitialIntervalMs());
        backOff.setMultiplier(consumerProps.getBackoffMultiplier());
        backOff.setMaxInterval(consumerProps.getBackoffMaxIntervalMs());

        return new DefaultErrorHandler(recoverer, backOff);
    }

    @Bean(name = BEYND_LISTENER_CONTAINER_FACTORY_BEAN_NAME)
    @ConditionalOnMissingBean(name = BEYND_LISTENER_CONTAINER_FACTORY_BEAN_NAME)
    public ConcurrentKafkaListenerContainerFactory<Object, Object> beyndKafkaListenerContainerFactory(
            ConsumerFactory<Object, Object> beyndConsumerFactory,
            CommonErrorHandler errorHandler,
            ObjectProvider<ProcessedEventRepository> processedEventRepositoryProvider
    ) {
        var factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();
        factory.setConsumerFactory(beyndConsumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        factory.setConcurrency(properties.getConsumer().getConcurrency());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);

        processedEventRepositoryProvider.ifAvailable(repo ->
                factory.setRecordInterceptor(new IdempotentRecordInterceptor(repo)));

        return factory;
    }
}
