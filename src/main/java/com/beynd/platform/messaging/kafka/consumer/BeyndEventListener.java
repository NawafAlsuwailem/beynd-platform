package com.beynd.platform.messaging.kafka.consumer;

import com.beynd.platform.messaging.kafka.consumer.config.ConsumerAutoConfiguration;
import org.springframework.core.annotation.AliasFor;
import org.springframework.kafka.annotation.KafkaListener;

import java.lang.annotation.*;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@KafkaListener(containerFactory = ConsumerAutoConfiguration.LISTENER_FACTORY)
public @interface BeyndEventListener {

    /**
     * Kafka topic to subscribe to.
     */
    @AliasFor(annotation = KafkaListener.class, attribute = "topics")
    String topic();

    /**
     * Event (Avro) type to be consumed.
     * This is primarily for documentation / future tooling;
     * Spring’s type resolution will use the method parameter.
     */
    Class<?> type();

    /**
     * Optional explicit group id override.
     */
    @AliasFor(annotation = KafkaListener.class, attribute = "groupId")
    String groupId() default "";
}
