package com.beynd.platform.messaging.kafka.consumer.idempotency;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.RecordInterceptor;

import java.time.Instant;

@Slf4j
@RequiredArgsConstructor
public class IdempotentRecordInterceptor<K, V>
        implements RecordInterceptor<K, V> {

        private final ProcessedEventRepository processedEventRepository;

        @Override
        public ConsumerRecord<K, V> intercept(
                ConsumerRecord<K, V> record,
                Consumer<K, V> consumer) {

                boolean exists = processedEventRepository.existsByTopicAndPartitionAndOffset(
                        record.topic(),
                        record.partition(),
                        record.offset()
                );

                if (exists) {
                        log.debug(
                                "[Idempotency] Skipping already processed record {}-{}@{}",
                                record.topic(),
                                record.partition(),
                                record.offset()
                        );
                        return null; // tells Spring Kafka to skip processing
                }

                return record;
        }

        @Override
        public void success(
                ConsumerRecord<K, V> record,
                Consumer<K, V> consumer) {

                processedEventRepository.save(
                        ProcessedEvent.builder()
                                .topic(record.topic())
                                .partition(record.partition())
                                .offset(record.offset())
                                .processedAt(Instant.now())
                                .build()
                );

                log.debug(
                        "[Idempotency] Marked record as processed {}-{}@{}",
                        record.topic(),
                        record.partition(),
                        record.offset()
                );
        }
}
