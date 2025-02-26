package com.crystaltechbd.crystal_kafka_module.consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.Set;

@Service
public class ReactiveKafkaConsumerService {
    private final KafkaReceiver<String, String> kafkaReceiver;
    private final ReceiverOptions<String, String> receiverOptions;
    Logger logger = LogManager.getLogger(ReactiveKafkaConsumerService.class);
    public ReactiveKafkaConsumerService(KafkaReceiver<String, String> kafkaReceiver, ReceiverOptions<String, String> receiverOptions) {
        this.kafkaReceiver = kafkaReceiver;
        this.receiverOptions = receiverOptions;
    }

    public Flux<ReceiverRecord<String, String>> consumeFromTopics(Set<String> topics) {
        ReceiverOptions<String, String> options = receiverOptions.subscription(topics);
        return KafkaReceiver.create(options)
                .receive()
                .doOnNext(record -> {
                    // Process the record dynamically
                    logger.info("Received message: key=%s value=%s from topic=%s%n",
                            record.key(), record.value(), record.topic());
                    record.receiverOffset().acknowledge();
                });
    }
}
