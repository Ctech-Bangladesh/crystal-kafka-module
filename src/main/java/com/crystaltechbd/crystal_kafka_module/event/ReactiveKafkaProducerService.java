package com.crystaltechbd.crystal_kafka_module.event;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

@Service
public class ReactiveKafkaProducerService {
    private final KafkaSender<String, String> kafkaSender;
    Logger logger = LogManager.getLogger(ReactiveKafkaProducerService.class);
    public ReactiveKafkaProducerService(KafkaSender<String, String> kafkaSender) {
        this.kafkaSender = kafkaSender;
    }

    public Mono<Void> sendMessage(String topic, String key, String message) {
        SenderRecord<String, String, String> senderRecord =
                SenderRecord.create(topic, null, System.currentTimeMillis(), key, message, null);

        return kafkaSender.send(Mono.just(senderRecord))
                .doOnNext(result -> logger.info("Sent message: {} to topic: {}", message, topic))
                .then();
    }
}
