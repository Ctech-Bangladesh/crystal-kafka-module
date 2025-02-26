package com.crystaltechbd.crystal_kafka_module.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@Service
public class GenericKafkaConsumer<T> {
    private KafkaReceiver<String, String> kafkaReceiver;
    private final ObjectMapper objectMapper;
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${spring.kafka.consumer.auto-offset-reset}")
    private String autoOffsetReset;
    public GenericKafkaConsumer() {
        this.objectMapper = new ObjectMapper();
    }

    public void subscribeToTopics(List<String> topics) {
        ReceiverOptions<String, String> receiverOptions = ReceiverOptions.<String, String>create(
                        Map.of("bootstrap.servers", bootstrapServers,
                                "group.id", groupId,
                                "auto.offset.reset", autoOffsetReset))
                .subscription(topics);

        kafkaReceiver = KafkaReceiver.create(receiverOptions);
    }

    public Flux<T> listen(Class<T> type) {
        return kafkaReceiver.receive()
                .map(ReceiverRecord::value)
                .flatMap(message -> deserializeMessage(message, type))
                .doOnNext(msg -> System.out.println("Received: " + msg));
    }

    Mono<T> deserializeMessage(String message, Class<T> type) {
        try {
            T object = objectMapper.readValue(message, type);
            return Mono.just(object);
        } catch (Exception e) {
            return Mono.error(new RuntimeException("Error deserializing message", e));
        }
    }
}
