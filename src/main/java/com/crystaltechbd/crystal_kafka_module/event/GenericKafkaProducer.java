package com.crystaltechbd.crystal_kafka_module.event;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;


import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class GenericKafkaProducer<T> {
    private final KafkaSender<String, String> kafkaSender;
    private final ObjectMapper objectMapper;
    private final AtomicInteger counter = new AtomicInteger();
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    public GenericKafkaProducer() {
        SenderOptions<String, String> senderOptions = SenderOptions.create(
                Map.of("bootstrap.servers", bootstrapServers,
                        "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                        "value.serializer", "org.apache.kafka.common.serialization.StringSerializer"));
        this.kafkaSender = KafkaSender.create(senderOptions);
        this.objectMapper = new ObjectMapper();
    }

    public Mono<Void> sendMessage(String topic, T message) {
        try {
            String jsonMessage = objectMapper.writeValueAsString(message);
            return kafkaSender.send(Mono.just(SenderRecord.create(topic, null, null, "key-" + counter.incrementAndGet(), jsonMessage, null)))
                    .then();
        } catch (Exception e) {
            return Mono.error(new RuntimeException("Error serializing message", e));
        }
    }

}




