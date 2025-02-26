package com.crystaltechbd.crystal_kafka_module.consumer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class GenericKafkaConsumerTest {

    @InjectMocks
    private GenericKafkaConsumer<MyMessage> genericKafkaConsumer;

    @Mock
    private KafkaReceiver<String, String> kafkaReceiver;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private ReceiverRecord<String, String> mockRecord;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testSubscribeToTopics() {
        // Given
        List<String> topics = List.of("test-topic");

        // When
        genericKafkaConsumer.subscribeToTopics(topics);

        // Then
        // Verify that KafkaReceiver is created with the correct options
        verify(kafkaReceiver).receive();
    }

    @Test
    public void testListen() throws JsonProcessingException {
        // Given
        String jsonMessage = "{\"key\":\"value\"}";
        MyMessage expectedMessage = new MyMessage("value");
        when(kafkaReceiver.receive()).thenReturn(Flux.just(mockRecord));
        when(mockRecord.value()).thenReturn(jsonMessage);
        when(objectMapper.readValue(jsonMessage, MyMessage.class)).thenReturn(expectedMessage);

        // When
        Flux<MyMessage> result = genericKafkaConsumer.listen(MyMessage.class);

        // Then
        result.subscribe(msg -> {
            assertEquals(expectedMessage, msg);
            System.out.println("Received message: " + msg);
        });

        // Verify that the deserialization method was called
        verify(objectMapper, times(1)).readValue(jsonMessage, MyMessage.class);
    }

    @Test
    public void testDeserializeMessage() {
        // Given
        String jsonMessage = "{\"key\":\"value\"}";
        MyMessage expectedMessage = new MyMessage("value");

        // When
        Mono<MyMessage> result = genericKafkaConsumer.deserializeMessage(jsonMessage, MyMessage.class);

        // Then
        MyMessage message = result.block();
        assertEquals(expectedMessage, message);
    }

    @Test
    public void testDeserializeMessage_Error() {
        // Given
        String invalidJsonMessage = "{\"key\":\"value\""; // Invalid JSON

        // When
        Mono<MyMessage> result = genericKafkaConsumer.deserializeMessage(invalidJsonMessage, MyMessage.class);

        // Then
        result.subscribe(
                msg -> {}, // No action on success
                error -> assertEquals("Error deserializing message", error.getMessage())
        );
    }

    // Sample Message Class for Testing
    public static class MyMessage {
        private String key;

        public MyMessage() {}

        public MyMessage(String key) {
            this.key = key;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof MyMessage)) return false;
            MyMessage myMessage = (MyMessage) o;
            return key.equals(myMessage.key);
        }

        @Override
        public int hashCode() {
            return key.hashCode();
        }
    }
}
