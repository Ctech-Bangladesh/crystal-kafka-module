package com.crystaltechbd.crystal_kafka_module.producer;

import com.crystaltechbd.crystal_kafka_module.event.GenericKafkaProducer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class GenericKafkaProducerTest {

    @InjectMocks
    private GenericKafkaProducer<MyMessage> genericKafkaProducer;

    @Mock
    private KafkaSender<String, String> kafkaSender;

    @Mock
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testSendMessage_Success() throws Exception {
        // Given
        String topic = "test-topic";
        MyMessage message = new MyMessage("value");
        String jsonMessage = "{\"key\":\"value\"}";

        when(objectMapper.writeValueAsString(message)).thenReturn(jsonMessage);
       // when(kafkaSender.send(any(Mono.class))).thenReturn(Mono.empty());

        // When
        Mono<Void> result = genericKafkaProducer.sendMessage(topic, message);

        // Then
        result.subscribe();
        // Verify that the message was serialized and sent
        verify(objectMapper, times(1)).writeValueAsString(message);
        verify(kafkaSender, times(1)).send(any(Mono.class));
    }

    @Test
    public void testSendMessage_SerializationError() throws Exception {
        // Given
        String topic = "test-topic";
        MyMessage message = new MyMessage("value");

        when(objectMapper.writeValueAsString(message)).thenThrow(new RuntimeException("Serialization error"));

        // When
        Mono<Void> result = genericKafkaProducer.sendMessage(topic, message);

        // Then
        result.subscribe(
                null,
                error -> assertEquals("Error serializing message", error.getMessage())
        );

        // Verify that the send method was not called due to serialization error
        verify(kafkaSender, never()).send(any(Mono.class));
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
