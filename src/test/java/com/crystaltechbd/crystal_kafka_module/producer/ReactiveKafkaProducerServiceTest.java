package com.crystaltechbd.crystal_kafka_module.producer;



import com.crystaltechbd.crystal_kafka_module.event.ReactiveKafkaProducerService;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class ReactiveKafkaProducerServiceTest {

    @Mock
    private ReactiveKafkaProducerService reactiveKafkaProducerService;

    @Mock
    private KafkaSender<String, String> kafkaSender;

    @Mock
    private SenderResult<String> senderResult;

    @Mock
    private Logger logger;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testSendMessage_Success() {
        // Given
        String topic = "test-topic";
        String key = "test-key";
        String message = "Hello Kafka";

        // Mocking the Kafka sender's behavior
       // when(kafkaSender.send(any(Mono.class))).thenReturn(Mono.just(senderResult));

        // When
        Mono<Void> result = reactiveKafkaProducerService.sendMessage(topic, key, message);

        // Then
        result.subscribe();
        // Verify that the send method was called with the correct sender record
        verify(kafkaSender, times(1)).send(any(Mono.class));
        // Verify that the logging occurs
        verify(logger, times(1)).info("Sent message: {} to topic: {}", message, topic);
    }

    @Test
    public void testSendMessage_Error() {
        // Given
        String topic = "test-topic";
        String key = "test-key";
        String message = "Hello Kafka";

        // Mocking the Kafka sender to simulate an error
        //when(kafkaSender.send(any(Mono.class))).thenReturn(Mono.error(new RuntimeException("Send error")));

        // When
        Mono<Void> result = reactiveKafkaProducerService.sendMessage(topic, key, message);

        // Then
        result.subscribe(
                null,
                error -> {
                    // Assert that an error occurred
                    assertEquals("Send error", error.getMessage());
                }
        );

        // Verify that the send method was called
        verify(kafkaSender, times(1)).send(any(Mono.class));
        // Verify that the logging occurs (in this case, it might not happen if the send fails before reaching the logger)
        verify(logger, never()).info(anyString(), any(), any());
    }
}
