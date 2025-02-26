package com.crystaltechbd.crystal_kafka_module.consumer;



import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.core.publisher.Flux;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class ReactiveKafkaConsumerServiceTest {

    @InjectMocks
    private ReactiveKafkaConsumerService reactiveKafkaConsumerService;

    @Mock
    private KafkaReceiver<String, String> kafkaReceiver;

    @Mock
    private ReceiverOptions<String, String> receiverOptions;

    @Mock
    private Logger logger;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testConsumeFromTopics() {
        // Given
        String topic = "test-topic";
        Set<String> topics = Collections.singleton(topic);
        ReceiverRecord<String, String> mockRecord = mock(ReceiverRecord.class);
        when(mockRecord.key()).thenReturn("test-key");
        when(mockRecord.value()).thenReturn("test-value");
        when(mockRecord.topic()).thenReturn(topic);
        //when(mockRecord.receiverOffset()).thenReturn(KafkaTestUtils.getCurrentOffset());

        // Create a Flux that emits the mock record
        Flux<ReceiverRecord<String, String>> flux = Flux.just(mockRecord);
        when(kafkaReceiver.receive()).thenReturn(flux);

        // When
        Flux<ReceiverRecord<String, String>> result = reactiveKafkaConsumerService.consumeFromTopics(topics);

        // Then
        result.subscribe();

        // Verify the processing of the record
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);

        // Verify that the logger was called with the correct message
        verify(logger, times(1)).info(
                argThat(arg -> arg.contains("Received message: key=" + keyCaptor.getValue() +
                        " value=" + valueCaptor.getValue() + " from topic=" + topicCaptor.getValue())),
                Optional.ofNullable(keyCaptor.capture()), valueCaptor.capture(), topicCaptor.capture());

        assertEquals("test-key", keyCaptor.getValue());
        assertEquals("test-value", valueCaptor.getValue());
        assertEquals(topic, topicCaptor.getValue());

        // Verify that the acknowledge method was called
        verify(mockRecord.receiverOffset()).acknowledge();
    }
}
