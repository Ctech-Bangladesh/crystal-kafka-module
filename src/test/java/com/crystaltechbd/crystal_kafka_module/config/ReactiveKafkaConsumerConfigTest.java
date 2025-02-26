package com.crystaltechbd.crystal_kafka_module.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@SpringBootTest
public class ReactiveKafkaConsumerConfigTest {

    @InjectMocks
    private ReactiveKafkaConsumerConfig reactiveKafkaConsumerConfig;

    @Mock
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Mock
    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Mock
    @Value("${spring.kafka.consumer.auto-offset-reset}")
    private String autoOffsetReset;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(bootstrapServers).thenReturn("localhost:9092");
        when(groupId).thenReturn("test-group");
        when(autoOffsetReset).thenReturn("earliest");
    }

    @Test
    public void testKafkaStringReceiverOptions() {
        ReceiverOptions<String, String> options = reactiveKafkaConsumerConfig.kafkaStringReceiverOptions();

        Map<String, Object> props = options.consumerProperties();

        assertEquals("localhost:9092", props.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("test-group", props.get(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals(StringDeserializer.class, props.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        assertEquals(StringDeserializer.class, props.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
        assertEquals("earliest", props.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertEquals(false, props.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    }

    @Test
    public void testKafkaObjectReceiverOptions() {
        ReceiverOptions<String, Object> options = reactiveKafkaConsumerConfig.kafkaObjectReceiverOptions();

        Map<String, Object> props = options.consumerProperties();

        assertEquals("localhost:9092", props.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("test-group", props.get(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals(StringDeserializer.class, props.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        assertEquals(StringDeserializer.class, props.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
        assertEquals("earliest", props.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertEquals(false, props.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    }

    @Test
    public void testKafkaStringReceiver() {
        KafkaReceiver<String, String> receiver = reactiveKafkaConsumerConfig.kafkaStringReceiver(reactiveKafkaConsumerConfig.kafkaStringReceiverOptions());

        assertEquals(KafkaReceiver.class, receiver.getClass());
    }

    @Test
    public void testKafkaObjectReceiver() {
        KafkaReceiver<String, Object> receiver = reactiveKafkaConsumerConfig.kafkaObjectReceiver(reactiveKafkaConsumerConfig.kafkaObjectReceiverOptions());

        assertEquals(KafkaReceiver.class, receiver.getClass());
    }
}
