package com.crystaltechbd.crystal_kafka_module.config;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import reactor.kafka.sender.KafkaSender;

@SpringBootTest(classes = ReactiveKafkaProducerConfig.class)
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=localhost:9092",
        "spring.kafka.producer.acks=all"
})
public class ReactiveKafkaProducerConfigTest {

    @Autowired
    private KafkaSender<String, String> reactiveStringkafkaSender;

    @Autowired
    private KafkaSender<String, Object> reactiveObjectkafkaSender;

    @Test
    public void testReactiveStringkafkaSenderBeanCreation() {
        // Verify that the KafkaSender bean for String messages is not null
        assertNotNull(reactiveStringkafkaSender, "ReactiveStringkafkaSender bean should have been created");
    }

    @Test
    public void testReactiveObjectkafkaSenderBeanCreation() {
        // Verify that the KafkaSender bean for Object messages is not null
        assertNotNull(reactiveObjectkafkaSender, "ReactiveObjectkafkaSender bean should have been created");
    }
}
