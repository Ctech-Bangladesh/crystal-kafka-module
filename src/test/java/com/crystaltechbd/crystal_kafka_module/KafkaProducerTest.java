package com.crystaltechbd.crystal_kafka_module;

import static org.mockito.Mockito.*;

import com.crystaltechbd.crystal_kafka_module.event.EventProducer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class KafkaProducerTest {

    @Autowired
    private EventProducer eventProducer;
    @Value("${spring.kafka.topic.name}")
    private String topicName;
    @Test
    public void testSendMessage() {
      EventProducer mockKafkaTemplate = mock(EventProducer.class);
        String patientInfo = "{ \"name\": \"Suvonkar Kundu\", \"email\": \"suvonkar.kundu@gmail.com\", \"contractNo\": \"01738019184\" }";

        eventProducer.sendMessage(patientInfo);

        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Object> messageCaptor = ArgumentCaptor.forClass(Object.class);
        verify(mockKafkaTemplate).sendMessage(patientInfo);
        assert topicCaptor.getValue().equals(topicName);
        assert messageCaptor.getValue().equals(patientInfo);
    }
}
