package com.crystaltechbd.crystal_kafka_module;

import static org.mockito.Mockito.*;

import com.crystaltechbd.crystal_kafka_module.consumer.KafkaMessageListener;
import com.crystaltechbd.crystal_kafka_module.dbconfig.KafkaToPostgres;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.annotation.KafkaListener;

@ExtendWith(MockitoExtension.class)
public class KafkaConsumerTest {

    @Mock
    private KafkaToPostgres databaseService; // Replace with your actual postgres Service
    @Mock
    private KafkaMessageListener kafkaMessageListener;

    @Test
    public void testKafkaConsumer() {
        String patientInfo = "{ \"name\": \"Suvonkar Kundu\", \"email\": \"suvonkar.kundu@gmail.com\", \"contractNo\": \"01738019184\" }";
        kafkaMessageListener.consumeEvents(patientInfo);
       // kafkaConsumerService.listen(patientInfo);

        verify(databaseService, times(1)).saveObjectToDB(patientInfo);
    }


}
