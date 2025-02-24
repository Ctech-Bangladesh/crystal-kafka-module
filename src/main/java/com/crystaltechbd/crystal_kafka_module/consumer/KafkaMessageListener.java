package com.crystaltechbd.crystal_kafka_module.consumer;


import com.crystaltechbd.crystal_kafka_module.dbconfig.KafkaToPostgres;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaMessageListener {

    Logger log = LoggerFactory.getLogger(KafkaMessageListener.class);
    KafkaToPostgres kafkaToPostgres = new KafkaToPostgres();

    @KafkaListener(
            topics = "${spring.kafka.topic.name}"
            ,groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consumeEvents(Object event) {
        log.info("consumer consume the events {} ", event.toString());
        kafkaToPostgres.saveObjectToDB(event);
    }
}
