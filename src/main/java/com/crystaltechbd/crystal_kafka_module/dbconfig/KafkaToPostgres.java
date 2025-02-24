package com.crystaltechbd.crystal_kafka_module.dbconfig;


import com.crystaltechbd.crystal_kafka_module.dbquery.QueryManagment;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class KafkaToPostgres {
    Logger log = LoggerFactory.getLogger(KafkaToPostgres.class);
    private final String DB_URL = DatabaseConfig.getUrl();
    private final String DB_USER = DatabaseConfig.getUser();
    private final String DB_PASSWORD = DatabaseConfig.getPassword();
    public void saveObjectToDB(Object object) {
        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER , DB_PASSWORD)) {
            PreparedStatement preparedStatement = connection.prepareStatement(QueryManagment.insertSQL);
            Object records = ((ConsumerRecord<?, ?>) object).value();
            System.out.println(records.toString());
            JsonObject jsonObject = JsonParser.parseString(records.toString())
                    .getAsJsonObject();
            preparedStatement.setString(1, String.valueOf(jsonObject.get("name")));
            preparedStatement.setString(2, String.valueOf(jsonObject.get("email")));
            preparedStatement.setString(3, String.valueOf(jsonObject.get("contactNo")));
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            log.info("Database Exception : {}", e.getMessage());
        }
    }
}
