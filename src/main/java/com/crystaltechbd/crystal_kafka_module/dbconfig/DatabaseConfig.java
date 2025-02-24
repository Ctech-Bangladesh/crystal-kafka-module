package com.crystaltechbd.crystal_kafka_module.dbconfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class DatabaseConfig {
    private static final Properties properties = new Properties();
    static Logger log = LoggerFactory.getLogger(DatabaseConfig.class);
    static {
        try (FileInputStream input = new FileInputStream("E:\\CTech\\hsms-consumer\\hsms-consumer\\src\\main\\resources\\config.properties")) {
            properties.load(input);
        } catch (IOException e) {
            log.info("Database Configuration {} ", e.getMessage());
        }
    }

    public static String getUrl() {
        return properties.getProperty("db.url");
    }

    public static String getUser() {
        return properties.getProperty("db.user");
    }

    public static String getPassword() {
        return properties.getProperty("db.password");
    }
}