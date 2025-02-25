package com.crystaltechbd.crystal_kafka_module.dbconfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class DatabaseConfig {
    private static final Properties properties = new Properties();
    static Logger log = LoggerFactory.getLogger(DatabaseConfig.class);
    @Value("${config.file.location}")
    private static String fileLocation;
    static {
        try (FileInputStream input = new FileInputStream(fileLocation)) {
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