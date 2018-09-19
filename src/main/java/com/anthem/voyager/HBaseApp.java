package com.anthem.voyager;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class HBaseApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseApp.class);
    private final FileProcessor processor;

    @Autowired
    public HBaseApp(FileProcessor processor) {
        this.processor = processor;
    }

    public static void main(String[] args) {
        SpringApplication.run(HBaseApp.class, args);
    }

    @Bean
    public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
        return args -> {
            LOGGER.info("Starting HBase interact app.");
            processor.queryFile();
        };
    }
}
