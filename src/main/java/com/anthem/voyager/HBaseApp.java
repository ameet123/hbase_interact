package com.anthem.voyager;


import com.anthem.voyager.config.AppConfig;
import com.anthem.voyager.config.AppProperties;
import com.anthem.voyager.service.ConcurrencyService;
import com.anthem.voyager.service.EventBus;
import com.anthem.voyager.service.FileProcessor;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class HBaseApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseApp.class);
    private final FileProcessor processor;
    private final EventBus bus;
    private final ConcurrencyService service;
    private final AppConfig appConfig;
    private Random rand = new Random();

    @Autowired
    public HBaseApp(FileProcessor processor, EventBus bus, ConcurrencyService service, AppConfig appConfig) {
        this.processor = processor;
        this.bus = bus;
        this.service = service;
        this.appConfig = appConfig;
    }

    public static void main(String[] args) {
        ConfigurableApplicationContext ctx = SpringApplication.run(HBaseApp.class, args);
        LOGGER.info("Shutting down the application.");
        ctx.close();
    }

    @Bean
    public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
        return args -> {
            LOGGER.info("Starting HBase interact app.");
            StopWatch watch = new StopWatch();
            watch.start();

            LOGGER.info("From datafile:{} outputDir:{}", appConfig.getData(), appConfig.getOutputDir());

            // The actual process
            processor.streamFile();
            Thread.sleep(AppProperties.SLEEP_AFTER_STREAM_MS);

            closeMe();
            watch.stop();
            LOGGER.info(">>Completed ALL in:{}", watch.getTime(TimeUnit.SECONDS));
        };
    }

    private void streamTest() {
        for (int i = 0; i < 100; i++) {
            bus.send("Sending from Main App-" + rand.nextInt(5000));
        }
    }

    /**
     * earlier , we were closing the executor service,
     * service.executorService().shutdown();
     * but no need, the app shuts down ok.
     * @throws InterruptedException
     */
    private void closeMe() throws InterruptedException {
        Thread.sleep(10000);
        bus.stop();
    }
}
