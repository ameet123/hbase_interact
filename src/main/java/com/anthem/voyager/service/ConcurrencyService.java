package com.anthem.voyager.service;

import com.anthem.voyager.config.AppConfig;
import com.anthem.voyager.config.AppProperties;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

@Configuration
public class ConcurrencyService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrencyService.class);
    private final AppConfig appConfig;

    @Autowired
    public ConcurrencyService(AppConfig appConfig) {
        this.appConfig = appConfig;
    }

    @Bean
    public ExecutorService executorService() {
        ThreadFactory threadFactory = new BasicThreadFactory.Builder().namingPattern(AppProperties.THREAD_PREFIX)
                .build();
        LOGGER.info("Creating excutor service with capacity:{}", appConfig.getThreadPoolSize());
        return Executors.newFixedThreadPool(appConfig.getThreadPoolSize(), threadFactory);
    }
}
