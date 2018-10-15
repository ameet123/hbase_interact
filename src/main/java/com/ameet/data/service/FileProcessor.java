package com.ameet.data.service;

import com.ameet.data.config.AppConfig;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * read the data file and process lines in it
 * This is one way of streaming messages into the bus, where we read a file and use the lines as messages.
 */
@Service
public class FileProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileProcessor.class);
    private final EventBus bus;
    private Path dataPath;

    @Autowired
    public FileProcessor(EventBus bus, AppConfig appConfig) {
        dataPath = Paths.get(appConfig.getData());
        this.bus = bus;
    }

    public void streamFile() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        final int[] cnt = {0};
        try {
            try (Stream<String> stream = Files.lines(dataPath)) {
                stream.forEach(o -> {
                            bus.send(o);
                            cnt[0]++;
                        }
                );
            }
        } catch (IOException e) {
            LOGGER.error("Error opening file:", e);
        }
        stopWatch.stop();
        LOGGER.info("Streamed in:{} ms.", cnt[0], stopWatch.getTime(TimeUnit.MILLISECONDS));
    }
}
