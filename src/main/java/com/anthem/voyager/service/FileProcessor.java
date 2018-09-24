package com.anthem.voyager.service;

import com.anthem.voyager.config.AppProperties;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * read the data file and process lines in it
 */
@Service
public class FileProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileProcessor.class);
    private final DBInteraction dbInteraction;
    private final EventBus bus;
    private Path dataPath;

    @Autowired
    public FileProcessor(DBInteraction dbInteraction, EventBus bus) {
        this.dbInteraction = dbInteraction;
        dataPath = Paths.get(AppProperties.DATA_FILE);
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

    public void insertFile() {
        try {
            try (Stream<String> stream = Files.lines(dataPath)) {
                List<String> keys = stream.collect(Collectors.toList());
                dbInteraction.insert(keys);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @param junkCountPercent number of afke records to create as a percentage of valid records
     */
    public List<String> queryFile(int junkCountPercent) {
        try {
            try (Stream<String> stream = Files.lines(dataPath)) {
                List<String> keys = stream.collect(Collectors.toList());
                String base = RandomStringUtils.random(10) + "-";
                IntStream.range(0, Math.round(junkCountPercent * keys.size())).forEach(i -> keys.add(base + i));
                return dbInteraction.gets(keys);
            }
        } catch (IOException e) {
            LOGGER.error("Err: querying", e);
        }
        return new ArrayList<>();
    }

    public void queryAndPut(int junkCountPercent) {
        List<String> toDo = queryFile(junkCountPercent);
        dbInteraction.insert(toDo);
    }
}
