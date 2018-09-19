package com.anthem.voyager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
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
    private String DATA_FILE = "/sample_hbase.csv";
    private Path dataPath;

    @Autowired
    public FileProcessor(DBInteraction dbInteraction) {
        this.dbInteraction = dbInteraction;
        try {
            dataPath = Paths.get(this.getClass().getResource(DATA_FILE).toURI());
        } catch (URISyntaxException e) {
            LOGGER.error("Err reading file");
        }
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

    public void queryFile() {
        try {
            try (Stream<String> stream = Files.lines(dataPath)) {
                List<String> keys = stream.collect(Collectors.toList());
                // add junk
                IntStream.range(0, 212).forEach(i -> keys.add("NON-" + i));
                dbInteraction.gets(keys);
            }
        } catch (IOException e) {
            LOGGER.error("Err: querying", e);
        }
    }
}
