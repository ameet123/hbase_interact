package com.anthem.voyager.api;

import com.anthem.voyager.StringProcessor;
import com.anthem.voyager.config.AppConfig;
import com.anthem.voyager.service.DBInteraction;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is the glue which provides a coherent api for performing the requirement of check and insert
 * it converts the line to fields, generates keys, checks and inserts and creates the dupe record output.
 * for each of these tasks, it may employ other components
 */
@Component
public class QualityApi {
    private static final Logger LOGGER = LoggerFactory.getLogger(QualityApi.class);
    private static final String DUPE_FILE_PREFIX = "duplicateClaims__";
    private final StringProcessor stringProcessor;
    private DBInteraction dbInteraction;
    private AtomicInteger fileCounter = new AtomicInteger(1);
    private AppConfig appConfig;

    /**
     * create output dir
     *
     * @param stringProcessor
     * @param dbInteraction
     */
    @Autowired
    public QualityApi(StringProcessor stringProcessor, DBInteraction dbInteraction, AppConfig appConfig) {
        this.stringProcessor = stringProcessor;
        this.dbInteraction = dbInteraction;
        this.appConfig = appConfig;
        File outputDir = new File(appConfig.getOutputDir());
        LOGGER.info("Creating output dir:{}", appConfig.getOutputDir());
        try {
            FileUtils.forceDelete(outputDir);
        } catch (IOException e) {
            LOGGER.error("Dir:{} probably does not exist");
        }
        try {
            FileUtils.forceMkdir(outputDir);
        } catch (IOException e) {
            LOGGER.error("Err: creating or deleting output dir:{}", appConfig.getOutputDir(), e);
            throw new RuntimeException("Error Creating output dir");
        }
    }

    /**
     * 1. process the lines into fields and get corresponding row keys as byte[] based on murmurhash int
     * 2. perform check and put
     * 3. get Dupe array
     *
     * @param lines lines from file.
     */
    public void ingest(List<String> lines) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        List<byte[]> rowKeys = stringProcessor.getRowKeyByteArray(lines);
        LOGGER.debug("Processing keys list:{}", rowKeys.size());
        List<Integer> notToDo = dbInteraction.checkAndPutKeys(rowKeys);
        List<Integer> fileDupes = stringProcessor.findFileDupes(rowKeys);
        Set<Integer> allDupes = new HashSet<>(notToDo);
        allDupes.addAll(fileDupes);

        if (!allDupes.isEmpty()) {
            // create file name
            String dupeDataFileName = DUPE_FILE_PREFIX + Thread.currentThread().getName() + "__" + fileCounter
                    .incrementAndGet();
            File dupeFile = Paths.get(appConfig.getOutputDir(), dupeDataFileName).toFile();
            LOGGER.info("Writing duplicate records to:{}", dupeFile.getAbsolutePath());
            List<String> dupeToWrite = new ArrayList<>();
            allDupes.forEach(counter -> dupeToWrite.add(lines.get(counter)));
            LOGGER.info(">> Total dupe records to write:{}", dupeToWrite.size());
            try {
                FileUtils.writeLines(dupeFile, dupeToWrite);
            } catch (IOException e) {
                LOGGER.error("Err: writing list to duplicate file", e);
            }
        }
        stopWatch.stop();
        LOGGER.info("Task done in:{} ms.", stopWatch.getTime(TimeUnit.MILLISECONDS));
    }
}
