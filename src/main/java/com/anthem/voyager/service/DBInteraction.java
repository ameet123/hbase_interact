package com.anthem.voyager.service;


import com.anthem.voyager.config.AppConfig;
import com.anthem.voyager.config.AppProperties;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
public class DBInteraction {
    private static final Logger LOGGER = LoggerFactory.getLogger(DBInteraction.class);
    private Table voyagerDQTable;

    /**
     * do enable kerberos debug, add this
     * System.setProperty("sun.security.krb5.debug", "true");
     */
    @Autowired
    public DBInteraction(AppConfig appConfig) {
        LOGGER.info("Create HBase connection and config");
        System.setProperty("java.security.krb5.conf", appConfig.getKrb());

        if (System.getProperty("os.name").toLowerCase().contains("win")) {
            System.setProperty("hadoop.home.dir", "C:\\Users\\AF55267\\Documents\\software\\winutils");
        }
        Configuration conf = HBaseConfiguration.create();
        conf.addResource(DBInteraction.class.getResource("/hbase-site.xml").getPath());
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hbase.security.authentication", "kerberos");
        try {
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(appConfig.getUser(), appConfig.getKeytab());
            Connection conn = ConnectionFactory.createConnection(conf);
            LOGGER.debug("Successfully created connection");
            voyagerDQTable = conn.getTable(TableName.valueOf(appConfig.getTable()));
            LOGGER.info("Table handle:{}", voyagerDQTable.getTableDescriptor().getNameAsString());
        } catch (IOException e) {
            LOGGER.error("Err connecting to Hbase", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * based on the row keys, perform an isExist check
     * what is not present, insert
     * what is present, return as byte[]
     *
     * @param keys byte[] of keys
     * @return byte[] of what is duplicate.
     */
    public List<Integer> checkAndPutKeys(List<byte[]> keys) {
        if (keys == null || keys.isEmpty()) {
            return new ArrayList<>();
        }
        LOGGER.info(">>Check count:{}", keys.size());
        PairOfArrays<byte[]> toDoAndNotToDo = keyExists(keys);
        List<byte[]> toDo = toDoAndNotToDo.toDo;
        List<Integer> notToDo = toDoAndNotToDo.notToDo;
        if (toDo != null && !toDo.isEmpty()) {
            LOGGER.info(">>Insert Count:{}", toDo.size());
            insertByteArray(toDo);
        } else {
            LOGGER.info("No rows to insert, bailing...");
        }
        return notToDo;
    }

    public void insertByteArray(List<byte[]> rowKeys) {
        List<Put> puts = rowKeys.parallelStream().map(r -> {
            Put p = new Put(r);
            p.addColumn(AppProperties.COL_FAMILY.getBytes(), AppProperties.COL_NAME.getBytes(), null);
            return p;
        }).collect(Collectors.toList());
        LOGGER.info("Total records in put:{}", puts.size());
        StopWatch stopwatch = new StopWatch();
        stopwatch.start();
        try {
            voyagerDQTable.put(puts);
        } catch (IOException e) {
            LOGGER.error("Err: Inserting records");
        }
        stopwatch.stop();
        LOGGER.info("{} records inserted in:{} ms.", rowKeys.size(), stopwatch.getTime(TimeUnit.MILLISECONDS));
    }

    public PairOfArrays<byte[]> keyExists(List<byte[]> rowKeys) {
        List<Get> gets = rowKeys.stream().map(Get::new).collect(Collectors.toList());
        LOGGER.info("Total records in get:{}", gets.size());
        List<byte[]> toDo = new ArrayList<>();
        List<byte[]> notToDo = new ArrayList<>();
        StopWatch stopwatch = new StopWatch();
        stopwatch.start();
        boolean[] existList;
        List<Integer> duplicateRecIndexes = new ArrayList<>();
        try {
            existList = voyagerDQTable.existsAll(gets);
            stopwatch.stop();
            for (int i = 0; i < existList.length; i++) {
                if (!existList[i]) {
                    toDo.add(rowKeys.get(i));
                } else {
                    duplicateRecIndexes.add(i);
                    notToDo.add(rowKeys.get(i));
                }
            }
            LOGGER.info("ToDo list:{} NotToDo list:{} in:{} ms.", toDo.size(), notToDo.size(), stopwatch.getTime
                    (TimeUnit.MILLISECONDS));

        } catch (IOException e) {
            LOGGER.error("IO Error", e);
        }
        return new PairOfArrays<>(toDo, duplicateRecIndexes);
    }

    static class PairOfArrays<T> {
        List<T> toDo;
        List<Integer> notToDo;

        PairOfArrays(List<T> toDo, List<Integer> notToDo) {
            this.toDo = toDo;
            this.notToDo = notToDo;
        }
    }
}