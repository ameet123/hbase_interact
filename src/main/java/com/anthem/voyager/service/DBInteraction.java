package com.anthem.voyager.service;


import com.anthem.voyager.config.AppConfig;
import com.anthem.voyager.config.AppProperties;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
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

    public void checkAndPut(List<String> keys) {
        if (keys == null || keys.isEmpty()) {
            return;
        }
        LOGGER.info(">>Check count:{}", keys.size());
        List<String> toDo = gets(keys);
        if (toDo == null || toDo.isEmpty()) {
            LOGGER.info("No rows to insert, bailing...");
            return;
        }
        LOGGER.info(">>Insert Count:{}", toDo.size());
        insert(toDo);
    }

    public void insert(List<String> rowKeys) {
        List<Put> puts = rowKeys.parallelStream().map(key -> {
            Put p = new Put(key.getBytes());
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

    public List<String> gets(List<String> rowKeys) {
        List<Get> gets = rowKeys.parallelStream().map(key -> new Get(key.getBytes())).collect(Collectors.toList());
        LOGGER.info("Total records in get:{}", gets.size());
        StopWatch stopwatch = new StopWatch();
        stopwatch.start();
        boolean[] existList;
        List<String> toDo = new ArrayList<>();
        try {
            existList = voyagerDQTable.existsAll(gets);
            stopwatch.stop();
            int[] validInvalidCnt = validInvalidCount(existList);
            LOGGER.info("-->Final: valid:{} invalid:{} in:{} ms.", validInvalidCnt[0], validInvalidCnt[1],
                    stopwatch.getTime(TimeUnit.MILLISECONDS));
            for (int i = 0; i < existList.length; i++) {
                if (!existList[i]) {
                    toDo.add(rowKeys.get(i));
                }
            }
            LOGGER.info("ToDo list size:{}", toDo.size());

        } catch (IOException e) {
            LOGGER.error("IO Error", e);
        }
        return toDo;
    }

    private int[] validInvalidCount(boolean[] existList) {
        int valid = 0, invalid = 0;
        for (boolean e : existList) {
            if (e) {
                valid++;
            } else {
                invalid++;
            }
        }
        return new int[]{valid, invalid};
    }
}