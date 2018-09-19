package com.anthem.voyager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class DBInteraction {
    private static final Logger LOGGER = LoggerFactory.getLogger(DBInteraction.class);
    private String USER = "AF55267@DEVAD.WELLPOINT.COM";
    private String TABLE_NAME = "voyager_dq";
    private String KEYTAB = "src/main/resources/hbase_interact.keytab";
    private Table voyagerDQTable;
    private Configuration conf;
    private Connection conn;

    public DBInteraction() {
        LOGGER.info("Create HBase connection and config");
        System.setProperty("java.security.krb5.conf", "src/main/resources/krb5.conf");
        System.setProperty("sun.security.krb5.debug", "true");
        if (System.getProperty("os.name").toLowerCase().contains("win")) {
            System.setProperty("hadoop.home.dir", "C:\\Users\\AF55267\\Documents\\software\\winutils");
        }
        conf = HBaseConfiguration.create();
        conf.addResource("src/main/resources/hbase-site.xml");
        try {
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(USER, KEYTAB);
            conn = ConnectionFactory.createConnection(conf);
            LOGGER.debug("Successfully created connection");
            voyagerDQTable = conn.getTable(TableName.valueOf(TABLE_NAME));
            LOGGER.info("Table handle:{}", voyagerDQTable.getTableDescriptor().getNameAsString());
        } catch (IOException e) {
            LOGGER.error("Err connecting to Hbase", e);
            throw new RuntimeException(e);
        }

    }

}
