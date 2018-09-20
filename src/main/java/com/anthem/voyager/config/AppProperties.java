package com.anthem.voyager.config;

public class AppProperties {

    public static final String THREAD_PREFIX = "hbaseRx-%d";
    public static final int THREAD_POOL_SIZE = 6;
    public static final int SOURCE_BUFFER_BACKPRESSURE = 50;
    public static final int PUBLISH_BUCKETING_SIZE = 5000;
    public static final int PUBLISH_SEC_THRESHOLD = 5;
    public static final String COL_NAME = "sanity_col";
    public static final String USER = "AF55267@DEVAD.WELLPOINT.COM";
    public static final String TABLE_NAME = "dv_hb_bdfrawz_nogbd_r1a_wh:voyager_dq";
    public static final String KEYTAB = "hbase_interact.keytab";
    public static final String COL_FAMILY = "all_cf";
    public static final String SMALL_FILE = "/sample_hbase.csv";
    private static final String MED_10K_FILE = "/sample_10k_hbase.csv";
    public static final String LG_200K_FILE = "/sample_large_hbase.csv";
    public static final String DATA_FILE = LG_200K_FILE;
}
