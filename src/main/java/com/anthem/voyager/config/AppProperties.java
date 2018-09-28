package com.anthem.voyager.config;

public class AppProperties {

    public static final String THREAD_PREFIX = "hbaseRx-%d";
    public static final String COL_FAMILY = "all_cf";
    public static final String COL_NAME = "sanity_col";
    public static final long SLEEP_AFTER_STREAM_MS = 5000;

    public static final int[][] FIELD_WIDTHS = {{6, 36}, {36, 37}, {893, 901}, {901, 909}, {994, 1009}, {1092, 1099}};
}
