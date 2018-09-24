package com.anthem.voyager.config;


import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "my")
public class AppConfig {
    private String krb;
    private String keytab;
    private String table;
    private String small;
    private String med;
    private String large;
    private int bucketSize;
    private int threadPoolSize;
    private int thresholdSec;
    private int backpressure;
    private String data;
    private String user;

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getKrb() {
        return krb;
    }

    public void setKrb(String krb) {
        this.krb = krb;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getKeytab() {
        return keytab;
    }

    public void setKeytab(String keytab) {
        this.keytab = keytab;
    }

    public String getSmall() {
        return small;
    }

    public void setSmall(String small) {
        this.small = small;
    }

    public String getMed() {
        return med;
    }

    public void setMed(String med) {
        this.med = med;
    }

    public String getLarge() {
        return large;
    }

    public void setLarge(String large) {
        this.large = large;
    }

    public int getBucketSize() {
        return bucketSize;
    }

    public void setBucketSize(int bucketSize) {
        this.bucketSize = bucketSize;
    }

    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    public void setThreadPoolSize(int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
    }

    public int getThresholdSec() {
        return thresholdSec;
    }

    public void setThresholdSec(int thresholdSec) {
        this.thresholdSec = thresholdSec;
    }

    public int getBackpressure() {
        return backpressure;
    }

    public void setBackpressure(int backpressure) {
        this.backpressure = backpressure;
    }
}