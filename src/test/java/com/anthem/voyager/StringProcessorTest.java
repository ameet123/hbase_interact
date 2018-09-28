package com.anthem.voyager;

import com.anthem.voyager.util.Util;
import org.apache.hadoop.hbase.util.MurmurHash3;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.anthem.voyager.config.AppProperties.FIELD_WIDTHS;

public class StringProcessorTest {
    private static final String SAMPLE_LINE = "418232181906244849000999            3INGACC01          8302GHINGGRP03 " +
            "  " +
            "     693768            INGR#GHB1                                                                        " +
            "        MARCIA       LEN               197003212101 MARCIA      LEN                19700321244410623    " +
            "         3                                                                                              " +
            "                                                                                                        " +
            "                                                                                                        " +
            "                                                                                                        " +
            "                                                                                                        " +
            "                                       F                              822        30* 004336  " +
            "2018081120180601201806010000000000200006911731        0310MG           " +
            "201}0000010000000001000001}00100000008271009101SINGULAIR    TAB 10MG         N                          " +
            "                        0102323MDCAID    IN " +
            "0000000007N0000003000}0000000748Q0000000000{0000000000{0000000000{0000000756L0000000000{0000000000" +
            "{0000000000{0000000756L0000000756L0000000000{0000000000{ " +
            "0000000000{0000000000{0000000000{0000000000{0000003000}                                                 " +
            "                                                   N0000000000{A0000000000091320{          " +
            "0000076100{0000000000{03  030000003000}0000000000                                                       " +
            "                                     " +
            "0000000000{0000000000{0000000000{0000000000{0000000000{0000000007N0000000748Q0000000000{0000000000" +
            "{0000000000{0000000000{0000000000{0000000000{0000000000{0000000000{0000000000{0000000000{               " +
            "                                                                                                        " +
            "                                                                                                        " +
            "                            032                                 " +
            "0000000000{0000000000{000000000000000000000{0000000000{0000000000{0000000000{  " +
            "0000000000{0000000000{0000000000{                                                                       " +
            "                             00000000000{0000000000{0000000000{0000000000000000000000{0000000000{DD     " +
            "                      0000000000{00000000000{0000000000{00000000000000000000000000000000000{            " +
            "                                                                                                        " +
            "                         000000000002456435447                    983255351            ANDREMENE        " +
            "                    MRENAK                                   20234 FARMINGTON RD                     " +
            "LIVONIA           MI48152                   44505050100330O094444MONTELUKAST SODIUM TAB 10 MG (EA       " +
            "                       N000Y TABS47020  0N481024 00                             00  0                   " +
            "                          OR               1  0000000000 20180101201801010000000000000000               " +
            "         0000000000      0000          0000000000{ 0000000000{                                          " +
            "                                                                                                        " +
            "                                                                                                 " +
            "0000000000{0000000000{0000000000{                                                                       " +
            "                                                                                                        " +
            "                         878 8997098                                                                    " +
            "                                                                                                        " +
            "                                                                                                        " +
            "                                                                                      \n";

    private StringProcessor stringProcessor = new StringProcessor();
    private MurmurHash3 hashHandle = new MurmurHash3();

    @Test
    public void test2DArray() {
        System.out.println(Arrays.deepToString(FIELD_WIDTHS));
    }

    @Test
    public void testParseSubstring() {
        for (int i = 0; i < FIELD_WIDTHS.length; i++) {
            String f = SAMPLE_LINE.substring(FIELD_WIDTHS[i][0], FIELD_WIDTHS[i][1]);
            System.out.println(String.format("field:[%d]=>%s", i, f));
        }
    }

    @Test
    public void testHashing() {
        List<String> lines = getLines();
        List<Integer> rowKeys = stringProcessor.getRowKey(lines);
        rowKeys.forEach(System.out::println);
        List<Integer> rowKeys2 = stringProcessor.getRowKey(lines);
        Assert.assertTrue(rowKeys.equals(rowKeys2));
    }

    @Test
    public void testHashingByteArray() {
        List<String> lines = new ArrayList<>();
        lines.add(SAMPLE_LINE);
        List<byte[]> rowKeys = stringProcessor.getRowKeyByteArray(lines);
        rowKeys.forEach(bytes -> System.out.println(Util.byteArrayToInt(bytes)));
    }

    @Test
    public void testHashConversion() {
        String s = stringProcessor.buildRowKey(SAMPLE_LINE);
        System.out.println(String.format("Row key:%s", s));
        int myHash = hashHandle.hash(s.getBytes());
        System.out.println(String.format("My Hash:%d", myHash));
        byte[] sHashArray = ByteBuffer.allocate(4).putInt(hashHandle.hash(s.getBytes())).array();
        System.out.println(String.format("Array back to int:%d", Util.byteArrayToInt(sHashArray)));
    }

    private List<String> getLines() {
        List<String> lines = new ArrayList<>();
        for (int[] FIELD_WIDTH : FIELD_WIDTHS) {
            lines.add(SAMPLE_LINE.substring(FIELD_WIDTH[0], FIELD_WIDTH[1]));
        }
        return lines;
    }
}