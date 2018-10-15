package com.ameet.data.service;

import com.ameet.data.config.AppProperties;
import com.ameet.data.util.Util;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.hadoop.hbase.util.MurmurHash3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class performs the task of building a row key as byte[] from incoming string.
 * This logic can be changed as desired to process the incoming line.
 * parse string and create field tokens
 * Instead of using HBase murmur3 hash, we use the one from guava as it allows us to create 128 bit hash, as opposed
 * to the 32-bit one.
 */
@Component
public class StringProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(StringProcessor.class);

    private MurmurHash3 hashHandle;
    private HashFunction murmur128Handle;

    public StringProcessor() {
        this.hashHandle = new MurmurHash3();
        this.murmur128Handle = Hashing.murmur3_128();
    }

    public List<Integer> getRowKey(List<String> lines) {
        return lines.stream().map(s -> hashHandle.hash(s.getBytes())).collect(Collectors.toList());
    }

    /**
     * this finds the duplicated in the list presented.
     * There is a possibility that some dupes may be missed. Because, this approach relies on duplicates being
     * detected by HBase. But in 2 simulataneous threads, it's possible that both check and get non-existence and
     * both try to insert, in turn missing the duplicates.
     * The fix is straightforward.
     * Design a new event bus, at beginning, while streaming records from the file, stream an additional message on
     * to this bus. This bus can then look at all the records and list duplicates. Completely eliminating this step.
     * May even be faster.
     *
     * @param rowKeys
     * @return
     */
    public List<Integer> findFileDupes(List<byte[]> rowKeys) {
        Set<Integer> uniq = new HashSet<>();
        List<Integer> dupes = new ArrayList<>();
        for (int i = 0; i < rowKeys.size(); i++) {
            if (!uniq.add(Util.byteArrayToInt(rowKeys.get(i)))) {
                dupes.add(i);
            }
        }
        if (!dupes.isEmpty()) {
            LOGGER.info(">>File dupes found:{}", dupes.size());
        }
        return dupes;
    }

    /**
     * ByteBuffer.allocate(4).putInt(hashHandle.hash(s.getBytes())).array()).
     * collect(Collectors.toList());
     *
     * @param lines keys
     * @return byte[] of keys
     */
    public List<byte[]> getRowKeyByteArray(List<String> lines) {
        return lines.stream().
                map(this::buildRowKeyHash).
                collect(Collectors.toList());
    }

    public String buildRowKey(String line) {
        StringBuilder sb = new StringBuilder();
        for (int[] FIELD_WIDTH : AppProperties.FIELD_WIDTHS) {
            String c = line.substring(FIELD_WIDTH[0], FIELD_WIDTH[1]);
            sb.append(c);
        }
        return sb.toString();
    }

    public byte[] buildRowKeyHash(String line) {
        StringBuilder sb = new StringBuilder();
        for (int[] FIELD_WIDTH : AppProperties.FIELD_WIDTHS) {
            String c = line.substring(FIELD_WIDTH[0], FIELD_WIDTH[1]);
            sb.append(c);
        }
        return murmur128Handle.hashString(sb.toString()).asBytes();
    }
}
