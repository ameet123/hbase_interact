package com.anthem.voyager;

import com.anthem.voyager.util.Util;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.MurmurHash3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.anthem.voyager.config.AppProperties.FIELD_WIDTHS;

/**
 * parse string and create field tokens
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

    public List<Get> buildKeyGet(List<String> keys) {
        return keys.stream().map(k -> new Get(k.getBytes())).collect(Collectors.toList());
    }

    public List<Integer> getRowKey(List<String> lines) {
        return lines.stream().map(s -> hashHandle.hash(s.getBytes())).collect(Collectors.toList());
    }

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
//                map(s -> ByteBuffer.allocate(4).putInt(hashHandle.hash(buildRowKey(s).getBytes())).array()).
        collect(Collectors.toList());
    }

    public String buildRowKey(String line) {
        StringBuilder sb = new StringBuilder();
        for (int[] FIELD_WIDTH : FIELD_WIDTHS) {
            String c = line.substring(FIELD_WIDTH[0], FIELD_WIDTH[1]);
            sb.append(c);
        }
        return sb.toString();
    }

    public byte[] buildRowKeyHash(String line) {
        StringBuilder sb = new StringBuilder();
        for (int[] FIELD_WIDTH : FIELD_WIDTHS) {
            String c = line.substring(FIELD_WIDTH[0], FIELD_WIDTH[1]);
            sb.append(c);
        }
        return murmur128Handle.hashString(sb.toString()).asBytes();
    }
}
