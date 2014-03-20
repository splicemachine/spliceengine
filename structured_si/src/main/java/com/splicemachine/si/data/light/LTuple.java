package com.splicemachine.si.data.light;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.Bytes;

import com.splicemachine.si.data.api.SRowLock;

public class LTuple extends Mutation {
    final byte[] key;
    final List<Cell> values;
    final SRowLock lock;

    public LTuple(byte[] key, List<Cell> values) {
        this(key, values, Maps.<String, byte[]>newHashMap(), null);
    }

    public LTuple(byte[] key, List<Cell> values, Map<String, byte[]> attributes) {
        this(key, values, attributes, null);
    }

    public LTuple(byte[] key, List<Cell> values, Map<String, byte[]> attributes, SRowLock lock) {
        this.key = key;
        this.values = values;
        this.lock = lock;
        for (Map.Entry<String, byte[]> attributePair : attributes.entrySet()) {
            super.setAttribute(attributePair.getKey(), attributePair.getValue());
        }
    }

    public LTuple(byte[] key, List<Cell> values, SRowLock lock) {
        this(key, values, Maps.<String, byte[]>newHashMap(), lock);
    }

    /*
     * Methods for interface compliance with OperationWithAttributes, we don't actually use them anywhere.
     */
    @Override
    public Map<String, Object> getFingerprint() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Object> toMap(int maxCols) {
        throw new UnsupportedOperationException();
    }

    public List<Cell> getValues() {
        List<Cell> keyValues = Lists.newArrayList(values);
        LStore.sortValues(keyValues);
        return keyValues;
    }

    @Override
    public String toString() {
        return "<" + Bytes.toString(key) + " " + values + ">";
    }

}
