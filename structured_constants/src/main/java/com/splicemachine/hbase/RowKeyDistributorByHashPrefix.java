package com.splicemachine.hbase;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Provides handy methods to distribute
 * 
 * @author Alex Baranau
 */
public class RowKeyDistributorByHashPrefix extends RowKeyDistributor {
    private Hasher hasher;

    /** Constructor reflection. DO NOT USE */
    public RowKeyDistributorByHashPrefix() {
    }

    public RowKeyDistributorByHashPrefix(Hasher hasher) {
        this.hasher = hasher;
    }

    public static interface Hasher {
        byte[] getHashPrefix(byte[] originalKey);

        byte[][] getAllPossiblePrefixes();

        int getPrefixLength(byte[] adjustedKey);
    }

    @Override
    public byte[] getDistributedKey(byte[] originalKey) {
        return Bytes.add(hasher.getHashPrefix(originalKey), originalKey);
    }

    @Override
    public byte[] getOriginalKey(byte[] adjustedKey) {
        int prefixLength = hasher.getPrefixLength(adjustedKey);
        if (prefixLength > 0) {
            return Bytes.tail(adjustedKey, adjustedKey.length - prefixLength);
        } else {
            return adjustedKey;
        }
    }

    @Override
    public byte[][] getAllDistributedKeys(byte[] originalKey) {
        byte[][] allPrefixes = hasher.getAllPossiblePrefixes();
        byte[][] keys = new byte[allPrefixes.length][];
        for (int i = 0; i < allPrefixes.length; i++) {
            keys[i] = Bytes.add(allPrefixes[i], originalKey);
        }
        return keys;
    }
}