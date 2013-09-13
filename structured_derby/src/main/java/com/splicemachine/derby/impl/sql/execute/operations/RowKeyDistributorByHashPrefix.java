package com.splicemachine.derby.impl.sql.execute.operations;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Provides handy methods to distribute
 * 
 * @author Alex Baranau
 */
public class RowKeyDistributorByHashPrefix extends AbstractRowKeyDistributor {
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

    public static class OneByteSimpleHash implements Hasher {
        /**
         * Creates a new instance of this class.
         */
        public OneByteSimpleHash() {
        }

        // Used to minimize # of created object instances
        // Should not be changed. TODO: secure that
        private static final byte[][] PREFIXES;

        static {
            PREFIXES = new byte[16][];
            for (int i = 0; i < 16; i++) {
                PREFIXES[i] = new byte[] { (byte) ( i * 0x10 ), (byte)0 };
            }
        }

        @Override
        public byte[] getHashPrefix(byte[] originalKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[][] getAllPossiblePrefixes() {
            return PREFIXES;
        }

        @Override
        public int getPrefixLength(byte[] adjustedKey) {
            return 2;
        }
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