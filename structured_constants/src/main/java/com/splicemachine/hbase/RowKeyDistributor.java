package com.splicemachine.hbase;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.util.Arrays;

/**
 * Defines the way row keys are distributed.
 */
public abstract class RowKeyDistributor {

    public abstract byte[] getDistributedKey(byte[] originalKey);

    public abstract byte[] getOriginalKey(byte[] adjustedKey);

    public abstract byte[][] getAllDistributedKeys(byte[] originalKey);

    /**
     * Gets all distributed intervals based on the original start & stop keys.
     * Used when scanning all buckets based on start/stop row keys. Should
     * return keys so that all buckets in which records between originalStartKey
     * and originalStopKey were distributed are "covered".
     * 
     * @param originalStartKey
     *            start key
     * @param originalStopKey
     *            stop key
     * @return array[Pair(startKey, stopKey)]
     */
    public Pair<byte[], byte[]>[] getDistributedIntervals(byte[] originalStartKey, byte[] originalStopKey) {
        byte[][] startKeys = getAllDistributedKeys(originalStartKey);
        byte[][] stopKeys;
        if (Arrays.equals(originalStopKey, HConstants.EMPTY_END_ROW)) {
            Arrays.sort(startKeys, Bytes.BYTES_RAWCOMPARATOR);
            // stop keys are the start key of the next interval
            stopKeys = getAllDistributedKeys(HConstants.EMPTY_BYTE_ARRAY);
            for (int i = 0; i < stopKeys.length - 1; i++) {
                stopKeys[i] = stopKeys[i + 1];
            }
            stopKeys[stopKeys.length - 1] = HConstants.EMPTY_END_ROW;
        } else {
            stopKeys = getAllDistributedKeys(originalStopKey);
            assert stopKeys.length == startKeys.length;
        }

        Pair<byte[], byte[]>[] intervals = new Pair[startKeys.length];
        for (int i = 0; i < startKeys.length; i++) {
            intervals[i] = new Pair<byte[], byte[]>(startKeys[i], stopKeys[i]);
        }

        return intervals;
    }

}