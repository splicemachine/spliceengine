package com.splicemachine.hbase;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author Scott Fines
 *         Date: 4/15/14
 */
public class SizedInterval implements Comparable<SizedInterval>{
    byte[] startKey;
    byte[] endKey;
    long bytes;

    SizedInterval(byte[] startKey, byte[] endKey, long bytes) {
        this.startKey = startKey;
        this.endKey = endKey;
        this.bytes = bytes;
    }

    @Override
    public int compareTo(SizedInterval o) {
        return Bytes.compareTo(startKey,o.startKey);
    }

    @Override
    public String toString() {
        return "{["+ Bytes.toStringBinary(startKey)+","+Bytes.toStringBinary(endKey)+"):"+bytes+"}";
    }
}
