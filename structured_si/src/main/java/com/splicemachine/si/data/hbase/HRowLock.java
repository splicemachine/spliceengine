package com.splicemachine.si.data.hbase;


import com.splicemachine.si.data.api.SRowLock;
import org.apache.hadoop.hbase.client.RowLock;

/**
 * Represents either an HBase region or table lock and allows them to be handled as the same type.
 */
public class HRowLock implements SRowLock {
    RowLock lock;
    Integer regionRowLock;

    public HRowLock(Integer regionRowLock) {
        this.regionRowLock = regionRowLock;
    }

    public HRowLock(org.apache.hadoop.hbase.client.RowLock lock) {
        this.lock = lock;
    }
}