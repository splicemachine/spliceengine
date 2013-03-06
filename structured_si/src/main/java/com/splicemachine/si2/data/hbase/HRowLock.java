package com.splicemachine.si2.data.hbase;


import com.splicemachine.si2.data.api.SRowLock;
import org.apache.hadoop.hbase.client.RowLock;

public class HRowLock implements SRowLock {
    RowLock lock;

    public HRowLock(org.apache.hadoop.hbase.client.RowLock lock) {
        this.lock = lock;
    }
}