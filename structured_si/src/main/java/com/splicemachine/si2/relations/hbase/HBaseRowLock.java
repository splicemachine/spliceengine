package com.splicemachine.si2.relations.hbase;


import com.splicemachine.si2.relations.api.RowLock;

public class HBaseRowLock implements RowLock {
    org.apache.hadoop.hbase.client.RowLock lock;

    public HBaseRowLock(org.apache.hadoop.hbase.client.RowLock lock) {
        this.lock = lock;
    }
}