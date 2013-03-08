package com.splicemachine.si2.data.hbase;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowLock;

import java.util.List;

public interface IHTableWriter {
    void write(HTableInterface table, Put put);
    void write(HTableInterface table, List puts);

    boolean checkAndPut(HTableInterface table, byte[] family, byte[] qualifier, byte[] value, Put put);

    RowLock lockRow(HTableInterface table, byte[] rowKey);
    void unLockRow(HTableInterface table, RowLock lock);
}
