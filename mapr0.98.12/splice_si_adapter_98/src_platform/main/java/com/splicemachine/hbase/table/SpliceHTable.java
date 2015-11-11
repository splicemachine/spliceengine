package com.splicemachine.hbase.table;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;
import com.splicemachine.hbase.SpliceBaseHTable;
import com.splicemachine.hbase.regioninfocache.RegionCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;

/**
 * @author Scott Fines
 *         Created on: 10/23/13
 */
public class SpliceHTable extends SpliceBaseHTable {

    public SpliceHTable(byte[] tableName, Configuration configuration, boolean retryAutomatically) throws IOException {
        super(tableName, configuration, retryAutomatically);
    }

    public SpliceHTable(byte[] tableName, HConnection connection, ExecutorService pool,
                        RegionCache regionCache) throws IOException {
        super(tableName, connection, pool, regionCache);
    }

    @Override
    public <R extends Message> Map<byte[], R> batchCoprocessorService(Descriptors.MethodDescriptor methodDescriptor, Message message, byte[] bytes, byte[] bytes2, R r) throws ServiceException, Throwable {
        return table.batchCoprocessorService(methodDescriptor, message, bytes, bytes2, r);
    }

    @Override
    public <R extends Message> void batchCoprocessorService(Descriptors.MethodDescriptor methodDescriptor, Message message, byte[] bytes, byte[] bytes2, R r, Batch.Callback<R> rCallback) throws ServiceException, Throwable {
        batchCoprocessorService(methodDescriptor, message, bytes, bytes2, r, rCallback);
    }
}