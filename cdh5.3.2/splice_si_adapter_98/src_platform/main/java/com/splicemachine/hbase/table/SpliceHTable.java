package com.splicemachine.hbase.table;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;
import com.splicemachine.hbase.SpliceBaseHTable;
import com.splicemachine.hbase.regioninfocache.RegionCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.CompareFilter;
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
    public <R extends Message> Map<byte[], R> batchCoprocessorService(Descriptors.MethodDescriptor methodDescriptor, Message request, byte[] startKey, byte[] endKey, R responsePrototype) throws ServiceException, Throwable {
        return table.batchCoprocessorService(methodDescriptor, request, startKey, endKey, responsePrototype);
    }

    @Override
    public <R extends Message> void batchCoprocessorService(Descriptors.MethodDescriptor methodDescriptor, Message request, byte[] startKey, byte[] endKey, R responsePrototype, Batch.Callback<R> callback) throws ServiceException, Throwable {
        table.batchCoprocessorService(methodDescriptor, request, startKey, endKey, responsePrototype, callback);
    }

    @Override
    public boolean checkAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp, byte[] value, RowMutations mutation) throws IOException {
        return table.checkAndMutate(row, family, qualifier, compareOp, value, mutation);
    }
}