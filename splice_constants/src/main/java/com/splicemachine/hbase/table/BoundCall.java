package com.splicemachine.hbase.table;

import org.apache.hadoop.hbase.client.coprocessor.Batch;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Created on: 10/23/13
 */
@Deprecated
public interface BoundCall<T,R> extends Batch.Call<T,R>{

    R call(byte[] startKey, byte[] stopKey, T instance) throws IOException;
}
