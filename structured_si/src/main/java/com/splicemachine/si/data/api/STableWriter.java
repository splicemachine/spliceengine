package com.splicemachine.si.data.api;

import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.List;

/**
 * Means of writing to Tables. To be used in conjunction with STableReader and SDataLib. This is an abstraction over the
 * HBase write operations.
 */
public interface STableWriter<Table, Mutation, Put, Delete> {
    void write(Table Table, Put put) throws IOException;
    void write(Table Table, Put put, Integer rowLock) throws IOException;
    void write(Table Table, Put put, boolean durable) throws IOException;
    void write(Table Table, List<Put> puts) throws IOException;
    OperationStatus[] writeBatch(Table table, Pair<Mutation, Integer>[] puts) throws IOException;

    void delete(Table Table, Delete delete, Integer lock) throws IOException;

		/**
		 * @param table the table to lock on
		 * @param rowKey the row to lock
		 * @return the id of the lock acquired, or {@code null} if the lock was not able to be acquired.
		 */
		Integer tryLock(Table table, byte[] rowKey) throws IOException;
		//TODO -sf- can we replace this with an actual Lock abstraction?
    Integer lockRow(Table Table, byte[] rowKey) throws IOException;
    void unLockRow(Table Table, Integer lock) throws IOException;
    boolean checkAndPut(Table Table, byte[] family, byte[] qualifier, byte[] expectedValue, Put put) throws IOException;
}
