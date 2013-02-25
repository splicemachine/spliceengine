package com.splicemachine.derby.impl.store.access;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Wrapper around HTable that gives us a hook for each HTable call.
 */

public class HTableWrapper implements HTableInterface{
	private static Logger LOG = Logger.getLogger(HTableWrapper.class);
	private final HTableInterface table;
	private int lastThreadId;

	public HTableWrapper(HTableInterface table) {
		lastThreadId = currentThreadId();
		this.table = table;
	}

	private int currentThreadId() {
		return System.identityHashCode(Thread.currentThread());
	}

	private void assertSameThread(String m) {
	}

	@Override
	public byte[] getTableName() {
		assertSameThread("getTableName");
		return table.getTableName();
	}

	@Override
	public Configuration getConfiguration() {
		assertSameThread("getConfiguration");
		return table.getConfiguration();
	}

	@Override
	public HTableDescriptor getTableDescriptor() throws IOException {
		assertSameThread("getTableDescriptor");
		return table.getTableDescriptor();
	}

	@Override
	public boolean exists(Get get) throws IOException {
		assertSameThread("exists");
		return table.exists(get);
	}

	@Override
	public void batch(List<Row> actions, Object[] results) throws IOException, InterruptedException {
		assertSameThread("batch");
		table.batch(actions, results);
	}

	@Override
	public Object[] batch(List<Row> actions) throws IOException, InterruptedException {
		assertSameThread("batch 2");
		return table.batch(actions);
	}

	@Override
	public Result get(Get get) throws IOException {
		assertSameThread("get");
		return table.get(get);
	}

	@Override
	public Result[] get(List<Get> gets) throws IOException {
		assertSameThread("get 2");
		return table.get(gets);
	}

	@Override
	public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
		assertSameThread("getRowOrBefore");
		return table.getRowOrBefore(row, family);
	}

	@Override
	public ResultScanner getScanner(Scan scan) throws IOException {
		assertSameThread("getScanner");
		return table.getScanner(scan);
	}

	@Override
	public ResultScanner getScanner(byte[] family) throws IOException {
		assertSameThread("getScanner 2");
		return table.getScanner(family);
	}

	@Override
	public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
		assertSameThread("getScanner 3");
		return table.getScanner(family, qualifier);
	}

	@Override
	public void put(Put put) throws IOException {
		assertSameThread("put");
		table.put(put);
	}

	@Override
	public void put(List<Put> puts) throws IOException {
		assertSameThread("put 2");
		table.put(puts);
	}

	@Override
	public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) throws IOException {
		assertSameThread("checkAndPut");
		return table.checkAndPut(row, family, qualifier, value, put);
	}

	@Override
	public void delete(Delete delete) throws IOException {
		assertSameThread("delete");
		table.delete(delete);
	}

	@Override
	public void delete(List<Delete> deletes) throws IOException {
		assertSameThread("delete 2");
		table.delete(deletes);
	}

	@Override
	public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete) throws IOException {
		assertSameThread("checkAndDelete");
		return table.checkAndDelete(row, family, qualifier, value, delete);
	}

	@Override
	public Result increment(Increment increment) throws IOException {
		assertSameThread("increment");
		return table.increment(increment);
	}

	@Override
	public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount) throws IOException {
		assertSameThread("incrementColumnValue");
		return table.incrementColumnValue(row, family, qualifier, amount);
	}

	@Override
	public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, boolean writeToWAL) throws IOException {
		assertSameThread("incrementColumnValue 2");
		return table.incrementColumnValue(row, family, qualifier, amount, writeToWAL);
	}

	@Override
	public boolean isAutoFlush() {
		assertSameThread("isAutoFlush");
		return table.isAutoFlush();
	}

	@Override
	public void flushCommits() throws IOException {
		assertSameThread("flushCommits");
		table.flushCommits();
	}

	@Override
	public void close() throws IOException {
		assertSameThread("close");
		table.close();
	}

	@Override
	public RowLock lockRow(byte[] row) throws IOException {
		assertSameThread("lockRow");
		return table.lockRow(row);
	}

	@Override
	public void unlockRow(RowLock rl) throws IOException {
		assertSameThread("unlockRow");
		table.unlockRow(rl);
	}

	@Override
	public <T extends CoprocessorProtocol> T coprocessorProxy(Class<T> protocol, byte[] row) {
		assertSameThread("coprocessorProxy");
		return table.coprocessorProxy(protocol, row);
	}

	@Override
	public <T extends CoprocessorProtocol, R> Map<byte[], R> coprocessorExec(Class<T> protocol, byte[] startKey, byte[] endKey, Batch.Call<T, R> callable) throws IOException, Throwable {
		assertSameThread("coprocessorExec");
		return table.coprocessorExec(protocol, startKey, endKey, callable);
	}

	@Override
	public <T extends CoprocessorProtocol, R> void coprocessorExec(Class<T> protocol, byte[] startKey, byte[] endKey, Batch.Call<T, R> callable, Batch.Callback<R> callback) throws IOException, Throwable {
		assertSameThread("coprocessorExec 2");
		table.coprocessorExec(protocol, startKey, endKey, callable, callback);
	}
}
