package com.splicemachine.hbase.table;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import com.google.common.collect.Lists;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import com.splicemachine.concurrent.MoreExecutors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * Better implementation of an HTablePool.
 *
 * The HBase HTablePool has an inherent Memory leak, in that tables will never
 * be closed and removed from the Pool at any time; thus the need for this class.
 *
 * This implementation attempts to balance the need to prevent an ever-growing cadre of
 * HTable instances with the performance penalty associated with constructing a new HTablePool.
 *
 * To this end, it uses a two-stage system--Tables are checked out of the pool, up to a (configurable)
 * max size. Once the maximum size is reached, those who wish to get an HTable instance must first wait
 * until a table has been released by another thread. If no maximum size is specified, then
 * a new HTable instance is created.
 *
 * When an HTable is released back to the pool, a (configurable) coreSize is checked. If the number
 * of created HTables is greater than the coreSize, then the table is placed onto a queue for asynchronous
 * removal.
 *
 * This asynchronous removal adds an interesting performance enhancement. When a caller requests a table,
 * it first checks if there are any "core" tables available. If there are, then one is returned. If there
 * are not, however, the caller attempts to "steal" an HTable from the closer queue. That is, if there
 * is an HTable which is scheduled for closing, then a caller may unschedule it and thus re-use it. Thus,
 * only if there are both no core HTables available *and* no HTables scheduled for closing available is
 * a new HTable created.
 *
 * @author Scott Fines
 * Created on: 5/7/13
 */
@SuppressWarnings("deprecation")
public class BetterHTablePool {
    private static final Logger LOG = Logger.getLogger(BetterHTablePool.class);
    private final ConcurrentMap<String,TablePool> tablePool;
    private final HTableInterfaceFactory tableFactory;
    private final int maxSize;
    private final int coreSize;

    public BetterHTablePool(HTableInterfaceFactory tableFactory,
                            long cleanerTime,TimeUnit cleanerTimeUnit,
                            int maxSize,
                            int coreSize) {
        tablePool = new ConcurrentHashMap<String, TablePool>();
        this.coreSize = coreSize;
        this.maxSize = maxSize;
        this.tableFactory = tableFactory;
        ScheduledExecutorService closer = MoreExecutors.namedSingleThreadScheduledExecutor("htablePool-cleaner");
        closer.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                Collection<HTableInterface> tablesToClose = Lists.newArrayList();
                for (TablePool pool : tablePool.values()) {
                    tablesToClose.clear();
                    pool.tablesToClose.drainTo(tablesToClose);
                    for (HTableInterface table : tablesToClose) {
                        try {
                            table.close();
                        } catch (IOException e) {
                            SpliceLogUtils.warn(LOG, "Unable to close HTable", e);
                        }
                    }
                }
            }
        }, 0l, cleanerTime, cleanerTimeUnit);
    }

    public HTableInterface getTable(String tableName){
        TablePool tables = tablePool.get(tableName);

        if(tables==null){
            tables = new TablePool(tableName,maxSize,coreSize);
            TablePool other = tablePool.putIfAbsent(tableName, tables);
            if(other!=null)
                tables = other;
        }

        try {
            return new ReturningHTable(tables.get(), tables);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private class TablePool{
        private final String tableName;
        private final BlockingQueue<HTable> tables;
        private final BlockingQueue<HTable> tablesToClose;
        private final int maxSize;
        private final int coreSize;
        private final AtomicInteger outstandingCount = new AtomicInteger(0);

        private TablePool(String tableName,int maxSize, int coreSize) {
            this.tables = new LinkedBlockingQueue<HTable>();
            this.tablesToClose = new LinkedBlockingQueue<HTable>();
            this.maxSize = maxSize;
            this.coreSize = coreSize;
            this.tableName = tableName;
        }

        private HTable get() throws InterruptedException {
            HTable table = tables.poll();
            if(table!=null) return table;

            if(outstandingCount.getAndIncrement()>=maxSize){
                //see if we can get a table from the tablesToClose queue
                //before we create it
                table = tablesToClose.poll();
                if(table==null)
                    table = tables.take(); // wait until tables become available
            }else{
                //see if we can get a table from the tablesToClose queue
                //before we create it
                table = tablesToClose.poll();
                if(table==null)
                    table = (HTable)tableFactory.createHTableInterface(SpliceConstants.config, Bytes.toBytes(tableName));
            }
            return table;
        }

        private void release(HTable table) throws IOException {
            if(outstandingCount.getAndDecrement()>=coreSize){
                tablesToClose.offer(table);
            }else{
                tables.offer(table);
            }
        }
    }

    public static class ReturningHTable implements HTableInterface {
        private final HTable table;
        private final TablePool pool;

        private ReturningHTable(HTable table, TablePool pool) {
            this.table = table;
            this.pool = pool;
        }

        @Override public void close() throws IOException {
            pool.release(table);
        }

        @Override
        public CoprocessorRpcChannel coprocessorService(byte[] row) {
            return table.coprocessorService(row);
        }

        @Override
        public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> service, byte[] startKey, byte[] endKey, Batch.Call<T, R> callable) throws ServiceException, Throwable {
            return table.coprocessorService(service, startKey, endKey, callable);
        }

        @Override
        public <T extends Service, R> void coprocessorService(Class<T> service, byte[] startKey, byte[] endKey,
                                                              Batch.Call<T, R> callable, Batch.Callback<R> callback) throws ServiceException, Throwable {
            table.coprocessorService(service, startKey, endKey, callable, callback);
        }

        @Override
        public HTableDescriptor getTableDescriptor() throws IOException {
            return table.getTableDescriptor();
        }


        @Override
        @Deprecated
        public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
            return table.getRowOrBefore(row, family);
        }

        @Override
        public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
            return table.getScanner(family, qualifier);
        }

        public List<HRegionLocation> getRegionsInRange(byte[] startKey, byte[] endKey) throws IOException{
           return table.getRegionsInRange(startKey, endKey);
        }

        @Override public byte[] getTableName() { return table.getTableName(); }

        @Override
        public TableName getName() {
            return table.getName();
        }

        @Override public Configuration getConfiguration() { return table.getConfiguration(); }
        @Override public boolean exists(Get get) throws IOException { return table.exists(get); }
        @Override public Boolean[] exists(List<Get> gets) throws IOException { return table.exists(gets); }
        @Override public Result get(Get get) throws IOException { return table.get(get); }
        @Override public Result[] get(List<Get> gets) throws IOException { return table.get(gets); }
        @Override public ResultScanner getScanner(Scan scan) throws IOException { return table.getScanner(scan); }
        @Override public ResultScanner getScanner(byte[] family) throws IOException { return table.getScanner(family); }
        @Override public void put(Put put) throws IOException { table.put(put); }
        @Override public void put(List<Put> puts) throws IOException { table.put(puts); }
        @Override public boolean isAutoFlush() { return table.isAutoFlush(); }
        @Override public void flushCommits() throws IOException { table.flushCommits(); }
        @Override public void delete(Delete delete) throws IOException { table.delete(delete); }
        @Override public void delete(List<Delete> deletes) throws IOException { table.delete(deletes); }

        @Override
        public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) throws IOException {
            return table.checkAndPut(row, family, qualifier, value, put);
        }

        @Override
        public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete) throws IOException {
            return table.checkAndDelete(row, family, qualifier, value, delete);
        }

        @Override public Result increment(Increment increment) throws IOException { return table.increment(increment); }

        @Override
        public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount) throws IOException {
            return table.incrementColumnValue(row, family, qualifier, amount);
        }

        @Override
        public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, Durability
                durability) throws IOException {
            return table.incrementColumnValue(row, family, qualifier, amount, durability);
        }

        @Override
        public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, boolean writeToWAL) throws IOException {
            return table.incrementColumnValue(row, family, qualifier, amount, writeToWAL);
        }

		@Override
		public void batch(List<? extends Row> actions, Object[] results) throws IOException, InterruptedException {
			table.batch(actions, results);
		}

		@Override
		public Object[] batch(List<? extends Row> actions) throws IOException, InterruptedException {
			return table.batch(actions);
		}

        @Override
        public <R> void batchCallback(List<? extends Row> actions, Object[] results, Batch.Callback<R> callback)
                throws IOException, InterruptedException {
            table.batchCallback(actions, results, callback);
        }

        @Override
        public <R> Object[] batchCallback(List<? extends Row> actions, Batch.Callback<R> callback) throws IOException, InterruptedException {
            return table.batchCallback(actions, callback);
        }

        @Override
		public void mutateRow(RowMutations rm) throws IOException {
			table.mutateRow(rm);
		}

		@Override
		public Result append(Append append) throws IOException {
			return table.append(append);
		}

		@Override
		public void setAutoFlush(boolean autoFlush) {
			table.setAutoFlush(autoFlush);			
		}

		@Override
		public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
			table.setAutoFlush(autoFlush, clearBufferOnFail);			
		}

        @Override
        public void setAutoFlushTo(boolean autoFlush) {
            table.setAutoFlushTo(autoFlush);
        }

        @Override
		public long getWriteBufferSize() {
			return table.getWriteBufferSize();
		}

		@Override
		public void setWriteBufferSize(long writeBufferSize) throws IOException {
			table.setWriteBufferSize(writeBufferSize);			
		}

        public HTable getDelegate() {
            return table;
        }

        public <R extends Message> Map<byte[], R> batchCoprocessorService(
                Descriptors.MethodDescriptor methodDescriptor, Message request,
                byte[] startKey, byte[] endKey, R responsePrototype) throws ServiceException, Throwable{
            return new Map<byte[], R>() {
                @Override
                public int size() {
                    return 0;
                }

                @Override
                public boolean isEmpty() {
                    return false;
                }

                @Override
                public boolean containsKey(Object key) {
                    return false;
                }

                @Override
                public boolean containsValue(Object value) {
                    return false;
                }

                @Override
                public R get(Object key) {
                    return null;
                }

                @Override
                public R put(byte[] key, R value) {
                    return null;
                }

                @Override
                public R remove(Object key) {
                    return null;
                }

                @Override
                public void putAll(Map<? extends byte[], ? extends R> m) {

                }

                @Override
                public void clear() {

                }

                @Override
                public Set<byte[]> keySet() {
                    return null;
                }

                @Override
                public Collection<R> values() {
                    return null;
                }

                @Override
                public Set<Entry<byte[], R>> entrySet() {
                    return null;
                }
            };
        }


        public <R extends Message> void batchCoprocessorService(Descriptors.MethodDescriptor methodDescriptor,
                                                                Message request, byte[] startKey, byte[] endKey, R responsePrototype,
                                                                Batch.Callback<R> callback) throws ServiceException, Throwable{

        }
    }
}
