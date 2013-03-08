package com.splicemachine.hbase;

import com.apple.jobjc.foundation.NSMapTableKeyCallBacks;
import com.splicemachine.derby.utils.SpliceUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Scott Fines
 * Created on: 3/8/13
 */
public class SpliceTablePool {
    private static final SpliceTablePool DEFAULT_POOL = new SpliceTablePool();

    private final ConcurrentMap<Key,SpliceTableWrapper> tablePool;

    public SpliceTablePool() {
        this.tablePool = new ConcurrentHashMap<Key, SpliceTableWrapper>();
    }

    public static SpliceTablePool defaultPool(){ return DEFAULT_POOL;};

    public SpliceTable getTable(Key tableKey) throws IOException {
        SpliceTableWrapper cachedEntry = tablePool.get(tableKey);
        if(cachedEntry==null||cachedEntry.references.getAndIncrement()<=0){
            SpliceTableWrapper wrapper = new SpliceTableWrapper(this,tableKey);
            while(true){
                SpliceTableWrapper retTable = tablePool.putIfAbsent(tableKey,wrapper);
                if(retTable==wrapper) return retTable; //we win!
                else if(retTable.references.getAndIncrement()>0){
                    //we lost
                    wrapper.close();
                    return retTable;
                }
            }
        }

        return cachedEntry;
    }

    public void closeTable(Key tableKey) throws IOException{
        SpliceTableWrapper cachedEntry = tablePool.get(tableKey);
        if(cachedEntry==null||cachedEntry.references.get()<0){
            //it's already closed, no worries
            return;
        }
        cachedEntry.close();
    }

    /*
     * Doesn't need to be public, since the returned Table entry takes case of removing
     * itself when it's reference count drops below 0
     */
    private void removeTable(Key tableKey){
        this.tablePool.remove(tableKey);
    }

    public static class Key{
        private final byte[] tableName;
        private final byte[] transId;

        private Key(byte[] tableName, byte[] transId) {
            this.tableName = tableName;
            this.transId = transId;
        }

    }

    public static Key newKey(long conglomId) {
        return new Key(Long.toString(conglomId).getBytes(),null);
    }

    public static Key newKey(long conglomId, byte[] txnId){
        return new Key(Long.toString(conglomId).getBytes(),txnId);
    }

    public static Key newKey(byte[] conglomBytes, byte[] txnId){
        return new Key(conglomBytes,txnId);
    }

    private static class SpliceTableWrapper implements SpliceTable{
        private final AtomicInteger references;
        final SafeTable table;
        private final Key tableKey;
        private final SpliceTablePool pool;

        public SpliceTableWrapper(SpliceTablePool pool,Key tableKey) throws IOException {
            this.tableKey = tableKey;
            this.table = SafeTable.create(SpliceUtils.config,tableKey.tableName);
            this.references = new AtomicInteger(1);
            this.pool = pool;
        }

        @Override
        public void close() throws IOException {
            //make sure that there's nothing left in any buffers.
            flushCommits();
            int refs = references.decrementAndGet();
            if(refs<=0){
                try{
                    table.close();
                }finally{
                    //no matter what, we don't want to use this table instance any more
                    pool.removeTable(tableKey);
                }
            }
        }

        @Override public RowLock lockRow(byte[] row) throws IOException { return table.lockRow(row); }
        @Override public void unlockRow(RowLock rl) throws IOException { table.unlockRow(rl); }
        @Override public byte[] getTableName() { return table.getTableName(); }
        @Override public Configuration getConfiguration() { return table.getConfiguration(); }
        @Override public HTableDescriptor getTableDescriptor() throws IOException { return table.getTableDescriptor(); }
        @Override public boolean exists(Get get) throws IOException { return table.exists(get); }
        @Override public Result get(Get get) throws IOException { return table.get(get); }
        @Override public Result[] get(List<Get> gets) throws IOException { return table.get(gets); }
        @Override public ResultScanner getScanner(Scan scan) throws IOException { return table.getScanner(scan); }
        @Override public ResultScanner getScanner(byte[] family) throws IOException { return table.getScanner(family); }
        @Override public void put(Put put) throws IOException { table.put(put); }
        @Override public void put(List<Put> puts) throws IOException { table.put(puts); }
        @Override public void delete(Delete delete) throws IOException { table.delete(delete); }
        @Override public void delete(List<Delete> deletes) throws IOException { table.delete(deletes); }
        @Override public Result increment(Increment increment) throws IOException { return table.increment(increment); }
        @Override public boolean isAutoFlush() { return table.isAutoFlush(); }
        @Override public void flushCommits() throws IOException { table.flushCommits(); }

        @Override public <T extends CoprocessorProtocol> T coprocessorProxy(Class<T> protocol, byte[] row) {
            return table.coprocessorProxy(protocol, row);
        }
        @Override public <T extends CoprocessorProtocol, R> Map<byte[], R> coprocessorExec(
                Class<T> protocol, byte[] startKey, byte[] endKey, Batch.Call<T, R> callable) throws Throwable {
            return table.coprocessorExec(protocol, startKey, endKey, callable);
        }
        @Override public <T extends CoprocessorProtocol, R> void coprocessorExec(
                Class<T> protocol, byte[] startKey, byte[] endKey, Batch.Call<T, R> callable,
                Batch.Callback<R> callback) throws Throwable {
            table.coprocessorExec(protocol, startKey, endKey, callable, callback);
        }
        @Override public void batch(List<Row> actions, Object[] results) throws IOException, InterruptedException {
            table.batch(actions, results);
        }
        @Override public Object[] batch(List<Row> actions) throws IOException, InterruptedException {
            return table.batch(actions);
        }
        @Override public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
            return table.getRowOrBefore(row, family);
        }
        @Override public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
            return table.getScanner(family, qualifier);
        }
        @Override public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put)
                throws IOException {
            return table.checkAndPut(row, family, qualifier, value, put);
        }
        @Override public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value,
                                                Delete delete) throws IOException {
            return table.checkAndDelete(row, family, qualifier, value, delete);
        }
        @Override public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount)
                throws IOException {
            return table.incrementColumnValue(row, family, qualifier, amount);
        }
        @Override public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier,
                                                   long amount, boolean writeToWAL) throws IOException {
            return table.incrementColumnValue(row, family, qualifier, amount, writeToWAL);
        }
    }

}
