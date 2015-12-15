package com.splicemachine.si.data.hbase;

import com.splicemachine.collections.CloseableIterator;
import com.splicemachine.collections.ForwardingCloseableIterator;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.storage.*;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;

/**
 * Wrapper that makes an HBase table comply with an interface that allows regions and tables to be used in a uniform manner.
 */
public class HbTable implements Partition<OperationWithAttributes,Delete, Get, Put,Result,Scan>{
    final Table table;

    public HbTable(Table table) {
        this.table = table;
    }

    @Override
    public DataScanner openScanner(DataScan scan,MetricFactory metricFactory) throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public DataResult getFkCounter(byte[] key,DataResult previous) throws IOException{
        if(previous==null)
            previous = new HResult();

        Get g = new Get(key);
        g.addColumn(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.SNAPSHOT_ISOLATION_FK_COUNTER_COLUMN_BYTES);
        assert previous instanceof HResult: "Programmer error: incorrect type for HbTable!";
        ((HResult)previous).set(table.get(g));
        return previous;
    }

    @Override
    public DataResult getLatest(byte[] key,DataResult previous) throws IOException{
        if(previous==null)
            previous = new HResult();

        Get g = new Get(key);
        g.setMaxVersions(1);
        assert previous instanceof HResult: "Programmer error: incorrect type for HbTable!";
        ((HResult)previous).set(table.get(g));
        return previous;
    }

    @Override
    public String getName() {
        try {
            return table.getTableDescriptor().getTableName().getNameAsString();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    @Override
    public void close() throws IOException {
        table.close();
    }

    @Override
    public Result get(Get get) throws IOException {
        final Result result = table.get(get);
        if (result.isEmpty()) {
           return null;
        } else {
            return result;
        }
    }

    @Override
    public CloseableIterator<Result> scan(Scan scan) throws IOException {
        if(scan.getStartRow() == null) {
            scan.setStartRow(new byte[]{});
        }
        final ResultScanner scanner = table.getScanner(scan);
				return new ForwardingCloseableIterator<Result>(scanner.iterator()) {
						@Override
						public void close() throws IOException {
							scanner.close();
						}
				};
    }

    @Override
    public DataScanner openScanner(DataScan scan) throws IOException{
        return openScanner(scan,Metrics.noOpMetricFactory());
    }

    @Override
    public void startOperation() throws IOException {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void closeOperation() throws IOException {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Lock getLock(byte[] rowKey, boolean waitForLock) {
        try {
            return lockRow(rowKey);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Lock lockRow(byte[] byteCopy) throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public Lock getRowLock(ByteSlice byteSlice) throws IOException{
        return getRowLock(byteSlice.array(),byteSlice.offset(),byteSlice.length());
    }

    @Override
    public Lock getRowLock(byte[] key,int keyOff,int keyLen) throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public void increment(byte[] rowKey, byte[] family, byte[] qualifier, long amount) throws IOException {
        throw new UnsupportedOperationException("Cannot increment with row lock at table level.");
    }

    @Override
    public void put(Put put) throws IOException {
        table.put(put);
    }

    @Override
    public void put(Put put, Lock rowLock) throws IOException {
        table.put(put);
    }

    @Override
    public void put(Put put, boolean durable) throws IOException {
        table.put(put);
    }

    @Override
    public void put(List<Put> puts) throws IOException {
        table.put(puts);
    }

    @Override
    public void put(DataPut put) throws IOException{
        assert put instanceof HPut: "Programmer error: wrong type for put!";
        Put p = ((HPut)put).unwrapDelegate();
        table.put(p);
    }

    @Override
    public boolean checkAndPut(byte[] family, byte[] qualifier, byte[] expectedValue, Put put) throws IOException {
        return table.checkAndPut(put.getRow(), family, qualifier, expectedValue, put);
    }

    @Override
    public void delete(Delete delete, Lock rowLock) throws IOException {
        throw new RuntimeException("not implemented");
    }

    @Override
    public byte[] getStartKey() {
        return HConstants.EMPTY_START_ROW;
    }

    @Override
    public byte[] getEndKey() {
        return HConstants.EMPTY_END_ROW;
    }

    @Override
    public boolean isClosed() {
        throw new RuntimeException("Not to be called on Table");
    }

    @Override
    public boolean isClosing() {
        throw new RuntimeException("not to be called on table");
    }

    @Override
    public Iterator<MutationStatus> writeBatch(DataPut[] toWrite) throws IOException{
        return null;
    }

    @Override
    public DataResultScanner openResultScanner(DataScan scan,MetricFactory metricFactory) throws IOException{
        return null;
    }

    @Override
    public DataResultScanner openResultScanner(DataScan scan) throws IOException{
        return null;
    }

    @Override
    public DataResult getLatest(byte[] rowKey,byte[] family,DataResult previous) throws IOException{
        return null;
    }

    @Override
    public void delete(DataDelete delete) throws IOException{

    }
}
