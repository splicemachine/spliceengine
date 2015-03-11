package com.splicemachine.derby.utils;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Callable;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.sql.execute.StandardCloseable;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.SIFactoryDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.NoPutResultSet;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import com.splicemachine.derby.impl.storage.SpliceResultScanner;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.metrics.IOStats;
import com.splicemachine.metrics.Metrics;

/**
 * @author Scott Fines
 *         Created on: 11/2/13
 */
public class StandardIterators {
	

    private StandardIterators() {
    }

    public static <T> StandardIterator<T> wrap(Iterable<T> data) {
        return new IteratorStandardIterator<T>(data.iterator());
    }

    public static <T> StandardIterator<T> wrap(Iterator<T> iterator) {
        return new IteratorStandardIterator<T>(iterator);
    }

    public static <T> IOStandardIterator<T> noIO(Iterable<T> data) {
        return new IteratorStandardIterator<T>(data.iterator());
    }

    public static StandardIterator<ExecRow> wrap(SpliceOperation op) {
        return new SpliceOpStandardIterator(op);
    }

    public static StandardIterator<ExecRow> wrap(NoPutResultSet NPRS) {
        return new ResultSetStandardIterator(NPRS);
    }

    public static PartitionAwareIterator<ExecRow> wrap(SpliceResultScanner resultScanner,
                                                       PairDecoder decoder,
                                                       int[] partitionKeyColumns,
                                                       DataValueDescriptor[] dvds) {
        return new SpliceResultScannerPartitionAwareIterator(resultScanner, decoder, partitionKeyColumns, dvds);
    }
    public static <T> StandardIterator<T> wrap(Callable<T> callable) {
        return new CallableStandardIterator<T>(callable);
    }

    public static <T> StandardIterator<T> wrap(Callable<T> callable, StandardCloseable c) {
        return new CallableStandardIterator<T>(callable, c);
    }

    public static IOStandardIterator<ExecRow> ioIterator(SpliceNoPutResultSet resultSet) {
        return new SpliceResultSetStandardIterator(resultSet);
    }

    private static class IteratorStandardIterator<T> implements IOStandardIterator<T> {
        private final Iterator<T> delegate;

        private IteratorStandardIterator(Iterator<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void open() throws StandardException, IOException {
        }//no-op

        @Override
        public void close() throws StandardException, IOException {
        } //no-op

        @Override
        public T next(SpliceRuntimeContext spliceRuntimeContext) throws
                StandardException,
                IOException {
            if (!delegate.hasNext())
                return null;
            return delegate.next();
        }

        @Override
        public IOStats getStats() {
            return Metrics.noOpIOStats();
        }
    }

    private static class SpliceOpStandardIterator implements StandardIterator<ExecRow> {
        private final SpliceOperation op;

        private SpliceOpStandardIterator(SpliceOperation op) {
            this.op = op;
        }

        @Override
        public void open() throws StandardException, IOException {
            op.open();
        }

        @Override
        public ExecRow next(SpliceRuntimeContext ctx) throws StandardException,
                IOException {
            return op.nextRow(ctx);
        }

        @Override
        public void close() throws StandardException, IOException {
            op.close();
        }
    }

    private static class SpliceResultSetStandardIterator implements IOStandardIterator<ExecRow> {
        private final SpliceNoPutResultSet noPut;

        private SpliceResultSetStandardIterator(SpliceNoPutResultSet noPut) {
            this.noPut = noPut;
        }

        @Override
        public void open() throws StandardException, IOException {
            noPut.open();
            noPut.openCore();
        }

        @Override
        public ExecRow next(SpliceRuntimeContext spliceRuntimeContext) throws
                StandardException,
                IOException {
            return noPut.getNextRowCore();
        }

        @Override
        public void close() throws StandardException, IOException {
            noPut.close();
        }

        @Override
        public IOStats getStats() {
            return noPut.getStats();
        }
    }

    private static class ResultSetStandardIterator implements StandardIterator<ExecRow> {
        private final NoPutResultSet noPut;

        private ResultSetStandardIterator(NoPutResultSet noPut) {
            this.noPut = noPut;
        }

        @Override
        public void open() throws StandardException, IOException {
            noPut.open();
            noPut.openCore();
        }

        @Override
        public ExecRow next(SpliceRuntimeContext spliceRuntimeContext) throws
                StandardException,
                IOException {
            return noPut.getNextRowCore();
        }

        @Override
        public void close() throws StandardException, IOException {
            noPut.close();
        }
    }

    private static class SpliceResultScannerPartitionAwareIterator<Data> implements PartitionAwareIterator<ExecRow> {
    	protected final SDataLib<Data,Put,Delete,Get,Scan> dataLib = SIFactoryDriver.siFactory.getDataLib();
    	private final SpliceResultScanner scanner;
        private final PairDecoder decoder;
        private final int[] partitionColumns;
        private byte[] partition;
        private DataValueDescriptor[] dvds;
        private MultiFieldDecoder keyDecoder;

        private SpliceResultScannerPartitionAwareIterator(SpliceResultScanner scanner,
                                                          PairDecoder decoder,
                                                          int[] partitionColumns,
                                                          DataValueDescriptor[] dvds) {
            this.scanner = scanner;
            this.decoder = decoder;
            this.partitionColumns = partitionColumns;
            this.dvds = dvds;
        }

        @Override
        public void open() throws StandardException, IOException {
            scanner.open();
        }

        private void createKeyDecoder() {
            if (keyDecoder == null)
                keyDecoder = MultiFieldDecoder.create();
            else
                keyDecoder.reset();
        }

        private void copyPartitionKey(Data kv) {
            // partition key is the first few columns of row key
            createKeyDecoder();
            byte[] key = dataLib.getDataRow(kv);
            keyDecoder.set(key, 9, key.length - 9); // skip 9 bytes for hash prefix (bucket# + uuid)
            for (int i = 0; i < partitionColumns.length; ++i) {
                DerbyBytesUtil.skip(keyDecoder, dvds[i]);
            }

            int offset = keyDecoder.offset();
            if (partition == null || Bytes.compareTo(partition, 0, partition.length, key, 0, offset) < 0) {
                partition = new byte[offset];
                System.arraycopy(key, 0, partition, 0, offset);
            }
        }

        @Override
        public ExecRow next(SpliceRuntimeContext ctx) throws StandardException, IOException {
            Result result = scanner.next();
            if(result==null) {
                partition = null;
                return null;
            }
            Data data = dataLib.matchDataColumn(result);
            copyPartitionKey(data);
            ExecRow row = decoder.decode(data);
            return row;
        }

        @Override
        public byte[] getPartition() {
            return partition;
        }

        @Override
        public void close() throws StandardException, IOException {
            scanner.close();
        }
    }

    public static class CallableStandardIterator<T> implements StandardIterator<T> {
        private final Callable<T> callable;
        private final StandardCloseable c;

        public CallableStandardIterator(Callable<T> callable) {
            this.callable = callable;
            this.c = null;
        }

        public CallableStandardIterator(Callable<T> callable, StandardCloseable c) {
            this.callable = callable;
            this.c = c;
        }

        @Override
        public void open() {
        }

        @Override
        public void close() throws StandardException, IOException {
            if (c != null) {
                c.close();
            }
        }

        @Override
        public T next(SpliceRuntimeContext spliceRuntimeContext) throws
                StandardException,
                IOException {
            try {
                return callable.call();
            } catch (StandardException se) {
                throw se;
            } catch (IOException ioe) {
                throw ioe;
            } catch (Exception e) {
                throw Exceptions.parseException(e);
            }
        }
    }
}
