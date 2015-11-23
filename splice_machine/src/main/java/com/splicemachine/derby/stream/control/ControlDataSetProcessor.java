package com.splicemachine.derby.stream.control;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.stream.index.HTableScannerIterator;
import com.splicemachine.derby.stream.index.HTableScannerBuilder;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.iterator.TableScannerIterator;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.si.impl.HTransactorFactory;
import com.splicemachine.si.impl.TransactionStorage;
import com.splicemachine.si.impl.TxnDataStore;
import com.splicemachine.si.impl.TxnRegion;
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.si.impl.rollforward.NoopRollForward;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;

/**
 * Created by jleach on 4/13/15.
 */
public class ControlDataSetProcessor implements DataSetProcessor {
    private int failBadRecordCount = -1;
    private boolean permissive;

    private static final Logger LOG = Logger.getLogger(ControlDataSetProcessor.class);
    @Override
    public <Op extends SpliceOperation, V> DataSet<V> getTableScanner(Op spliceOperation, TableScannerBuilder siTableBuilder, String tableName) throws StandardException {
        TxnRegion localRegion = new TxnRegion(null, NoopRollForward.INSTANCE, NoOpReadResolver.INSTANCE,
                TransactionStorage.getTxnSupplier(), TransactionStorage.getIgnoreTxnSupplier(), TxnDataStore.getDataStore(), HTransactorFactory.getTransactor());


                siTableBuilder
                        .scanner(new ControlMeasuredRegionScanner(Bytes.toBytes(tableName),siTableBuilder.getScan()))
                        .region(localRegion);
        TableScannerIterator tableScannerIterator = new TableScannerIterator(siTableBuilder,spliceOperation);
        spliceOperation.registerCloseable(tableScannerIterator);
        return new ControlDataSet(tableScannerIterator);
    }

    @Override
    public <V> DataSet<V> getHTableScanner(HTableScannerBuilder hTableBuilder, String tableName) throws StandardException {
        TxnRegion localRegion = new TxnRegion(null, NoopRollForward.INSTANCE, NoOpReadResolver.INSTANCE,
                TransactionStorage.getTxnSupplier(), TransactionStorage.getIgnoreTxnSupplier(),
                TxnDataStore.getDataStore(), HTransactorFactory.getTransactor());

        hTableBuilder
                .scanner(new ControlMeasuredRegionScanner(Bytes.toBytes(tableName),hTableBuilder.getScan()))
                .region(localRegion)
                .metricFactory(Metrics.noOpMetricFactory());
        HTableScannerIterator tableScannerIterator = new HTableScannerIterator(hTableBuilder);
        return new ControlDataSet(tableScannerIterator);
    }
    public <Op extends SpliceOperation, V> DataSet<V> getTableScanner(Activation activation, TableScannerBuilder siTableBuilder, String tableName) throws StandardException {
        TxnRegion localRegion = new TxnRegion(null, NoopRollForward.INSTANCE, NoOpReadResolver.INSTANCE,
                TransactionStorage.getTxnSupplier(), TransactionStorage.getIgnoreTxnSupplier(), TxnDataStore.getDataStore(), HTransactorFactory.getTransactor());


        siTableBuilder
                .scanner(new ControlMeasuredRegionScanner(Bytes.toBytes(tableName),siTableBuilder.getScan()))
                .region(localRegion);
        TableScannerIterator tableScannerIterator = new TableScannerIterator(siTableBuilder,null);
        return new ControlDataSet(tableScannerIterator);
    }

    @Override
    public <V> DataSet<V> getEmpty() {
        return new ControlDataSet<>(Collections.<V>emptyList());
    }

    @Override
    public <V> DataSet<V> singleRowDataSet(V value) {
        return new ControlDataSet<>(Lists.newArrayList(value));
    }

    @Override
    public <V> DataSet<V> singleRowDataSet(V value, SpliceOperation op, boolean isLast) {
        return singleRowDataSet(value);
    }

    @Override
    public <K,V> PairDataSet<K, V> singleRowPairDataSet(K key, V value) {
        return new ControlPairDataSet<>(Arrays.asList(new Tuple2<K, V>(key, value)));
    }

    @Override
    public <Op extends SpliceOperation> OperationContext createOperationContext(Op spliceOperation) {
        OperationContext operationContext = new ControlOperationContext(spliceOperation);
        spliceOperation.setOperationContext(operationContext);
        return operationContext;
    }

    @Override
    public <Op extends SpliceOperation> OperationContext createOperationContext(Activation activation) {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void setJobGroup(String jobName, String jobDescription) {
    }

    @Override
    public PairDataSet<String, InputStream> readWholeTextFile(String s, SpliceOperation op) {
        Path path = new Path(s);
        InputStream rawStream = null;
        try {
            CompressionCodecFactory factory = new CompressionCodecFactory(SpliceConstants.config);
            CompressionCodec codec = factory.getCodec(path);
            FileSystem fs = FileSystem.get(SpliceConstants.config);
            FSDataInputStream fileIn = fs.open(path);
            InputStream value;
            if (codec != null) {
                value = codec.createInputStream(fileIn);
            } else {
                value = fileIn;
            }
            return singleRowPairDataSet(s, value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (rawStream != null) {
                try {
                    rawStream.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }

    @Override
    public PairDataSet<String, InputStream> readWholeTextFile(String s) {
        return readWholeTextFile(s, null);
    }
    
    @Override
    public DataSet<String> readTextFile(String s) {
        Path path = new Path(s);
        InputStream rawStream = null;
        try {
            CompressionCodecFactory factory = new CompressionCodecFactory(SpliceConstants.config);
            CompressionCodec codec = factory.getCodec(path);
            FileSystem fs = FileSystem.get(SpliceConstants.config);
            FSDataInputStream fileIn = fs.open(path);
            final InputStream value;
            if (codec != null) {
                value = codec.createInputStream(fileIn);
            } else {
                value = fileIn;
            }
            Iterable iterable;

            return new ControlDataSet<String>(new Iterable<String>() {
                @Override
                public Iterator<String> iterator() {
                    return new TextFileIterator(value);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (rawStream != null) {
                try {
                    rawStream.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }

    @Override
    public DataSet<String> readTextFile(String s, SpliceOperation op) {
        return readTextFile(s);
    }

    @Override
    public <K, V> PairDataSet<K, V> getEmptyPair() {
        return new ControlPairDataSet(Collections.emptyList());
    }

    @Override
    public <V> DataSet<V> createDataSet(Iterable<V> value) {
        return new ControlDataSet<>(value);
    }

    @Override
    public void setSchedulerPool(String pool) {
        // no op
    }

    private class TextFileIterator implements Iterator<String>{

        Scanner scanner;
        public TextFileIterator(InputStream inputStream) {
            this.scanner = new Scanner(inputStream);
        }

        @Override
        public void remove() {}

        @Override
        public String next () {
            return scanner.nextLine();
        }

        @Override
        public boolean hasNext() {
            return scanner.hasNextLine();
        }

    }

    @Override
    public void setPermissive() {
        permissive = true;
    }

    @Override
    public void setFailBadRecordCount(int failBadRecordCount) {
        this.failBadRecordCount = failBadRecordCount;
    }
}