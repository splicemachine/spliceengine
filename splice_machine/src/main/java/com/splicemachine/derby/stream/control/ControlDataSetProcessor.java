package com.splicemachine.derby.stream.control;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.load.spark.WholeTextInputFormat;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.iterator.TableScannerIterator;
import com.splicemachine.si.impl.HTransactorFactory;
import com.splicemachine.si.impl.TransactionStorage;
import com.splicemachine.si.impl.TxnDataStore;
import com.splicemachine.si.impl.TxnRegion;
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.si.impl.rollforward.NoopRollForward;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.log4j.Logger;
import org.sparkproject.guava.common.collect.ArrayListMultimap;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;

/**
 * Created by jleach on 4/13/15.
 */
public class ControlDataSetProcessor implements DataSetProcessor {
    private static final Logger LOG = Logger.getLogger(ControlDataSetProcessor.class);
    @Override
    public <Op extends SpliceOperation, V> DataSet<V> getTableScanner(Op spliceOperation, TableScannerBuilder siTableBuilder, String tableName) throws StandardException {
        TxnRegion localRegion = new TxnRegion(null, NoopRollForward.INSTANCE, NoOpReadResolver.INSTANCE,
                TransactionStorage.getTxnSupplier(), TransactionStorage.getIgnoreTxnSupplier(), TxnDataStore.getDataStore(), HTransactorFactory.getTransactor());


                siTableBuilder
                        .scanner(new ControlMeasuredRegionScanner(Bytes.toBytes(tableName),siTableBuilder.getScan()))
                        .region(localRegion);
        return new ControlDataSet(new TableScannerIterator(siTableBuilder,spliceOperation));
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
    public <K,V> PairDataSet<K, V> singleRowPairDataSet(K key, V value) {
        ArrayListMultimap<K,V> map = ArrayListMultimap.create();
        map.put(key, value);
        return new ControlPairDataSet<>(map);
    }

    @Override
    public <Op extends SpliceOperation> OperationContext createOperationContext(Op spliceOperation) {
        return new ControlOperationContext(spliceOperation);
    }

    @Override
    public void setJobGroup(String jobName, String jobDescription) {

    }

    @Override
    public PairDataSet<String, InputStream> readTextFile(String s) {
        Path path = new Path(s);
        InputStream rawStream = null;
        GZIPInputStream unzippedStream;
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
    public <K, V> PairDataSet<K, V> getEmptyPair() {
        return new ControlPairDataSet(ArrayListMultimap.create());
    }

    @Override
    public <V> DataSet<V> createDataSet(Iterable<V> value) {
        return new ControlDataSet<>(value);
    }
}