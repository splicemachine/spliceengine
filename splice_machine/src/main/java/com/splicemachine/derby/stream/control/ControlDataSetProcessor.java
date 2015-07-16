package com.splicemachine.derby.stream.control;

import com.google.common.collect.Lists;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
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
import org.apache.hadoop.hbase.util.*;
import org.apache.log4j.Logger;
import org.sparkproject.guava.common.collect.ArrayListMultimap;

import java.util.Collections;

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
    public <Op extends SpliceOperation> OperationContext createOperationContext(Op spliceOperation) {
        return new ControlOperationContext(spliceOperation);
    }

    @Override
    public void setJobGroup(String jobName, String jobDescription) {

    }

    @Override
    public PairDataSet<String, String> readTextFile(String s) {
        return null;
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