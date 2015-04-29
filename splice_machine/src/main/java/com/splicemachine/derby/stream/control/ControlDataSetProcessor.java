package com.splicemachine.derby.stream.control;

import com.google.common.collect.Lists;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.stream.DataSet;
import com.splicemachine.derby.stream.DataSetProcessor;
import com.splicemachine.derby.stream.OperationContext;
import com.splicemachine.derby.stream.iterator.TableScannerIterator;
import com.splicemachine.si.impl.HTransactorFactory;
import com.splicemachine.si.impl.TransactionStorage;
import com.splicemachine.si.impl.TxnDataStore;
import com.splicemachine.si.impl.TxnRegion;
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.si.impl.rollforward.NoopRollForward;
import org.apache.hadoop.hbase.util.*;
import org.apache.log4j.Logger;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Collections;

/**
 * Created by jleach on 4/13/15.
 */
public class ControlDataSetProcessor<Op extends SpliceOperation,K,V> implements DataSetProcessor<Op,K,V> {
    private static final Logger LOG = Logger.getLogger(ControlDataSetProcessor.class);
    @Override
    public DataSet< V> getTableScanner(Op spliceOperation, TableScannerBuilder siTableBuilder, String tableName) throws StandardException {
        TxnRegion localRegion = new TxnRegion(null, NoopRollForward.INSTANCE, NoOpReadResolver.INSTANCE,
                TransactionStorage.getTxnSupplier(), TransactionStorage.getIgnoreTxnSupplier(), TxnDataStore.getDataStore(), HTransactorFactory.getTransactor());
                siTableBuilder
                        .scanner(new ControlMeasuredRegionScanner(Bytes.toBytes(tableName),siTableBuilder.getScan()))
                        .region(localRegion);
        return new ControlDataSet(new TableScannerIterator(siTableBuilder,spliceOperation));
    }

    @Override
    public DataSet< V> getEmpty() {
        return new ControlDataSet<>(Collections.<V>emptyList());
    }

    @Override
    public DataSet< V> singleRowDataSet(V value) {
        return new ControlDataSet<>(Lists.newArrayList(value));
    }

    @Override
    public OperationContext createOperationContext(Op spliceOperation) {
        return new ControlOperationContext(spliceOperation);
    }

    @Override
    public void setJobGroup(String jobName, String jobDescription) {

    }

    @Override
    public DataSet< V> createDataSet(Iterable<V> value) {
        return new ControlDataSet<>(value);
    }
}