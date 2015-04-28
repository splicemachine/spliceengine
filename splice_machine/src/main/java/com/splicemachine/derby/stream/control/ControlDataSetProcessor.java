package com.splicemachine.derby.stream.control;

import com.google.common.collect.Lists;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.spark.SpliceSpark;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SITableScanner;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.stream.DataSet;
import com.splicemachine.derby.stream.DataSetProcessor;
import com.splicemachine.derby.stream.OperationContext;
import com.splicemachine.si.impl.HTransactorFactory;
import com.splicemachine.si.impl.TransactionStorage;
import com.splicemachine.si.impl.TxnDataStore;
import com.splicemachine.si.impl.TxnRegion;
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.si.impl.rollforward.NoopRollForward;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.util.*;
import org.apache.log4j.Logger;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Collections;
import java.util.Iterator;

/**
 * Created by jleach on 4/13/15.
 */
public class ControlDataSetProcessor<Op extends SpliceOperation,K,V> implements DataSetProcessor<Op,K,V> {
    private static final Logger LOG = Logger.getLogger(ControlDataSetProcessor.class);
    @Override
    public DataSet< V> getTableScanner(Op spliceOperation, TableScannerBuilder siTableBuilder, String tableName,SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        TxnRegion localRegion = new TxnRegion(null, NoopRollForward.INSTANCE, NoOpReadResolver.INSTANCE,
                TransactionStorage.getTxnSupplier(), TransactionStorage.getIgnoreTxnSupplier(), TxnDataStore.getDataStore(), HTransactorFactory.getTransactor());
                siTableBuilder
                        .scanner(new ControlMeasuredRegionScanner(Bytes.toBytes(tableName),siTableBuilder.getScan()))
                        .region(localRegion);
        return new ControlDataSet(new TableScannerIterator(siTableBuilder,spliceRuntimeContext));
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
    public OperationContext createOperationContext(Op spliceOperation, SpliceRuntimeContext spliceRuntimeContext) {
        return new ControlOperationContext(spliceOperation,spliceRuntimeContext);
    }

    @NotThreadSafe
    private class TableScannerIterator implements Iterable<LocatedRow>, Iterator<LocatedRow> {
        protected TableScannerBuilder siTableBuilder;
        protected SITableScanner tableScanner;
        protected boolean initialized;
        protected SpliceRuntimeContext spliceRuntimeContext;
        private ExecRow execRow;
        boolean slotted;
        boolean hasNext;
        int rows = 0;
        public TableScannerIterator(TableScannerBuilder siTableBuilder, SpliceRuntimeContext spliceRuntimeContext) {
            this.siTableBuilder = siTableBuilder;
            this.spliceRuntimeContext = spliceRuntimeContext;
        }

        @Override
        public Iterator<LocatedRow> iterator() {
            return this;
        }

        @Override
        public boolean hasNext() {
            try {
                if (slotted)
                    return hasNext;
                slotted = true;
                if (!initialized) {
                    tableScanner = siTableBuilder.build();
                    tableScanner.open();
                }
                execRow = tableScanner.next(spliceRuntimeContext);
                if (execRow==null) {
                    tableScanner.close();
                    initialized = false;
                    hasNext = false;
                    return hasNext;
                } else {
                    hasNext = true;
                }
                return hasNext;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public LocatedRow next() {
            slotted = false;
            rows++;
            LocatedRow locatedRow = new LocatedRow(tableScanner.getCurrentRowLocation(),execRow.getClone());
            return locatedRow;
        }

        @Override
        public void remove() {
            throw new RuntimeException("Not Implemented");
        }
    }

    @Override
    public void setJobGroup(String jobName, String jobDescription) {

    }

    @Override
    public DataSet< V> createDataSet(Iterable<V> value) {
        return new ControlDataSet<>(value);
    }
}