package com.splicemachine.derby.stream.iapi;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.stream.index.HTableScannerBuilder;
import com.splicemachine.db.iapi.sql.Activation;;

import java.io.InputStream;

/**
 * Created by jleach on 4/13/15.
 */
public interface DataSetProcessor {
    <Op extends SpliceOperation, V> DataSet<V> getTableScanner(Op spliceOperation,TableScannerBuilder siTableBuilder, String tableName) throws StandardException;
    <V> DataSet<V> getHTableScanner(HTableScannerBuilder hTableBuilder, String tableName) throws StandardException;
    <Op extends SpliceOperation, V> DataSet<V> getTableScanner(final Activation activation, TableScannerBuilder siTableBuilder, String tableName) throws StandardException;
    <V> DataSet<V> getEmpty();
    <V> DataSet<V> singleRowDataSet(V value);
    <V> DataSet<V> createDataSet(Iterable<V> value);

    <K,V> PairDataSet<K, V> singleRowPairDataSet(K key, V value);

    <Op extends SpliceOperation> OperationContext<Op> createOperationContext(Op spliceOperation);
    <Op extends SpliceOperation> OperationContext<Op> createOperationContext(Activation activation);
    void setJobGroup(String jobName, String jobDescription);
    PairDataSet<String,InputStream> readWholeTextFile(String path);
    DataSet<String> readTextFile(String path);
    <K,V> PairDataSet<K, V> getEmptyPair();
}