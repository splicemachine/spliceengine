package com.splicemachine.derby.stream.iapi;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.stream.index.HTableScannerBuilder;
import com.splicemachine.db.iapi.sql.Activation;
import org.apache.hadoop.hbase.TableName;;

import java.io.InputStream;

/**
 *
 * Higher Level Constructs for getting Datasets and manipulating the processsing mechanisms.
 *
 * Created by jleach on 4/13/15.
 */
public interface DataSetProcessor {
    /**
     * TableScanner interface for scanning data in HBase structures.
     *
     * @param spliceOperation
     * @param siTableBuilder
     * @param tableName
     * @param <Op>
     * @param <V>
     * @return
     * @throws StandardException
     */
    <Op extends SpliceOperation, V> DataSet<V> getTableScanner(Op spliceOperation,TableScannerBuilder siTableBuilder, TableName tableName) throws StandardException;

    /**
     * TableScanner Builder for Statistics Operations
     *
     * @param hTableBuilder
     * @param tableName
     * @param <V>
     * @return
     * @throws StandardException
     */
    <V> DataSet<V> getHTableScanner(HTableScannerBuilder hTableBuilder, TableName tableName) throws StandardException;
    <Op extends SpliceOperation, V> DataSet<V> getTableScanner(final Activation activation, TableScannerBuilder siTableBuilder, TableName tableName) throws StandardException;
    /**
     * Get an empty dataset
     *
     * @param <V>
     * @return
     */
    <V> DataSet<V> getEmpty();

    <V> DataSet<V> getEmpty(String name);

    /**
     * Generate a single row dataset from a value.
     *
     * @param value
     * @param <V>
     * @return
     */
    <V> DataSet<V> singleRowDataSet(V value);

    <V> DataSet<V> singleRowDataSet(V value, SpliceOperation op, boolean isLast);
    
    /**
     * Create a dataset from a provided Iterable.
     *
     * @param value
     * @param <V>
     * @return
     */
    <V> DataSet<V> createDataSet(Iterable<V> value);

    /**
     *
     * Create a single row PairDataSet
     * @param key
     * @param value
     * @param <K>
     * @param <V>
     * @return
     */
    <K,V> PairDataSet<K, V> singleRowPairDataSet(K key, V value);

    /**
     *
     * Create an operation context for executing a function.
     *
     * @param spliceOperation
     * @param <Op>
     * @return
     */
    <Op extends SpliceOperation> OperationContext<Op> createOperationContext(Op spliceOperation);

    /**
     *
     * Create an operation context based only on the supplied activation
     *
     * @param activation
     * @param <Op>
     * @return
     */
    <Op extends SpliceOperation> OperationContext<Op> createOperationContext(Activation activation);

    /**
     *
     * Set the job group for execution.
     * @param jobName
     * @param jobDescription
     */
    void setJobGroup(String jobName, String jobDescription);

    /**
     * Read a whole text file from path.
     *
     * @param path
     * @return
     */
    PairDataSet<String,InputStream> readWholeTextFile(String path);

    PairDataSet<String,InputStream> readWholeTextFile(String path, SpliceOperation op);

    /**
     *
     * Read a text file that will be split in blocks when splittable compression algorithms are
     * utilized.
     *
     * @param path
     * @return
     */
    DataSet<String> readTextFile(String path);

    DataSet<String> readTextFile(String path, SpliceOperation op);
    
    /**
     * Get an empty PairDataSet
     *
     * @param <K>
     * @param <V>
     * @return
     */
    <K,V> PairDataSet<K, V> getEmptyPair();

    /**
     * Set the scheduler pool for execution (if appropriate)
     *
     * @param pool
     */
    void setSchedulerPool(String pool);

    /**
     *
     * Set whether failures are swallowed vs. being thrown up the stack.
     */
    void setPermissive();
    void setFailBadRecordCount(int failBadRecordCount);
}
