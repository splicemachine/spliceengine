/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.stream.iapi;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.compile.ExplainNode;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.function.Partitioner;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.system.CsvOptions;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

/**
 * Higher level constructs for getting datasets and manipulating the processing mechanisms.
 *
 *
 *
 */
public interface DataSetProcessor {
    enum Type {CONTROL,SPARK}

    Type getType();

    <Op extends SpliceOperation, V> ScanSetBuilder<V> newScanSet(Op spliceOperation,String tableName) throws StandardException;

    <V> DataSet<V> getEmpty();

    <V> DataSet<V> getEmpty(String name);

    <V> DataSet<V> getEmpty(String name, OperationContext context);

    /**
     * Generates a single row dataset from a value.
     */
    <V> DataSet<V> singleRowDataSet(V value);

    <V> DataSet<V> singleRowDataSet(V value, Object caller);
    
    /**
     * Creates a dataset from a provided Iterable.
     */
    <V> DataSet<V> createDataSet(Iterator<V> value);

    <V> DataSet<V> createDataSet(Iterator<V> value, String name);

    /**
     * Creates a single row PairDataSet
     */
    <K,V> PairDataSet<K, V> singleRowPairDataSet(K key, V value);

    /**
     * Creates an operation context for executing a function.
     *
     */
    <Op extends SpliceOperation> OperationContext<Op> createOperationContext(Op spliceOperation);

    /**
     * Creates an operation context based only on the supplied activation
     */
    <Op extends SpliceOperation> OperationContext<Op> createOperationContext(Activation activation);

    /**
     * Sets the job group for execution.
     */
    void setJobGroup(String jobName, String jobDescription);

    /**
     * Reads a whole text file from path.
     */
    PairDataSet<String,InputStream> readWholeTextFile(String path) throws StandardException;

    PairDataSet<String,InputStream> readWholeTextFile(String path, SpliceOperation op) throws StandardException;

    /**
     * Reads a text file that will be split in blocks when splittable compression algorithms are
     * utilized.
     */
    DataSet<String> readTextFile(String path) throws StandardException;

    DataSet<String> readTextFile(String path, SpliceOperation op) throws StandardException;

    /**
     * Gets an empty PairDataSet
     */
    <K,V> PairDataSet<K, V> getEmptyPair();

    /**
     * Sets the scheduler pool for execution (if appropriate)
     */
    void setSchedulerPool(String pool);

    /**
     * Sets whether failures are logged (up to <code>badRecordThreshold</code> vs. immediately thrown up the stack.
     * @param statusDirectory the director to which bad record files should be written
     * @param importFileName the name of the import file. Will be used to determine the bad record file name.
     * @param badRecordThreshold the tolerance to which we should accept bad records
     */
    void setPermissive(String statusDirectory, String importFileName, long badRecordThreshold);

    /*
     * Stops the given job
     */
    void stopJobGroup(String jobName);

    Partitioner getPartitioner(DataSet<ExecRow> dataSet, ExecRow template, int[] keyDecodingMap, boolean[] keyOrder, int[] rightHashKeys);

    /**
     *
     * Reads Parquet files given the scan variables.  The qualifiers in conjunctive normal form
     * will be applied in the parquet storage layer.
     *
     * @param baseColumnMap
     * @param location
     * @param context
     * @param qualifiers
     * @param probeValue
     * @param execRow
     * @param <V>
     * @return
     * @throws StandardException
     */
    <V> DataSet<V> readParquetFile(StructType schema, int[] baseColumnMap, int[] partitionColumnMap, String location,
                                   OperationContext context, Qualifier[][] qualifiers, DataValueDescriptor probeValue,
                                   ExecRow execRow, boolean useSample, double sampleFraction) throws StandardException;

    /**
     *
     * Reads Avro files given the scan variables.
     * @param baseColumnMap
     * @param location
     * @param context
     * @param qualifiers
     * @param probeValue
     * @param execRow
     * @param <V>
     * @return
     * @throws StandardException
     */
    <V> DataSet<V> readAvroFile(StructType schema, int[] baseColumnMap, int[] partitionColumnMap, String location,
                                OperationContext context, Qualifier[][] qualifiers, DataValueDescriptor probeValue,
                                ExecRow execRow, boolean useSample, double sampleFraction) throws StandardException;


    /**
     *  Create a empty external file based on the storage format specified in the method
     *  This is useful to have always a consitent system where we don't try to query on a file
     *  that doesn't exist.
     *  This is currently use when we do "CREATE EXTERNAL TABLE..."
     *
     * @param fields
     * @param baseColumnMap
     * @param partitionBy
     * @param storageAs
     * @param location
     * @throws StandardException
     */
    void createEmptyExternalFile(StructField[] fields, int[] baseColumnMap, int[] partitionBy, String storageAs, String location, String compression) throws StandardException ;

    /**
     * Get external schema. This used to verify and make sure that what is really provided in the external fil
     * will match the definition in the CreateTableOperation.
     * Splice Machine implement natively the Spark interface so we use this to the constraint check.
     * @param storedAs
     * @param location
     * @param csvOptions
     * @return
     */
    StructType getExternalFileSchema(String storedAs, String location, boolean mergeSchema, CsvOptions csvOptions) throws StandardException;
    /**
     * This is used when someone modify the external table outside of Splice.
     * One need to refresh the schema table if the underlying file have been modify outside Splice because
     * Splice has now way to know when this happen
     * This method is used with a procedure look at SYSCS_UTIL.SYSCS_REFRESH_EXTERNAL_TABLE
     * @param location
     */
    void refreshTable(String location);
    /**
     *
     * Reads in-memory version given the scan variables.  The qualifiers are applied to the in-memory version.
     *
     * @param conglomerateId
     * @param baseColumnMap
     * @param location
     * @param context
     * @param qualifiers
     * @param probeValue
     * @param execRow
     * @param <V>
     * @return
     * @throws StandardException
     */
    <V> DataSet<V> readPinnedTable(long conglomerateId, int[] baseColumnMap, String location,
                                   OperationContext context, Qualifier[][] qualifiers, DataValueDescriptor probeValue, ExecRow execRow) throws StandardException ;

    /**
     *
     * Reads ORC files given the scan variables.  The qualifiers in conjunctive normal form
     * will be applied in the parquet storage layer.
     *
     * @param baseColumnMap
     * @param location
     * @param context
     * @param qualifiers
     * @param probeValue
     * @param execRow
     * @param <V>
     * @return
     * @throws StandardException
     */
    <V> DataSet<V> readORCFile(int[] baseColumnMap, int[] partitionColumnMap, String location,
                               OperationContext context, Qualifier[][] qualifiers, DataValueDescriptor probeValue, ExecRow execRow,
                               boolean useSample, double sampleFraction, boolean statsjob) throws StandardException;

    /**
     *
     * Reads Text files given the scan variables.  The qualifiers in conjunctive normal form
     * will be applied in the parquet storage layer.
     *
     * @param <V>
     * @param op
     * @param location
     * @param csvOptions
     * @param context
     * @param execRow
     * @param baseColumnMap
     * @return
     * @throws StandardException
     */
    <V> DataSet<ExecRow> readTextFile(SpliceOperation op, String location, CsvOptions csvOptions, int[] baseColumnMap,
                                         OperationContext context, Qualifier[][] qualifiers, DataValueDescriptor probeValue, ExecRow execRow,
                                         boolean useSample, double sampleFraction) throws StandardException;

    /**
     *
     * Drops the in-memory version of the table.
     *
     * @param conglomerateId
     * @throws StandardException
     */
    void dropPinnedTable(long conglomerateId) throws StandardException;


    /**
     *  Returns true if the table is currently cached in-memory.
     * @param conglomerateId
     * @throws StandardException
     */

    Boolean isCached(long conglomerateId) throws StandardException;

    TableChecker getTableChecker(String schemaName, String tableName, DataSet tableDataSet, KeyHashDecoder decoder,
                                 ExecRow key, TxnView txn, boolean fix, int[] baseColumnMap, boolean isSystemTable);

    // Operations related to spark explain ->
    boolean isSparkExplain();
    ExplainNode.SparkExplainKind getSparkExplainKind();
    void setSparkExplain(ExplainNode.SparkExplainKind newValue);
    void prependSpliceExplainString(String explainString);
    void appendSpliceExplainString(String explainString);
    void prependSparkExplainStrings(List<String> stringsToAdd, boolean firstOperationSource, boolean lastOperationSource);
    void popSpliceOperation();
    void finalizeTempOperationStrings();
    List<String> getNativeSparkExplain();
    int getOpDepth();
    void incrementOpDepth();
    void decrementOpDepth();
    void resetOpDepth();
    // <- End operations related to spark explain.

    <V> DataSet<ExecRow> readKafkaTopic(String topicName, OperationContext op) throws StandardException;
}
