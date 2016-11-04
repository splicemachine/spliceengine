/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.stream.iapi;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.function.Partitioner;

import java.io.InputStream;
import java.util.Iterator;

/**
 * Higher level constructs for getting datasets and manipulating the processing mechanisms.
 *
 * @author jleach
 */
public interface DataSetProcessor {
    enum Type {LOCAL,SPARK};

    Type getType();

    <Op extends SpliceOperation, V> ScanSetBuilder<V> newScanSet(Op spliceOperation,String tableName) throws StandardException;

    <V> DataSet<V> getEmpty();

    <V> DataSet<V> getEmpty(String name);

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

    void clearBroadcastedOperation();

    /*
     * Stops the given job
     */
    void stopJobGroup(String jobName);

    Partitioner getPartitioner(DataSet<LocatedRow> dataSet, ExecRow template, int[] keyDecodingMap, boolean[] keyOrder, int[] rightHashKeys);
}
