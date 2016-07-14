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

package com.splicemachine.derby.stream.spark;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.control.ControlDataSet;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.derby.stream.output.update.UpdatePipelineWriter;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TxnView;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import scala.util.Either;

import java.util.Collections;

/**
 * @author Scott Fines
 *         Date: 1/25/16
 */
public class SparkUpdateDataSetWriter<K,V> implements DataSetWriter{
    private JavaPairRDD<K,Either<Exception, V>> rdd;
    private final OperationContext operationContext;
    private final Configuration conf;
    private long heapConglom;
    private int[] formatIds;
    private int[] columnOrdering;
    private int[] pkCols;
    private FormatableBitSet pkColumns;
    private String tableVersion;
    private ExecRow execRowDefinition;
    private FormatableBitSet heapList;
    private transient TxnView txn;

    public SparkUpdateDataSetWriter(JavaPairRDD<K, Either<Exception, V>> rdd,
                                    OperationContext operationContext,
                                    Configuration conf,
                                    long heapConglom,
                                    int[] formatIds,
                                    int[] columnOrdering,
                                    int[] pkCols,
                                    FormatableBitSet pkColumns,
                                    String tableVersion,
                                    ExecRow execRowDefinition,
                                    FormatableBitSet heapList){
        this.rdd=rdd;
        this.operationContext=operationContext;
        this.conf=conf;
        this.heapConglom=heapConglom;
        this.formatIds=formatIds;
        this.columnOrdering=columnOrdering;
        this.pkCols=pkCols;
        this.pkColumns=pkColumns;
        this.tableVersion=tableVersion;
        this.execRowDefinition=execRowDefinition;
        this.heapList=heapList;
    }

    @Override
    public DataSet<LocatedRow> write() throws StandardException{
        rdd.saveAsNewAPIHadoopDataset(conf); //actually does the writing
        if (operationContext.getOperation() != null) {
            operationContext.getOperation().fireAfterStatementTriggers();
        }
        ValueRow valueRow=new ValueRow(1);
        valueRow.setColumn(1,new SQLLongint(operationContext.getRecordsWritten()));
        return new SparkDataSet<>(SpliceSpark.getContext().parallelize(Collections.singletonList(new LocatedRow(valueRow)), 1));
    }

    @Override
    public void setTxn(TxnView childTxn){
        this.txn = childTxn;
    }

    @Override
    public TableWriter getTableWriter() throws StandardException{
        return new UpdatePipelineWriter(heapConglom,formatIds,columnOrdering,pkCols,pkColumns,tableVersion,
                txn, execRowDefinition,heapList,operationContext);
    }

    @Override
    public TxnView getTxn(){
        if(txn==null)
            return operationContext.getTxn();
        else
            return txn;
    }

    @Override
    public byte[] getDestinationTable(){
        return Bytes.toBytes(heapConglom);
    }
}
