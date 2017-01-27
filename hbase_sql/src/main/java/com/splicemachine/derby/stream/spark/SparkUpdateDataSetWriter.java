/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
