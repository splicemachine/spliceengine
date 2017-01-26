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
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.derby.stream.output.direct.DirectPipelineWriter;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TxnView;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.util.Either;

import java.util.Collections;

/**
 * @author Scott Fines
 *         Date: 1/25/16
 */
public class SparkDirectDataSetWriter<K,V> implements DataSetWriter{
    private final JavaPairRDD<K, Either<Exception, V>> rdd;
    private final JavaSparkContext context;
    private final OperationContext opContext;
    private final Configuration conf;
    private boolean skipIndex;
    private long destConglom;
    private TxnView txn;

    public SparkDirectDataSetWriter(JavaPairRDD<K, Either<Exception, V>> rdd,
                                    JavaSparkContext context,
                                    OperationContext opContext,
                                    Configuration conf,
                                    boolean skipIndex,
                                    long destConglom,
                                    TxnView txn){
        this.rdd=rdd;
        this.context=context;
        this.opContext=opContext;
        this.conf=conf;
        this.skipIndex=skipIndex;
        this.destConglom=destConglom;
        this.txn=txn;
    }

    @Override
    public DataSet<LocatedRow> write() throws StandardException{
        rdd.saveAsNewAPIHadoopDataset(conf);
        ValueRow valueRow=new ValueRow(1);
        valueRow.setColumn(1,new SQLLongint(0));
        return new SparkDataSet<>(context.parallelize(Collections.singletonList(new LocatedRow(valueRow)), 1));
    }

    @Override
    public void setTxn(TxnView childTxn){
        this.txn = childTxn;
    }

    @Override
    public TableWriter getTableWriter() throws StandardException{
        return new DirectPipelineWriter(destConglom,txn,opContext,skipIndex);
    }

    @Override
    public TxnView getTxn(){
        return txn;
    }

    @Override
    public byte[] getDestinationTable(){
        return Bytes.toBytes(destConglom);
    }
}
