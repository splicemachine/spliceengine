package com.splicemachine.derby.stream.spark;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.impl.sql.execute.ValueRow;
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

import java.util.Collections;

/**
 * @author Scott Fines
 *         Date: 1/25/16
 */
public class SparkUpdateDataSetWriter<K,V> implements DataSetWriter{
    private JavaPairRDD<K,V> rdd;
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

    public SparkUpdateDataSetWriter(JavaPairRDD<K, V> rdd,
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
        operationContext.getOperation().fireAfterStatementTriggers();
        ValueRow valueRow=new ValueRow(1);
        valueRow.setColumn(1,new SQLInteger((int)operationContext.getRecordsWritten()));
        return new ControlDataSet<>(Collections.singletonList(new LocatedRow(valueRow)));
    }

    @Override
    public void setTxn(TxnView childTxn){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public TableWriter getTableWriter() throws StandardException{
        return new UpdatePipelineWriter(heapConglom,formatIds,columnOrdering,pkCols,pkColumns,tableVersion,
                operationContext.getTxn(), execRowDefinition,heapList,operationContext);
    }

    @Override
    public TxnView getTxn(){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public byte[] getDestinationTable(){
        return Bytes.toBytes(heapConglom);
    }
}
