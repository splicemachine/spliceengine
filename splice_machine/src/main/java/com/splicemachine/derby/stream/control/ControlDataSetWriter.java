package com.splicemachine.derby.stream.control;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.derby.stream.output.insert.InsertPipelineWriter;

import java.util.Collections;

/**
 * @author Scott Fines
 *         Date: 1/12/16
 */
public class ControlDataSetWriter<K> implements DataSetWriter{
    private final ControlPairDataSet<K, ExecRow> dataSet;
    private final OperationContext operationContext;
    private final InsertPipelineWriter pipelineWriter;

    public ControlDataSetWriter(ControlPairDataSet<K, ExecRow> dataSet,InsertPipelineWriter pipelineWriter,OperationContext opContext){
        this.dataSet=dataSet;
        this.operationContext=opContext;
        this.pipelineWriter=pipelineWriter;
    }

    @Override
    public DataSet<LocatedRow> write() throws StandardException{
        /*
         * -sf- we shouldn't need to do transactional management here, because Derby manages internal
         * savepoints around insertion.
         */
        try{
            operationContext.getOperation().fireBeforeStatementTriggers();
            pipelineWriter.open(operationContext.getOperation().getTriggerHandler(),operationContext.getOperation());
            pipelineWriter.write(dataSet.values().toLocalIterator());
            ValueRow valueRow=new ValueRow(1);
            valueRow.setColumn(1,new SQLInteger((int)operationContext.getRecordsWritten()));
            return new ControlDataSet<>(Collections.singletonList(new LocatedRow(valueRow)));
        }catch(Exception e){
            throw StandardException.plainWrapException(e);
        }finally{
            try{
                if(pipelineWriter!=null)
                    pipelineWriter.close();
                operationContext.getOperation().fireAfterStatementTriggers();
            }catch(Exception e){
                throw StandardException.plainWrapException(e);
            }

        }
    }
}
