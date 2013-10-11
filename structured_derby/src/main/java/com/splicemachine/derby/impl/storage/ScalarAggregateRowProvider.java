package com.splicemachine.derby.impl.storage;

import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceGenericAggregator;
import com.splicemachine.derby.utils.marshall.RowDecoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecAggregator;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;

/**
 * When no row is returned from the actual operation, this provides a default value ONCE.
 *
 * @author Scott Fines
 * Created on: 5/21/13
 */
public class ScalarAggregateRowProvider extends ClientScanProvider{
    private boolean defaultReturned = false;

    private final ExecAggregator[] execAggregators;
    private final SpliceGenericAggregator[] genericAggregators;

    public ScalarAggregateRowProvider(String type,
                                      byte[] tableName,
                                      Scan scan,
                                      RowDecoder decoder,
                                      SpliceRuntimeContext spliceRuntimeContext,
                                      SpliceGenericAggregator[] aggregates) throws StandardException {
        super(type,tableName, scan,decoder, spliceRuntimeContext);
        this.genericAggregators = aggregates;
        this.execAggregators = new ExecAggregator[genericAggregators.length];
        for(int i=0;i<genericAggregators.length;i++){
            execAggregators[i] = genericAggregators[i].getAggregatorInstance();
        }
    }

    @Override
    public ExecRow next() throws StandardException, IOException {
        ExecRow finalRow = null;
        while(hasNext()){
            ExecRow row = super.next();
            if(finalRow==null)
                finalRow = row.getNewNullRow();
            for(int i=0;i<genericAggregators.length;i++){
                ExecAggregator aggregate = execAggregators[i];
                SpliceGenericAggregator genericAgg = genericAggregators[i];
                aggregate.add(genericAgg.getResultColumnValue(row));
            }
        }

        if(finalRow!=null){
            for(int i=0;i<genericAggregators.length;i++){
                ExecAggregator aggregate = execAggregators[i];
                SpliceGenericAggregator genericAgg = genericAggregators[i];
                genericAgg.setResultColumn(finalRow,aggregate.getResult());
            }
        }

        return finalRow;
    }

    @Override
    public boolean hasNext() throws StandardException, IOException {
        boolean hasNext = super.hasNext();
        if(hasNext){
            defaultReturned =true;
            return hasNext;
        }else if(!defaultReturned){
            currentRow = decoder.getTemplate();
            defaultReturned = true;
            populated = true;
            return true;
        }else return false;
    }
}
