package com.splicemachine.derby.impl.sql.execute.operations.distinctscalar;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.operations.framework.AbstractStandardIterator;
import com.splicemachine.derby.impl.sql.execute.operations.framework.GroupedRow;
import com.splicemachine.derby.utils.StandardIterator;
import com.splicemachine.derby.utils.marshall.KeyMarshall;
import com.splicemachine.derby.utils.marshall.KeyType;
import com.splicemachine.encoding.MultiFieldEncoder;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

import java.io.IOException;

/**
 * @author Scott Fines
 * Created on: 11/1/13
 */
public class DistinctScalarAggregateIterator extends AbstractStandardIterator {
    private final DistinctAggregateBuffer buffer;
    private MultiFieldEncoder keyEncoder;
    private KeyMarshall keyHasher;
    private boolean completed;
    private int[] keyColumns;

    public DistinctScalarAggregateIterator(DistinctAggregateBuffer buffer,StandardIterator<ExecRow> source, int[] keyColumns) {
    	super(source);
        this.buffer = buffer;
        this.keyHasher = KeyType.BARE;
        this.keyColumns = keyColumns;
    }

    public GroupedRow next(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
    	if (!completed) {
	        boolean shouldContinue;
	        GroupedRow toReturn = null;
	        do{
	            ExecRow nextRow = source.next(spliceRuntimeContext);
	            shouldContinue = nextRow!=null;
	            if(!shouldContinue)
	                continue; //iterator exhausted, break from the loop
	            toReturn = buffer.add(getGroupingKey(nextRow),nextRow);;
	            shouldContinue = toReturn==null;
	        }while(shouldContinue);
	
	        if(toReturn!=null)
	            return toReturn;
	        completed=true;
    	}
            if(buffer.size()>0)
                return buffer.getFinalizedRow();
            return null;
    }

    private byte[] getGroupingKey(ExecRow row) throws StandardException {
        if(keyEncoder==null){
            keyEncoder = MultiFieldEncoder.create(SpliceDriver.getKryoPool(),row.nColumns());
        }
        keyEncoder.reset();
        ((KeyMarshall)keyHasher).encodeKey(row.getRowArray(), keyColumns, null, null, keyEncoder);
        return keyEncoder.build();
    }
}
