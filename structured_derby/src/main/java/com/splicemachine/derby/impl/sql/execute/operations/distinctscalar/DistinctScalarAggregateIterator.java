package com.splicemachine.derby.impl.sql.execute.operations.distinctscalar;

import com.splicemachine.derby.hbase.SpliceDriver;
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
public class DistinctScalarAggregateIterator implements StandardIterator<GroupedRow>{
    private final DistinctAggregateBuffer buffer;
    private final StandardIterator<ExecRow> source;
    private MultiFieldEncoder keyEncoder;
    private KeyMarshall keyHasher;
    private boolean completed;
    private int[] keyColumns;

    public DistinctScalarAggregateIterator(DistinctAggregateBuffer buffer,StandardIterator<ExecRow> source, int[] keyColumns) {
        this.buffer = buffer;
        this.source = source;
        this.keyHasher = KeyType.BARE;
        this.keyColumns = keyColumns;
    }

    @Override
    public void open() throws StandardException, IOException {
        source.open();
    }

    public GroupedRow next() throws StandardException, IOException {
    	if (!completed) {
	        boolean shouldContinue;
	        GroupedRow toReturn = null;
	        do{
	            ExecRow nextRow = source.next();
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

    public void close() throws IOException, StandardException {
        source.close();
    }
}
