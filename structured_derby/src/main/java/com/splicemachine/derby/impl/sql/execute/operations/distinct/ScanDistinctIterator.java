package com.splicemachine.derby.impl.sql.execute.operations.distinct;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
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
public class ScanDistinctIterator extends AbstractStandardIterator{
    private final DistinctBuffer buffer;
    private final int[] distinctColumns;
    private MultiFieldEncoder groupKeyEncoder;
    private KeyMarshall groupKeyHasher;
    private boolean completed = false;
    private long rowsRead;

    public ScanDistinctIterator(DistinctBuffer buffer,
                                 StandardIterator<ExecRow> source,
                                 int[] distinctColumns) {
    	super(source);
        this.buffer = buffer;
        this.groupKeyHasher = KeyType.BARE;
        this.distinctColumns = distinctColumns;
    }

    public GroupedRow next(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        //return any previously evicted rows first
        if(completed){
            if(buffer.size()>0)
                return buffer.getFinalizedRow();
            else return null;
        }

        boolean shouldContinue;
        GroupedRow toReturn = null;
        do{
			SpliceBaseOperation.checkInterrupt(rowsRead,SpliceConstants.interruptLoopCheck);
            ExecRow nextRow = source.next(spliceRuntimeContext);
            shouldContinue = nextRow!=null;
            if(!shouldContinue)
                continue; //iterator exhausted, break from the loop
            toReturn = buffer.add(getGroupingKey(nextRow), nextRow);
            shouldContinue = toReturn==null;
            rowsRead++;
        }while(shouldContinue);

        if(toReturn!=null)
            return toReturn;
        /*
         * We can only get here if we exhaust the iterator without evicting a record, which
         * means that we have completed our steps.
         */
        completed=true;

        //we've exhausted the iterator, so return an entry from the buffer
        if(buffer.size()>0)
            return buffer.getFinalizedRow();

        //the buffer has nothing in it either, just return null
        return null;
    }


    private byte[] getGroupingKey(ExecRow row) throws StandardException {
        if(groupKeyEncoder==null)
            groupKeyEncoder = MultiFieldEncoder.create(SpliceDriver.getKryoPool(),distinctColumns.length);
        groupKeyEncoder.reset();
        groupKeyHasher.encodeKey(row.getRowArray(),distinctColumns,null,null,groupKeyEncoder);
        return groupKeyEncoder.build();
    }

}
