package com.splicemachine.derby.impl.sql.execute.operations.distinct;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.impl.sql.execute.operations.framework.AbstractStandardIterator;
import com.splicemachine.derby.impl.sql.execute.operations.framework.GroupedRow;
import com.splicemachine.derby.utils.StandardIterator;
import com.splicemachine.derby.utils.marshall.KeyMarshall;
import com.splicemachine.encoding.MultiFieldEncoder;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.lucene.util.ArrayUtil;

import java.io.IOException;

/**
 *
 *
 */
public class SinkDistinctIterator extends AbstractStandardIterator {
	public byte[] lastKey;
	public ExecRow currentRow;
	public long rowsRead;
	public boolean completed;
    private MultiFieldEncoder groupKeyEncoder;
    private KeyMarshall groupKeyHasher;
    private final int[] distinctColumns;
    private GroupedRow toReturn = new GroupedRow();

    public SinkDistinctIterator(StandardIterator<ExecRow> source,int[] distinctColumns) { 
    	super(source);
    	this.distinctColumns = distinctColumns;
    }

    @Override
    public GroupedRow next(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
    	if (completed)
    		return null;
    	boolean shouldContinue;
        byte[] groupingKey;
        do{
			SpliceBaseOperation.checkInterrupt(rowsRead,SpliceConstants.interruptLoopCheck);
            ExecRow nextRow = source.next(spliceRuntimeContext);
            shouldContinue = nextRow!=null;
            if(!shouldContinue) {
            	if (lastKey != null && currentRow != null) {
            		toReturn.setGroupingKey(lastKey);
            		toReturn.setRow(currentRow);
            	} else {
            		return null;
            	}
            	continue; //iterator exhausted, break from the loop
            }
            groupingKey = getGroupingKey(nextRow);
            if (!ArrayUtil.equals(lastKey, 0, groupingKey, 0, groupingKey.length)) {
            	toReturn.setGroupingKey(lastKey);
            	toReturn.setRow(currentRow);
            	lastKey = groupingKey;
            	currentRow = nextRow;
            	shouldContinue = false;
            } 
            rowsRead++;
        }while(shouldContinue);

        if(toReturn!=null)
            return toReturn;
       
        /*
         * We can only get here if we exhaust the iterator without evicting a record, which
         * means that we have completed our steps.
         */
        completed=true;
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
