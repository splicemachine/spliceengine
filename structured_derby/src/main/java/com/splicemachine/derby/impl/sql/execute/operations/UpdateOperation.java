package com.splicemachine.derby.impl.sql.execute.operations;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.impl.sql.execute.UpdateConstantAction;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.Puts;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * @author jessiezhang
 */
public class UpdateOperation extends DMLWriteOperation{
	private static final Logger LOG = Logger.getLogger(UpdateOperation.class);

    public UpdateOperation(NoPutResultSet source, GeneratedMethod generationClauses,GeneratedMethod checkGM, Activation activation) 
    		throws StandardException {
    	super(source, generationClauses, checkGM, activation);
    }
    
    @Override
	public void init(SpliceOperationContext context){
		SpliceLogUtils.trace(LOG,"init with regionScanner %s",regionScanner);
		super.init(context);
		heapConglom = ((UpdateConstantAction)constants).getConglomerateId();
	}

	@Override
	public long sink() {
		SpliceLogUtils.trace(LOG,"sink on transactinID="+transactionID);
		long numSunk=0l;
		ExecRow nextRow=null;
		//Use HTable to do inserts instead of HeapConglomerateController - see Bug 188
		HTableInterface htable = SpliceAccessManager.getFlushableHTable(Bytes.toBytes(""+heapConglom));
		try {
			while((nextRow = source.getNextRowCore())!=null){
				SpliceLogUtils.trace(LOG,"UpdateOperation sink, nextRow="+nextRow);
				htable.put(Puts.buildUpdate(currentRowLocation, nextRow.getRowArray(), null, this.transactionID.getBytes()));
				numSunk++;
			}
			htable.flushCommits();
			htable.close();
		} catch (Exception e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,e);
		}
		return numSunk;
	}

	
	@Override
	public String toString() {
		return "Update{destTable="+heapConglom+",source=" + source + "}";
	}
}
