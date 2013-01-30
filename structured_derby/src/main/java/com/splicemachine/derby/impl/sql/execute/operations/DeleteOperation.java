package com.splicemachine.derby.impl.sql.execute.operations;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.impl.sql.execute.DeleteConstantAction;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * 
 * @author jessiezhang
 *
 */
public class DeleteOperation extends DMLWriteOperation{
	private static final Logger LOG = Logger.getLogger(DeleteOperation.class);
	protected  boolean cascadeDelete;


	public DeleteOperation(NoPutResultSet source, Activation activation) throws StandardException {
		super(source, null, null, activation);
	}

	public DeleteOperation(NoPutResultSet source, ConstantAction passedInConstantAction, Activation activation) throws StandardException {
		super(source, null, null);
	}

	@Override
	public void init(SpliceOperationContext context){
		SpliceLogUtils.trace(LOG,"DeleteOperation init with regionScanner %s",regionScanner);
		super.init(context);
		heapConglom = ((DeleteConstantAction)constants).getConglomerateId();
	}

	@Override
	public long sink() {
		SpliceLogUtils.trace(LOG,"sink on transactinID="+transactionID);
		long numSunk=0l;
		ExecRow nextRow=null;
		HTableInterface htable = SpliceAccessManager.getFlushableHTable(Bytes.toBytes(""+heapConglom));
		try {
			while((nextRow = source.getNextRowCore())!=null){
				SpliceLogUtils.trace(LOG,"DeleteOperation sink, nextRow="+nextRow);
				//TODO: need a buffered delete
				htable.delete(SpliceUtils.delete(currentRowLocation, this.transactionID.getBytes()) );
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
		return "Delete{destTable="+heapConglom+",source=" + source + "}";
	}
}
