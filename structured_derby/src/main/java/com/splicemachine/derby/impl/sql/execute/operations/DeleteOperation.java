package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.stats.SinkStats;
import com.splicemachine.derby.stats.ThroughputStats;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.BatchTable;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.impl.sql.execute.DeleteConstantAction;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * 
 * @author jessiezhang
 *
 */
public class DeleteOperation extends DMLWriteOperation{
	private static final Logger LOG = Logger.getLogger(DeleteOperation.class);
	protected  boolean cascadeDelete;

	public DeleteOperation(){
		super();
	}

	public DeleteOperation(NoPutResultSet source, Activation activation) throws StandardException {
		super(source, activation);
	}

	public DeleteOperation(NoPutResultSet source, ConstantAction passedInConstantAction, Activation activation) throws StandardException {
		super(source, activation);
	}

	@Override
	public void init(SpliceOperationContext context){
		SpliceLogUtils.trace(LOG,"DeleteOperation init with regionScanner %s",regionScanner);
		super.init(context);
		heapConglom = ((DeleteConstantAction)constants).getConglomerateId();
	}

	@Override
	public SinkStats sink() throws IOException {
		SpliceLogUtils.trace(LOG,"sink on transactinID="+transactionID);
        SinkStats.SinkAccumulator stats = SinkStats.uniformAccumulator();
        stats.start();
		ExecRow nextRow;
		HTableInterface htable = BatchTable.create(SpliceUtils.config,Long.toString(heapConglom).getBytes());
		try {
            do{
                long processStart = System.nanoTime();
                nextRow = source.getNextRowCore();
                if(nextRow==null) continue;
                stats.processAccumulator().tick(System.nanoTime()-processStart);

                processStart = System.nanoTime();
                //there is a row to delete, so delete it
                SpliceLogUtils.trace(LOG, "DeleteOperation sink, nextRow=" + nextRow);
                RowLocation locToDelete = (RowLocation) nextRow.getColumn(nextRow.nColumns()).getObject();
                htable.delete(SpliceUtils.delete(locToDelete, this.transactionID.getBytes()) );

                stats.sinkAccumulator().tick(System.nanoTime()-processStart);
            }while(nextRow!=null);
			htable.flushCommits();
			htable.close();
		} catch (Exception e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,e);
		}
        return stats.finish();
	}


	@Override
	public String toString() {
		return "Delete{destTable="+heapConglom+",source=" + source + "}";
	}
}
