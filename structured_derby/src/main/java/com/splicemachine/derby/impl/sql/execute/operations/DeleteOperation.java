package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.Mutations;
import com.splicemachine.hbase.CallBuffer;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.impl.sql.execute.DeleteConstantAction;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

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
		recordConstructorTime(); 
	}

	public DeleteOperation(NoPutResultSet source, ConstantAction passedInConstantAction, Activation activation) throws StandardException {
		super(source, activation);
		recordConstructorTime(); 
	}

	@Override
	public void init(SpliceOperationContext context) throws StandardException{
		SpliceLogUtils.trace(LOG,"DeleteOperation init with regionScanner %s",regionScanner);
		super.init(context);
		heapConglom = ((DeleteConstantAction)constants).getConglomerateId();
	}

    @Override
    public OperationSink.Translator getTranslator() throws IOException {
        return new OperationSink.Translator() {
            @Nonnull
            @Override
            public List<Mutation> translate(@Nonnull ExecRow row) throws IOException {
                RowLocation locToDelete;
                try {
                    locToDelete = (RowLocation)row.getColumn(row.nColumns()).getObject();
                    return Collections.singletonList(Mutations.getDeleteOp(getTransactionID(),locToDelete.getBytes()));
                } catch (StandardException e) {
                    throw Exceptions.getIOException(e);
                }
            }
        };
    }

    @Override
	public TaskStats sink() throws IOException {
		SpliceLogUtils.trace(LOG,"sink on transactinID="+getTransactionID());
        TaskStats.SinkAccumulator stats = TaskStats.uniformAccumulator();
        stats.start();
        SpliceLogUtils.trace(LOG, ">>>>statistics starts for sink for DeleteOperation at "+stats.getStartTime());
		ExecRow nextRow;
		try {
            CallBuffer<Mutation> writeBuffer = SpliceDriver.driver().getTableWriter().writeBuffer(Long.toString(heapConglom).getBytes());
            do{
                long processStart = System.nanoTime();
                nextRow = source.getNextRowCore();
                if(nextRow==null) continue;
                stats.readAccumulator().tick(System.nanoTime()-processStart);

                processStart = System.nanoTime();
                //there is a row to delete, so delete it
                SpliceLogUtils.trace(LOG, "DeleteOperation sink, nextRow=" + nextRow);
                RowLocation locToDelete = (RowLocation) nextRow.getColumn(nextRow.nColumns()).getObject();
                writeBuffer.add(Mutations.getDeleteOp(getTransactionID(),locToDelete.getBytes()));

                stats.writeAccumulator().tick(System.nanoTime()-processStart);
            }while(nextRow!=null);
            writeBuffer.flushBuffer();
            writeBuffer.close();
		} catch (Exception e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,e);
		}
        //return stats.finish();
		TaskStats ss = stats.finish();
		SpliceLogUtils.trace(LOG, ">>>>statistics finishes for sink for DeleteOperation at "+stats.getFinishTime());
        return ss;
	}


	@Override
	public String toString() {
		return "Delete{destTable="+heapConglom+",source=" + source + "}";
	}
}
