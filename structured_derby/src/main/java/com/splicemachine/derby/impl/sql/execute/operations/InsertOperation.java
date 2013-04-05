package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.Serializer;
import com.splicemachine.derby.stats.SinkStats;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.Puts;
import com.splicemachine.hbase.CallBuffer;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.impl.sql.execute.InsertConstantAction;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * 
 * @author Scott Fines
 *
 * TODO:
 * 	1. Basic Inserts (insert 1 row, insert multiple small rows) - Done SF
 *  2. Insert with subselect (e.g. insert into t (name) select name from a) - Done SF
 *  3. Triggers (do with Coprocessors)
 *  4. Primary Keys (do with Coprocessors)
 *  5. Secondary Indices (do with Coprocessors)
 */
public class InsertOperation extends DMLWriteOperation {
    private static final long serialVersionUID = 1l;
	private static final Logger LOG = Logger.getLogger(InsertOperation.class);
	
	public InsertOperation(){
		super();
	}
	
	public InsertOperation(NoPutResultSet source,
							GeneratedMethod generationClauses, 
							GeneratedMethod checkGM) throws StandardException{
		super(source, generationClauses, checkGM, source.getActivation());
		recordConstructorTime(); 
	}

	@Override
	public void init(SpliceOperationContext context){
		super.init(context);
		heapConglom = ((InsertConstantAction)constants).getConglomerateId();

        if(constants instanceof InsertConstantAction){
            int[] pks = ((InsertConstantAction)constants).getPkColumns();
            if(pks!=null)
                pkColumns = fromIntArray(pks);
        }
	}
	
	@Override
	public SinkStats sink() throws IOException {
//		SpliceLogUtils.trace(LOG,"sink on transactinID="+transactionID);
		/*
		 * write out the data to the correct location.
		 * 
		 * If you compare this implementation to that of InsertResultSet, you'll notice
		 * that there is a whole lot less going on. That's because Triggers, Primary Keys, Check
		 * Constraints, and Secondary Indices are all handled through Coprocessors, and are thus transparent
		 * to the writer. This dramatically simplifies this code, at the cost of adding conceptual complexity
		 * in coprocessor logic
		 */
        SinkStats.SinkAccumulator stats = SinkStats.uniformAccumulator();
        stats.start();
        SpliceLogUtils.trace(LOG, ">>>>statistics starts for sink for InsertOperation at "+stats.getStartTime());
		ExecRow nextRow=null;
		//Use HTable to do inserts instead of HeapConglomerateController - see Bug 188
        Serializer serializer = new Serializer();
		try {
            CallBuffer<Mutation> writer = SpliceDriver.driver().getTableWriter()
                    .writeBuffer(Long.toString(heapConglom).getBytes());
            DataValueDescriptor[] template = ((SpliceOperation)source).getExecRowDefinition().getRowArray();
            RowSerializer rowKeySerializer =
                    new RowSerializer(template,pkColumns,pkColumns==null);
            do{
                long start =System.nanoTime();

                nextRow = source.getNextRowCore();
                if(nextRow==null)continue;
                stats.processAccumulator().tick(System.nanoTime()-start);

                start = System.nanoTime();
//                SpliceLogUtils.trace(LOG,"InsertOperation sink, nextRow="+nextRow);

                byte[] rowKey = rowKeySerializer.serialize(nextRow.getRowArray());
                writer.add(Puts.buildInsert(rowKey, nextRow.getRowArray(), this.getTransactionID(), serializer)); // Buffered

                stats.sinkAccumulator().tick(System.nanoTime()-start);
            }while(nextRow!=null);
            writer.flushBuffer();
            writer.close();
		} catch (Exception e) {
			//TODO -sf- abort transaction
            if(Exceptions.shouldLogStackTrace(e))
                SpliceLogUtils.logAndThrowRuntime(LOG,e);
            else
                throw Exceptions.getIOException(e);
		}
		SinkStats ss = stats.finish();
		SpliceLogUtils.trace(LOG, ">>>>statistics finishes for sink for InsertOperation at "+stats.getFinishTime());
        return ss;
	}

	
	@Override
	public String toString() {
		return "Insert{destTable="+heapConglom+",source=" + source + "}";
	}

}
