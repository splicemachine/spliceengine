package com.splicemachine.derby.impl.job.operation;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.DerbyFactory;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.sql.execute.operations.DMLWriteOperation;
import com.splicemachine.job.Task;
import com.splicemachine.si.api.TxnView;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Map;

/**
 * @author Scott Fines
 * Created on: 4/3/13
 */
public class OperationJob extends SpliceConstants implements CoprocessorJob,Externalizable {
	private static final DerbyFactory derbyFactory = DerbyFactoryDriver.derbyFactory;
    private Scan scan;
    private SpliceObserverInstructions instructions;
    private HTableInterface table;
    private int taskPriority;
    private boolean readOnly;
		private boolean recordStats;
		private long statementId;
		private String xplainSchema;
    private String jobId;

    public OperationJob(){}

    public OperationJob(Scan scan,
												SpliceObserverInstructions instructions,
												HTableInterface table,
												boolean recordStats,
												long statementId,
												String xplainSchema) {
        this.scan = scan;
        this.instructions = instructions;
        this.table = table;
        this.taskPriority = operationTaskPriority;
				this.recordStats = recordStats;
				this.statementId = statementId;
				this.xplainSchema = xplainSchema;
        this.jobId = String.format("%s-%s", operationString(instructions.getTopOperation()),
                                    SpliceDriver.driver().getUUIDGenerator().nextUUID());
    }

    private static String operationString(SpliceOperation op){
        return String.format("%s[%s]", op.getClass().getSimpleName(), op.resultSetNumber());
    }

    @Override
    public String getJobId() {
        return jobId;
    }

    public Scan getScan(){
        return scan;
    }

    public SpliceObserverInstructions getInstructions(){
        return instructions;
    }

    @Override
    public Map<? extends RegionTask, Pair<byte[], byte[]>> getTasks() {
        return Collections.singletonMap(new SinkTask(getJobId(),scan,
                                                        instructions.getSpliceRuntimeContext().getParentTaskId(),
                                                        taskPriority),
                Pair.newPair(scan.getStartRow(),scan.getStopRow()));
    }

    public HTableInterface getTable(){
        return table;
    }

    @Override
    public byte[] getDestinationTable() {
        SpliceOperation topOperation = instructions.getTopOperation();
        if(topOperation instanceof DMLWriteOperation)
            return ((DMLWriteOperation)topOperation).getDestinationTable();
        return null;
    }

    @Override
    public TxnView getTxn() {
        return instructions.getTxn();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(jobId);
        derbyFactory.writeScanExternal(out, scan);
        out.writeObject(instructions);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        jobId = (String)in.readObject();
        scan = derbyFactory.readScanExternal(in);
        instructions = (SpliceObserverInstructions)in.readObject();
    }

    @Override
    public <T extends Task> Pair<T, Pair<byte[], byte[]>> resubmitTask(T originalTask, byte[] taskStartKey, byte[] taskEndKey) {
        return Pair.newPair(originalTask,Pair.newPair(taskStartKey,taskEndKey));
    }
}
