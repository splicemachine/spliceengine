package com.splicemachine.derby.impl.job.operation;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.job.Task;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.impl.TransactionId;

/**
 * @author Scott Fines
 * Created on: 4/3/13
 */
public class OperationJob extends SpliceConstants implements CoprocessorJob,Externalizable {
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
												boolean readOnly,
												boolean recordStats,
												long statementId,
												String xplainSchema) {
        this.scan = scan;
        this.instructions = instructions;
        this.table = table;
        this.taskPriority = operationTaskPriority;
        this.readOnly = readOnly;
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
        return Collections.singletonMap(new SinkTask(getJobId(),scan,instructions.getTransactionId(), readOnly, taskPriority),
                Pair.newPair(scan.getStartRow(),scan.getStopRow()));
    }

    public HTableInterface getTable(){
        return table;
    }

    @Override
    public TransactionId getParentTransaction() {
        return HTransactorFactory.getTransactionManager().transactionIdFromString(instructions.getTransactionId());
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    // FIXME: Part of old Writable interface - use protoBuf
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(jobId);
//        scan.write(out);
        out.writeObject(instructions);
    }

    // FIXME: Part of old Writable interface - use protoBuf
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        jobId = (String)in.readObject();
        scan = new Scan();
//        scan.readFields(in);
        instructions = (SpliceObserverInstructions)in.readObject();
    }

    @Override
    public <T extends Task> Pair<T, Pair<byte[], byte[]>> resubmitTask(T originalTask, byte[] taskStartKey, byte[] taskEndKey) {
        return Pair.newPair(originalTask,Pair.newPair(taskStartKey,taskEndKey));
    }
}
