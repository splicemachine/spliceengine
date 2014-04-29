package com.splicemachine.derby.impl.job.operation;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.job.Task;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.impl.TransactionId;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;

/**
 * @author Scott Fines
 *         Date: 4/22/14
 */
public class MultiScanOperationJob implements CoprocessorJob, Externalizable {
    private List<Scan> scans;
    private SpliceObserverInstructions instructions;
    private HTableInterface table;
    private int taskPriority;
    private boolean readOnly;
    private String jobId;

    public MultiScanOperationJob() {
    }

    public MultiScanOperationJob(List<Scan> scans,
                                 SpliceObserverInstructions instructions,
                                 HTableInterface table,
                                 int taskPriority,
                                 boolean readOnly) {
        this.scans = scans;
        this.instructions = instructions;
        this.table = table;
        this.taskPriority = taskPriority;
        this.readOnly = readOnly;
        this.jobId = String.format("%s-%s", operationString(instructions.getTopOperation()),
                SpliceDriver.driver().getUUIDGenerator().nextUUID());
    }

    private static String operationString(SpliceOperation op) {
        return String.format("%s[%s]", op.getClass().getSimpleName(), op.resultSetNumber());
    }

    @Override
    public Map<? extends RegionTask, Pair<byte[], byte[]>> getTasks() throws Exception {
        Map<SinkTask, Pair<byte[], byte[]>> taskMap = Maps.newHashMap();
        for (Scan scan : scans) {
            SinkTask task = new SinkTask(getJobId(), scan, instructions.getTransactionId(), readOnly, taskPriority);
            taskMap.put(task, Pair.newPair(scan.getStartRow(), scan.getStopRow()));
        }
        return taskMap;
    }

    @Override
    public HTableInterface getTable() {
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

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(jobId);
//				out.writeInt(scans.size());
//				for(Scan scan:scans){
//						scan.write(out);
//				}
        out.writeObject(instructions);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        jobId = (String) in.readObject();
//				int scanCount = in.readInt();
//				scans = Lists.newArrayListWithCapacity(scanCount);
//				for(int i=0;i<scanCount;i++){
//						Scan scan = new Scan();
//						scan.readFields(in);
//						scans.add(scan);
//				}
        instructions = (SpliceObserverInstructions) in.readObject();
    }

    @Override
    public String getJobId() {
        return jobId;
    }

    @Override
    public <T extends Task> Pair<T, Pair<byte[], byte[]>> resubmitTask(T originalTask, byte[] taskStartKey, byte[] taskEndKey) throws IOException {
                /*
				 * Tasks are not guaranteed to be contiguous (only ordered), so we need to make sure that
				 * we adjust the end key to fit within the containing scan. Otherwise, we may read data on TEMP
				 * that does not belong to us.
				 */
        for (Scan scan : scans) {
            byte[] scanStart = scan.getStartRow();
            if (BytesUtil.endComparator.compare(taskEndKey, scanStart) <= 0) continue; //does not contain us
            byte[] scanStop = scan.getStopRow();
            if (BytesUtil.startComparator.compare(taskStartKey, scanStop) >= 0) continue; //dont not contain us

            if (BytesUtil.endComparator.compare(taskEndKey, scanStop) > 0)
                taskEndKey = scanStop;
            break;
        }
        return Pair.newPair(originalTask, Pair.newPair(taskStartKey, taskEndKey));
    }
}
