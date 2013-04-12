package com.splicemachine.derby.impl.job.operation;

import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.utils.SpliceUtils;
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
 *         Created on: 4/3/13
 */
public class OperationJob implements CoprocessorJob,Externalizable {
    private static final int DEFAULT_OPERATION_PRIORITY = 3;
    public static final int operationTaskPriority;

    static{
        operationTaskPriority = SpliceUtils.config.getInt("splice.task.operationPriority",DEFAULT_OPERATION_PRIORITY);
    }

    private Scan scan;
    private SpliceObserverInstructions instructions;
    private HTableInterface table;
    private int taskPriority;

    public OperationJob(){}

    public OperationJob(Scan scan, SpliceObserverInstructions instructions, HTableInterface table) {
        this.scan = scan;
        this.instructions = instructions;
        this.table = table;
        this.taskPriority = operationTaskPriority;
    }

    @Override
    public String getJobId() {
        return instructions.getTopOperation().getUniqueSequenceID();
    }

    public Scan getScan(){
        return scan;
    }

    public SpliceObserverInstructions getInstructions(){
        return instructions;
    }

    @Override
    public Map<? extends RegionTask, Pair<byte[], byte[]>> getTasks() {
        return Collections.singletonMap(new SinkTask(getJobId(),scan,instructions,taskPriority),
                Pair.newPair(scan.getStartRow(),scan.getStartRow()));
    }

    public HTableInterface getTable(){
        return table;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        scan.write(out);
        out.writeObject(instructions);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        scan = new Scan();
        scan.readFields(in);
        instructions = (SpliceObserverInstructions)in.readObject();
    }
}
