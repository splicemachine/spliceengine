package com.splicemachine.derby.impl.job.operation;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationRegionScanner;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.operations.DMLWriteOperation;
import com.splicemachine.derby.impl.sql.execute.operations.OperationSink;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceZooKeeperManager;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.job.Status;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.sql.Activation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 4/3/13
 */
public class SinkTask extends ZkTask {
    private static final long serialVersionUID = 2l;
    private static final Logger LOG = Logger.getLogger(SinkTask.class);
    private HRegion region;

    private Scan scan;
    private SpliceObserverInstructions instructions;

    /**
     * Serialization Constructor.
     */
    public SinkTask(){
        super();
    }

    public SinkTask(String jobId,
                    Scan scan,
                    String transactionId,
                    boolean readOnly,
                    int priority) {
        super(jobId,priority,transactionId,readOnly);
        this.scan = scan;
    }

    @Override
    public void prepareTask(RegionCoprocessorEnvironment rce,SpliceZooKeeperManager zooKeeper) throws ExecutionException {
        //make sure that our task id is properly set
        this.region = rce.getRegion();
        super.prepareTask(rce, zooKeeper);
    }

    @Override
    public boolean invalidateOnClose() {
        return true;
    }

    @Override
    public void execute() throws ExecutionException, InterruptedException {
        SpliceLogUtils.trace(LOG,"executing task %s",getTaskId());
        SpliceTransactionResourceImpl impl = null;
        try {
            impl = new SpliceTransactionResourceImpl();
            ContextService.getFactory().setCurrentContextManager(impl.getContextManager());
            impl.marshallTransaction(status.getTransactionId());

            if(instructions==null)
                instructions = SpliceUtils.getSpliceObserverInstructions(scan);
            Activation activation = instructions.getActivation(impl.getLcc());
            SpliceOperationContext opContext = new SpliceOperationContext(region,
                    scan,activation,instructions.getStatement(),impl.getLcc(),true,instructions.getTopOperation());
            //init the operation stack

            SpliceOperation op = instructions.getTopOperation();
            op.init(opContext);

            OperationSink opSink = OperationSink.create(op, Bytes.toBytes(getTaskId()));

            TaskStats stats;
            if(op instanceof DMLWriteOperation)
                stats = opSink.sink(((DMLWriteOperation)op).getDestinationTable());
            else
                stats = opSink.sink(SpliceConstants.TEMP_TABLE_BYTES);
            status.setStats(stats);

            SpliceLogUtils.trace(LOG,"task %s sunk successfully, closing",getTaskId());
        } catch (Exception e) {
            if(e instanceof ExecutionException)
                throw (ExecutionException)e;
            else if(e instanceof InterruptedException)
                throw (InterruptedException)e;
            else throw new ExecutionException(e);
        } finally {
            if (impl != null) {
                impl.cleanup();
            }
        }
    }

    @Override
    public boolean isCancelled() throws ExecutionException {
        return status.getStatus()==Status.CANCELLED;
    }

    public HRegion getRegion() {
        return region;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        scan.write(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        scan  = new Scan();
        scan.readFields(in);
    }

    @Override
    protected String getTaskType() {
        if(instructions==null)
            instructions = SpliceUtils.getSpliceObserverInstructions(scan);
        return instructions.getTopOperation().getClass().getSimpleName();
    }
}
