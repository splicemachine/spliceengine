package com.splicemachine.derby.impl.job.operation;

import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationRegionScanner;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.SpliceZooKeeperManager;
import com.splicemachine.job.Status;
import com.splicemachine.si.impl.SITransactionId;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.sql.Activation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
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
    private static final long serialVersionUID = 1l;
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
                    SpliceObserverInstructions instructions,
                    boolean readOnly,
                    int priority) {
        super(jobId,priority,instructions.getTransactionId(),readOnly);
        this.scan = scan;
        this.instructions = instructions;
    }

    @Override
    public void prepareTask(HRegion region,SpliceZooKeeperManager zooKeeper) throws ExecutionException {
        //make sure that our task id is properly set
        this.region = region;
        super.prepareTask(region, zooKeeper);
    }

    @Override
    public boolean invalidateOnClose() {
        return true;
    }

    @Override
    public void execute() throws ExecutionException, InterruptedException {
        SpliceLogUtils.trace(LOG,"executing task %s",getTaskId());
        try {
            SpliceTransactionResourceImpl impl = new SpliceTransactionResourceImpl();
            ContextService.getFactory().setCurrentContextManager(impl.getContextManager());
            impl.marshallTransaction(status.getTransactionId());
            Activation activation = instructions.getActivation(impl.getLcc());
            SpliceOperationContext opContext = new SpliceOperationContext(region,scan,activation,instructions.getStatement(),impl.getLcc());
            SpliceOperationRegionScanner spliceScanner = new SpliceOperationRegionScanner(instructions.getTopOperation(),opContext);

            SpliceLogUtils.trace(LOG, "sinking task %s", getTaskId());
            TaskStats stats = spliceScanner.sink();
            status.setStats(stats);

            SpliceLogUtils.trace(LOG,"task %s sunk successfully, closing",getTaskId());
            spliceScanner.close();
        } catch (Exception e) {
            if(e instanceof ExecutionException)
                throw (ExecutionException)e;
            else if(e instanceof InterruptedException)
                throw (InterruptedException)e;
            else throw new ExecutionException(e);
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
        out.writeObject(instructions);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        scan  = new Scan();
        scan.readFields(in);

        instructions = (SpliceObserverInstructions)in.readObject();
    }
}
