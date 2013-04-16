package com.splicemachine.derby.impl.job.operation;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationRegionScanner;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.job.ZooKeeperTask;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.job.Status;
import com.splicemachine.si.api.ParentTransactionManager;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 4/3/13
 */
public class SinkTask extends ZooKeeperTask {
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
                    int priority) {
        super(jobId,priority);
        this.scan = scan;
        this.instructions = instructions;
    }

    @Override
    public void prepareTask(HRegion region, RecoverableZooKeeper zooKeeper) throws ExecutionException {
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
            ParentTransactionManager.runInParentTransaction(instructions.getTransactionId(), new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    run();
                    return null;
                }
            });
        } catch (Exception e) {
            if(e instanceof ExecutionException)
                throw (ExecutionException)e;
            else if(e instanceof InterruptedException)
                throw (InterruptedException)e;
            else throw new ExecutionException(e);
        }
    }

    private void run() throws InterruptedException, StandardException, ExecutionException {
        Connection runningConnection = null;
        try{
            runningConnection = SpliceDriver.driver().acquireConnection();

            LanguageConnectionContext lcc = runningConnection.unwrap(EmbedConnection.class).getLanguageConnection();
            SpliceUtils.setThreadContext(lcc);
            Activation activation = instructions.getActivation(lcc);

            SpliceOperationContext opContext = new SpliceOperationContext(region,
                    scan,activation,instructions.getStatement(),runningConnection);
            SpliceOperationRegionScanner spliceScanner = new SpliceOperationRegionScanner(instructions.getTopOperation(),opContext);

            SpliceLogUtils.trace(LOG, "sinking task %s", getTaskId());
            TaskStats stats = spliceScanner.sink();
            status.setStats(stats);

            SpliceLogUtils.trace(LOG,"task %s sunk successfully, closing",getTaskId());
            spliceScanner.close();
        } catch (SQLException e) {
            SpliceLogUtils.error(LOG,"Exception encountered dealing with Connection pool",e);
            throw new ExecutionException(e);
        } catch (IOException e) {
            SpliceLogUtils.error(LOG,"Exception encountered dealing with Connection pool",e);
            throw new ExecutionException(e);
        }finally{
            try {
                SpliceDriver.driver().closeConnection(runningConnection);
            } catch (SQLException e) {
                SpliceLogUtils.error(LOG,"Exception encountered dealing with Connection pool",e);
                throw new ExecutionException(e);
            }
        }
    }

    @Override
    public boolean isCancelled() throws ExecutionException {
        return status.getStatus()==Status.CANCELLED;
    }

    @Override
    public int getPriority() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    protected String getTaskType() {
        return instructions.getTopOperation().getClass().getSimpleName();
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
