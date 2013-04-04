package com.splicemachine.derby.impl.job.coprocessor;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationRegionScanner;
import com.splicemachine.job.Status;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.job.OperationJob;
import com.splicemachine.derby.impl.job.ZooKeeperTask;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.si2.txn.TransactionManager;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 4/3/13
 */
public class SinkTask extends ZooKeeperTask {
    private HRegion region;

    private volatile String taskId;
    private Scan scan;
    private SpliceObserverInstructions instructions;

    /**
     * Serialization Constructor.
     */
    public SinkTask(){
        super();
    }

    public SinkTask(Scan scan,
                    SpliceObserverInstructions instructions,
                    RecoverableZooKeeper zooKeeper,
                    HRegion region) {
        super(buildTaskId(region),zooKeeper);
        this.region = region;
        this.scan = scan;
        this.instructions = instructions;
    }

    public SinkTask(OperationJob job, RecoverableZooKeeper zooKeeper, HRegion region){
        this(job.getScan(), job.getInstructions(), zooKeeper,region);
    }

    @Override
    public void execute() throws ExecutionException, InterruptedException {
        TransactionManager.setParentTransactionId(instructions.getTransactionId());

        Connection runningConnection = null;
        try{
            runningConnection = SpliceDriver.driver().acquireConnection();

            LanguageConnectionContext lcc = runningConnection.unwrap(EmbedConnection.class).getLanguageConnection();
            SpliceUtils.setThreadContext(lcc);
            Activation activation = instructions.getActivation(lcc);

            SpliceOperationContext opContext = new SpliceOperationContext(region,
                    scan,activation,instructions.getStatement(),runningConnection);
            SpliceOperationRegionScanner spliceScanner = new SpliceOperationRegionScanner(instructions.getTopOperation(),opContext);
            spliceScanner.sink();
            spliceScanner.close();
        } catch (SQLException e) {
            throw new ExecutionException(e);
        } catch (IOException e) {
            throw new ExecutionException(e);
        }finally{
            TransactionManager.setParentTransactionId(null);
            try {
                SpliceDriver.driver().closeConnection(runningConnection);
            } catch (SQLException e) {
                throw new ExecutionException(e);
            }
        }
    }

    @Override
    public boolean isCancelled() throws ExecutionException {
        return status.getStatus()==Status.CANCELLED;
    }

    @Override
    public String getTaskId() {
        if(taskId==null){
            taskId = buildTaskId(region);
        }
        return taskId;
    }

    private static String buildTaskId(HRegion region) {
        return region.getTableDesc().getNameAsString()+"/"+region.getRegionNameAsString();
    }

    public HRegion getRegion() {
        return region;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        scan.write(out);
        out.writeObject(instructions);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        scan  = new Scan();
        scan.readFields(in);

        instructions = (SpliceObserverInstructions)in.readObject();
    }
}
