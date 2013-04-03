package com.splicemachine.derby.impl.hbase.job.coprocessor;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationRegionScanner;
import com.splicemachine.derby.hbase.job.Status;
import com.splicemachine.derby.hbase.job.Task;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.hbase.job.OperationJob;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.si2.txn.TransactionManager;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 4/3/13
 */
public class OperationTask implements Task {
    private OperationTaskStatus context;
    private final HRegion region;

    private final RecoverableZooKeeper zooKeeper;

    private volatile String taskId;

    public OperationTask(Scan scan,
                         SpliceObserverInstructions instructions,
                         RecoverableZooKeeper zooKeeper,
                         HRegion region) {
        this.zooKeeper = zooKeeper;
        this.region = region;
        this.context = new OperationTaskStatus(scan,instructions,Status.PENDING,null);
    }

    public OperationTask(OperationJob job,RecoverableZooKeeper zooKeeper,HRegion region){
        this(job.getScan(), job.getInstructions(), zooKeeper,region);
    }

    public OperationTaskStatus getTaskStatus(){ return context;}

    @Override
    public void markStarted() throws ExecutionException, CancellationException {
        context.setStatus(Status.EXECUTING);

        updateStatus(true);
    }

    @Override
    public void markCompleted() throws ExecutionException {
        context.setStatus(Status.COMPLETED);
        updateStatus(false);
    }

    @Override
    public void markFailed(Throwable error) throws ExecutionException {
        context.setStatus(Status.FAILED);
        context.setError(error);

        updateStatus(false);
    }

    @Override
    public void markCancelled() throws ExecutionException {
        context.setStatus(Status.CANCELLED);

        updateStatus(false);
    }

    @Override
    public void execute() throws ExecutionException, InterruptedException {
        TransactionManager.setParentTransactionId(context.instructions.getTransactionId());

        Connection runningConnection = null;
        try{
            runningConnection = SpliceDriver.driver().acquireConnection();

            LanguageConnectionContext lcc = runningConnection.unwrap(EmbedConnection.class).getLanguageConnection();
            SpliceUtils.setThreadContext(lcc);
            Activation activation = context.instructions.getActivation(lcc);

            SpliceOperationContext opContext = new SpliceOperationContext(region,
                    context.scan,activation,context.instructions.getStatement(),runningConnection);
            SpliceOperationRegionScanner spliceScanner = new SpliceOperationRegionScanner(context.instructions.getTopOperation(),opContext);
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
        return context.getStatus()==Status.CANCELLED;
    }

    @Override
    public String getTaskId() {
        return taskId;
    }

    private void updateStatus(boolean cancelOnNoNode) throws ExecutionException {
        try {
            zooKeeper.setData(taskId ,context.toBytes(),-1);
        } catch (KeeperException e) {
            if(e.code()== KeeperException.Code.NONODE&&cancelOnNoNode){
                markCancelled();
                throw new CancellationException();
            }else{
                throw new ExecutionException(e);
            }
        } catch (InterruptedException e) {
            throw new ExecutionException(e);
        } catch (IOException e) {
            throw new ExecutionException(e);
        }
    }

    public HRegion getRegion() {
        return region;
    }

    public SpliceObserverInstructions getInstructions() {
        return context.instructions;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public static class OperationTaskStatus extends TaskStatus{
        private Scan scan;
        private SpliceObserverInstructions instructions;

        public OperationTaskStatus() { }

        public OperationTaskStatus(Scan scan, SpliceObserverInstructions instructions,Status status, Throwable error) {
            super(status, error);
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
            scan = new Scan();
            scan.readFields(in);
            instructions = (SpliceObserverInstructions)in.readObject();
        }
    }

}
