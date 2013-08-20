package com.splicemachine.derby.impl.job;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorTaskScheduler;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.utils.ErrorReporter;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.job.Status;
import com.splicemachine.job.TaskStatus;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.api.TransactorControl;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.utils.ByteDataOutput;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceZooKeeperManager;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Created on: 5/9/13
 */
public abstract class ZkTask extends SpliceConstants implements RegionTask,Externalizable {
    private static final long serialVersionUID = 5l;
    protected final Logger LOG;
    protected TaskStatus status;
    //job scheduler creates the task id
    protected byte[] taskId;
    protected String jobId;
    protected SpliceZooKeeperManager zkManager;
    private int priority;
    private String parentTxnId;
    private boolean readOnly;
    private String taskPath;

    protected ZkTask() {
        this.LOG = Logger.getLogger(this.getClass());
        this.status = new TaskStatus(Status.PENDING,null);
    }

    /**
     *
     * @param jobId
     * @param priority
     * @param parentTxnId the parent Transaction id, or {@code null} if
     *                    the task is non-transactional.
     * @param readOnly
     */
    protected ZkTask(String jobId, int priority,String parentTxnId,boolean readOnly) {
        this.LOG = Logger.getLogger(this.getClass());
        this.jobId = jobId;
        this.priority = priority;
        this.status = new TaskStatus(Status.PENDING,null);
        this.parentTxnId = parentTxnId;
        this.readOnly = readOnly;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        jobId = in.readUTF();
        priority = in.readInt();
        if(in.readBoolean())
            parentTxnId = in.readUTF();
        readOnly = in.readBoolean();
        int taskIdSize = in.readInt();
        if(taskIdSize>0){
            taskId = new byte[taskIdSize];
            in.readFully(taskId);
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(jobId);
        out.writeInt(priority);
        out.writeBoolean(parentTxnId!=null);
        if(parentTxnId!=null)
            out.writeUTF(parentTxnId);
        out.writeBoolean(readOnly);
        if(taskId!=null){
            out.writeInt(taskId.length);
            out.write(taskId);
        }else
            out.writeInt(0);
    }

    @Override
    public int getPriority() {
        return priority;
    }

    @Override
    public void prepareTask(RegionCoprocessorEnvironment rce, SpliceZooKeeperManager zooKeeper)
                                                                throws ExecutionException {
        taskId = SpliceUtils.getUniqueKey();
        taskPath = jobId+"_"+ getTaskType()+"-"+ BytesUtil.toHex(taskId);
        this.zkManager = zooKeeper;
        try {
            //create a child transaction
            if(parentTxnId!=null){
                TransactorControl transactor = HTransactorFactory.getTransactorControl();
                TransactionId parent = transactor.transactionIdFromString(parentTxnId);
                try {
                    TransactionId childTxnId  = transactor.beginChildTransaction(parent, !readOnly, !readOnly);
                    status.setTxnId(childTxnId.getTransactionIdString());
                } catch (IOException e) {
                    throw new ExecutionException("Unable to acquire child transaction",e);
                }
            }

            //create a status node
            final byte[] statusData = statusToBytes();

            taskPath= zooKeeper.execute(new SpliceZooKeeperManager.Command<String>() {
                @Override
                public String execute(RecoverableZooKeeper zooKeeper) throws InterruptedException, KeeperException {
                    return zooKeeper.create(SpliceConstants.zkSpliceTaskPath+"/"+taskPath,
                            statusData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                }
            });
        } catch (InterruptedException e) {
            throw new ExecutionException(e);
        } catch (KeeperException e) {
            throw new ExecutionException(e);
        } catch (IOException e) {
            throw new ExecutionException(e);
        }

        //attach a listener to the job node
        checkNotCancelled();
    }


    protected abstract String getTaskType();

    private void checkNotCancelled() throws ExecutionException{
        Stat stat;
        try {
            stat = zkManager.execute(new SpliceZooKeeperManager.Command<Stat>() {
                @Override
                public Stat execute(RecoverableZooKeeper zooKeeper) throws InterruptedException, KeeperException {
                    return zooKeeper.exists(CoprocessorTaskScheduler.getJobPath()+"/"+jobId,new Watcher() {
                        @Override
                        public void process(WatchedEvent event) {
                            if(event.getType()!= Event.EventType.NodeDeleted)
                                return;

                            switch(status.getStatus()){
                                case FAILED:
                                case COMPLETED:
                                case CANCELLED:
                                    return;
                            }

                            try{
                                markCancelled(false);
                            }catch(ExecutionException ee){
                                SpliceLogUtils.error(LOG,"Unable to cancel task with id "+ getTaskId(),ee.getCause());
                            }
                        }
                    });
                }
            });
            if(stat==null)
                markCancelled(false);
        } catch (InterruptedException e) {
            throw new ExecutionException(e);
        } catch (KeeperException e) {
            throw new ExecutionException(e);
        }
    }

    private void markCancelled(boolean propagate) throws ExecutionException{
        switch(status.getStatus()){
            case FAILED:
            case COMPLETED:
            case CANCELLED:
                return;
        }
        status.setStatus(Status.CANCELLED);
        if(propagate)
            updateStatus(false);
    }

    private void updateStatus(final boolean cancelOnError) throws ExecutionException{
        try{
            final byte[] status = statusToBytes();
            zkManager.executeUnlessExpired(new SpliceZooKeeperManager.Command<Void>() {
                @Override
                public Void execute(RecoverableZooKeeper zooKeeper) throws InterruptedException, KeeperException {
                    zooKeeper.setData(taskPath,status,-1);
                    return null;
                }
            });
        } catch (IOException e) {
            throw new ExecutionException(e);
        } catch (InterruptedException e) {
            throw new ExecutionException(e);
        } catch (KeeperException e) {
            if(e.code()== KeeperException.Code.NONODE&&cancelOnError){
                status.setStatus(Status.CANCELLED);
                throw new CancellationException();
            }
            throw new ExecutionException(e);
        }
    }

    private byte[] statusToBytes() throws IOException {
        ByteDataOutput bdo = new ByteDataOutput();
        bdo.writeObject(status);
        return bdo.toByteArray();
    }

    @Override
    public void markStarted() throws ExecutionException, CancellationException {
        setStatus(Status.EXECUTING,true);
    }

    private void setStatus(Status newStatus, boolean cancelOnError) throws ExecutionException{
        SpliceLogUtils.trace(LOG,"Marking task %s %s",taskId,newStatus);
        status.setStatus(newStatus);
        updateStatus(cancelOnError);
    }

    @Override
    public void markCompleted() throws ExecutionException {
        setStatus(Status.COMPLETED,false);
    }

    @Override
    public void markFailed(Throwable error) throws ExecutionException {
        ErrorReporter.get().reportError(this.getClass(),error);
        switch (status.getStatus()) {
            case INVALID:
            case FAILED:
            case COMPLETED:
                SpliceLogUtils.warn(LOG,"Received task error after entering " + status.getStatus()+" state, ignoring",error);
                return;
        }
        status.setError(error);
        setStatus(Status.FAILED,false);
    }

    @Override
    public void markCancelled() throws ExecutionException {
        setStatus(Status.CANCELLED,false);
    }

    @Override
    public boolean isCancelled() throws ExecutionException {
        return status.getStatus()==Status.CANCELLED;
    }

    @Override
    public byte[] getTaskId() {
        return taskId;
    }

    @Override
    public TaskStatus getTaskStatus() {
        return status;
    }

    @Override
    public void markInvalid() throws ExecutionException {
        setStatus(Status.INVALID,false);
    }

    @Override
    public boolean isInvalidated() {
        return status.getStatus()==Status.INVALID;
    }

    @Override
    public void cleanup() throws ExecutionException {
        throw new UnsupportedOperationException(
                "Tasks can not be cleaned up on the Task side--use JobScheduler.cleanupJob() instead");
    }

    @Override
    public String getTaskNode() {
        return taskPath;
    }
}
