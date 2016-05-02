package com.splicemachine.derby.impl.job;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.utils.ErrorReporter;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.job.Status;
import com.splicemachine.job.TaskStatus;
import com.splicemachine.si.api.TransactionOperations;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnLifecycleManager;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.*;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceZooKeeperManager;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
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
public abstract class ZkTask implements RegionTask, Externalizable{
    protected static final SDataLib dataLib=SIFactoryDriver.siFactory.getDataLib();
    private static final long serialVersionUID=6l;
    protected final Logger LOG;
    protected TaskStatus status;
    //job scheduler creates the task id
    protected byte[] taskId;
    protected String jobId;
    protected SpliceZooKeeperManager zkManager;
    private int priority;

    private String taskPath;

    protected byte[] parentTaskId=null;
    protected TaskWatcher taskWatcher;

    private volatile Thread executionThread;
    protected long prepareTimestamp;
    protected long waitTimeNs;
    protected HRegion region;

    private TxnView parentTxn;
    private transient Txn txn; //the child transaction that we operate against

    protected ZkTask(){
        this.LOG=Logger.getLogger(this.getClass());
        this.status=new TaskStatus(Status.PENDING,null);
    }

    /**
     * @param jobId
     * @param priority
     */
    protected ZkTask(String jobId,int priority){
        this(jobId,priority,null);
    }

    protected ZkTask(String jobId,int priority,
                     byte[] parentTaskId){
        this.LOG=Logger.getLogger(this.getClass());
        this.jobId=jobId;
        this.priority=priority;
        this.status=new TaskStatus(Status.PENDING,null);
        this.parentTaskId=parentTaskId;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        jobId=in.readUTF();
        priority=in.readInt();
        if(in.readBoolean())
            parentTxn=TransactionOperations.getOperationFactory().readTxn(in);
        int taskIdSize=in.readInt();
        if(taskIdSize>0){
            taskId=new byte[taskIdSize];
            in.readFully(taskId);
        }
        if(in.readBoolean()){
            parentTaskId=new byte[in.readInt()];
            in.readFully(parentTaskId);
        }
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        out.writeUTF(jobId);
        out.writeInt(priority);
        out.writeBoolean(parentTxn!=null);
        if(parentTxn!=null)
            TransactionOperations.getOperationFactory().writeTxn(parentTxn,out);
        if(taskId!=null){
            out.writeInt(taskId.length);
            out.write(taskId);
        }else
            out.writeInt(0);
        out.writeBoolean(parentTaskId!=null);
        if(parentTaskId!=null){
            out.writeInt(parentTaskId.length);
            out.write(parentTaskId);
        }
    }


    @Override
    public void execute() throws ExecutionException, InterruptedException{
        setupTransaction();

        waitTimeNs=System.nanoTime()-prepareTimestamp;
        try{
            doExecute();
        }finally{
            taskWatcher.task=null;
        }
    }

    protected TxnView getParentTxn(){
        return parentTxn;
    }

    protected void setupTransaction() throws ExecutionException{
    /*
     * Create the Child transaction.
     *
     * We do this here rather than elsewhere (like inside the JobScheduler) so
     * that we avoid timing out the transaction when we have to sit around and
     * wait for a long time.
     */
        if(parentTxn!=null){
            TxnLifecycleManager tc=TransactionLifecycle.getLifecycleManager();
            try{
                txn=beginChildTransaction(parentTxn,tc);
                status.setTxn(txn);
            }catch(IOException e){
                throw new ExecutionException("Unable to acquire child transaction",e);
            }
        }
    }

    protected void elevateTransaction(byte[] table) throws IOException{
        txn=txn.elevateToWritable(table);
        status.setTxn(txn);
    }

    protected Txn beginChildTransaction(TxnView parentTxn,TxnLifecycleManager tc) throws IOException{
        return tc.beginChildTransaction(parentTxn,null);
//				return transactor.beginChildTransaction(parent, !readOnly, !readOnly);
    }

    protected abstract void doExecute() throws ExecutionException, InterruptedException;

    @Override
    public void prepareTask(byte[] start,byte[] stop,RegionCoprocessorEnvironment rce,SpliceZooKeeperManager zooKeeper)
            throws ExecutionException{
        taskId=SpliceUtils.getUniqueKey();
        taskPath=jobId+"_"+getTaskType()+"-"+Bytes.toLong(taskId);
        this.zkManager=zooKeeper;
        this.region=rce.getRegion();
        try{
            //create a status node
            final byte[] statusData=statusToBytes();

            taskPath=zooKeeper.execute(new SpliceZooKeeperManager.Command<String>(){
                @Override
                public String execute(RecoverableZooKeeper zooKeeper) throws InterruptedException, KeeperException{
                    String path=SpliceConstants.zkSpliceTaskPath+"/"+taskPath;
                    zooKeeper.getZooKeeper().create(path,statusData,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL,new AsyncCallback.StringCallback(){
                        @Override
                        public void processResult(int rc,String path,Object ctx,String name){
                            if(LOG.isTraceEnabled())
                                LOG.trace("Task path created");
                        }
                    },this);
//                    return zooKeeper.create(path,
//                            statusData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                    return path;
                }
            });
        }catch(InterruptedException|KeeperException|IOException e){
            throw new ExecutionException(e);
        }

        //attach a listener to the job node
        checkNotCancelled();

        prepareTimestamp=System.nanoTime();
    }

    @Override
    public void setParentTxnInformation(TxnView parentTxn){
        this.parentTxn=parentTxn;
    }

    protected abstract String getTaskType();

    private void checkNotCancelled() throws ExecutionException{
        Stat stat;
        try{
            taskWatcher=new TaskWatcher(this);
            stat=zkManager.execute(new SpliceZooKeeperManager.Command<Stat>(){
                @Override
                public Stat execute(RecoverableZooKeeper zooKeeper) throws InterruptedException, KeeperException{
                    return zooKeeper.exists(SpliceUtils.zkSpliceJobPath+"/"+jobId,taskWatcher);
                }
            });
            if(stat==null)
                markCancelled(false);
        }catch(InterruptedException e){
            throw new ExecutionException(e);
        }catch(KeeperException e){
            throw new ExecutionException(e);
        }
    }

    @Override
    public void markCancelled(boolean propagate) throws ExecutionException{
        switch(status.getStatus()){
            case FAILED:
            case COMPLETED:
            case CANCELLED:
                return;
        }
        status.setStatus(Status.CANCELLED);
        if(propagate)
            updateStatus(false);

        if(executionThread!=null){
            LOG.info("Task "+Bytes.toLong(taskId)+" has been cancelled, interrupting worker thread");
            executionThread.interrupt();
        }
    }

    private void updateStatus(final boolean cancelOnError) throws ExecutionException{
        boolean interrupted=Thread.currentThread().isInterrupted();
        Thread.interrupted(); //clear the interrupt status so that this will complete
        try{
            final byte[] status=statusToBytes();
            zkManager.executeUnlessExpired(new SpliceZooKeeperManager.Command<Void>(){
                @Override
                public Void execute(RecoverableZooKeeper zooKeeper) throws InterruptedException, KeeperException{
                    zooKeeper.setData(taskPath,status,-1);
                    return null;
                }
            });
        }catch(IOException e){
            throw new ExecutionException(e);
        }catch(InterruptedException e){
            throw new ExecutionException(e);
        }catch(KeeperException e){
            if(e.code()==KeeperException.Code.NONODE && cancelOnError){
                status.setStatus(Status.CANCELLED);
                throw new CancellationException();
            }
            throw new ExecutionException(e);
        }finally{
            if(interrupted)
                Thread.currentThread().interrupt();
        }
    }

    private byte[] statusToBytes() throws IOException{
        return status.toBytes();
    }

    @Override
    public void markStarted() throws ExecutionException, CancellationException{
        executionThread=Thread.currentThread();
        TaskStatus taskStatus=getTaskStatus();
        //if task has already been cancelled, then throw it away
        if(taskStatus.getStatus()==Status.CANCELLED) throw new CancellationException();
        setStatus(Status.EXECUTING,true);
    }

    private void setStatus(Status newStatus,boolean cancelOnError) throws ExecutionException{
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"Marking task %s %s",Bytes.toLong(getTaskId()),newStatus);
        status.setStatus(newStatus);
        updateStatus(cancelOnError);
    }

    @Override
    public void markCompleted() throws ExecutionException{
        setStatus(Status.COMPLETED,false);
    }

    @Override
    public void markFailed(Throwable error) throws ExecutionException{
        ErrorReporter.get().reportError(this.getClass(),error);
        switch(status.getStatus()){
            case INVALID:
            case FAILED:
            case COMPLETED:
                SpliceLogUtils.warn(LOG,"["+Bytes.toLong(getTaskId())+"]:Received task error after entering "+status.getStatus()+" state, ignoring",error);
                return;
        }
        status.setError(error);
        setStatus(Status.FAILED,false);
    }

    @Override
    public void markCancelled() throws ExecutionException{
        markCancelled(false);
    }

    @Override
    public boolean isCancelled() throws ExecutionException{
        return status.getStatus()==Status.CANCELLED;
    }

    @Override
    public byte[] getTaskId(){
        return taskId;
    }

    @Override
    public TaskStatus getTaskStatus(){
        return status;
    }

    @Override
    public void markInvalid() throws ExecutionException{
        if(taskWatcher!=null)
            taskWatcher.task=null;
        //only invalidate if we are in the PENDING state, otherwise let it go through or fail
        if(getTaskStatus().getStatus()!=Status.PENDING)
            return;
        setStatus(Status.INVALID,false);
    }

    @Override
    public boolean isInvalidated(){
        return status.getStatus()==Status.INVALID;
    }

    @Override
    public void cleanup() throws ExecutionException{
        taskWatcher.task=null;
        throw new UnsupportedOperationException(
                "Tasks can not be cleaned up on the Task side--use JobScheduler.cleanupJob() instead");
    }

    @Override
    public String getTaskNode(){
        return taskPath;
    }

    @Override
    public boolean isSplittable(){
        return false;
    }

    @Override
    public Txn getTxn(){
        return txn;
    }

    @Override
    public byte[] getParentTaskId(){
        return parentTaskId;
    }

    @Override
    public String getJobId(){
        return jobId;
    }

    @Override
    public boolean isMaintenanceTask(){
        return false;
    }

    protected static class TaskWatcher implements Watcher{
        private volatile ZkTask task;

        private TaskWatcher(ZkTask task){
            this.task=task;
        }

        public void setTask(ZkTask task){
            this.task=task;
        }

        @Override
        public void process(WatchedEvent event){
            if(event.getType()!=Event.EventType.NodeDeleted)
                return;


						/*
                         * If the watch was triggered after
						 * dereferencing us, then we don't care about it
						 */
            if(task==null) return;

            if(task.LOG.isTraceEnabled())
                task.LOG.trace("Received node deleted notice from ZooKeeper, attempting cancellation");

            switch(task.status.getStatus()){
                case FAILED:
                case COMPLETED:
                case CANCELLED:
                    if(task.LOG.isTraceEnabled())
                        task.LOG.trace("Node is already in a finalize state, ignoring cancellation attempt");
                    task=null;
                    return;
            }

            try{
                task.markCancelled(false);
            }catch(ExecutionException ee){
                SpliceLogUtils.error(task.LOG,"Unable to cancel task with id "+Bytes.toLong(task.getTaskId()),ee.getCause());
            }finally{
                task=null;
            }
        }
    }

    private void encodeParentTxn(ObjectOutput out) throws IOException{
        out.writeLong(parentTxn.getTxnId());
        out.writeLong(parentTxn.getParentTxnId());
        out.writeByte(parentTxn.getIsolationLevel().encode());
        out.writeBoolean(parentTxn.allowsWrites());
        out.writeBoolean(parentTxn.isAdditive());
    }

    private TxnView decodeTxn(ObjectInput in) throws IOException{
        long parentTxnId=in.readLong();
        long pParentTxnId=in.readLong();
        Txn.IsolationLevel level=Txn.IsolationLevel.fromByte(in.readByte());
        boolean allowWrites=in.readBoolean();
        boolean additive=in.readBoolean();

        TxnView ppParent;
        if(pParentTxnId<0)
            ppParent=Txn.ROOT_TRANSACTION;
        else{
            /*
             *TransactionStorage.getTxnSupplier() will most likely return a lazy parent transaction,
             * which is good since the grandparent is unlikely to be accessed very often.
             */
            ppParent=new LazyTxnView(pParentTxnId,TransactionStorage.getTxnSupplier());
        }

        return new InheritingTxnView(ppParent,parentTxnId,parentTxnId,level,
                true,additive,true,allowWrites,
                -1l,-1l,Txn.State.ACTIVE);
    }
}
