package com.splicemachine.derby.impl.job.scheduler;

import com.google.common.base.Preconditions;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.job.coprocessor.TaskFutureContext;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.AttemptsExhaustedException;
import com.splicemachine.derby.utils.TransactionUtils;
import com.splicemachine.job.Status;
import com.splicemachine.job.TaskFuture;
import com.splicemachine.job.TaskStatus;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.api.TransactorControl;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceZooKeeperManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 9/17/13
 */
class RegionTaskControl implements Comparable<RegionTaskControl>,TaskFuture {
    private static final Logger LOG = Logger.getLogger(RegionTaskControl.class);
    private final byte[] startRow;
    private final RegionTask task;
    private final JobControl jobControl;
    private final TaskFutureContext taskFutureContext;
    private final int tryNum;
    private final SpliceZooKeeperManager zkManager;
    private volatile TaskStatus taskStatus = new TaskStatus(Status.PENDING,null);
    private volatile boolean refresh = true;
    /*
     * Flag to indicate that this task has been resubmitted and not to
     * retry resubmissions.
     */
    private volatile boolean trashed = false;

    RegionTaskControl(byte[] startRow,
                      RegionTask task,
                      JobControl jobControl,
                      TaskFutureContext taskFutureContext,
                      int tryNum,
                      SpliceZooKeeperManager zkManager) {
        this.startRow = startRow;
        this.task = task;
        this.jobControl = jobControl;
        this.taskFutureContext = taskFutureContext;
        this.tryNum = tryNum;
        this.zkManager = zkManager;
    }

    @Override
    public int compareTo(RegionTaskControl o) {
        if(o==null) return 1;
        int compare = Bytes.compareTo(startRow, o.startRow);
        if(compare!=0) return compare;

        //lexicographically sort based on the taskNode
        String taskNode = taskFutureContext.getTaskNode();
        String otherTasknode = o.taskFutureContext.getTaskNode();
        return taskNode.compareTo(otherTasknode);
    }

    @Override
    public Status getStatus() throws ExecutionException {
            /*
             * These states are permanent--once entered they cannot be escaped, so there's
             * no point in refreshing even if an event is fired
             */
        switch (taskStatus.getStatus()) {
            case INVALID:
            case FAILED:
            case COMPLETED:
            case CANCELLED:
                return taskStatus.getStatus();
        }
            /*
             * Unless we've received an event from ZooKeeper telling us the status has changed,
             * there's no need to fetch it. Thus, the watcher switches a refresh flag.
             */
        if(refresh){
                /*
                 * There is an inherent race-condition in this flag. Basically, what can happen is that
                 * the getData() call can return successfully (thus setting the watch), but before the
                 * old TaskStatus data has finished deserializing, the Watch is fired with a data changed
                 * situation. The Watch will set refresh = true, but if we set refresh = false at the end
                 * of this loop, then we can possibly override the setting in refresh that was set by the Watch.
                 * Thus, we reset the refresh flag here, and thus allow the watch to set the refresh flag without
                 * fear of being overridden.
                 */
            refresh=false;
            try{
                byte[] data = zkManager.executeUnlessExpired(new SpliceZooKeeperManager.Command<byte[]>() {
                    @Override
                    public byte[] execute(RecoverableZooKeeper zooKeeper) throws InterruptedException, KeeperException {
                        try{
                            return zooKeeper.getData(
                                    taskFutureContext.getTaskNode(),
                                    new org.apache.zookeeper.Watcher() {
                                        @Override
                                        public void process(WatchedEvent event) {
                                            SpliceLogUtils.trace(LOG, "Received %s event on node %s",
                                                    event.getType(), event.getPath());
                                            refresh=true;
                                            if(!trashed)
                                                jobControl.taskChanged(RegionTaskControl.this);
                                        }
                                    },new Stat());
                        }catch(KeeperException ke){
                            if(ke.code()== KeeperException.Code.NONODE){
                                    /*
                                     * The Task status node was deleted. This happens in two situations:
                                     *
                                     * 1. Job cleanup
                                     * 2. Session Expiration of the Region operator.
                                     *
                                     * Since we are clearly not in the cleanup phase, then the server
                                     * responsible for executing this task has failed, so we need to
                                     * re-submit this task.
                                     *
                                     * The resubmission happens elsewhere, we just need to inform the
                                     * caller that we are in an INVALID state, and it'll re-submit. Assume
                                     * then that null = INVALID.
                                     */
                                return null;
                            }
                            throw ke;
                        }
                    }
                });
                if(data==null){
                        /*
                         * Node failure, assume this = Status.INVALID
                         */
                    taskStatus.setStatus(Status.INVALID);
                }else{
                    taskStatus = TaskStatus.fromBytes(data);
                }
            } catch (InterruptedException e) {
                throw new ExecutionException(e);
            } catch (KeeperException e) {
                throw new ExecutionException(e);
            } catch (ClassNotFoundException e) {
                throw new ExecutionException(e);
            } catch (IOException e) {
                throw new ExecutionException(e);
            }
        }

        return taskStatus.getStatus();
    }

    @Override
    public void complete() throws ExecutionException {
        while(true){
            Status runningStatus = getStatus();
            switch (runningStatus) {
                case INVALID:
                    jobControl.markInvalid(this);
                    resubmit();
                    break;
                case FAILED:
                    dealWithError();
                    break;
                case COMPLETED:
                    return;
                case CANCELLED:
                    throw new CancellationException();
            }

            synchronized (taskFutureContext){
                try {
                    taskFutureContext.wait();
                } catch (InterruptedException e) {
                    //just spin
                }
            }
        }
    }

    @Override public double getEstimatedCost() { return taskFutureContext.getEstimatedCost(); }
    @Override public byte[] getTaskId() { return taskFutureContext.getTaskId(); }
    @Override public TaskStats getTaskStats() { return taskStatus.getStats(); }

/*************************************************************************************************/
    /*Package-local operators. Mainly used by JobControl */

    //get the zk node for this task
    String getTaskNode() {
        return taskFutureContext.getTaskNode();
    }

    //get the start row for the task
    byte[] getStartRow() {
        return startRow;
    }

    //convenience method for setting the state to failed
    void fail(Throwable cause) {
        taskStatus.setError(cause);
        taskStatus.setStatus(Status.FAILED);
    }

    //get the underlying task instance
    RegionTask getTask() {
        return task;
    }

    //deal with an error state. Retry if possible, otherwise, bomb out with a wrapper around a StandardException
    void dealWithError() throws ExecutionException{
        if(taskStatus.shouldRetry()) {
            resubmit();
        } else {
            throw new ExecutionException(taskStatus.getError());
        }
    }

    //get the current number of attempts that have been made to execute this task
    int tryNumber() {
        return tryNum;
    }

    /*
     * Roll back the transaction as safely as possible.
     *
     * If the task is nontransactional, does nothing
     *
     * If forceRollback = true, then we will do a precondition check to ensure that this task HAS a transaction, and explode if it doesn't. This
     * protects us from programmer error. However, as invalidations can result in tasks not having a child transaction, we can also be
     * relaxed about this check.
     */
    boolean rollback(int maxTries) {
        if(!task.isTransactional()){
            //no need to roll back a nontransactional task
            return true;
        }
        String tId = taskStatus.getTransactionId();
        if(tId==null){
            //emit a warning in case
            SpliceLogUtils.info(LOG,"Attempting to rollback a transactional task with no transaction. Task: "+ getTaskNode());
            return true;
        }

        try {
            return TransactionUtils.rollback(HTransactorFactory.getTransactorControl(),tId,maxTries); //TODO -sf- make 5 configurable
        } catch (AttemptsExhaustedException e) {
            fail(e);
            return false;
        }
    }



    boolean commit(int maxTries){
        if(!task.isTransactional()){
            return true;
        }
        String tId = taskStatus.getTransactionId();
        Preconditions.checkNotNull(tId,"Transactional task has no transaction");

        TransactorControl txnControl = HTransactorFactory.getTransactorControl();

        try {
            return TransactionUtils.commit(txnControl,tId,maxTries);//TODO -sf- make 5 configurable
        } catch (AttemptsExhaustedException e) {
            fail(e);
            return false;
        }
    }


/******************************************************************************************/
    /*private helper methods*/

    /*
     * Resubmits the task. Rolls back its child transaction, then resubmits
     */
    private void resubmit() throws ExecutionException{
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"resubmitting task %s",task.getTaskNode());
        trashed=true;

        if(rollback(5)) //TODO -sf- make this configurable
            jobControl.resubmit(this,tryNum);
        else
            throw new ExecutionException(taskStatus.getError());
    }

    public Throwable getError() {
        return taskStatus.getError();
    }

    public static void main(String... args) throws Exception{
        for(int i=0;i<16;i++){
            System.out.println(BytesUtil.toHex(new byte[]{(byte)(i*0x10)}));
        }
    }
}
