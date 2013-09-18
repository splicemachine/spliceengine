package com.splicemachine.derby.impl.job.scheduler;

import com.google.common.base.Preconditions;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.job.coprocessor.TaskFutureContext;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.AttemptsExhaustedException;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.hbase.writer.WriteUtils;
import com.splicemachine.job.Status;
import com.splicemachine.job.TaskFuture;
import com.splicemachine.job.TaskStatus;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.api.TransactionStatus;
import com.splicemachine.si.api.TransactorControl;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.utils.ByteDataInput;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceZooKeeperManager;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
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
        return Bytes.compareTo(startRow,o.startRow);
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
                    ByteDataInput bdi = new ByteDataInput(data);
                    taskStatus = (TaskStatus)bdi.readObject();
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
            throw new ExecutionException(Exceptions.getWrapperException(taskStatus.getErrorMessage(), taskStatus.getErrorCode()));
        }
    }

    //get the current number of attempts that have been made to execute this task
    int tryNumber() {
        return tryNum;
    }

    //rollback this task's transaction (if it has one)
    boolean rollback(int numTries) {
        if(!task.isTransactional()){
            //no need to roll back a nontransactional task
            return true;
        }
        if(numTries<0){
            fail(new AttemptsExhaustedException("Unable to roll back transaction after many retries"));
            return false;
        }

        String tId = taskStatus.getTransactionId();
        Preconditions.checkNotNull(tId, "Transactional task has no transaction");

        TransactorControl txnControl = HTransactorFactory.getTransactorControl();
        TransactionId txnId = txnControl.transactionIdFromString(tId);
        try{
            txnControl.rollback(txnId);
            return true;
        } catch (IOException e) {
            /*
             * We encountered a problem rolling back. We need to make sure that we properly rolled
             * back (and that we don't need to fail everything)
             */
            TransactionStatus status;
            try {
                status = getTransactionStatus(txnId,txnControl,5);
            } catch (RetriesExhaustedException e1) {
                LOG.error("Unable to read transaction status after 5 attempts. Something is really wrong, failing entire job",e1);
                fail(new AttemptsExhaustedException(e));
                return false;
            }
            switch (status) {
                case ROLLED_BACK:
                    //we succeeded!
                    return false;
                case ACTIVE:
                    //we are able to attempt the roll back again
                    return rollback(numTries-1);
                case COMMITTING:
                    throw new IllegalStateException("Saw a Transaction state of COMMITTING where it wasn't expected");
                case COMMITTED:
                    /*
                     * On the one hand, rolling back a committed transaction does nothing. On the other, why are we
                     * hitting this? Probably a programmer's error, throw an assertion error
                     */
                    throw new IllegalStateException("Attempted to roll back a COMMITTED transaction");
                default:
                    //we encountered an error that we can't recover from, kill the entire job
                    fail(new DoNotRetryIOException("Unable to rollback transaction",e));
                    return false;
            }
        }
    }

    //commit this task's transaction (if it has one)
    boolean commit(int maxTries,int tryCount){
        if(!task.isTransactional()){
            //no need to roll back a nontransactional task
            return true;
        }
        if(tryCount<0){
            fail(new AttemptsExhaustedException("Unable to commit transaction after " + maxTries + " retries"));
            return false;
        }

        String tId = taskStatus.getTransactionId();
        Preconditions.checkNotNull(tId, "Transactional task has no transaction");

        TransactorControl txnControl = HTransactorFactory.getTransactorControl();
        TransactionId txnId = txnControl.transactionIdFromString(tId);
        try{
            txnControl.commit(txnId);
            return true;
        } catch (IOException e) {
           /*
            * we got an error trying to commit. This leaves us in an awkward state. Because
            * the error could have come before OR after a successful write to HBase, we don't know
            * what state the actual transaction is. We must read that state, and decide what to do next.
            * In some cases, we can just retry the commit. In others, we have to retry the operation
            */
            TransactionStatus status;
            try {
                status = getTransactionStatus(txnId,txnControl,maxTries);
            } catch (RetriesExhaustedException e1) {
                //we cannot even get the transaction status, we're screwed.
                //we have no choice but to bomb out the entire job
                fail(new AttemptsExhaustedException(e1.getMessage()));
                return false;
            }
            switch (status) {
                case COMMITTED:
                    //we successfully commit!
                    return true;
                case ACTIVE:
                    //we can retry just the commit
                    return commit(maxTries, tryCount-1);
                case COMMITTING:
                    throw new IllegalStateException("Transaction "+ tId+" was seen in the state of COMMITTING when it should not be");
                case ROLLED_BACK:
                    LOG.error("Attempting to commit a rolled back transaction. This is likely to be a programmer error");
                    fail(new IOException("Transaction " + tId + " has been rolled back"));
                    return false;
                default:
                    //our transaction failed. We have to retry it
                    fail(new IOException("failed to commit"));
                    return false;

            }
        }
    }

/******************************************************************************************/
    /*private helper methods*/

    //get the transaction status (retrying if necessary)
    private TransactionStatus getTransactionStatus(TransactionId txnId,
                                                   TransactorControl txnControl,
                                                   int retriesLeft) throws RetriesExhaustedException {
        if(retriesLeft<0)
            throw new RetriesExhaustedException("Unable to obtain transaction status after 5 attempts");

        try{
            return txnControl.getTransactionStatus(txnId);
        } catch (IOException e) {
            LOG.error("Unable to read transaction status, retrying up to "+ retriesLeft+" more times", e);
            try {
                Thread.sleep(WriteUtils.getWaitTime(5-retriesLeft,1000));
            } catch (InterruptedException e1) {
                LOG.info("Interrupted while waiting to retry Transaction table lookup");
            }
            return getTransactionStatus(txnId,txnControl,retriesLeft-1);
        }
    }

    //resubmit this task. Roll back it's transaction and then resubmit
    private void resubmit() throws ExecutionException{
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"resubmitting task %s",task.getTaskNode());

        if(rollback(tryNum))
            jobControl.resubmit(this,tryNum);
        else
            throw new ExecutionException(Exceptions.getWrapperException(taskStatus.getErrorMessage(),taskStatus.getErrorCode()));
    }
}
