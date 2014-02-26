package com.splicemachine.derby.utils;

import com.splicemachine.derby.impl.job.scheduler.SchedulerTracer;
import com.splicemachine.hbase.writer.WriteUtils;
import com.splicemachine.si.api.TransactionStatus;
import com.splicemachine.si.api.TransactionManager;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Convenience utilities for working with Transactions
 *
 * @author Scott Fines
 * Created on: 10/9/13
 */
public class TransactionUtils {
    private static final Logger LOG = Logger.getLogger(TransactionUtils.class);


    public static boolean rollback(TransactionManager txnControl, String txnId, int maxTries) throws AttemptsExhaustedException{
        return rollback(txnControl,txnControl.transactionIdFromString(txnId),0,maxTries);
    }

    public static boolean rollback(TransactionManager txnControl, TransactionId tId,int tryCount,int maxTries) throws AttemptsExhaustedException {
        if(tryCount>maxTries){
            throw new AttemptsExhaustedException("Unable to roll back transaction after many retries");
        }

        if(tId==null){
            //emit a warning in case
            SpliceLogUtils.info(LOG, "Attempting to rollback a null transaction");
            return false;
        }
        try{
            SchedulerTracer.traceTaskRollback(tId);
            txnControl.rollback(tId);
            return true;
        } catch (IOException e) {
            /*
             * We encountered a problem rolling back. We need to make sure that we properly rolled
             * back (and that we don't need to fail everything)
             */
            TransactionStatus status = getTransactionStatus(tId,txnControl,0,maxTries);
            switch (status) {
                case ROLLED_BACK:
                    //we succeeded!
                    return true;
                case ACTIVE:
                    //we are able to attempt the roll back again
                    return rollback(txnControl, tId, tryCount+1, maxTries);
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
                    throw new AttemptsExhaustedException("Unable to rollback transaction",e);
            }
        }
    }

    public static boolean commit(TransactionManager txnControl, String txnId, int maxTries) throws AttemptsExhaustedException{
        return commit(txnControl,txnControl.transactionIdFromString(txnId),0,maxTries);
    }

    public static boolean commit( TransactionManager txnControl, TransactionId txnId,int tryCount,int maxTries) throws AttemptsExhaustedException {
        try{
            SchedulerTracer.traceTaskCommit(txnId);
            txnControl.commit(txnId);
            return true;
        } catch (IOException e) {
           /*
            * we got an error trying to commit. This leaves us in an awkward state. Because
            * the error could have come before OR after a successful write to HBase, we don't know
            * what state the actual transaction is. We must read that state, and decide what to do next.
            * In some cases, we can just retry the commit. In others, we have to retry the operation
            */
            TransactionStatus status = getTransactionStatus(txnId,txnControl,0,maxTries);
            switch (status) {
                case COMMITTED:
                    //we successfully commit!
                    return true;
                case ACTIVE:
                    //we can retry just the commit
                    return commit(txnControl,txnId,tryCount+1,maxTries);
                case COMMITTING:
                    throw new IllegalStateException("Transaction "+ txnId+" was seen in the state of COMMITTING when it should not be");
                case ROLLED_BACK:
                    LOG.error("Attempting to commit a rolled back transaction. This is likely to be a programmer error");
                    throw new AttemptsExhaustedException("Transaction "+ txnId+" has been rolled back");
                default:
                    //our transaction failed. We have to retry it
                    throw new AttemptsExhaustedException("Failed to commit,state is "+ status);
            }
        }
    }

    //get the transaction status (retrying if necessary)
    public static  TransactionStatus getTransactionStatus(TransactionId txnId,
                                                   TransactionManager txnControl,
                                                   int tryCount,
                                                   int maxTries) throws AttemptsExhaustedException {
        if(tryCount>maxTries)
            throw new AttemptsExhaustedException("Unable to obtain transaction status after 5 attempts");

        try{
            return txnControl.getTransactionStatus(txnId);
        } catch (IOException e) {
            int retriesLeft = maxTries-tryCount;
            LOG.error("Unable to read transaction status, retrying up to "+ retriesLeft+" more times", e);
            try {
                Thread.sleep(WriteUtils.getWaitTime(tryCount, 1000));
            } catch (InterruptedException e1) {
                LOG.info("Interrupted while waiting to retry Transaction table lookup");
            }
            return getTransactionStatus(txnId,txnControl,tryCount+1,maxTries);
        }
    }
}
