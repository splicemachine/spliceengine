package com.splicemachine.derby.hbase;

import com.splicemachine.async.ConnectionResetException;
import com.splicemachine.async.RecoverableException;
import com.splicemachine.pipeline.exception.ErrorState;
import com.splicemachine.si.api.CannotCommitException;
import com.splicemachine.db.iapi.error.StandardException;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;

import java.io.IOException;
import java.net.SocketTimeoutException;

/**
 * Class for holding unified logic between hbase-94 and hbase-98
 * exception translation logic.
 *
 * @author Scott Fines
 *         Date: 12/3/14
 */
public abstract class SkeletonExceptionTranslator implements ExceptionTranslator{
    @Override
    public boolean needsTransactionalRetry(Throwable t) {
        t = getRootCause(t);
        if(t instanceof CannotCommitException) return true;
        if(isCallTimeoutException(t)) return true;
        if(t instanceof SocketTimeoutException) return true;
        if(t instanceof RecoverableException) return true;
        return false;
    }

    @Override
    public boolean canFinitelyRetry(Throwable t) {
        t = getRootCause(t);
        if(isCallTimeoutException(t)) return true;
        else if(isConnectException(t)) return true;
        else if (t instanceof DoNotRetryIOException) return false;
        else if (t instanceof IOException) return true;
        else return false;
    }

    @Override
    public boolean canInfinitelyRetry(Throwable t) {
        t = getRootCause(t);
        if(isNotServingRegionException(t)
                || isWrongRegionException(t)
                || isRegionTooBusyException(t)
                || t instanceof RecoverableException
                || t instanceof ServerNotRunningYetException) return true;
        if(t instanceof StandardException){
            StandardException se = (StandardException)t;
            if(ErrorState.SPLICE_REGION_OFFLINE.getSqlState().equals(se.getSqlState())){
                /*
                 * SpliceRegionOffline is an error message that we throw that is a translation
                 * of a NotServingRegionException, WrongRegionException, or FailedServerException.
                 * All of these should be retried up to the query timeout (i.e. infinitely)
                 */
                return true;
            }
        }
        return false;
    }

    protected abstract Throwable getRootCause(Throwable t);
}
