package com.splicemachine.derby.hbase;

import com.splicemachine.si.api.CannotCommitException;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.DoNotRetryIOException;

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
        return isNotServingRegionException(t)
                || isWrongRegionException(t)
                || isRegionTooBusyException(t);
    }

    protected abstract Throwable getRootCause(Throwable t);
}
