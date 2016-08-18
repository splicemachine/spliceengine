package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.ipc.RemoteException;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;

/**
 * Created by jleach on 8/17/16.
 */
public class SpliceFailFastInterceptor extends RetryingCallerInterceptor {
    private static final RetryingCallerInterceptorContext NO_OP_CONTEXT =
            new NoOpRetryingInterceptorContext();
    protected SpliceFailFastInterceptor() {
        super();
    }

    @Override
    public RetryingCallerInterceptorContext createEmptyContext() {
        return NO_OP_CONTEXT;
    }

    @Override
    public void handleFailure(RetryingCallerInterceptorContext context, Throwable t) throws IOException {

        if (t instanceof UndeclaredThrowableException) {
            t = t.getCause();
        }
        if (t instanceof RemoteException) {
            RemoteException re = (RemoteException)t;
            t = re.unwrapRemoteException();
        }
        if (t instanceof DoNotRetryIOException) {
            throw (DoNotRetryIOException)t;
        }
        if (t instanceof IOException) {
            throw (IOException) t;
        }
        throw new IOException(t);
    }

    @Override
    public void intercept(RetryingCallerInterceptorContext abstractRetryingCallerInterceptorContext) throws IOException {

    }

    @Override
    public void updateFailureInfo(RetryingCallerInterceptorContext context) {
    }

    @Override
    public String toString() {
        return null;
    }
}
