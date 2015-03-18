package com.splicemachine.derby.hbase;

import com.google.common.base.Throwables;
import com.splicemachine.pipeline.exception.IndexNotSetUpException;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.exceptions.RegionMovedException;
import org.apache.hadoop.hbase.ipc.RemoteWithExtrasException;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.regionserver.WrongRegionException;
import org.apache.hadoop.ipc.RemoteException;

import java.net.ConnectException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 12/3/14
 */
public class Hbase98ExceptionTranslator extends SkeletonExceptionTranslator{
    public static Hbase98ExceptionTranslator INSTANCE = new Hbase98ExceptionTranslator();

    @Override
    protected Throwable getRootCause(Throwable t) {
        if(t instanceof RemoteException)
            t = ((RemoteException)t).unwrapRemoteException();

        t = Throwables.getRootCause(t);
        if(t instanceof RetriesExhaustedWithDetailsException ){
            RetriesExhaustedWithDetailsException rewde = (RetriesExhaustedWithDetailsException)t;
            List<Throwable> causes = rewde.getCauses();
            if(causes!=null&&causes.size()>0)
                t = causes.get(0);
        }
        return t;
    }

    @Override
    public boolean isCallTimeoutException(Throwable t) {
        return t instanceof RpcClient.CallTimeoutException;
    }

    @Override
    public boolean isPleaseHoldException(Throwable t){
        if(t.getCause() instanceof RemoteWithExtrasException){
            t = ((RemoteWithExtrasException)t.getCause()).unwrapRemoteException();
        }
        if(t instanceof RemoteWithExtrasException){
            t = ((RemoteWithExtrasException)t).unwrapRemoteException();
        }
        return t instanceof PleaseHoldException;
    }

    @Override
    public boolean isNotServingRegionException(Throwable t) {
        return t instanceof NotServingRegionException || isRemoteWithExtras(t, NotServingRegionException.class.getCanonicalName());
    }

    @Override
    public boolean isWrongRegionException(Throwable t) {
        return t instanceof WrongRegionException || isRemoteWithExtras(t, WrongRegionException.class.getCanonicalName()) ||
                t instanceof RegionMovedException || isRemoteWithExtras(t, RegionMovedException.class.getCanonicalName());
    }

    @Override
    public boolean isRegionTooBusyException(Throwable t) {
        return t instanceof RegionTooBusyException || isRemoteWithExtras(t, RegionTooBusyException.class.getCanonicalName());
    }

    @Override
    public boolean isInterruptedException(Throwable t) {
        return t instanceof InterruptedException || isRemoteWithExtras(t, InterruptedException.class.getCanonicalName());
    }

    @Override
    public boolean isConnectException(Throwable t) {
        return t instanceof ConnectException || isRemoteWithExtras(t, ConnectException.class.getCanonicalName());
    }

    @Override
    public boolean isIndexNotSetupException(Throwable t) {
        return t instanceof IndexNotSetUpException || isRemoteWithExtras(t, IndexNotSetUpException.class.getCanonicalName());
    }

    @Override
    public boolean isFailedServerException(Throwable t) {
        return t instanceof RpcClient.FailedServerException;
    }

	@Override
	public boolean isDoNotRetryIOException(Throwable t) {
        return t instanceof DoNotRetryIOException || isRemoteWithExtras(t, DoNotRetryIOException.class.getCanonicalName());
	}

    private boolean isRemoteWithExtras(Throwable t, String className) {
        if (t instanceof RemoteWithExtrasException &&
                ((RemoteWithExtrasException) t).getClassName() != null &&
                ((RemoteWithExtrasException) t).getClassName().equals(className))
            return true;
        return false;
    }
}
