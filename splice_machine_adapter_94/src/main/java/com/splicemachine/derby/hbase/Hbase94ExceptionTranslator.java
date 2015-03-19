package com.splicemachine.derby.hbase;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.ipc.HBaseClient;
import org.apache.hadoop.hbase.regionserver.WrongRegionException;

import com.splicemachine.pipeline.exception.IndexNotSetUpException;

import java.net.ConnectException;

/**
 * @author Scott Fines
 *         Date: 12/3/14
 */
public class Hbase94ExceptionTranslator extends SkeletonExceptionTranslator{
				public static Hbase94ExceptionTranslator INSTANCE = new Hbase94ExceptionTranslator();

				private Hbase94ExceptionTranslator(){}

    @Override
    protected Throwable getRootCause(Throwable t) {
				return com.splicemachine.pipeline.exception.Exceptions.getRootCause(t);
    }

    @Override
    public boolean isNotServingRegionException(Throwable t) {
        return t instanceof NotServingRegionException;
    }

    @Override
    public boolean isWrongRegionException(Throwable t) {
        return t instanceof WrongRegionException;
    }

    @Override
    public boolean isRegionTooBusyException(Throwable t) {
        return t instanceof RegionTooBusyException;
    }

    @Override
    public boolean isInterruptedException(Throwable t) {
        return t instanceof InterruptedException;
    }

    @Override
    public boolean isConnectException(Throwable t) {
        return t instanceof ConnectException;
    }

    @Override
    public boolean isIndexNotSetupException(Throwable t) {
        return t instanceof IndexNotSetUpException;
    }

    @Override
    public boolean isPleaseHoldException(Throwable t){
        return t instanceof PleaseHoldException;
    }

    @Override
    public boolean isCallTimeoutException(Throwable t) {
        return t instanceof HBaseClient.CallTimeoutException;
    }

    @Override
    public boolean isFailedServerException(Throwable t) {
        return t instanceof HBaseClient.FailedServerException;
    }

	@Override
	public boolean isDoNotRetryIOException(Throwable t) {
        return t instanceof DoNotRetryIOException;
	}
}
