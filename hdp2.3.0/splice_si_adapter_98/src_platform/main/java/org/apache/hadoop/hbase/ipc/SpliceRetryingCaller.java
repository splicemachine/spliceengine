package org.apache.hadoop.hbase.ipc;

import org.apache.hadoop.hbase.client.RetryingCallable;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 5/13/15
 */
public class SpliceRetryingCaller<T> implements SpliceRetryingCall<T>{
    private final RetryingCallable<T> callable;

    public SpliceRetryingCaller(RetryingCallable<T> callable){
        this.callable=callable;
    }

    @Override
    public void prepare(boolean reload) throws IOException{
        callable.prepare(reload);
    }

    @Override
    public void throwable(Throwable t,boolean retrying){
        callable.throwable(t,retrying);
    }

    @Override
    public T call(int callTimeout) throws Exception{
        return callable.call(callTimeout);
    }

    @Override
    public String getExceptionMessageAdditionalDetail(){
        return callable.getExceptionMessageAdditionalDetail();
    }

    @Override
    public long sleep(long pause,int tries){
        return callable.sleep(pause,tries);
    }

    @Override
    public void close(){
        //no-op
    }
}
