/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.hbase.ipc;

import com.splicemachine.pipeline.client.SpliceRetryingCall;
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
