/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
