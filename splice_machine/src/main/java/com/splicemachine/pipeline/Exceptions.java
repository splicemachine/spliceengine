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

package com.splicemachine.pipeline;

import org.spark_project.guava.base.Throwables;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.impl.driver.SIDriver;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Created on: 3/5/13
 */
public class Exceptions {

//    private static final Logger LOG = Logger.getLogger(Exceptions.class);

    private Exceptions(){} //can't make me

    public static StandardException parseException(Throwable e) {
        return parseException(e,PipelineDriver.driver().exceptionFactory(),SIDriver.driver().getExceptionFactory());
    }

    public static StandardException parseException(Throwable e,PipelineExceptionFactory pef, ExceptionFactory baseEf) {
        if(e instanceof StandardException)
            return (StandardException)e;
        Throwable rootCause = Throwables.getRootCause(e);
        if (rootCause instanceof StandardException) {
            return (StandardException) rootCause;
        }
        if(pef!=null){
           e = pef.processPipelineException(rootCause);
        }else{
            assert baseEf!=null: "Cannot parse exception without an Exception Factory";
            e = baseEf.processRemoteException(rootCause);
        }

        ErrorState state = ErrorState.stateFor(e);

        return state.newException(e);
    }

    /*
     * Wraps the underlying exception in an IOException, WITHOUT regard to retryability.
     */
    public static IOException rawIOException(Throwable t){
        t = Throwables.getRootCause(t);
        if(t instanceof IOException) return (IOException)t;
        else return new IOException(t);
    }

    public static IOException getIOException(Throwable t,PipelineExceptionFactory ef){
        t = Throwables.getRootCause(t);
        if(t instanceof StandardException){
            return ef.doNotRetry(t);
        }
        else if(t instanceof IOException) return (IOException)t;
        else return new IOException(t);
    }

    public static IOException getIOException(Throwable t){
        return getIOException(t,PipelineDriver.driver().exceptionFactory());
    }

    public static void throwAsRuntime(Throwable t) {
        Exceptions.<RuntimeException> doThrow(t);
    }

    @SuppressWarnings("unchecked")
    static <T extends Throwable> void doThrow(Throwable t) throws T {
        throw (T) t;
    }

}
