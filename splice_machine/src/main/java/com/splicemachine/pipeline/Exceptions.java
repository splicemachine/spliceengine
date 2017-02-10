/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

        return state.newException(rootCause);
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
