package com.splicemachine.pipeline;

import org.sparkproject.guava.base.Throwables;
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
        Throwable t = Throwables.getRootCause(e);
        if(t instanceof StandardException) return (StandardException)t;

        ErrorState state = ErrorState.stateFor(e);

        return state.newException(rootCause);
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


}
