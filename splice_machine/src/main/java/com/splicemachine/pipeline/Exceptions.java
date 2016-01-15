package com.splicemachine.pipeline;

import com.google.common.base.Throwables;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Created on: 3/5/13
 */
public class Exceptions {

//    private static final Logger LOG = Logger.getLogger(Exceptions.class);

    private Exceptions(){} //can't make me

    public static StandardException parseException(Throwable e) {
        Throwable rootCause = Throwables.getRootCause(e);
        if (rootCause instanceof StandardException) {
            return (StandardException) rootCause;
        }
        PipelineExceptionFactory ef=PipelineDriver.driver().exceptionFactory();

        e = ef.processPipelineException(rootCause);
        ErrorState state = ErrorState.stateFor(rootCause);

//        if (rootCause instanceof RetriesExhaustedWithDetailsException) {
//            return parseException((RetriesExhaustedWithDetailsException) rootCause);
//        } else if (rootCause instanceof ConstraintViolation.ConstraintViolationException) {
//            return toStandardException((ConstraintViolation.ConstraintViolationException) rootCause);
//        } else if(rootCause instanceof SparkException) {
//            if (rootCause.getMessage().contains("com.splicemachine.db.iapi.error.StandardException")) {
//                // the root cause of the Spark failure was a standard exception, let's parse it
//                String message = rootCause.getMessage();
//                Matcher m = Pattern.compile("messageId\\\":\\\"(\\d+)\\\"").matcher(message);
//                if (m.find()) {
//                    return StandardException.newException(m.group(1));
//                }
//            }
//        } else if(rootCause instanceof SpliceDoNotRetryIOException){
//            SpliceDoNotRetryIOException spliceException = (SpliceDoNotRetryIOException)rootCause;
//            Exception unwrappedException = SpliceDoNotRetryIOExceptionWrapping.unwrap(spliceException);
//            return parseException(unwrappedException);
//        } else if(rootCause instanceof DoNotRetryIOException ){
//						if(rootCause.getMessage()!=null && rootCause.getMessage().contains("rpc timeout")) {
//								return StandardException.newException(MessageId.QUERY_TIMEOUT, "Increase hbase.rpc.timeout");
//						}
//        } else if(rootCause instanceof SpliceStandardException){
//            return ((SpliceStandardException)rootCause).generateStandardException();
//        }else if(rootCause instanceof RemoteException){
//            rootCause = ((RemoteException)rootCause).unwrapRemoteException();
//        }
//
//        ErrorState state = ErrorState.stateFor(rootCause);
        return state.newException(rootCause);
    }

//    public static StandardException parseException(RetriesExhaustedWithDetailsException rewde){
//        List<Throwable> causes = rewde.getCauses();
//        /*
//         * Look for any exception that can be converted into a known error type (instead of a DATA_UNEXPECTED_EXCEPTION)
//         */
//        for(Throwable t:causes){
//            if(isExpected(t))
//                return parseException(t);
//        }
//        if(causes.size()>0)
//            return StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,causes.get(0));
//        else
//            return StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,rewde);
//
//    }

//    private static boolean isExpected(Throwable rootCause){
//        ErrorState state = ErrorState.stateFor(rootCause);
//        return state != ErrorState.DATA_UNEXPECTED_EXCEPTION;
//    }
//
//    public static StandardException toStandardException(ConstraintViolation.ConstraintViolationException violationException) {
//        String errorCode;
//        if (violationException instanceof ConstraintViolation.PrimaryKeyViolation || violationException instanceof ConstraintViolation.UniqueConstraintViolation) {
//            errorCode = SQLState.LANG_DUPLICATE_KEY_CONSTRAINT;
//        } else if (violationException instanceof ConstraintViolation.ForeignKeyConstraintViolation) {
//            errorCode = SQLState.LANG_FK_VIOLATION;
//        } else {
//            throw new IllegalStateException("wasn't expecting: " + violationException.getClass().getSimpleName());
//        }
//        ConstraintContext cc = violationException.getConstraintContext();
//        if (cc != null) {
//            return StandardException.newException(errorCode, cc.getMessages());
//        } else {
//            return StandardException.newException(errorCode);
//        }
//    }

    /**
     * Create an IOException that will not lose details of the given StandardException when sent over wire by Hbase RPC.
     */
//    public static SpliceDoNotRetryIOException getIOException(StandardException se){
//        return SpliceDoNotRetryIOExceptionWrapping.wrap(se);
//    }

    public static IOException getIOException(Throwable t){
        t = Throwables.getRootCause(t);
        if(t instanceof StandardException){
            return PipelineDriver.driver().exceptionFactory().doNotRetry(t);
//            return getIOException((StandardException)t);
        }
        else if(t instanceof IOException) return (IOException)t;
        else return new IOException(t);
    }


}
