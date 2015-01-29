package com.splicemachine.pipeline.exception;

import com.google.common.base.Throwables;
import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.constraint.ConstraintContext;
import com.splicemachine.pipeline.constraint.ConstraintViolation;
import com.splicemachine.pipeline.constraint.Constraints;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.si.api.CannotCommitException;
import com.splicemachine.si.impl.WriteConflict;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.MessageId;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.regionserver.LeaseException;
import org.apache.hadoop.hbase.regionserver.WrongRegionException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.hbase.client.ScannerTimeoutException;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.ConnectException;
import java.util.List;

/**
 * @author Scott Fines
 *         Created on: 3/5/13
 */
public class Exceptions {

    private static final Logger LOG = Logger.getLogger(Exceptions.class);

    private Exceptions(){} //can't make me

    public static StandardException parseException(Throwable e) {
        Throwable rootCause = Throwables.getRootCause(e);
        if (rootCause instanceof StandardException) {
            return (StandardException) rootCause;
        }

        if (rootCause instanceof RetriesExhaustedWithDetailsException) {
            return parseException((RetriesExhaustedWithDetailsException) rootCause);
        } else if (rootCause instanceof ConstraintViolation.ConstraintViolationException) {
            return toStandardException((ConstraintViolation.ConstraintViolationException) rootCause);
        } else if (rootCause instanceof com.splicemachine.async.RemoteException) {
            com.splicemachine.async.RemoteException re = (com.splicemachine.async.RemoteException) rootCause;
            String fullMessage = re.getMessage();
            String type = re.getType();
            try{
                return parseException((Throwable)Class.forName(type).getConstructor(String.class).newInstance(fullMessage));
            }catch(ClassNotFoundException cnfe){
                //just parse  the actual remote directly
                ErrorState state = ErrorState.stateFor(rootCause);
                return state.newException(rootCause);
            } catch (NoSuchMethodException e1) {
                //just parse  the actual remote directly
                ErrorState state = ErrorState.stateFor(rootCause);
                return state.newException(rootCause);
            } catch (InvocationTargetException e1) {
                ErrorState state = ErrorState.stateFor(rootCause);
                return state.newException(rootCause);
            } catch (InstantiationException e1) {
                ErrorState state = ErrorState.stateFor(rootCause);
                return state.newException(rootCause);
            } catch (IllegalAccessException e1) {
                ErrorState state = ErrorState.stateFor(rootCause);
                return state.newException(rootCause);
            }
        } else if(rootCause instanceof SpliceDoNotRetryIOException){
            SpliceDoNotRetryIOException spliceException = (SpliceDoNotRetryIOException)rootCause;
            Exception unwrappedException = SpliceDoNotRetryIOExceptionWrapping.unwrap(spliceException);
            return parseException(unwrappedException);
        } else if(rootCause instanceof DoNotRetryIOException ){
						if(rootCause.getMessage()!=null && rootCause.getMessage().contains("rpc timeout")) {
								return StandardException.newException(MessageId.QUERY_TIMEOUT, "Increase hbase.rpc.timeout");
						}
        } else if(rootCause instanceof SpliceStandardException){
            return ((SpliceStandardException)rootCause).generateStandardException();
        }else if(rootCause instanceof RemoteException){
            rootCause = ((RemoteException)rootCause).unwrapRemoteException();
        }

        ErrorState state = ErrorState.stateFor(rootCause);
        return state.newException(rootCause);
    }

    public static StandardException parseException(RetriesExhaustedWithDetailsException rewde){
        List<Throwable> causes = rewde.getCauses();
        /*
         * Look for any exception that can be converted into a known error type (instead of a DATA_UNEXPECTED_EXCEPTION)
         */
        for(Throwable t:causes){
            if(isExpected(t))
                return parseException(t);
        }
        if(causes.size()>0)
            return StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,causes.get(0));
        else
            return StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,rewde);

    }

    private static boolean isExpected(Throwable rootCause){
        ErrorState state = ErrorState.stateFor(rootCause);
        return state != ErrorState.DATA_UNEXPECTED_EXCEPTION;
    }

    public static StandardException toStandardException(ConstraintViolation.ConstraintViolationException violationException) {
        String errorCode;
        if (violationException instanceof ConstraintViolation.PrimaryKeyViolation || violationException instanceof ConstraintViolation.UniqueConstraintViolation) {
            errorCode = SQLState.LANG_DUPLICATE_KEY_CONSTRAINT;
        } else if (violationException instanceof ConstraintViolation.ForeignKeyConstraintViolation) {
            errorCode = SQLState.LANG_FK_VIOLATION;
        } else {
            throw new IllegalStateException("wasn't expecting: " + violationException.getClass().getSimpleName());
        }
        ConstraintContext cc = violationException.getConstraintContext();
        if (cc != null) {
            return StandardException.newException(errorCode, cc.getMessages());
        } else {
            return StandardException.newException(errorCode);
        }
    }

    /**
     * Create an IOException that will not lose details of the given StandardException when sent over wire by Hbase RPC.
     */
    public static SpliceDoNotRetryIOException getIOException(StandardException se){
        return SpliceDoNotRetryIOExceptionWrapping.wrap(se);
    }

    public static IOException getIOException(Throwable t){
        t = Throwables.getRootCause(t);
        if(t instanceof StandardException) return getIOException((StandardException)t);
        else if(t instanceof com.splicemachine.async.RemoteException){
            return getRemoteIOException((com.splicemachine.async.RemoteException)t);
        }
        else if(t instanceof IOException) return (IOException)t;
        else return new IOException(t);
    }

    protected static IOException getRemoteIOException(com.splicemachine.async.RemoteException t) {
        String text = t.getMessage();
        String type = t.getType();
        try{
            return getIOException((Throwable)Class.forName(type).getConstructor(String.class).newInstance(text));
        } catch (InvocationTargetException e) {
            return new IOException(t);
        } catch (NoSuchMethodException e) {
            return new IOException(t);
        } catch (ClassNotFoundException e) {
            return new IOException(t);
        } catch (InstantiationException e) {
            return new IOException(t);
        } catch (IllegalAccessException e) {
            return new IOException(t);
        }
    }

    /**
     * Determine if we should dump a stack trace to the log file.
     *
     * This is to filter out exceptions from the log that don't need to write an error to the
     * log (Primary Key violations, or other user errors, or something that can be retried without
     * punishment).
     *
     * @param e the exception to check
     * @return true if the stack trace should be logged.
     */
    public static boolean shouldLogStackTrace(Throwable e) {
        if(e instanceof ConstraintViolation.PrimaryKeyViolation) return false;
        if(e instanceof ConstraintViolation.UniqueConstraintViolation) return false;
        if(e instanceof IndexNotSetUpException) return false;
        return true;
    }

    public static Throwable fromString(WriteResult result) {
    	if (LOG.isTraceEnabled())
    		SpliceLogUtils.trace(LOG, "fromString with writeresult=%s",result);    	
        Code writeErrorCode = result.getCode();

        if(writeErrorCode!=null){
						switch(writeErrorCode){
								case FAILED:
										return new IOException(result.getErrorMessage());
								case WRITE_CONFLICT:
										return WriteConflict.fromString(result.getErrorMessage());
								case SUCCESS:
										return null; //won't happen
								case PRIMARY_KEY_VIOLATION:
								case UNIQUE_VIOLATION:
								case FOREIGN_KEY_VIOLATION:
								case CHECK_VIOLATION:
										return Constraints.constraintViolation(writeErrorCode,result.getConstraintContext());
								case NOT_SERVING_REGION:
										return new NotServingRegionException();
								case WRONG_REGION:
										return new WrongRegionException();
								case REGION_TOO_BUSY:
										return new RegionTooBusyException();
								case NOT_RUN:
										//won't happen
										return new IOException("Unexpected NotRun code for an error");
						}
        }
        return new DoNotRetryIOException(result.getErrorMessage());
    }

    public static boolean shouldRetry(Throwable error) {
        if (error instanceof ConnectException) {
            //we can safely retry connection refused issued
            return error.getMessage().contains("Connection refused");
        }
        if (error instanceof StandardException) return false;
        /*
         * CannotCommitException is a DoNotRetryException because we want the HBase client
         * to automatically stop retrying if it's encountered (it's a logical error, rather than
         * environmental). However, we still want to be able to retry tasks etc. that
         * encounter CannotCommit errors.
         */
        return error instanceof CannotCommitException || !(error instanceof DoNotRetryIOException);
    }

    public static Throwable getRootCause(Throwable error) {
				//unwrap RemoteException wrappers
				if(error instanceof RemoteException){
						error = ((RemoteException)error).unwrapRemoteException();
				}
        error = Throwables.getRootCause(error);
        if(error instanceof RetriesExhaustedWithDetailsException ){
            RetriesExhaustedWithDetailsException rewde = (RetriesExhaustedWithDetailsException)error;
            List<Throwable> causes = rewde.getCauses();
            if(causes!=null&&causes.size()>0)
                error = causes.get(0);
        }
        return error;
    }

    public static String getErrorCode(Throwable error) {
        if(error instanceof StandardException){
            return ((StandardException)error).getSqlState();
        }
        return SQLState.DATA_UNEXPECTED_EXCEPTION;
    }

    public static boolean isScannerTimeoutException(Throwable error) {
        boolean scannerTimeout = false;
        if (error instanceof LeaseException ||
            error instanceof ScannerTimeoutException) {
            scannerTimeout = true;
        }
        return scannerTimeout;

    }

    public static class LangFormatException extends DoNotRetryIOException{
        public LangFormatException() { }
        public LangFormatException(String message) { super(message); }
        public LangFormatException(String message, Throwable cause) { super(message, cause); }
    }


}
