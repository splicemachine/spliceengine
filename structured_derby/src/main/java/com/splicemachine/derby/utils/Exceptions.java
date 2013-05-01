package com.splicemachine.derby.utils;

import com.google.common.base.Throwables;
import com.splicemachine.derby.impl.sql.execute.constraint.Constraint;
import com.splicemachine.derby.impl.sql.execute.constraint.ConstraintViolation;
import com.splicemachine.derby.impl.sql.execute.constraint.Constraints;
import com.splicemachine.derby.impl.sql.execute.index.IndexNotSetUpException;
import com.splicemachine.hbase.MutationResponse;
import com.splicemachine.hbase.MutationResult;
import com.splicemachine.si.impl.WriteConflict;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;

import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Created on: 3/5/13
 */
public class Exceptions {

    private Exceptions(){} //can't make me

    public static StandardException parseException(Throwable e){
        Throwable rootCause = Throwables.getRootCause(e);
        if(rootCause instanceof StandardException) return (StandardException)rootCause;

        if(rootCause instanceof ConstraintViolation.PrimaryKeyViolation){
            return StandardException.newException(SQLState.LANG_ADD_PRIMARY_KEY_FAILED1);
        }else if (rootCause instanceof ConstraintViolation.UniqueConstraintViolation){
            return StandardException.newException(SQLState.LANG_DUPLICATE_KEY_CONSTRAINT);
        }else if(rootCause instanceof LangFormatException){
            LangFormatException lfe = (LangFormatException)rootCause;
            return StandardException.newException(SQLState.LANG_FORMAT_EXCEPTION,lfe.getMessage());
        }else if (rootCause instanceof RetriesExhaustedWithDetailsException){
            RetriesExhaustedWithDetailsException rewde = (RetriesExhaustedWithDetailsException)rootCause;
            List<Throwable> causes = rewde.getCauses();
            //unwrap and throw any constraint violation errors
            for(Throwable t:causes){
                if(t instanceof DoNotRetryIOException) return parseException(t);
            }
        }

        return StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,rootCause);
    }

    public static IOException getIOException(StandardException se){
        if(se.getMessageId().equals(SQLState.LANG_FORMAT_EXCEPTION))
            return new LangFormatException(se.getMessage(),se);

        return new IOException(se);
    }

    public static IOException getIOException(Throwable t){
        if(t instanceof StandardException) return getIOException((StandardException)t);
        else if(t instanceof IOException) return (IOException)t;
        else return new IOException(t);
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

    public static Throwable fromString(String s) {
        MutationResult.Code writeErrorCode = MutationResult.Code.parse(s);
        if(writeErrorCode!=null){
            if(writeErrorCode== MutationResult.Code.WRITE_CONFLICT)
                return new WriteConflict(s);
            else if(writeErrorCode==MutationResult.Code.FAILED)
                return new DoNotRetryIOException(s);
            else return Constraints.constraintViolation(writeErrorCode);
        }
        return new DoNotRetryIOException(s);
    }

    public static class LangFormatException extends DoNotRetryIOException{
        public LangFormatException() { }
        public LangFormatException(String message) { super(message); }
        public LangFormatException(String message, Throwable cause) { super(message, cause); }
    }
}
