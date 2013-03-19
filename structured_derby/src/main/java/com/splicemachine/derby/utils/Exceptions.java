package com.splicemachine.derby.utils;

import com.google.common.base.Throwables;
import com.splicemachine.derby.impl.load.SpliceImportCoprocessor;
import com.splicemachine.derby.impl.sql.execute.constraint.ConstraintViolation;
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

    public static class LangFormatException extends DoNotRetryIOException{
        public LangFormatException() { }
        public LangFormatException(String message) { super(message); }
        public LangFormatException(String message, Throwable cause) { super(message, cause); }
    }
}
