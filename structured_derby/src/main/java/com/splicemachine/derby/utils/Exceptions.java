package com.splicemachine.derby.utils;

import com.splicemachine.derby.impl.sql.execute.constraint.ConstraintViolation;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;

import java.util.List;

/**
 * @author Scott Fines
 *         Created on: 3/5/13
 */
public class Exceptions {

    private Exceptions(){} //can't make me

    public static StandardException parseException(Throwable e){
        if(e instanceof ConstraintViolation.PrimaryKeyViolation){
            //primary Key violation
            return StandardException.newException(SQLState.LANG_ADD_PRIMARY_KEY_FAILED1,e);
        }else if (e instanceof ConstraintViolation.UniqueConstraintViolation){
            return StandardException.newException(SQLState.LANG_DUPLICATE_KEY_CONSTRAINT,e);
        }else if (e instanceof RetriesExhaustedWithDetailsException){
            RetriesExhaustedWithDetailsException rewde = (RetriesExhaustedWithDetailsException)e;
            List<Throwable> causes = rewde.getCauses();
            //unwrap and throw any constraint violation errors
            for(Throwable t:causes){
                if(t instanceof DoNotRetryIOException) return parseException(t);
            }
        }
        return StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,e);
    }

}
