package com.splicemachine.derby.utils;

import com.splicemachine.derby.impl.sql.execute.index.ConstraintViolation;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;

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
        }
        return StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,e);
    }
}
