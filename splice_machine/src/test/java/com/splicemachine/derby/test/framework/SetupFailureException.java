package com.splicemachine.derby.test.framework;

import java.sql.SQLException;

/**
 * @author Scott Fines
 *         Date: 6/21/16
 */
public class SetupFailureException extends Exception{
    public SetupFailureException(SQLException e){
        super(e.getMessage(),e);
    }
}
