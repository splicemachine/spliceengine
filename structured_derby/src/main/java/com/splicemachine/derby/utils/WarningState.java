package com.splicemachine.derby.utils;

import org.apache.derby.iapi.error.StandardException;

import java.sql.SQLWarning;

/**
 * @author Scott Fines
 *         Date: 9/10/14
 */
public enum WarningState {
    XPLAIN_STATEMENT_ID("SE016");

    private final String sqlState;

    WarningState(String sqlState) {
        this.sqlState = sqlState;
    }

    public String getSqlState(){
        return sqlState;
    }

    public SQLWarning newWarning(Object...args){
        return StandardException.newWarning(getSqlState(),args);
    }
}
