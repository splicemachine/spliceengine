package com.splicemachine.db.iapi.error;

import java.sql.SQLSyntaxErrorException;

public class SQLSyntaxErrorExceptionArgs extends SQLSyntaxErrorException {

    private Object[] args;

    public SQLSyntaxErrorExceptionArgs(String reason, String SQLState, int vendorCode, Throwable cause, Object[] args) {
        super(reason, SQLState, vendorCode, cause);
        this.args = args;
    }

    public SQLSyntaxErrorExceptionArgs(String reason, String SQLState, int vendorCode) {
        super(reason, SQLState, vendorCode);
    }

    public Object[] getArgs() {
        return args;
    }

    public void setArgs(Object[] args) {
        this.args = args;
    }

}
