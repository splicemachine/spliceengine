package com.splicemachine.db.client.am;

import java.sql.SQLException;

public class SqlExceptionArgs extends SqlException {

    Object[] args;

    public SqlExceptionArgs(LogWriter logWriter, Sqlca sqlca) {
        super(logWriter, sqlca);
        args = sqlca.sqlErrorArgs;
    }

    public SQLException getSQLException() {
        if ( wrappedException_ != null )
        {
            return wrappedException_;
        }

        // When we have support for JDBC 4 SQLException subclasses, this is
        // where we decide which exception to create
        SQLException sqle = exceptionFactory.getSQLException(getMessage(), getSQLState(),
                getErrorCode(), args);
        sqle.initCause(this);

        // Set up the nextException chain
        if ( nextException_ != null )
        {
            // The exception chain gets constructed automatically through
            // the beautiful power of recursion
            sqle.setNextException(nextException_.getSQLException());
        }

        return sqle;
    }

    public Object[] getArgs() {
        return args;
    }
}
