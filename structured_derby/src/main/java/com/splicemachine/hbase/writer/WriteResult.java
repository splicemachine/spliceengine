package com.splicemachine.hbase.writer;

import com.splicemachine.derby.impl.sql.execute.constraint.ConstraintContext;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 *         Created on: 8/8/13
 */
public class WriteResult implements Externalizable{
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public String getErrorMessage() {
        return null;  //To change body of created methods use File | Settings | File Templates.
    }

    public WriteResult.Code getCode() {
        return null;  //To change body of created methods use File | Settings | File Templates.
    }

    public ConstraintContext getConstraintContext() {
        return null;
    }

    public enum Code {
        FAILED,
        WRITE_CONFLICT,
        SUCCESS,
        PRIMARY_KEY_VIOLATION, UNIQUE_VIOLATION, FOREIGN_KEY_VIOLATION, CHECK_VIOLATION, NOT_RUN
    }
}
