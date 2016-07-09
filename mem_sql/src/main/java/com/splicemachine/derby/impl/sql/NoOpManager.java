package com.splicemachine.derby.impl.sql;

import java.sql.SQLException;

import com.splicemachine.management.Manager;

/**
 * TODO: JC - 7/9/16
 */
public class NoOpManager implements Manager {
    private static NoOpManager ourInstance=new NoOpManager();

    public static NoOpManager getInstance(){
        return ourInstance;
    }

    private NoOpManager(){ }

    @Override
    public void enableEnterprise(char[] value) throws SQLException {
        // no-op
    }
}
