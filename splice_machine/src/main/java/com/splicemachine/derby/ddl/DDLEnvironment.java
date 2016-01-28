package com.splicemachine.derby.ddl;

import com.splicemachine.SqlExceptionFactory;
import com.splicemachine.access.api.SConfiguration;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/31/15
 */
public interface DDLEnvironment{
    DDLController getController();

    DDLWatcher getWatcher();

    SConfiguration getConfiguration();

    void configure(SqlExceptionFactory exceptionFactory,SConfiguration config) throws IOException;
}
