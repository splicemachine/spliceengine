package com.splicemachine.derby.ddl;

import com.splicemachine.access.api.SConfiguration;

/**
 * @author Scott Fines
 *         Date: 12/31/15
 */
public interface DDLEnvironment{
    DDLController getController();

    DDLWatcher getWatcher();

    SConfiguration getConfiguration();

    void configure(SConfiguration config);
}
