package com.splicemachine.derby.ddl;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.si.impl.driver.SIDriver;

import java.util.Collection;
import java.util.Collections;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public class MemDDLEnvironment implements DDLEnvironment{
    private final DirectDDL ddl = new DirectDDL();
    @Override
    public DDLController getController(){
        return ddl;
    }

    @Override
    public DDLWatcher getWatcher(){
        return ddl;
    }

    @Override
    public SConfiguration getConfiguration(){
        return SIDriver.driver().getConfiguration();
    }
}
