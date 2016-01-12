package com.splicemachine.derby.ddl;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.si.impl.driver.SIDriver;

import java.util.Collection;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public class MemDDLEnvironment implements DDLEnvironment{
    @Override
    public DDLController getController(){
        return new DDLController(){
            @Override
            public String notifyMetadataChange(DDLMessage.DDLChange change) throws StandardException{
                throw new UnsupportedOperationException("IMPLEMENT");
            }

            @Override
            public void finishMetadataChange(String changeId) throws StandardException{
                throw new UnsupportedOperationException("IMPLEMENT");
            }
        };
    }

    @Override
    public DDLWatcher getWatcher(){
        return new DDLWatcher(){
            @Override
            public void start() throws StandardException{

            }

            @Override
            public Collection<DDLMessage.DDLChange> getTentativeDDLs(){
                return null;
            }

            @Override
            public void registerDDLListener(DDLListener listener){

            }

            @Override
            public void unregisterDDLListener(DDLListener listener){

            }

            @Override
            public boolean canUseCache(TransactionManager xact_mgr){
                return false;
            }
        };
    }

    @Override
    public SConfiguration getConfiguration(){
        return SIDriver.driver().getConfiguration();
    }
}
