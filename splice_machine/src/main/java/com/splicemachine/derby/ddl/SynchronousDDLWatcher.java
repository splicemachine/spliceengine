/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.ddl;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import org.apache.log4j.Logger;
import com.splicemachine.SqlExceptionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.si.api.txn.TxnSupplier;

/**
 * @author Scott Fines
 *         Date: 1/15/16
 */
public class SynchronousDDLWatcher implements DDLWatcher, CommunicationListener{
    private static final Logger LOG=Logger.getLogger(AsynchronousDDLWatcher.class);


    private final Set<DDLListener> ddlListeners=new CopyOnWriteArraySet<>();
    private final DDLWatchChecker checker;
    private final DDLWatchRefresher refresher;

    public SynchronousDDLWatcher(Clock clock,
                                 SConfiguration config,
                                 SqlExceptionFactory exceptionFactory,
                                 DDLWatchChecker ddlWatchChecker,
                                 TxnSupplier txnSupplier){
        long maxDdlWait=config.getMaxDdlWait() << 1;
        this.checker=ddlWatchChecker;
        this.refresher=new DDLWatchRefresher(checker,
                clock,
                exceptionFactory,
                maxDdlWait,
                txnSupplier);
    }

    @Override
    public void start() throws IOException{

        try{
            if(!checker.initialize(this))
                return;

            refresher.refreshDDL(ddlListeners);
        }catch(IOException e){
            LOG.error("Unable to initialize DDL refresher and checker");
        }
    }

    @Override
    public Collection<DDLMessage.DDLChange> getTentativeDDLs(){
        return refresher.tentativeDDLChanges();
    }

    @Override
    public void registerDDLListener(DDLListener listener){
        ddlListeners.add(listener);
    }

    @Override
    public void unregisterDDLListener(DDLListener listener){
        ddlListeners.remove(listener);
    }

    @Override
    public boolean canUseCache(TransactionManager xact_mgr){
        return refresher.canUseCache(xact_mgr);
    }

    @Override
    public boolean canUseSPSCache(TransactionManager txnMgr){
        return refresher.canUseSPSCache(txnMgr);
    }

    @Override
    public void onCommunicationEvent(String node){
        try{
            refresher.refreshDDL(ddlListeners);
        }catch(IOException e){
            LOG.error("Unable to refresh DDL",e);
        }
    }
}
