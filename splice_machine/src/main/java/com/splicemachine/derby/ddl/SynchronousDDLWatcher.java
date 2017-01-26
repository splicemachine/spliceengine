/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
import com.splicemachine.si.api.filter.TransactionReadController;
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

    public SynchronousDDLWatcher(TransactionReadController txnController,
                                 Clock clock,
                                 SConfiguration config,
                                 SqlExceptionFactory exceptionFactory,
                                 DDLWatchChecker ddlWatchChecker,
                                 TxnSupplier txnSupplier){
        long maxDdlWait=config.getMaxDdlWait() << 1;
        this.checker=ddlWatchChecker;
        this.refresher=new DDLWatchRefresher(checker,
                txnController,
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
