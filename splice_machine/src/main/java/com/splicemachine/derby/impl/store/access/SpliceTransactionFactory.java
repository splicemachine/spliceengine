/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.store.access;

import com.splicemachine.access.configuration.SQLConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.services.locks.LockFactory;
import com.splicemachine.db.iapi.services.monitor.ModuleControl;
import com.splicemachine.db.iapi.services.monitor.ModuleSupportable;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.store.access.FileResource;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.db.iapi.types.J2SEDataValueFactory;
import com.splicemachine.db.io.StorageFactory;
import com.splicemachine.derby.impl.sql.execute.LazyDataValueFactory;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnLifecycleManager;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

public class SpliceTransactionFactory implements ModuleControl, ModuleSupportable{
    private static Logger LOG=Logger.getLogger(SpliceTransactionFactory.class);

    protected static final String USER_CONTEXT_ID="UserTransaction";
    protected static final String NESTED_READONLY_USER_CONTEXT_ID="NestedRawReadOnlyUserTransaction";

    protected SpliceLockFactory lockFactory;
    protected DataValueFactory dataValueFactory;
    protected ContextService contextFactory;
    protected HBaseStore hbaseStore;
    protected StorageFactory storageFactory;
    protected FileResource fileHandler;

    public LockFactory getLockFactory(){
        return lockFactory;
    }

    public Object getXAResourceManager() throws StandardException{
        return null;
    }

    /**
     * place a transaction which was created outside of derby (e.g. across a serialization boundary, or a
     * manually-created transaction) within the context of Derby, so that derby system calls can be used.
     * <p/>
     * This is relatively cheap--it creates a new object, but registers the existing transaction, rather than
     * creating a new one.
     *
     * @param transName the name of the transaction
     * @param txn       the transaction to marshall
     * @return a derby representation of the transaction
     * @throws StandardException if something goes wrong (which it isn't super likely to do)
     */
    public Transaction marshalTransaction(String transName,TxnView txn) throws StandardException{
        try{
            return new SpliceTransactionView(NoLockSpace.INSTANCE,this,dataValueFactory,transName,txn);
        }catch(Exception e){
            SpliceLogUtils.logAndThrow(LOG,"marshallTransaction failure",Exceptions.parseException(e));
            return null; // can't happen
        }
    }

    private void checkContextAndStore(HBaseStore hbaseStore,ContextManager contextMgr,String methodName){
        if(contextMgr!=contextFactory.getCurrentContextManager())
            LOG.error(methodName+": passed in context mgr not the same as current context mgr");
        if(this.hbaseStore!=hbaseStore){
            SpliceLogUtils.debug(LOG,"%s: passed in hbase store not the same as current context's store",methodName);
        }
    }

    /**
     * Starts a new transaction with the given name.
     * <p/>
     * This will create a new top-level transaction. As such, it will likely be an expensive network operation.
     *
     * @param hbaseStore the hbase store relevant to the transaction
     * @param contextMgr the context manager
     * @param transName  the name of the transaction
     * @return a new transaction with the specified name and context.
     * @throws StandardException if something goes wrong creating the transaction
     */
    public Transaction startTransaction(HBaseStore hbaseStore,ContextManager contextMgr,String transName) throws StandardException{
        checkContextAndStore(hbaseStore,contextMgr,"startTransaction");

        return startCommonTransaction(hbaseStore,contextMgr,dataValueFactory,transName,false,USER_CONTEXT_ID);
    }

    /**
     * Start a "nested" transaction.
     * <p/>
     * Derby's nested transaction is functionally equivalent to Splice's child transaction, and this method
     * will actually create a child transaction.
     *
     * @param hbaseStore the hbase store relevant to the transaction
     * @param contextMgr the context manager
     * @param parentTxn  the parent transaction
     * @return a new child transaction of the parent transaction
     * @throws StandardException if something goes wrong
     */
    public Transaction startNestedTransaction(HBaseStore hbaseStore,
                                              ContextManager contextMgr,
                                              TxnView parentTxn) throws StandardException{
        checkContextAndStore(hbaseStore,contextMgr,"startNestedTransaction");

        return startNestedTransaction(hbaseStore,contextMgr,
                dataValueFactory,null,false,NESTED_READONLY_USER_CONTEXT_ID,false,parentTxn);
    }

    /**
     * Find or create a user-level transaction.
     * <p/>
     * If the current context already has a created user level transaction, then it will return the value
     * which currently exists. Otherwise, it will create a new user-level transaction.
     * <p/>
     * Because of the possibility of creating a new transaction, it may use a network call, which makes this
     * a potentially expensive call.
     *
     * @param hbaseStore the hbase store relevant to the transaction
     * @param contextMgr the context manager
     * @param transName  the name of the transaction
     * @return a new user-level transaction
     * @throws StandardException if something goes wrong
     */
    public Transaction findUserTransaction(HBaseStore hbaseStore,
                                           ContextManager contextMgr,
                                           String transName) throws StandardException{
        checkContextAndStore(hbaseStore,contextMgr,"findUserTransaction");

        SpliceTransactionContext tc=(SpliceTransactionContext)contextMgr.getContext(USER_CONTEXT_ID);
        if(tc==null){
            //We don't have a transaction yet, so create a new top-level transaction. This may require a network call
            SpliceLogUtils.trace(LOG,"findUserTransaction, transaction controller is null for UserTransaction");
            return startCommonTransaction(hbaseStore,contextMgr,dataValueFactory,transName,false,USER_CONTEXT_ID);
        }else{
            //we already have a transaction, so just return that one
            SpliceLogUtils.trace(LOG,"findUserTransaction, transaction controller is NOT null for UserTransaction");
            return tc.getTransaction();
        }
    }

    protected final SpliceTransaction startNestedTransaction(HBaseStore hbaseStore,
                                                             ContextManager contextMgr,
                                                             DataValueFactory dataValueFactory,
                                                             String transName,
                                                             boolean abortAll,
                                                             String contextName,
                                                             boolean additive,
                                                             TxnView parentTxn){
        try{
            TxnLifecycleManager lifecycleManager=SIDriver.driver().lifecycleManager();
            /*
			 * All transactions start as read only.
			 *
			 * If parentTxn!=null, then this will create a read-only child transaction (which is essentially
			 * a duplicate of the parent transaction); this requires no network calls immediately, but will require
			 * 2 network calls when elevateTransaction() is called.
			 *
			 * if parentTxn==null, then this will make a call to the timestamp source to generate a begin timestamp
			 * for a read-only transaction; this requires a single network call.
			 */
            Txn txn=lifecycleManager.beginChildTransaction(parentTxn,Txn.IsolationLevel.SNAPSHOT_ISOLATION,additive,null);
            SpliceTransaction trans=new SpliceTransaction(NoLockSpace.INSTANCE,this,dataValueFactory,transName,txn);
            trans.setTransactionName(transName);

            SpliceTransactionContext context=new SpliceTransactionContext(contextMgr,contextName,trans,abortAll,hbaseStore);

            if(LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG,"transaction type=%s,%s",context.getIdName(),txn);

            return trans;
        }catch(Exception e){
            throw new RuntimeException(e);
        }
    }

    /**
     * Start a new transaction.
     * <p/>
     * If parentTxn==null, then this will create a new top-level transaction (e.g. a child of the ROOT_TRANSACTION).
     * Performing this generally requires a network call, and so is expensive.
     * <p/>
     * If parentTxn!=null, then this may create a "read-only child transaction"; this is in effect a copy of the
     * parent transaction's information along with some additional logic for when elevation occur. Therefore, this
     * way is likely very inexpensive to call this method, but it will be doubly expensive when elevateTransaction()
     * is called (as it will require 2 network calls to elevate).
     *
     * @see com.splicemachine.si.api.txn.TxnLifecycleManager#beginChildTransaction(com.splicemachine.si.api.txn.TxnView,com.splicemachine.si.api.txn.Txn.IsolationLevel,boolean,byte[])
     */
    protected final SpliceTransaction startCommonTransaction(HBaseStore hbaseStore,
                                                             ContextManager contextMgr,
                                                             DataValueFactory dataValueFactory,
                                                             String transName,
                                                             boolean abortAll,
                                                             String contextName){
        SpliceTransaction trans=new SpliceTransaction(NoLockSpace.INSTANCE,this,dataValueFactory,transName);
        trans.setTransactionName(transName);

        SpliceTransactionContext context=new SpliceTransactionContext(contextMgr,contextName,trans,abortAll,hbaseStore);

        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"transaction type=%s,%s",context.getIdName(),transName);

        return trans;
    }

    public SpliceTransaction createPastTransaction(HBaseStore hbaseStore,
                                                   ContextManager contextMgr,
                                                   String transName,
                                                   long transactionId) {
        SpliceTransaction trans=new PastTransaction(NoLockSpace.INSTANCE,this,dataValueFactory,transName, transactionId);
        trans.setTransactionName(transName);

        SpliceTransactionContext context=new SpliceTransactionContext(contextMgr, USER_CONTEXT_ID, trans,false,hbaseStore);

        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"transaction type=%s,%s",context.getIdName(),transName);

        return trans;
    }

    @Override
    public boolean canSupport(Properties properties){
        SpliceLogUtils.debug(LOG,"SpliceTransactionFactory -canSupport");
        return true;
    }

    public void boot(boolean create,Properties properties) throws StandardException{
        dataValueFactory=new LazyDataValueFactory();
        ((J2SEDataValueFactory) dataValueFactory).boot(create,properties);
        contextFactory=ContextService.getFactory();
        lockFactory=new SpliceLockFactory();
        lockFactory.boot(create,properties);
        storageFactory = StorageFactoryService.newStorageFactory();
        try{
            storageFactory.init(SIDriver.driver().getConfiguration().getStorageFactoryHome(), SQLConfiguration.SPLICE_DB, null, null);
        }catch(IOException ioe){
            throw StandardException.newException(SQLState.FILE_UNEXPECTED_EXCEPTION,ioe);
        }
        FileResourceFactory frf = FileResourceFactoryService.loadFileResourceFactory();
        fileHandler = frf.newFileResource(storageFactory);
    }

    public void stop(){
        SpliceLogUtils.debug(LOG,"SpliceTransactionFactory -stop");
    }

    public FileResource getFileHandler(){
        return fileHandler;
    }

    /**
     * Privileged startup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  Object bootServiceModule
    (
            final boolean create, final Object serviceModule,
            final String factoryInterface, final Properties properties
    )
            throws StandardException
    {
        try {
            return AccessController.doPrivileged
                    (
                            new PrivilegedExceptionAction<Object>()
                            {
                                public Object run()
                                        throws StandardException
                                {
                                    return Monitor.bootServiceModule( create, serviceModule, factoryInterface, properties );
                                }
                            }
                    );
        } catch (PrivilegedActionException pae)
        {
            throw StandardException.plainWrapException( pae );
        }
    }
}
