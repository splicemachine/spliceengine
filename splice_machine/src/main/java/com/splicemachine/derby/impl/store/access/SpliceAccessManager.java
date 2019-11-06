/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import org.spark_project.guava.base.Preconditions;
import com.splicemachine.derby.impl.db.SpliceDatabase;
import com.splicemachine.derby.utils.ConglomerateUtils;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.cache.CacheManager;
import com.splicemachine.db.iapi.services.cache.Cacheable;
import com.splicemachine.db.iapi.services.cache.CacheableFactory;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.services.daemon.Serviceable;
import com.splicemachine.db.iapi.services.locks.LockFactory;
import com.splicemachine.db.iapi.services.monitor.ModuleControl;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.services.property.PropertyFactory;
import com.splicemachine.db.iapi.services.property.PropertySetCallback;
import com.splicemachine.db.iapi.services.property.PropertyUtil;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.store.access.AccessFactory;
import com.splicemachine.db.iapi.store.access.AccessFactoryGlobals;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.access.TransactionInfo;
import com.splicemachine.db.iapi.store.access.conglomerate.Conglomerate;
import com.splicemachine.db.iapi.store.access.conglomerate.ConglomerateFactory;
import com.splicemachine.db.iapi.store.access.conglomerate.MethodFactory;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.shared.common.reference.Attribute;
import com.splicemachine.si.api.txn.TxnView;
import org.apache.log4j.Logger;
import com.splicemachine.utils.SpliceLogUtils;


public class SpliceAccessManager implements AccessFactory, CacheableFactory, ModuleControl, PropertySetCallback {
    private static Logger LOG = Logger.getLogger(SpliceAccessManager.class);
    private Hashtable implhash;
    private HBaseStore rawstore;
    private int system_lock_level = TransactionController.MODE_RECORD;
    private Hashtable formathash;
    private Properties serviceProperties;
    private PropertyConglomerate xactProperties;
    private PropertyFactory 	pf;
    protected ConglomerateFactory conglom_map[];
    protected SpliceDatabase database;

    public SpliceAccessManager() {
        implhash   = new Hashtable();
        formathash = new Hashtable();
    }

    PropertyConglomerate getTransactionalProperties() {
        return xactProperties;
    }

    private void boot_load_conglom_map() throws StandardException {
        conglom_map = new ConglomerateFactory[2];
        MethodFactory mfactory = findMethodFactoryByImpl("heap");
        if (mfactory == null || !(mfactory instanceof ConglomerateFactory)) {
            throw StandardException.newException(
                    SQLState.AM_NO_SUCH_CONGLOMERATE_TYPE, "heap");
        }
        conglom_map[ConglomerateFactory.HEAP_FACTORY_ID] = (ConglomerateFactory) mfactory;
        mfactory = findMethodFactoryByImpl("BTREE");
        if (mfactory == null || !(mfactory instanceof ConglomerateFactory)) {
            throw StandardException.newException(
                    SQLState.AM_NO_SUCH_CONGLOMERATE_TYPE, "BTREE");
        }
        conglom_map[ConglomerateFactory.BTREE_FACTORY_ID] = (ConglomerateFactory) mfactory;
    }

    protected int getSystemLockLevel() {
        return system_lock_level;
    }

    protected void bootLookupSystemLockLevel(TransactionController tc) throws StandardException {
        if (isReadOnly() || !PropertyUtil.getServiceBoolean(tc, Property.ROW_LOCKING, true)) {
            system_lock_level = TransactionController.MODE_TABLE;
        }
    }

    protected long getNextConglomId(int factory_type)throws StandardException {
        if (SanityManager.DEBUG) {
            // current code depends on this range, if we ever need to expand the
            // range we can claim bits from the high order of the long.
            SanityManager.ASSERT(factory_type >= 0x00 && factory_type <= 0x0f);
        }
        try {
            long conglomid  = ConglomerateUtils.getNextConglomerateId();
            return((conglomid << 4) | factory_type);
        } catch (IOException e) {
            throw StandardException.newException(SQLState.COMMUNICATION_ERROR,e);
        }
    }



    /**
     * Given a conglomid, return the factory which "owns" it.
     * <p>
     * A simple lookup on the boot time built table which maps the low order
     * 4 bits into which factory owns the conglomerate.
     * <p>
     *
     * @param conglom_id The conglomerate id of the conglomerate to look up.
     *
     * @return The ConglomerateFactory which "owns" this conglomerate.
     *
     * @exception  StandardException  Standard exception policy.
     **/
    private ConglomerateFactory getFactoryFromConglomId(long conglom_id) throws StandardException {
        try {
            return(conglom_map[((int) (0x0f & conglom_id))]);
        }
        catch (java.lang.ArrayIndexOutOfBoundsException e) {
            // just in case language passes in a bad factory id.
            throw StandardException.newException(SQLState.STORE_CONGLOMERATE_DOES_NOT_EXIST,conglom_id);
        }
    }


    /**
     * Find a conglomerate by conglomid in the cache.
     * <p>
     * Look for a conglomerate given a conglomid.  If in cache return it,
     * otherwise fault in an entry by asking the owning factory to produce
     * an entry.
     * <p>
     *
     * @return The conglomerate object identified by "conglomid".
     *
     * @param conglomid The conglomerate id of the conglomerate to look up.
     *
     * @exception  StandardException  Standard exception policy.
     **/
    Conglomerate conglomCacheFind(TransactionManager xact_mgr,long conglomid) throws StandardException {
        Conglomerate conglomerate = null;
        if (database!=null && database.getDataDictionary() !=null) {
            conglomerate = database.getDataDictionary().getDataDictionaryCache().conglomerateCacheFind(xact_mgr,conglomid);
        }
        if (conglomerate != null)
            return conglomerate;
        conglomerate = getFactoryFromConglomId(conglomid).readConglomerate(xact_mgr, conglomid);
        if (conglomerate!=null && database!=null && database.getDataDictionary() !=null)
            database.getDataDictionary().getDataDictionaryCache().conglomerateCacheAdd(conglomid,conglomerate,xact_mgr);
        return conglomerate;
    }

    /**
     * Update a conglomerate directory entry.
     * <p>
     * Update the Conglom column of the Conglomerate Directory.  The
     * Conglomerate with id "conglomid" is replaced by "new_conglom".
     * <p>
     *
     * @param conglomid   The conglomid of conglomerate to replace.
     * @param new_conglom The new Conglom to update the conglom column to.
     *
     * @exception  StandardException  Standard exception policy.
     **/
	/* package */ void conglomCacheUpdateEntry(
            long            conglomid,
            Conglomerate    new_conglom)
            throws StandardException {
            if (database!=null && database.getDataDictionary() != null) {
                database.getDataDictionary().getDataDictionaryCache().conglomerateDescriptorCacheRemove(conglomid);
                database.getDataDictionary().getDataDictionaryCache().conglomerateCacheRemove(conglomid);
                database.getDataDictionary().getDataDictionaryCache().conglomerateCacheAdd(conglomid, new_conglom);
            }
    }

    /**
     * Add a newly created conglomerate to the cache.
     * <p>
     *
     * @param conglomid   The conglomid of conglomerate to replace.
     * @param conglom     The Conglom to add.
     *
     * @exception  StandardException  Standard exception policy.
     **/
    public void conglomCacheAddEntry(
            long            conglomid,
            Conglomerate    conglom)
            throws StandardException {
        if (database!=null && database.getDataDictionary()!=null)
            database.getDataDictionary().getDataDictionaryCache().conglomerateCacheAdd(conglomid,conglom);
    }

    /**
     * Remove an entry from the cache.
     * <p>
     *
     * @param conglomid   The conglomid of conglomerate to replace.
     *
     * @exception  StandardException  Standard exception policy.
     **/
    public void conglomCacheRemoveEntry(long conglomid)
            throws StandardException {
        database.getDataDictionary().getDataDictionaryCache().conglomerateCacheRemove(conglomid);
        database.getDataDictionary().getDataDictionaryCache().conglomerateDescriptorCacheRemove(conglomid);
    }



    /**************************************************************************
     * Public Methods implementing AccessFactory Interface:
     **************************************************************************
     */

    /**
     Database creation finished.  Tell RawStore.
     @exception StandardException standard Derby error policy
     */
    public void createFinished() throws StandardException
    {
        rawstore.createFinished();
    }

    /**
     Find an access method that implements a format type.
     @see AccessFactory#findMethodFactoryByFormat
     **/
    public MethodFactory findMethodFactoryByFormat(UUID format)
    {
        MethodFactory factory;

        // See if there's an access method that supports the desired
        // format type as its primary format type.
        factory = (MethodFactory) formathash.get(format);
        if (factory != null)
            return factory;

        // No primary format.  See if one of the access methods
        // supports it as a secondary format.
        Enumeration e = formathash.elements();
        while (e.hasMoreElements())
        {
            factory = (MethodFactory) e.nextElement();
            if (factory.supportsFormat(format))
                return factory;
        }

        // No such implementation.
        return null;
    }

    /**
     Find an access method that implements an implementation type.
     @see AccessFactory#findMethodFactoryByImpl
     **/
    public MethodFactory findMethodFactoryByImpl(String impltype)
            throws StandardException
    {
        // See if there's an access method that supports the desired
        // implementation type as its primary implementation type.
        MethodFactory factory = (MethodFactory) implhash.get(impltype);
        if (factory != null)
            return factory;

        // No primary implementation.  See if one of the access methods
        // supports the implementation type as a secondary.
        Enumeration e = implhash.elements();
        while (e.hasMoreElements())
        {
            factory = (MethodFactory) e.nextElement();
            if (factory.supportsImplementation(impltype))
                return factory;
        }
        factory = null;

        // try and load an implementation.  a new properties object needs
        // to be created to hold the conglomerate type property, since
        // that value is specific to the conglomerate we want to boot, not
        // to the service as a whole
        Properties conglomProperties = new Properties(serviceProperties);
        conglomProperties.put(AccessFactoryGlobals.CONGLOM_PROP, impltype);

        try {
            factory =
                    (MethodFactory) Monitor.bootServiceModule(
                            false, this, MethodFactory.MODULE,
                            impltype, conglomProperties);
        } catch (StandardException se) {
            if (!se.getMessageId().equals(SQLState.SERVICE_MISSING_IMPLEMENTATION))
                throw se;
        }

        conglomProperties = null;

        if (factory != null) {
            registerAccessMethod(factory);
            return factory;
        }

        // No such implementation.
        return null;
    }

    public LockFactory getLockFactory() {
        return rawstore.getLockFactory();
    }


    public TransactionController getTransaction(ContextManager cm) throws StandardException {
        return getAndNameTransaction(cm, AccessFactoryGlobals.USER_TRANS_NAME);
    }

    public TransactionController getAndNameTransaction( ContextManager cm, String transName) throws StandardException {
				/*
				 * This call represents the top-level transactional access point. E.g., the top-level user transaction
				 * is created by a call to this method. Thereafter, the transaction created here should be passed around
				 * through the LanguageConnectionContext, Tasks, etc. without accidentally creating a new one.
				 */
        if (LOG.isDebugEnabled())
            LOG.debug("in SpliceAccessManager - getAndNameTransaction, transName="+transName);
        if (cm == null)
            return null;  // XXX (nat) should throw exception

				/*
				 * See if there's already a transaction context. If there is, then we just work with the one
				 * already created. Otherwise, we have to make a new one
				 */
        SpliceTransactionManagerContext rtc = (SpliceTransactionManagerContext)
                cm.getContext(AccessFactoryGlobals.RAMXACT_CONTEXT_ID);

        if(rtc!=null)
            return rtc.getTransactionManager(); //we already have a transaction from the context

        if (LOG.isDebugEnabled())
            LOG.debug("in SpliceAccessManager - getAndNameTransaction, SpliceTransactionManagerContext is null");
				/*
				 * We need to create a new transaction controller. Maybe we already have a transaction, but
				 * if not, then we need to create one.
				 *
				 * Note that this puts the raw store transaction context above the access context, which is
				 * required for error handling assumptions to be correct.
				 */
        Transaction rawtran = rawstore.findUserTransaction(cm, transName);
        SpliceTransactionManager rt = new SpliceTransactionManager(this, rawtran, null);

        rtc = new SpliceTransactionManagerContext(cm, AccessFactoryGlobals.RAMXACT_CONTEXT_ID, rt, false /* abortAll */);
        TransactionController tc = rtc.getTransactionManager();
        if (xactProperties != null) {
            rawtran.setup(tc);
        }
        return tc;
    }

    public TransactionController marshallTransaction( ContextManager cm , TxnView txn) throws StandardException {
        SpliceLogUtils.debug(LOG, "marshallTransaction called for transaction {%s} for context manager {%s}",txn, cm);
        Preconditions.checkNotNull(cm, "ContextManager is null");
        /*
         * First check to see if the transaction is already assigned to this context. If so, there is no need
         * to do anything.
         */
        SpliceTransactionManagerContext rtc = (SpliceTransactionManagerContext)cm.getContext(AccessFactoryGlobals.RAMXACT_CONTEXT_ID);
        if(rtc==null){
            /*
             * Don't marshall the transaction if we already have that transaction in our current thread context
             */
            SpliceTransactionManagerContext otherRtc = (SpliceTransactionManagerContext)ContextService.getContext(AccessFactoryGlobals.RAMXACT_CONTEXT_ID);
            if(otherRtc!=null){
                TxnView txnInformation=otherRtc.getTransactionManager().getRawTransaction().getTxnInformation();
                if(txn.equals(txnInformation)){
                    return otherRtc.getTransactionManager();
                }
            }
            /*
             * We don't have a transaction for this context, so push our known transaction down
             */
            Transaction transaction=rawstore.marshallTransaction(cm,AccessFactoryGlobals.USER_TRANS_NAME,txn);
            SpliceTransactionManager transactionManager=new SpliceTransactionManager(this,transaction,null);
            rtc=new SpliceTransactionManagerContext(cm,AccessFactoryGlobals.RAMXACT_CONTEXT_ID,transactionManager,false /* abortAll */);
        }

        return rtc.getTransactionManager();
    }

    /**
     * Start a global transaction.
     * <p>
     * Get a transaction controller with which to manipulate data within
     * the access manager.  Implicitly creates an access context.
     * <p>
     * Must only be called if no other transaction context exists in the
     * current context manager.  If another transaction exists in the context
     * an exception will be thrown.
     * <p>
     * The (format_id, global_id, branch_id) triplet is meant to come exactly
     * from a javax.transaction.xa.Xid.  We don't use Xid so that the system
     * can be delivered on a non-1.2 vm system and not require the javax classes
     * in the path.
     *
     * @param cm        The context manager for the current context.
     * @param format_id the format id part of the Xid - ie. Xid.getFormatId().
     * @param global_id the global transaction identifier part of XID - ie.
     *                  Xid.getGlobalTransactionId().
     * @param branch_id The branch qualifier of the Xid - ie.
     *                  Xid.getBranchQaulifier()
     *
     * @exception StandardException Standard exception policy.
     * @see TransactionController
     **/
    public /* XATransactionController */ Object startXATransaction(
            ContextManager  cm,
            int             format_id,
            byte[]          global_id,
            byte[]          branch_id)
            throws StandardException
    {
        SpliceTransactionManager stm = null;

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(global_id != null);
            SanityManager.ASSERT(branch_id != null);
        }

        if (cm == null)
            return null;  // XXX (nat) should throw exception

        // See if there's already a transaction context.
        SpliceTransactionManagerContext rtc = (SpliceTransactionManagerContext)
                cm.getContext(AccessFactoryGlobals.RAMXACT_CONTEXT_ID);

        if (rtc == null)
        {
            // No transaction context.  Create or find a raw store transaction,
            // make a context for it, and push the context.  Note this puts the
            // raw store transaction context above the access context, which is
            // required for error handling assumptions to be correct.
            Transaction rawtran =
                    rawstore.startGlobalTransaction(
                            cm, format_id, global_id, branch_id);

            stm = new SpliceTransactionManager(this, rawtran, null);

//            rtc = new SpliceTransactionManagerContext(cm, AccessFactoryGlobals.RAMXACT_CONTEXT_ID, stm, false /* abortAll */);

            // RESOLVE - an XA transaction can only commit once so, if we
            // acquire readlocks.

            if (xactProperties != null)
            {
                rawtran.setup(stm);

                // HACK - special support has been added to the commitNoSync
                // of a global xact, to allow committing of read only xact,
                // which will allow subsequent activity on the xact keeping
                // the same global transaction id.
                stm.commitNoSync(
                        TransactionController.RELEASE_LOCKS |
                                TransactionController.READONLY_TRANSACTION_INITIALIZATION);
            }

            // HACK - special support has been added to the commitNoSync
            // of a global xact, to allow committing of read only xact,
            // which will allow subsequent activity on the xact keeping
            // the same global transaction id.
            stm.commitNoSync(
                    TransactionController.RELEASE_LOCKS |
                            TransactionController.READONLY_TRANSACTION_INITIALIZATION);
        }
        else
        {
            // throw an error.
            if (SanityManager.DEBUG)
                SanityManager.THROWASSERT(
                        "RAMTransactionContext found on stack.");
        }

        return(stm);
    }


    /**
     * Return the XAResourceManager associated with this AccessFactory.
     * <p>
     * Returns an object which can be used to implement the "offline"
     * 2 phase commit interaction between the accessfactory and outstanding
     * transaction managers taking care of in-doubt transactions.
     *
     * @return The XAResourceManager associated with this accessfactory.
     *
     **/
    public /* XAResourceManager */ Object getXAResourceManager()
            throws StandardException
    {
        return(rawstore.getXAResourceManager());
    }

    public void registerAccessMethod(MethodFactory factory)
    {
        // Put the access method's primary implementation type in
        // a hash table so we can find it quickly.
        implhash.put(factory.primaryImplementationType(), factory);

        // Put the access method's primary format in a hash table
        // so we can find it quickly.
        formathash.put(factory.primaryFormat(), factory);
    }

    public boolean isReadOnly()
    {
        return rawstore.isReadOnly();
    }

    private void addPropertySetNotification(PropertySetCallback who, TransactionController tc) {

        pf.addPropertySetNotification(who);

        // set up the initial values by calling the validate and apply methods.
        // the map methods are not called as they will have been called
        // at runtime when the user set the property.
        Dictionary d = new Hashtable();
        try {
            xactProperties.getProperties(tc,d,false/*!stringsOnly*/,false/*!defaultsOnly*/);
        } catch (StandardException se) {
            return;
        }

        boolean dbOnly = PropertyUtil.isDBOnly(d);

        who.init(dbOnly, d);
    }

    public TransactionInfo[] getTransactionInfo()
    {
        return rawstore.getTransactionInfo();
    }

    public void freeze() throws StandardException
    {
        rawstore.freeze();
    }

    public void unfreeze() throws StandardException
    {
        rawstore.unfreeze();
    }

    public void backup(
            String  backupDir,
            boolean wait)
            throws StandardException
    {
        rawstore.backup(backupDir, wait);
    }


    public void checkpoint() throws StandardException
    {
        rawstore.checkpoint();
    }

    public void waitForPostCommitToFinishWork()
    {
        rawstore.waitUntilQueueIsEmpty();
    }

    /**************************************************************************
     * Public Methods implementing ModuleControl Interface:
     **************************************************************************
     */
    public void boot(boolean create, Properties startParams) throws StandardException {
        this.serviceProperties = startParams;

        boot_load_conglom_map();

        rawstore = new HBaseStore();
        rawstore.boot(true, serviceProperties);

				// Note: we also boot this module here since we may start Derby
				// system from store access layer, as some of the unit test case,
				// not from JDBC layer.(See
				// /protocol/Database/Storage/Access/Interface/T_AccessFactory.java)
				// If this module has already been booted by the JDBC layer, this will
				// have no effect at all.
				Monitor.bootServiceModule(create, this, com.splicemachine.db.iapi.reference.Module.PropertyFactory, startParams);


        // Read in the conglomerate directory from the conglom conglom
        // Create the conglom conglom from within a separate system xact
        SpliceTransactionManager tc = (SpliceTransactionManager) getAndNameTransaction(
                ContextService.getFactory().getCurrentContextManager(),AccessFactoryGlobals.USER_TRANS_NAME);

        // looking up lock_mode is dependant on access booting, but
        // some boot routines need lock_mode and
        // system_default_locking_policy, so during boot do table level
        // locking and then look up the "right" locking level.

        if (SanityManager.DEBUG) {
            for (int i = 0;i < TransactionController.ISOLATION_SERIALIZABLE;i++) {
				/*
      SanityManager.ASSERT(
          table_level_policy[i] != null,
          "table_level_policy[" + i + "] is null");
      SanityManager.ASSERT(
          record_level_policy[i] != null,
          "record_level_policy[" + i + "] is null");
				 */ // XXX - TODO REIMPLIMENT TABLE_LEVEL and RECORD_LEVEL
            }
        }

        //tc.commit();

				// set up the property validation
				pf = (PropertyFactory) Monitor.findServiceModule(this, com.splicemachine.db.iapi.reference.Module.PropertyFactory);

        if(create)
            ((SpliceTransaction)tc.getRawTransaction()).elevate(Bytes.toBytes("boot"));
        // set up the transaction properties.  On J9, over NFS, runing on a
        // power PC coprossor, the directories were created fine, but create
        // db would fail when trying to create this first file in seg0.
        xactProperties = new PropertyConglomerate(tc, create, startParams, pf);
        // see if there is any properties that raw store needs to know
        // about
        rawstore.getRawStoreProperties(tc);

        // now that access and raw store are booted, do the property lookup
        // which may do conglomerate access.
        bootLookupSystemLockLevel(tc);

        // set up the callbacl for the lock manager with initialization
        addPropertySetNotification(getLockFactory(), tc);

        // make sure user cannot change these properties
        addPropertySetNotification(this,tc);

        tc.commit();

        tc.destroy();
        tc = null;

        if (SanityManager.DEBUG)
        {
            // RESOLVE - (mikem) currently these constants need to be the
            // same, but for modularity reasons there are 2 sets.  Probably
            // should only be one set.  For now just make sure they are the
            // same value.
        }
    }

    public void stop() {
    }

	/* Methods of the PropertySetCallback interface */

    // This interface is implemented to ensure the user cannot change the
    // encryption provider or algorithm.

    public void init(boolean dbOnly, Dictionary p) {
    }

    public boolean validate(String key, Serializable value, Dictionary p) throws StandardException {
        if (key.equals(Attribute.CRYPTO_ALGORITHM)) {
            throw StandardException.newException(SQLState.ENCRYPTION_NOCHANGE_ALGORITHM);
        }
        if (key.equals(Attribute.CRYPTO_PROVIDER)) {
            throw StandardException.newException(SQLState.ENCRYPTION_NOCHANGE_PROVIDER);
        }
        return true;
    }

    public Serviceable apply(String key, Serializable value, Dictionary p, TransactionController tc) throws StandardException {
        return null;
    }

    public Serializable map(String key, Serializable value, Dictionary p) throws StandardException {
        return null;
    }

    // ///////////////////////////////////////////////////////////////

	/*
	 ** CacheableFactory interface
	 */

    public Cacheable newCacheable(CacheManager cm) {
        return new CacheableConglomerate();
    }

    public HBaseStore getRawStore () {
        return this.rawstore;
    }

    public void setDatabase(SpliceDatabase database) {
        this.database = database;
    }

}

