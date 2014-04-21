package com.splicemachine.derby.impl.store.access;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import com.google.common.base.Preconditions;
import com.splicemachine.derby.ddl.DDLChange;
import com.splicemachine.derby.utils.ConglomerateUtils;
import com.splicemachine.hbase.table.BetterHTablePool;
import com.splicemachine.hbase.table.SpliceHTableFactory;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.impl.DDLFilter;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.cache.CacheFactory;
import org.apache.derby.iapi.services.cache.CacheManager;
import org.apache.derby.iapi.services.cache.Cacheable;
import org.apache.derby.iapi.services.cache.CacheableFactory;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.daemon.Serviceable;
import org.apache.derby.iapi.services.locks.LockFactory;
import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.property.PropertyFactory;
import org.apache.derby.iapi.services.property.PropertySetCallback;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.store.access.AccessFactory;
import org.apache.derby.iapi.store.access.AccessFactoryGlobals;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.access.TransactionInfo;
import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;
import org.apache.derby.iapi.store.access.conglomerate.ConglomerateFactory;
import org.apache.derby.iapi.store.access.conglomerate.MethodFactory;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;
import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.ContainerKey;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.shared.common.reference.Attribute;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceUtilities;


public class SpliceAccessManager extends SpliceUtilities implements AccessFactory, CacheableFactory, ModuleControl, PropertySetCallback {
	private static Logger LOG = Logger.getLogger(SpliceAccessManager.class);
    private Hashtable implhash;
	private HBaseStore rawstore;
	private int system_lock_level = TransactionController.MODE_RECORD;
	protected static BetterHTablePool singleRPCPool;
	protected static BetterHTablePool flushablePool;
	private Hashtable formathash;
	private Properties serviceProperties;
	LockingPolicy system_default_locking_policy;
	private PropertyConglomerate xactProperties;
	private PropertyFactory 	pf;
	protected LockingPolicy table_level_policy[];
	protected LockingPolicy record_level_policy[];
	protected ConglomerateFactory conglom_map[];
	private CacheManager    conglom_cache;
    private volatile DDLFilter ddlDemarcationPoint = null;
    private volatile boolean cacheDisabled = false;
    private ConcurrentMap<String, DDLChange> ongoingDDLChanges = new ConcurrentHashMap<String, DDLChange>();

	public SpliceAccessManager() {
		implhash   = new Hashtable();
		formathash = new Hashtable();
        singleRPCPool = new BetterHTablePool(new SpliceHTableFactory(),
                tablePoolCleanerInterval,
                TimeUnit.SECONDS,tablePoolMaxSize,tablePoolCoreSize);
        flushablePool = new BetterHTablePool(new SpliceHTableFactory(false),
                tablePoolCleanerInterval,
                TimeUnit.SECONDS,tablePoolMaxSize,tablePoolCoreSize);
	}

	protected LockingPolicy getDefaultLockingPolicy() {
		return(system_default_locking_policy);
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
	
//	public static HTablePool getHTableRPCPool() {
//		return singleRPCPool;
//	}

//	public static HTablePool getHTableFlushablePool() {
//		return flushablePool;
//	}

	protected int getSystemLockLevel() {
		return system_lock_level;
	}

	protected void bootLookupSystemLockLevel(TransactionController tc) throws StandardException {
		if (isReadOnly() || !PropertyUtil.getServiceBoolean(tc, Property.ROW_LOCKING, true)) {
			system_lock_level = TransactionController.MODE_TABLE;
		}
	}

	private long conglom_nextid = 0;

	protected long getNextConglomId(int factory_type)throws StandardException {
		long    conglomid;
		if (SanityManager.DEBUG) {
			// current code depends on this range, if we ever need to expand the
			// range we can claim bits from the high order of the long.
			SanityManager.ASSERT(factory_type >= 0x00 && factory_type <= 0x0f);
		}

		synchronized (conglom_cache) {
			try {
				conglomid  = ConglomerateUtils.getNextConglomerateId();
			} catch (IOException e) {
				throw StandardException.newException(SQLState.COMMUNICATION_ERROR,e);
			}
		}
		return((conglomid << 4) | factory_type);
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
			throw StandardException.newException(SQLState.STORE_CONGLOMERATE_DOES_NOT_EXIST, new Long(conglom_id));
		}
	}


	/**************************************************************************
	 * Conglomerate Cache routines:
	 **************************************************************************
	 */

	/**
	 * ACCESSMANAGER CONGLOMERATE CACHE - 
	 * <p>
	 * Every conglomerate in the system is described by an object which 
	 * implements Conglomerate.  This object basically contains the parameters
	 * which describe the metadata about the conglomerate that store needs
	 * to know - like types of columns, number of keys, number of columns, ...
	 * <p>
	 * It is up to each conglomerate to maintain it's own description, and
	 * it's factory must be able to read this info from disk and return it
	 * from the ConglomerateFactory.readConglomerate() interface.
	 * <p>
	 * This cache simply maintains an in memory copy of these conglomerate
	 * objects, key'd by conglomerate id.  By caching, this avoids the cost
	 * of reading the conglomerate info from disk on each subsequent query
	 * which accesses the conglomerate.
	 * <p>
	 * The interfaces and internal routines which deal with this cache are:
	 * conglomCacheInit() - initializes the cache at boot time.
	 *
	 *
	 *
	 **/

	/**
	 * Initialize the conglomerate cache.
	 * <p>
	 * Simply calls the cache manager to create the cache with some hard
	 * coded defaults for size.
	 * <p>
	 * @exception  StandardException  Standard exception policy.
	 **/
	private void conglomCacheInit() throws StandardException {
		// Get a cache factory to create the conglomerate cache.
		CacheFactory cf = (CacheFactory) Monitor.startSystemModule(org.apache.derby.iapi.reference.Module.CacheFactory);
		// Now create the conglomerate cache.
		conglom_cache = cf.newCacheManager(this, AccessFactoryGlobals.CFG_CONGLOMDIR_CACHE, 200, 300);
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
	/* package */ Conglomerate conglomCacheFind(TransactionManager xact_mgr,long conglomid) throws StandardException {
		Conglomerate conglom       = null;
		Long         conglomid_obj = new Long(conglomid);
        boolean bypassCache = cacheDisabled || !canSeeDDLDemarcationPoint(xact_mgr);
        if (bypassCache) {
            return getFactoryFromConglomId(conglomid).readConglomerate(xact_mgr, new ContainerKey(0, conglomid));
        }

		synchronized (conglom_cache) {
			CacheableConglomerate cache_entry = 
					(CacheableConglomerate) conglom_cache.findCached(conglomid_obj);
			
			if (cache_entry != null) {
				conglom = cache_entry.getConglom();
				conglom_cache.release(cache_entry);
			}
			else {
				conglom = getFactoryFromConglomId(conglomid).readConglomerate(xact_mgr, new ContainerKey(0, conglomid));
				if (conglom != null) {
					// on cache miss, put the missing conglom in the cache.
					cache_entry = (CacheableConglomerate) this.conglom_cache.create(conglomid_obj, conglom);
					this.conglom_cache.release(cache_entry);
				}
			}
		}
		return(conglom);
	}

    private boolean canSeeDDLDemarcationPoint(TransactionManager xact_mgr) {
        try {
            // If the transaction is older than the latest DDL operation (can't see it), bypass the cache
            return ddlDemarcationPoint == null || ddlDemarcationPoint.isVisibleBy(xact_mgr.getActiveStateTxIdString());
        } catch (IOException e) {
            // Stay on the safe side, assume it's not visible
            return false;
        }
    }

    /**
	 * Invalide the current Conglomerate Cache.
	 * <p>
	 * Abort of certain operations will invalidate the contents of the 
	 * cache.  Longer term we could just invalidate those entries, but
	 * for now just invalidate the whole cache.
	 * <p>
	 *
	 * @exception  StandardException  Standard exception policy.
	 **/
	/* package */ protected void conglomCacheInvalidate() {
		synchronized (conglom_cache) {
			conglom_cache.discard(null);
		}
		return;
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
					throws StandardException
					{
		Long         conglomid_obj = new Long(conglomid);

		synchronized (conglom_cache)
		{
			// remove the current entry
			CacheableConglomerate conglom_entry = (CacheableConglomerate) 
					conglom_cache.findCached(conglomid_obj);

			if (conglom_entry != null)
				conglom_cache.remove(conglom_entry);

			// insert the updated entry.
			conglom_entry = (CacheableConglomerate) 
					conglom_cache.create(conglomid_obj, new_conglom);
			conglom_cache.release(conglom_entry);
		}

		return;
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
			throws StandardException
	{
		synchronized (conglom_cache)
		{
			// insert the updated entry.
			CacheableConglomerate conglom_entry = (CacheableConglomerate) 
					conglom_cache.create(new Long(conglomid), conglom);
			conglom_cache.release(conglom_entry);
		}

		return;
	}

	/**
	 * Remove an entry from the cache.
	 * <p>
	 *
	 * @param conglomid   The conglomid of conglomerate to replace.
	 *
	 * @exception  StandardException  Standard exception policy.
	 **/
	/* package */ void conglomCacheRemoveEntry(long conglomid)
			throws StandardException
			{
		synchronized (conglom_cache)
		{
			CacheableConglomerate conglom_entry = (CacheableConglomerate) 
					conglom_cache.findCached(new Long(conglomid));

			if (conglom_entry != null)
				conglom_cache.remove(conglom_entry);
		}

		return;
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

    public void startDDLChange(DDLChange ddlChange) {
        cacheDisabled = true;
        ongoingDDLChanges.put(ddlChange.getIdentifier(), ddlChange);
        conglomCacheInvalidate();
    }

    public void finishDDLChange(String identifier) {
        DDLChange ddlChange = ongoingDDLChanges.remove(identifier);
        if (ddlChange != null) {
            try {
                DDLFilter ddlFilter = HTransactorFactory.getTransactionReadController().newDDLFilter(ddlChange.getParentTransactionId(),ddlChange.getTransactionId());
                if (ddlFilter.compareTo(ddlDemarcationPoint) > 0) {
                    ddlDemarcationPoint = ddlFilter;
                }
            } catch (IOException e) {
                LOG.error("Couldn't create ddlFilter", e);
            }
        }
        cacheDisabled = ongoingDDLChanges.size() > 0;
    }

    public TransactionController getAndNameTransaction( ContextManager cm, String transName) throws StandardException {
		if (LOG.isDebugEnabled())
			LOG.debug("in SpliceAccessManager - getAndNameTransaction, transName="+transName);
		if (cm == null)
			return null;  // XXX (nat) should throw exception

		// See if there's already a transaction context.
		SpliceTransactionManagerContext rtc = (SpliceTransactionManagerContext)
				cm.getContext(AccessFactoryGlobals.RAMXACT_CONTEXT_ID);

		if (rtc == null)
		{
			if (LOG.isDebugEnabled())
				LOG.debug("in SpliceAccessManager - getAndNameTransaction, SpliceTransactionManagerContext is null");
			// No transaction context.  Create or find a raw store transaction,
			// make a context for it, and push the context.  Note this puts the
			// raw store transaction context above the access context, which is
			// required for error handling assumptions to be correct.
			Transaction rawtran = rawstore.findUserTransaction(cm, transName);
			SpliceTransactionManager rt      = new SpliceTransactionManager(this, rawtran, null);

			rtc = new SpliceTransactionManagerContext(cm, AccessFactoryGlobals.RAMXACT_CONTEXT_ID, rt, false /* abortAll */);

			TransactionController tc = rtc.getTransactionManager();

			if (xactProperties != null) {
				rawtran.setup(tc);
				//tc.commit();
			}
            rawtran.setDefaultLockingPolicy(system_default_locking_policy);
			//LOG.debug(">>>>>in SpliceAccessManager - getAndNameTransaction, transID="+rawtran.getActiveStateTxIdString());
			//tc.commit();

			return tc;
		} else
			SpliceLogUtils.debug(LOG,"in SpliceAccessManager - getAndNameTransaction, SpliceTransactionManagerContext is NOT null, so reuse transaction");
		return rtc.getTransactionManager();
	}

	public TransactionController marshallTransaction( ContextManager cm , String transactionID) throws StandardException {
		SpliceLogUtils.debug(LOG, "marshallTransaction called for transaction {%s} for context manager {%s}",transactionID, cm);
		Preconditions.checkNotNull(cm, "ContextManager is null");
		Transaction transaction = rawstore.marshallTransaction(cm, AccessFactoryGlobals.USER_TRANS_NAME, transactionID);
		SpliceTransactionManager transactionManager = new SpliceTransactionManager(this, transaction, null);
		SpliceTransactionManagerContext rtc = new SpliceTransactionManagerContext(cm, AccessFactoryGlobals.RAMXACT_CONTEXT_ID, transactionManager, false /* abortAll */);
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

			rtc = 
					new SpliceTransactionManagerContext(cm, AccessFactoryGlobals.RAMXACT_CONTEXT_ID, stm, false /* abortAll */);

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

			rawtran.setDefaultLockingPolicy(system_default_locking_policy);

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

	/**
	 * Start the replication master role for this database.
	 * @param dbmaster The master database that is being replicated.
	 * @param host The hostname for the slave
	 * @param port The port the slave is listening on
	 * @param replicationMode The type of replication contract.
	 * Currently only asynchronous replication is supported, but
	 * 1-safe/2-safe/very-safe modes may be added later.
	 * @exception StandardException Standard Derby exception policy,
	 * thrown on error.
	 */
	public void startReplicationMaster(String dbmaster, String host, int port,
			String replicationMode)
					throws StandardException {
		rawstore.startReplicationMaster(dbmaster, host, port, replicationMode);
	}

	/**
	 * @see org.apache.derby.iapi.store.access.AccessFactory#failover(String dbname).
	 */
	public void failover(String dbname) throws StandardException {
		rawstore.failover(dbname);
	}

	/**
	 * Stop the replication master role for this database.
	 * 
	 * @exception StandardException Standard Derby exception policy,
	 * thrown on error.
	 */
	public void stopReplicationMaster() throws StandardException {
		rawstore.stopReplicationMaster();
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


	public void backupAndEnableLogArchiveMode(
			String  backupDir, 
			boolean deleteOnlineArchivedLogFiles,
			boolean wait)
					throws StandardException 
	{
		rawstore.backupAndEnableLogArchiveMode(backupDir, 
				deleteOnlineArchivedLogFiles, 
				wait);
	}

	public void disableLogArchiveMode(boolean deleteOnlineArchivedLogFiles)
			throws StandardException
	{
		rawstore.disableLogArchiveMode(deleteOnlineArchivedLogFiles);
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

		if (create) {
			// if we are creating the db, then just start the conglomid's at
			// 1, and proceed from there.  If not create, we delay 
			// initialization of this until the first ddl which needs a new
			// id.
			conglom_nextid = 1;
		}

		rawstore = new HBaseStore();
		rawstore.boot(true, serviceProperties);

		// Note: we also boot this module here since we may start Derby
		// system from store access layer, as some of the unit test case,
		// not from JDBC layer.(See
		// /protocol/Database/Storage/Access/Interface/T_AccessFactory.java)
		// If this module has already been booted by the JDBC layer, this will 
		// have no effect at all.
		Monitor.bootServiceModule(create, this, org.apache.derby.iapi.reference.Module.PropertyFactory, startParams);

		// Create the in-memory conglomerate directory

		conglomCacheInit();

		// Read in the conglomerate directory from the conglom conglom
		// Create the conglom conglom from within a separate system xact
		SpliceTransactionManager tc = (SpliceTransactionManager) getAndNameTransaction(
						ContextService.getFactory().getCurrentContextManager(),AccessFactoryGlobals.USER_TRANS_NAME);

		// looking up lock_mode is dependant on access booting, but
		// some boot routines need lock_mode and
		// system_default_locking_policy, so during boot do table level
		// locking and then look up the "right" locking level.

		int lock_mode = LockingPolicy.MODE_CONTAINER;

		system_default_locking_policy =
				tc.getRawStoreXact().newLockingPolicy(
						lock_mode,
						TransactionController.ISOLATION_SERIALIZABLE, true);


		// RESOLVE - code reduction - get rid of this table, and somehow
		// combine it with the raw store one.

		table_level_policy = new LockingPolicy[6];

		table_level_policy[TransactionController.ISOLATION_NOLOCK] =
				tc.getRawStoreXact().newLockingPolicy(
						LockingPolicy.MODE_CONTAINER,
						TransactionController.ISOLATION_NOLOCK, true);

		table_level_policy[TransactionController.ISOLATION_READ_UNCOMMITTED] =
				tc.getRawStoreXact().newLockingPolicy(
						LockingPolicy.MODE_CONTAINER,
						TransactionController.ISOLATION_READ_UNCOMMITTED, true);

		table_level_policy[TransactionController.ISOLATION_READ_COMMITTED] =
				tc.getRawStoreXact().newLockingPolicy(
						LockingPolicy.MODE_CONTAINER,
						TransactionController.ISOLATION_READ_COMMITTED, true);

		table_level_policy[TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK] =
				tc.getRawStoreXact().newLockingPolicy(
						LockingPolicy.MODE_CONTAINER,
						TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK,
						true);

		table_level_policy[TransactionController.ISOLATION_REPEATABLE_READ] =
				tc.getRawStoreXact().newLockingPolicy(
						LockingPolicy.MODE_CONTAINER,
						TransactionController.ISOLATION_REPEATABLE_READ, true);

		table_level_policy[TransactionController.ISOLATION_SERIALIZABLE] =
				tc.getRawStoreXact().newLockingPolicy(
						LockingPolicy.MODE_CONTAINER,
						TransactionController.ISOLATION_SERIALIZABLE, true);

		record_level_policy = new LockingPolicy[6];

		record_level_policy[TransactionController.ISOLATION_NOLOCK] =
				tc.getRawStoreXact().newLockingPolicy(
						LockingPolicy.MODE_RECORD,
						TransactionController.ISOLATION_NOLOCK, true);

		record_level_policy[TransactionController.ISOLATION_READ_UNCOMMITTED] =
				tc.getRawStoreXact().newLockingPolicy(
						LockingPolicy.MODE_RECORD,
						TransactionController.ISOLATION_READ_UNCOMMITTED, true);

		record_level_policy[TransactionController.ISOLATION_READ_COMMITTED] =
				tc.getRawStoreXact().newLockingPolicy(
						LockingPolicy.MODE_RECORD,
						TransactionController.ISOLATION_READ_COMMITTED, true);

		record_level_policy[TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK] =
				tc.getRawStoreXact().newLockingPolicy(
						LockingPolicy.MODE_RECORD,
						TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK,
						true);

		record_level_policy[TransactionController.ISOLATION_REPEATABLE_READ] =
				tc.getRawStoreXact().newLockingPolicy(
						LockingPolicy.MODE_RECORD,
						TransactionController.ISOLATION_REPEATABLE_READ, true);

		record_level_policy[TransactionController.ISOLATION_SERIALIZABLE] =
				tc.getRawStoreXact().newLockingPolicy(
						LockingPolicy.MODE_RECORD,
						TransactionController.ISOLATION_SERIALIZABLE, true);

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
		pf = (PropertyFactory) Monitor.findServiceModule(this, org.apache.derby.iapi.reference.Module.PropertyFactory);

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

		lock_mode = (getSystemLockLevel() == TransactionController.MODE_TABLE ? LockingPolicy.MODE_CONTAINER : LockingPolicy.MODE_RECORD);

		system_default_locking_policy =tc.getRawStoreXact().newLockingPolicy(lock_mode,TransactionController.ISOLATION_SERIALIZABLE, true);

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
			SanityManager.ASSERT(
					TransactionController.OPENMODE_USE_UPDATE_LOCKS ==
					ContainerHandle.MODE_USE_UPDATE_LOCKS);
			SanityManager.ASSERT(
					TransactionController.OPENMODE_SECONDARY_LOCKED ==
					ContainerHandle.MODE_SECONDARY_LOCKED);
			SanityManager.ASSERT(
					TransactionController.OPENMODE_BASEROW_INSERT_LOCKED ==
					ContainerHandle.MODE_BASEROW_INSERT_LOCKED);
			SanityManager.ASSERT(
					TransactionController.OPENMODE_FORUPDATE ==
					ContainerHandle.MODE_FORUPDATE);
			SanityManager.ASSERT(
					TransactionController.OPENMODE_FOR_LOCK_ONLY ==
					ContainerHandle.MODE_OPEN_FOR_LOCK_ONLY);
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

	public Serviceable apply(String key, Serializable value, Dictionary p) throws StandardException {
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

	public static HTableInterface getHTable(Long id) {
//		if (LOG.isTraceEnabled())
//			LOG.trace("Getting HTable " + id);
		return singleRPCPool.getTable(TableName.valueOf(Long.toString(id)).getNameAsString());
	}

	public static HTableInterface getHTable(TableName tableName) {
//		if (LOG.isTraceEnabled())
//			LOG.trace("Getting HTable " + Bytes.toString(tableName));
		return singleRPCPool.getTable(tableName.getNameAsString());
	}

	public static HTableInterface getFlushableHTable(byte[] tableName) {
//		if (LOG.isTraceEnabled())
//			LOG.trace("Getting HTable " + Bytes.toString(tableName));
		return flushablePool.getTable(Bytes.toString(tableName));
	}

}

