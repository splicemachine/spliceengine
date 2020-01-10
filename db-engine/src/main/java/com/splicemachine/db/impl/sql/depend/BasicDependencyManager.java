/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.depend;

import com.splicemachine.db.catalog.DependableFinder;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ContextId;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.Parser;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.conn.StatementContext;
import com.splicemachine.db.iapi.sql.depend.*;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.sql.catalog.DataDictionaryCache;
import com.splicemachine.db.impl.sql.catalog.TableKey;
import com.splicemachine.db.impl.sql.compile.CreateViewNode;

import java.io.IOException;
import java.util.*;

/**
 * The dependency manager tracks needs that dependents have of providers.
 * <p>
 * A dependency can be either persistent or non-persistent. Persistent
 * dependencies are stored in the data dictionary, and non-persistent
 * dependencies are stored within the dependency manager itself (in memory).
 * <p>
 * <em>Synchronization:</em> The need for synchronization is different depending
 * on whether the dependency is an in-memory dependency or a stored dependency.
 * When accessing and modifying in-memory dependencies, Java synchronization
 * must be used (specifically, we synchronize on {@code this}). When accessing
 * and modifying stored dependencies, which are stored in the data dictionary,
 * we expect that the locking protocols will provide the synchronization needed.
 * Note that stored dependencies should not be accessed while holding the
 * monitor of {@code this}, as this may result in deadlocks. So far the need
 * for synchronization across both in-memory and stored dependencies hasn't
 * occurred.
 */
public class BasicDependencyManager implements DependencyManager {

    /**
     * DataDictionary for this database.
     */
    private final DataDictionary dd;

    /**
     * Map of in-memory dependencies for Dependents.
     * In-memory means that one or both of the Dependent
     * or Provider are non-persistent (isPersistent() returns false).
     *
     * Key is the UUID of the Dependent (from getObjectID()).
     * Value is a List containing Dependency objects, each
     * of which links the same Dependent to a Provider.
     * Dependency objects in the List are unique.
     *
     */
    //@GuardedBy("this")
    private final Map<UUID, List<Dependency>> dependents = new HashMap<>();

    /**
     * Map of in-memory dependencies for Providers.
     * In-memory means that one or both of the Dependent
     * or Provider are non-persistent (isPersistent() returns false).
     *
     * Key is the UUID of the Provider (from getObjectID()).
     * Value is a List containing Dependency objects, each
     * of which links the same Provider to a Dependent.
     * Dependency objects in the List are unique.
     *
     */
    //@GuardedBy("this")
    private final Map<UUID, List<Dependency>> providers = new HashMap<>();


	//
	// DependencyManager interface
	//

	/**
		adds a dependency from the dependent on the provider.
		This will be considered to be the default type of
		dependency, when dependency types show up.
		<p>
		Implementations of addDependency should be fast --
		performing alot of extra actions to add a dependency would
		be a detriment.

		@param d the dependent
		@param p the provider

		@exception StandardException thrown if something goes wrong
	 */
	public void addDependency(Dependent d, Provider p, ContextManager cm) throws StandardException {
        if (dd.canUseDependencyManager()) {
			addDependency(d, p, cm, null);
		}
	}

    /**
     * Adds the dependency to the data dictionary or the in-memory dependency
     * map.
     * <p>
     * The action taken is detmermined by whether the dependent and/or the
     * provider are persistent.
     *
     * @param d the dependent
     * @param p the provider
     * @param cm context manager
     * @param tc transaction controller, used to determine if any transactional
     *      operations should be attempted carried out in a nested transaction.
     *      If {@code tc} is {@code null}, the user transaction is used.
     * @throws StandardException if adding the dependency fails
     */
    private void addDependency(Dependent d, Provider p, ContextManager cm, TransactionController tc) throws StandardException {
        // Dependencies are either in-memory or stored, but not both.
        if (! d.isPersistent() || ! p.isPersistent()) {
            addInMemoryDependency(d, p, cm);
        } else {
            addStoredDependency(d, p, cm, tc);
        }
    }

    /**
     * Adds the dependency as an in-memory dependency.
     *
     * @param d the dependent
     * @param p the provider
     * @param cm context manager
     * @throws StandardException if adding the dependency fails
     * @see #addStoredDependency
     */
    private synchronized void addInMemoryDependency(Dependent d, Provider p,
                                                    ContextManager cm) throws StandardException {
        Dependency dy = new BasicDependency(d, p);

        // Duplicate dependencies are not added to the lists.
        // If we find that the dependency we are trying to add in
        // one list is a duplicate, then it should be a duplicate in the
        // other list.
        boolean addedToDeps;
        boolean addedToProvs = false;

        addedToDeps = addDependencyToTable(dependents, d.getObjectID(), dy);
        if (addedToDeps) {
            addedToProvs = addDependencyToTable(providers, p.getObjectID(), dy);
        } else if (SanityManager.DEBUG) {
            addedToProvs = addDependencyToTable(providers, p.getObjectID(), dy);
        }

        // Dependency should have been added to both or neither.
		// No Longer the case due to with clauses
/*        if (SanityManager.DEBUG) {
            if (addedToDeps != addedToProvs) {
                SanityManager.THROWASSERT(
                    "addedToDeps (" + addedToDeps +
                    ") and addedToProvs (" +
                    addedToProvs + ") are expected to agree");
            }
        }
*/
        // Add the dependency to the StatementContext, so that
        // it can be cleared on a pre-execution error.
        StatementContext sc = (StatementContext) cm.getContext(
                ContextId.LANG_STATEMENT);
        sc.addDependency(dy);
    }

    /**
     * Adds the dependency as a stored dependency.
     * <p>
     * We expect that transactional locking (in the data dictionary) is enough
     * to protect us from concurrent changes when adding stored dependencies.
     * Adding synchronization here and accessing the data dictionary (which is
     * transactional) may cause deadlocks.
     *
     * @param d the dependent
     * @param p the provider
     * @param cm context manager
     * @param tc transaction controller (may be {@code null})
     * @throws StandardException if adding the dependency fails
     * @see #addInMemoryDependency
     */
    private void addStoredDependency(Dependent d, Provider p,
                                     ContextManager cm,
                                     TransactionController tc)
            throws StandardException {
        LanguageConnectionContext lcc = getLanguageConnectionContext(cm);
        // tc == null means do it in the user transaction
        TransactionController tcToUse =
                (tc == null) ? lcc.getTransactionExecute() : tc;
        // Call the DataDictionary to store the dependency.

        if (!tcToUse.isElevated()) {
            StandardException.plainWrapException(new IOException("addStoreDependency: not writeable"));
        }

        dd.addDescriptor(new DependencyDescriptor(d, p), null,
                         DataDictionary.SYSDEPENDS_CATALOG_NUM, true,
                         tcToUse, false);
    }

	/**
		drops a single dependency

		@param d the dependent
		@param p the provider

		@exception StandardException thrown if something goes wrong
	 */
	private	void dropDependency(LanguageConnectionContext lcc, Dependent d, Provider p) throws StandardException
	{
		if (SanityManager.DEBUG) {
			// right now, this routine isn't called for in-memory dependencies
			if (! d.isPersistent() || ! p.isPersistent())
			{
				SanityManager.NOTREACHED();
			}
		}

		DependencyDescriptor dependencyDescriptor = new DependencyDescriptor(d, p);

		dd.dropStoredDependency( dependencyDescriptor,
								 lcc.getTransactionExecute() );
	}


	/**
		mark all dependencies on the named provider as invalid.
		When invalidation types show up, this will use the default
		invalidation type. The dependencies will still exist once
		they are marked invalid; clearDependencies should be used
		to remove dependencies that a dependent has or provider gives.
		<p>
		Implementations of this can take a little time, but are not
		really expected to recompile things against any changes
		made to the provider that caused the invalidation. The
		dependency system makes no guarantees about the state of
		the provider -- implementations can call this before or
		after actually changing the provider to its new state.
		<p>
		Implementations should throw StandardException
		if the invalidation should be disallowed.

		@param p the provider
		@param action	The action causing the invalidate

		@exception StandardException thrown if unable to make it invalid
	 */
	public void invalidateFor(Provider p, int action, LanguageConnectionContext lcc) throws StandardException {
		/*
		** Non-persistent dependencies are stored in memory, and need to
		** use "synchronized" to ensure their lists don't change while
		** the invalidation is taking place.  Persistent dependencies are
		** stored in the data dictionary, and we should *not* do anything
		** transactional (like reading from a system table) from within
		** a synchronized method, as it could cause deadlock.
		**
		** Presumably, the transactional locking in the data dictionary
		** is enough to protect us, so that we don't have to put any
		** synchronization in the DependencyManager.
		*/
		if (p.isPersistent())
			coreInvalidateFor(p, action, lcc);
		else {
			synchronized (this) {
				coreInvalidateFor(p, action, lcc);
			}
		}

        if (p instanceof TableDescriptor) {
            flushCachesBasedOnTableDescriptor(((TableDescriptor)p), dd);
        }
	}

	@Override
	public Collection<Dependency> find(UUID dependencyUUID) throws StandardException{
		synchronized(this){
            return dependents.get(dependencyUUID);
        }
	}

	/**
     * A version of invalidateFor that does not provide synchronization among
     * invalidators.
     *
     * @param p the provider
     * @param action the action causing the invalidation
     * @param lcc language connection context
     *
     * @throws StandardException if something goes wrong
     */
	private void coreInvalidateFor(Provider p, int action, LanguageConnectionContext lcc) throws StandardException {
		List<Dependency> list = getDependents(p);

        if (list.isEmpty()) {
			return;
		}


		// affectedCols is passed in from table descriptor provider to indicate
		// which columns it cares; subsetCols is affectedCols' intersection
		// with column bit map found in the provider of SYSDEPENDS line to
		// find out which columns really matter.  If SYSDEPENDS line's
		// dependent is view (or maybe others), provider is table, yet it
		// doesn't have column bit map because the view was created in a
		// previous version of server which doesn't support column dependency,
		// and we really want it to have (such as in drop column), in any case
		// if we passed in table descriptor to this function with a bit map,
		// we really need this, we generate the bitmaps on the fly and update
		// SYSDEPENDS

		FormatableBitSet affectedCols = null, subsetCols = null;
		if (p instanceof TableDescriptor)
		{
			affectedCols = ((TableDescriptor) p).getReferencedColumnMap();
			if (affectedCols != null)
				subsetCols = new FormatableBitSet(affectedCols.getLength());
		}

		{
			StandardException noInvalidate = null;
			// We cannot use an iterator here as the invalidations can remove
			// entries from this list.
			for (int ei = list.size() - 1; ei >= 0; ei--)
			{
				if (ei >= list.size())
					continue;
				Dependency dependency = list.get(ei);
				Dependent dep = dependency.getDependent();
				if (dep == null) {
					continue;
				}

				if (affectedCols != null)
				{
					TableDescriptor td = (TableDescriptor) dependency.getProvider();
					FormatableBitSet providingCols = td.getReferencedColumnMap();
					if (providingCols == null)
					{
						if (dep instanceof ViewDescriptor)
						{
							ViewDescriptor vd = (ViewDescriptor) dep;
							SchemaDescriptor compSchema;
							compSchema = dd.getSchemaDescriptor(vd.getCompSchemaId(), null);
							CompilerContext newCC = lcc.pushCompilerContext(compSchema);
							Parser	pa = newCC.getParser();

							// Since this is always nested inside another SQL
							// statement, so topLevel flag should be false
							CreateViewNode cvn = (CreateViewNode)pa.parseStatement(
												vd.getViewText());

							// need a current dependent for bind
							newCC.setCurrentDependent(dep);
							cvn.bindStatement();
							ProviderInfo[] providerInfos = cvn.getProviderInfo();
							lcc.popCompilerContext(newCC);

							boolean		interferent = false;
                            for (ProviderInfo providerInfo : providerInfos) {
                                Provider provider;
                                provider = (Provider) providerInfo.
                                        getDependableFinder().getDependable(dd, providerInfo.getObjectId());
                                if (provider instanceof TableDescriptor) {
                                    TableDescriptor tab = (TableDescriptor) provider;
                                    FormatableBitSet colMap = tab.getReferencedColumnMap();
                                    if (colMap == null)
                                        continue;
                                    // if later on an error is raised such as in
                                    // case of interference, this dependency line
                                    // upgrade will not happen due to rollback
                                    tab.setReferencedColumnMap(null);
                                    dropDependency(lcc, vd, tab);
                                    tab.setReferencedColumnMap(colMap);
                                    addDependency(vd, tab, lcc.getContextManager());

                                    if (tab.getObjectID().equals(td.getObjectID())) {
                                        System.arraycopy(affectedCols.getByteArray(), 0,
                                                subsetCols.getByteArray(), 0,
                                                affectedCols.getLengthInBytes());
                                        subsetCols.and(colMap);
                                        if (subsetCols.anySetBit() != -1) {
                                            interferent = true;
                                            ((TableDescriptor) p).setReferencedColumnMap(subsetCols);
                                        }
                                    }
                                }    // if provider instanceof TableDescriptor
                            }    // for providerInfos
							if (! interferent)
								continue;
						}	// if dep instanceof ViewDescriptor
						else
							((TableDescriptor) p).setReferencedColumnMap(null);
					}	// if providingCols == null
					else
					{
						System.arraycopy(affectedCols.getByteArray(), 0, subsetCols.getByteArray(), 0, affectedCols.getLengthInBytes());
						subsetCols.and(providingCols);
						if (subsetCols.anySetBit() == -1)
							continue;
						((TableDescriptor) p).setReferencedColumnMap(subsetCols);
					}
				}

				// generate a list of invalidations that fail.
				try {
					dep.prepareToInvalidate(p, action, lcc);
				} catch (StandardException sqle) {

					if (noInvalidate == null) {
						noInvalidate = sqle;
					} else {
						try {
							sqle.initCause(noInvalidate);
							noInvalidate = sqle;
						} catch (IllegalStateException ise) {
							// We weren't able to chain the exceptions. That's
							// OK, since we always have the first exception we
							// caught. Just skip the current exception.
						}
					}
				}
				if (noInvalidate == null) {

					if (affectedCols != null)
						((TableDescriptor) p).setReferencedColumnMap(affectedCols);

					// REVISIT: future impl will want to mark the individual
					// dependency as invalid as well as the dependent...
					dep.makeInvalid(action, lcc);
				}
			}

			if (noInvalidate != null)
				throw noInvalidate;
		}
	}

    private static void flushCachesBasedOnTableDescriptor(TableDescriptor td,DataDictionary dd) throws StandardException {
        // cache flushing for table dependencies happen in each dependent type's impl via dep.makeInvalid(action, lcc)
        // See GenericPreparedStatement#makeInvalid()
        DataDictionaryCache cache = dd.getDataDictionaryCache();
        TableKey tableKey = new TableKey(td.getSchemaDescriptor().getUUID(), td.getName());
        cache.nameTdCacheRemove(tableKey);
        cache.oidTdCacheRemove(td.getUUID());

        // Remove Conglomerate Level and Statistics Caching..
        for (ConglomerateDescriptor cd: td.getConglomerateDescriptorList()) {
            cache.partitionStatisticsCacheRemove(cd.getConglomerateNumber());
            cache.conglomerateCacheRemove(cd.getConglomerateNumber());
            cache.conglomerateDescriptorCacheRemove(cd.getConglomerateNumber());
        }

    }

	/**
		Erases all of the dependencies the dependent has, be they
		valid or invalid, of any dependency type.  This action is
		usually performed as the first step in revalidating a
		dependent; it first erases all the old dependencies, then
		revalidates itself generating a list of new dependencies,
		and then marks itself valid if all its new dependencies are
		valid.
		<p>
		There might be a future want to clear all dependencies for
		a particular provider, e.g. when destroying the provider.
		However, at present, they are assumed to stick around and
		it is the responsibility of the dependent to erase them when
		revalidating against the new version of the provider.
		<p>
		clearDependencies will delete dependencies if they are
		stored; the delete is finalized at the next commit.

		@param d the dependent
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void clearDependencies(LanguageConnectionContext lcc, Dependent d) throws StandardException {
		clearDependencies(lcc, d, null);
	}

	/**
	 */
	public void clearDependencies(LanguageConnectionContext lcc,
									Dependent d, TransactionController tc) throws StandardException {
//        dd.startWriting(lcc);
        UUID id = d.getObjectID();
        // Remove all the stored dependencies.
        if (d.isPersistent()) {
            boolean wait = (tc == null);
            dd.dropDependentsStoredDependencies(id,
                            ((wait)?lcc.getTransactionExecute():tc),
                            wait);
        }

        // Now remove the in-memory dependencies, if any.
        synchronized(this) {
            List<Dependency> deps = dependents.get(id);
            if (deps != null) {
                // go through the list notifying providers to remove the dependency from their lists
                for (Dependency dy : deps) {
                    clearProviderDependency(dy.getProviderKey(), dy);
                }
                dependents.remove(id);
            }
        }
    }

	/**
	 * Clear the specified in memory dependency.
	 * This is useful for clean-up when an exception occurs.
	 * (We clear all in-memory dependencies added in the current
	 * StatementContext.)
	 */
    public synchronized void clearInMemoryDependency(Dependency dy) {
        if (dy.getDependent() == null) {
            // nothing to clear
            return;
        }
        final UUID deptId = dy.getDependent().getObjectID();
        final UUID provId = dy.getProviderKey();
        List<Dependency> deps = dependents.get(deptId);

        // NOTE - this is a NEGATIVE Sanity mode check, in sane mode we continue
        // to ensure the dependency manager is consistent.
        if (!SanityManager.DEBUG && deps == null) {
            // dependency has already been removed
            return;
        }

        List<Dependency> provs = providers.get(provId);

        if (SanityManager.DEBUG)
        {
            // if both are null then everything is OK
            if ((deps != null) || (provs != null)) {

                // ensure that the Dependency dy is either
                // in both lists or in neither. Even if dy
                // is out of the list we can have non-null
                // deps and provs here because other dependencies
                // with the the same providers or dependents can exist

                //
                int depCount = 0;
                if (deps != null) {
                    for (Dependency dep : deps) {
                        if (dy.equals(dep))
                            depCount++;
                    }
                }

                int provCount = 0;
                if (provs != null) {
                    for (Dependency prov : provs) {
                        if (dy.equals(prov))
                            provCount++;
                    }
                }

                SanityManager.ASSERT(depCount == provCount,
                        "Dependency count mismatch count in deps: " + depCount +
                        ", count in provs " + provCount +
                        ", dy.getDependent().getObjectID() = " + deptId +
                        ", dy.getProvider().getObjectID() = " + provId);
            }

            // dependency has already been removed,
            // matches code that is protected by !DEBUG above
            if (deps == null)
                return;
        }

        // dependency has already been removed
        if (provs == null)
            return;


        deps.remove(dy);
        if (deps.isEmpty())
            dependents.remove(deptId);
        provs.remove(dy);
        if (provs.isEmpty())
            providers.remove(provId);
    }

	/**
	 * @see DependencyManager#getPersistentProviderInfos
	 *
	 * @exception StandardException		Thrown on error
	 */
    public ProviderInfo[] getPersistentProviderInfos(Dependent dependent) throws StandardException {
		List<Provider> list = getProviders(dependent);
        if (list.isEmpty()) {
			return new ProviderInfo[0];
		}

        Iterator<Provider> provIter = list.iterator();
        List<BasicProviderInfo> pih = new ArrayList<>();
        while (provIter.hasNext()) {
            Provider p = provIter.next();

            if (p.isPersistent()) {
                pih.add(new BasicProviderInfo(p.getObjectID(), p.getDependableFinder(), p.getObjectName()));
            }
		}

		return pih.toArray(new ProviderInfo[pih.size()]);
	}

    /**
	 * @see DependencyManager#getPersistentProviderInfos
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ProviderInfo[] getPersistentProviderInfos(ProviderList pl) throws StandardException {
		Enumeration<Provider> e = pl.elements();
		int	numProviders = 0;

		/*
		** We make 2 passes - the first to count the number of persistent
 		** providers and the second to populate the array of ProviderInfos.
		*/
		while (e != null && e.hasMoreElements()) {
			Provider prov = e.nextElement();
			if (prov.isPersistent()) {
				numProviders++;
			}
		}

		e = pl.elements();
        ProviderInfo[] retval = new ProviderInfo[numProviders];
		int piCtr = 0;
		while (e != null && e.hasMoreElements()) {
			Provider prov = e.nextElement();

			if (prov.isPersistent()) {
				retval[piCtr++] = new BasicProviderInfo(
									prov.getObjectID(),
									prov.getDependableFinder(),
									prov.getObjectName());
			}
		}

		return retval;
	}

	/**
	 * @see DependencyManager#clearColumnInfoInProviders
	 *
	 * @param pl		provider list
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void clearColumnInfoInProviders(ProviderList pl) throws StandardException {
		Enumeration<Provider> e = pl.elements();
		while (e.hasMoreElements()) {
			Provider pro = e.nextElement();
			if (pro instanceof TableDescriptor)
				((TableDescriptor) pro).setReferencedColumnMap(null);
		}
	}

	/**
 	 * Copy dependencies from one dependent to another.
	 *
	 * @param copy_From the dependent to copy from
	 * @param copyTo the dependent to copy to
	 * @param persistentOnly only copy persistent dependencies
	 * @param cm		Current ContextManager
	 *
	 * @exception StandardException		Thrown on error.
	 */
	public void copyDependencies(Dependent	copy_From,
								Dependent	copyTo,
								boolean		persistentOnly,
								ContextManager cm) throws StandardException
	{
		copyDependencies(copy_From, copyTo, persistentOnly, cm, null);
	}

	/**
	 * @inheritDoc
	 */
    public void copyDependencies(
									Dependent	copy_From,
									Dependent	copyTo,
									boolean		persistentOnly,
									ContextManager cm,
									TransactionController tc) throws StandardException {

		List<Provider> list = getProviders(copy_From);
        for (Provider provider : list) {
            if (!persistentOnly || provider.isPersistent()) {
                this.addDependency(copyTo, provider, cm, tc);
            }
        }
	}


	/**
	 * Returns a string representation of the SQL action, hence no
	 * need to internationalize, which is causing the invokation
	 * of the Dependency Manager.
	 *
	 * @param action		The action
	 *
	 * @return String	The String representation
	 */
	public String getActionString(int action)
	{
		switch (action)
		{
			case ALTER_TABLE:
				return "ALTER TABLE";

			case RENAME: //for rename table and column
				return "RENAME";

			case RENAME_INDEX:
				return "RENAME INDEX";

			case COMPILE_FAILED:
				return "COMPILE FAILED";

			case DROP_TABLE:
				return "DROP TABLE";

			case DROP_INDEX:
				return "DROP INDEX";

			case DROP_VIEW:
				return "DROP VIEW";

			case CREATE_INDEX:
				return "CREATE INDEX";

			case ROLLBACK:
				return "ROLLBACK";

			case CHANGED_CURSOR:
				return "CHANGED CURSOR";

			case CREATE_CONSTRAINT:
				return "CREATE CONSTRAINT";

			case DROP_CONSTRAINT:
				return "DROP CONSTRAINT";

			case DROP_METHOD_ALIAS:
				return "DROP ROUTINE";

			case PREPARED_STATEMENT_RELEASE:
				return "PREPARED STATEMENT RELEASE";

			case DROP_SPS:
				return "DROP STORED PREPARED STATEMENT";

			case USER_RECOMPILE_REQUEST:
				return "USER REQUESTED INVALIDATION";

			case BULK_INSERT:
				return "BULK INSERT";

		    case CREATE_VIEW:
				return "CREATE_VIEW";

			case DROP_JAR:
				return "DROP_JAR";

			case REPLACE_JAR:
				return "REPLACE_JAR";

			case SET_CONSTRAINTS_ENABLE:
				return "SET_CONSTRAINTS_ENABLE";

			case SET_CONSTRAINTS_DISABLE:
				return "SET_CONSTRAINTS_DISABLE";

			case INTERNAL_RECOMPILE_REQUEST:
				return "INTERNAL RECOMPILE REQUEST";

			case CREATE_TRIGGER:
				return "CREATE TRIGGER";

			case DROP_TRIGGER:
				return "DROP TRIGGER";

			case SET_TRIGGERS_ENABLE:
				return "SET TRIGGERS ENABLED";

			case SET_TRIGGERS_DISABLE:
				return "SET TRIGGERS DISABLED";

			case MODIFY_COLUMN_DEFAULT:
				return "MODIFY COLUMN DEFAULT";

			case COMPRESS_TABLE:
				return "COMPRESS TABLE";

			case DROP_COLUMN:
				return "DROP COLUMN";

			case DROP_COLUMN_RESTRICT:
				return "DROP COLUMN RESTRICT";

		    case DROP_STATISTICS:
				return "DROP STATISTICS";

			case UPDATE_STATISTICS:
				return "UPDATE STATISTICS";

		    case TRUNCATE_TABLE:
			    return "TRUNCATE TABLE";

		    case DROP_SYNONYM:
			    return "DROP SYNONYM";

		    case REVOKE_PRIVILEGE:
			    return "REVOKE PRIVILEGE";

		    case REVOKE_PRIVILEGE_RESTRICT:
			    return "REVOKE PRIVILEGE RESTRICT";

		    case REVOKE_ROLE:
				return "REVOKE ROLE";

		    case RECHECK_PRIVILEGES:
				return "RECHECK PRIVILEGES";

            case DROP_SEQUENCE:
				return "DROP SEQUENCE";

            case DROP_UDT:
				return "DROP TYPE";
            case DROP_AGGREGATE:
       			return "DROP DERBY AGGREGATE";
            default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT("getActionString() passed an invalid value (" + action + ")");
				}
				// NOTE: This is not internationalized because we should never
				// reach here.
				return "UNKNOWN";
		}
	}

	/**
	 * Count the number of active dependencies, both stored and in memory,
	 * in the system.
	 *
	 * @return int		The number of active dependencies in the system.

		@exception StandardException thrown if something goes wrong
	 */
	public int countDependencies() throws StandardException {
        // Add the stored dependencies.
        int numDependencies = dd.getAllDependencyDescriptorsList().size();
        synchronized(this) {
            Iterator<List<Dependency>> deps = dependents.values().iterator();
            Iterator<List<Dependency>> provs = providers.values().iterator();

            // Count the in memory dependencies.
            while (deps.hasNext()) {
                numDependencies += deps.next().size();
            }

            while (provs.hasNext()) {
                numDependencies += provs.next().size();
            }
        }
        return numDependencies;
	}

	//
	// class interface
	//
	public BasicDependencyManager(DataDictionary dd) {
        this.dd = dd;
	}

	//
	// class implementation
	//

	/**
	 * Add a new dependency to the specified table if it does not
	 * already exist in that table.
	 *
	 * @return boolean		Whether or not the dependency get added.
	 */
    private boolean addDependencyToTable(Map<UUID, List<Dependency>> table, UUID key, Dependency dy) {

		List<Dependency> deps = table.get(key);
		if (deps == null) {
			deps = new ArrayList<>();
			deps.add(dy);
			table.put(key, deps);
		}
		else {
			/* Make sure that we're not adding a duplicate dependency */
			UUID provKey = dy.getProvider().getObjectID();
			UUID depKey = dy.getDependent().getObjectID();

			Iterator<Dependency> it = deps.iterator();
			while (it.hasNext()) {
				Dependency curDY = it.next();
				Dependent curDependent = curDY.getDependent();
				if (curDependent == null) {
					it.remove();
				} else if (curDY.getProvider().getObjectID() == null || (curDY.getProvider().getObjectID().equals(provKey) && curDependent.getObjectID().equals(depKey))) {
					// Check for dupes and dynamic with clauses
					return false;
				}
			}

			deps.add(dy);
		}

		if (SanityManager.DEBUG) {

			if (SanityManager.DEBUG_ON("memoryLeakTrace")) {

				if (table.size() > 100)
					System.out.println("memoryLeakTrace:BasicDependencyManager:table " + table.size());
				if (deps.size() > 50)
					System.out.println("memoryLeakTrace:BasicDependencyManager:deps " + deps.size());
			}
		}

		return true;
	}

	/**
	 * removes a dependency for a given provider. assumes
	 * that the dependent removal is being dealt with elsewhere.
	 * Won't assume that the dependent only appears once in the list.
	 */
    //@GuardedBy("this")
    private void clearProviderDependency(UUID p, Dependency d) {
		List<Dependency> deps = providers.get(p);

		if (deps == null)
			return;

		deps.remove(d);

		if (deps.isEmpty())
			providers.remove(p);
	}

	/**
	 * Replace the DependencyDescriptors in an List with Dependencys.
	 *
	 * @param storedList	The List of DependencyDescriptors representing
	 *						stored dependencies.
	 * @param providerForList The provider if this list is being created
	 *                        for a list of dependents. Null otherwise.
	 *
	 * @return List		The converted List
	 *
	 * @exception StandardException thrown if something goes wrong
	 */
    private List<Dependency> getDependencyDescriptorList(List<DependencyDescriptor> storedList,
                                                         Provider providerForList) throws StandardException {
        List<Dependency> returnList = new ArrayList<>();
        if (storedList.isEmpty()) {
            return returnList;
        }

	   /* For each DependencyDescriptor, we need to instantiate
        * object descriptors of the appropriate type for both
		* the dependent and provider, create a Dependency with
		* that Dependent and Provider. */
        for (DependencyDescriptor aStoredList : storedList) {
            Provider tempP;
            DependableFinder finder = aStoredList.getDependentFinder();
            Dependent tempD = (Dependent) finder.getDependable(dd, aStoredList.getUUID());

            if (providerForList != null) {
                // Use the provider being passed in.
                tempP = providerForList;

                // Sanity check the object identifiers match.
                if (SanityManager.DEBUG && !tempP.getObjectID().equals(aStoredList.getProviderID())) {
                    SanityManager.THROWASSERT("mismatch providers");
                }
            } else {
                finder = aStoredList.getProviderFinder();
                tempP = (Provider) finder.getDependable(dd, aStoredList.getProviderID());

            }
            returnList.add(new BasicDependency(tempD, tempP));
        }
        return returnList;
    }

	/**
	 * Returns the LanguageConnectionContext to use.
	 *
	 * @param cm	Current ContextManager
	 *
	 * @return LanguageConnectionContext	The LanguageConnectionContext to use.
	 */
	protected LanguageConnectionContext getLanguageConnectionContext(ContextManager cm)
	{
		// find the language context.
		return (LanguageConnectionContext) cm.getContext(LanguageConnectionContext.CONTEXT_ID);
	}

	/**
     * Returns a list of all providers that this dependent has (even invalid
     * ones). Includes all dependency types.
     *
     * @param d the dependent
     * @return A list of providers (possibly empty).
     * @throws StandardException thrown if something goes wrong
     */
    private List<Provider> getProviders (Dependent d) throws StandardException {
        List<Provider> provs = new ArrayList<>();
        synchronized (this) {
            List<Dependency> deps = dependents.get(d.getObjectID());
            if (deps != null) {
                for (Dependency dep : deps) {
                    provs.add(dep.getProvider());
                }
            }
        }

        // If the dependent is persistent, we have to take stored dependencies
        // into consideration as well.
        if (d.isPersistent()) {
            List<DependencyDescriptor> dependentsDescriptorList = dd.getDependentsDescriptorList(d.getObjectID().toString());
            List<Dependency> storedList = getDependencyDescriptorList(dependentsDescriptorList, null);
            for (Dependency aStoredList : storedList) {
                provs.add(aStoredList.getProvider());
            }
		}
        return provs;
	}

    /**
     * Returns an enumeration of all dependencies that this
     * provider is supporting for any dependent at all (even
     * invalid ones). Includes all dependency types.
     *
     * @param p the provider
     * @return A list of dependents (possibly empty).
     * @throws StandardException if something goes wrong
	 */
	private List<Dependency> getDependents (Provider p) throws StandardException {
        List<Dependency> deps = new ArrayList<>();
        synchronized (this) {
            List<Dependency> memDeps = providers.get(p.getObjectID());
            if (memDeps != null) {
                deps.addAll(memDeps);
            }
        }

        // If the provider is persistent, then we have to add providers for
        // stored dependencies as well.
        if (p.isPersistent()) {
            List<DependencyDescriptor> providersDescriptorList = dd.getProvidersDescriptorList(p.getObjectID().toString());
            List<Dependency> storedList = getDependencyDescriptorList(providersDescriptorList, p);
            deps.addAll(storedList);
        }
        return deps;
	}
}
