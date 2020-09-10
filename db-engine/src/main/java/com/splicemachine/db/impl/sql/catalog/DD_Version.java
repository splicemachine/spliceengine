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

package com.splicemachine.db.impl.sql.catalog;

import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.io.Formatable;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.CatalogRowFactory;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.store.access.ConglomerateController;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.sql.dictionary.IndexRowGenerator;
import com.splicemachine.db.iapi.store.access.ScanController;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;

import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.info.ProductVersionHolder;
import com.splicemachine.db.iapi.util.IdUtil;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.Properties;

/**
 * Generic code for upgrading data dictionaries.
 * Currently has all minor version upgrade logic.
 * <p>
 * A word about minor vs. major upgraded.  Minor
 * upgrades must be backwards/forwards compatible.
 * So they cannot version classes or introduce new
 * classes.  Major releases are only backwards compatible;
 * they will run against an old database, but not the
 * other way around.  So they can introduce new classes,
 * etc.
 *
 */

public	class DD_Version implements	Formatable
{
	////////////////////////////////////////////////////////////////////////
	//
	//	STATE
	//
	////////////////////////////////////////////////////////////////////////

	protected transient	DataDictionaryImpl	bootingDictionary;

	protected int majorVersionNumber;
	protected int minorVersionNumber;

	////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	////////////////////////////////////////////////////////////////////////

	/**
	  *	Public niladic constructor needed for Formatable interface.
	  */
	public	DD_Version() {}


	/**
	 * Construct a Version for the currently booting data dictionary.
	 * The minor version is set by the subclass.
	 *
	 * @param	bootingDictionary	The booting dictionary that needs to be upgraded.
	 */
	DD_Version( DataDictionaryImpl bootingDictionary, int majorVersionNumber)
	{
		this.majorVersionNumber = majorVersionNumber;
		this.minorVersionNumber = getJBMSMinorVersionNumber();
		this.bootingDictionary = bootingDictionary;
	}

	////////////////////////////////////////////////////////////////////////
	//
	//	OVERRIDE OBJECT METHODS
	//
	////////////////////////////////////////////////////////////////////////

	/**
	  *	Stringify this Version.
	  *
	  *	@return	String representation of this Version.
	  */
	public	String	toString()
	{
		return DD_Version.majorToString(majorVersionNumber);
	}

	private static String majorToString(int majorVersionNumber) {
		switch (majorVersionNumber) {
		case DataDictionary.DD_VERSION_CS_5_0:
			return "5.0";
		case DataDictionary.DD_VERSION_CS_5_1:
			return "5.1";
		case DataDictionary.DD_VERSION_CS_5_2:
			return "5.2";
		case DataDictionary.DD_VERSION_CS_8_1:
			return "8.1";
		case DataDictionary.DD_VERSION_CS_10_0:
			return "10.0";
		case DataDictionary.DD_VERSION_DERBY_10_1:
			return "10.1";
		case DataDictionary.DD_VERSION_DERBY_10_2:
			return "10.2";
		case DataDictionary.DD_VERSION_DERBY_10_3:
			return "10.3";
		case DataDictionary.DD_VERSION_DERBY_10_4:
			return "10.4";
		case DataDictionary.DD_VERSION_DERBY_10_5:
			return "10.5";
		case DataDictionary.DD_VERSION_DERBY_10_6:
			return "10.6";
		case DataDictionary.DD_VERSION_DERBY_10_7:
			return "10.7";
		case DataDictionary.DD_VERSION_DERBY_10_8:
			return "10.8";
		case DataDictionary.DD_VERSION_DERBY_10_9:
			return "10.9";
		default:
			return null;
		}
	}

	////////////////////////////////////////////////////////////////////////
	//
	//	DataDictionary SPECIFIC
	//
	////////////////////////////////////////////////////////////////////////

	/**
	 * Upgrade the data dictionary catalogs to the version represented by this
	 * DD_Version.
	 *
	 * @param dictionaryVersion the version of the data dictionary tables.
	 * @exception StandardException Ooops
	 */
	void upgradeIfNeeded(DD_Version dictionaryVersion,
								TransactionController tc, Properties startParams)
		 throws StandardException
	{
		// database has been upgrade with a later engine version than this?
		if (dictionaryVersion.majorVersionNumber > majorVersionNumber) {
			throw StandardException.newException(SQLState.LANG_CANT_UPGRADE_CATALOGS,  
				dictionaryVersion, this);
		}


		boolean minorOnly = false;
		boolean performMajorUpgrade = false;
		boolean softUpgradeRun = false;
		boolean isReadOnly = bootingDictionary.af.isReadOnly();	

		if (dictionaryVersion.majorVersionNumber == majorVersionNumber) {

			// exact match of engine to database, do nothing.
			if (dictionaryVersion.minorVersionNumber == minorVersionNumber)
				return;

			// database and engine at same major level
			minorOnly = true;

		} else {
           
			if (Monitor.isFullUpgrade(startParams, dictionaryVersion.toString())) {
				performMajorUpgrade = true;
			} else {
				softUpgradeRun = true;
			}
		}

		// make sure we have a clean transaction for the upgrade
		tc.commit();

		if (performMajorUpgrade) {
			// real upgrade changes. Get user name of current user.
			String userName = IdUtil.getUserNameFromURLProps(startParams);
			doFullUpgrade(tc, dictionaryVersion.majorVersionNumber,IdUtil.getUserAuthorizationId(userName));
		}

		if (!minorOnly && !isReadOnly) {
			// apply changes that can be made and will continue to work
			// against previous version.

			// See if we have already applied these changes.
			DD_Version softUpgradeVersion = (DD_Version) tc.getProperty(
											DataDictionary.SOFT_DATA_DICTIONARY_VERSION);

			// need to apply them if we have never performed a soft upgrade
			// or only a soft upgrade using a previous version.
			int softUpgradeMajorVersion = 0;
			if (softUpgradeVersion != null)
				softUpgradeMajorVersion = softUpgradeVersion.majorVersionNumber;

			if (softUpgradeMajorVersion < majorVersionNumber) {
				applySafeChanges( tc, dictionaryVersion.majorVersionNumber, softUpgradeMajorVersion);
			}
		}

		// changes such as invalidating SPS so they will recompile against
		// the new internal classes.
		// this method also changes the on-disk format version on the disk and in-memory as well.
		handleMinorRevisionChange(tc, dictionaryVersion, softUpgradeRun);

		// commit any upgrade
		tc.commit();
	}

	/**
		Apply changes that can safely be made in soft upgrade.
		Any changes must not prevent the database from being re-booted
		by the a Derby engine at the older version fromMajorVersionNumber.
		<BR>
		Examples are fixes to catalog meta data, e.g. fix nullability of
		a system column.

		<BR>
		<B>Upgrade items for 10.1</B>
		<UL>
		<LI> None.
		</UL>
	  *
	  * @param	tc	transaction controller
	  * @param	fromMajorVersionNumber	version of the on-disk database
	    @param  lastSoftUpgradeVersion last engine to perform a soft upgrade that made changes.
	  *
	  *	@exception StandardException  Standard Derby error policy.
	  */
	private	void	applySafeChanges(TransactionController tc, int fromMajorVersionNumber, int lastSoftUpgradeVersion)
		throws StandardException
	{

		/*
		 * OLD Cloudscape 5.1 upgrade code, Derby does not support
		 * upgrade from Cloudscape 5.x databases. If it ever is changed
		 * to do so, this code would be useful.
		 * 
		 * 
		if (lastSoftUpgradeVersion <= DataDictionary.DD_VERSION_CS_5_1)
		{

			// All these soft upgrade actions are new in 5.2 (first ever soft upgrade)
			if (fromMajorVersionNumber <= DataDictionary.DD_VERSION_CS_5_0)
				modifySysTableNullability(tc,
					DataDictionaryImpl.SYSALIASES_CATALOG_NUM);

			if (fromMajorVersionNumber <= DataDictionary.DD_VERSION_CS_5_1)
				modifySysTableNullability(tc,
					DataDictionaryImpl.SYSSTATEMENTS_CATALOG_NUM);

		}
		*/

		/*
		 * Derby soft upgrade code
		 */
		if (lastSoftUpgradeVersion <= DataDictionary.DD_VERSION_DERBY_10_2)
		{
			if (fromMajorVersionNumber <= DataDictionary.DD_VERSION_DERBY_10_2)
			{
				modifySysTableNullability(tc,
					DataDictionaryImpl.SYSSTATEMENTS_CATALOG_NUM);
			
				modifySysTableNullability(tc,
					DataDictionaryImpl.SYSVIEWS_CATALOG_NUM);
			}
		}
		
		tc.setProperty(DataDictionary.SOFT_DATA_DICTIONARY_VERSION, this, true);
	}

	/**
		Do full upgrade.  Apply changes that can NOT be safely made in soft upgrade.
		
		<BR>
		<B>Upgrade items for every new release</B>
		<UL>
		<LI> Drop and recreate the stored versions of the JDBC database metadata queries
		</UL>
		
		<BR>
		<B>Upgrade items for 10.1</B>
		<UL>
		<LI> None.
		</UL>
		
	  *
	  * @param	tc	transaction controller
	  * @param	fromMajorVersionNumber	version of the on-disk database
	  * @param	aid	 AuthorizationID of current user to be made Database Owner
	  *
	  *	@exception StandardException  Standard Derby error policy.
	  */
	private	void	doFullUpgrade(TransactionController tc, int fromMajorVersionNumber, String aid)
		throws StandardException
	{
		// Only supports upgrade from Derby 10.0 releases onwards
		if (fromMajorVersionNumber < DataDictionary.DD_VERSION_CS_10_0)
		{
			throw StandardException.newException(SQLState.UPGRADE_UNSUPPORTED,
					DD_Version.majorToString(fromMajorVersionNumber), this);			
		}

		//Drop and recreate the stored versions of the JDBC database metadata queries
		//This is to make sure that we have the stored versions of JDBC database
		//metadata queries matching with this release of the engine.
		bootingDictionary.updateMetadataSPSes(tc);

		/*
		 * OLD Cloudscape 5.1 upgrade code, Derby does not support
		 * upgrade from Cloudscape 5.x databases. If it ever is changed
		 * to do so, this code would be useful.
	
		if (fromMajorVersionNumber <= DataDictionary.DD_VERSION_CS_5_1)
		{
			// drop sps in SYSIBM, SYSIBM, recreate SYSIBM, SYSDUMMY1, populate SYSDUMMY1, create procs
			dropJDBCMetadataSPSes(tc, true);
			SchemaDescriptor sd = bootingDictionary.getSchemaDescriptor("SYSIBM", null, false);
			if (sd != null)
				bootingDictionary.dropSchemaDescriptor("SYSIBM", tc);
			sd = bootingDictionary.getSysIBMSchemaDescriptor();
			bootingDictionary.addDescriptor(sd, null, DataDictionary.SYSSCHEMAS_CATALOG_NUM, false, tc);
			bootingDictionary.upgradeMakeCatalog(tc, DataDictionary.SYSDUMMY1_CATALOG_NUM);
			bootingDictionary.populateSYSDUMMY1(tc);
			bootingDictionary.create_SYSIBM_procedures(tc);
			bootingDictionary.createSystemSps(tc);
		}
		
		*/

        HashSet  newlyCreatedRoutines = new HashSet();
        
		if (fromMajorVersionNumber <= DataDictionary.DD_VERSION_DERBY_10_3)
		{
			// Add new system catalogs created for roles
			bootingDictionary.upgradeMakeCatalog(
				tc, DataDictionary.SYSROLES_CATALOG_NUM);
		}

        if (fromMajorVersionNumber <= DataDictionary.DD_VERSION_DERBY_10_1)
        {
            // add catalogs 1st, subsequent procedure adding may depend on
            // catalogs.

			// Add new system catalogs created for grant and revoke
			bootingDictionary.upgradeMakeCatalog(
                tc, DataDictionary.SYSTABLEPERMS_CATALOG_NUM);
			bootingDictionary.upgradeMakeCatalog(
                tc, DataDictionary.SYSCOLPERMS_CATALOG_NUM);
			bootingDictionary.upgradeMakeCatalog(
                tc, DataDictionary.SYSROUTINEPERMS_CATALOG_NUM);
        }

        if (fromMajorVersionNumber == DataDictionary.DD_VERSION_CS_10_0)
        {
            // This upgrade depends on the SYSUTIL schema, which only exists
            // since 10.0.  Will not work to upgrade any db previous to 10.0,
            // thus only checks for 10.0 rather than <= 10.0.
            bootingDictionary.create_10_1_system_procedures(
                tc,
                newlyCreatedRoutines,
                bootingDictionary.getSystemUtilSchemaDescriptor().getUUID());
        }

        if (fromMajorVersionNumber <= DataDictionary.DD_VERSION_DERBY_10_5)
        {
            // On ugrade from versions before 10.6, create system procedures
            // added in 10.6.
            bootingDictionary.create_10_6_system_procedures(tc,
                    newlyCreatedRoutines);
            
            // On upgrade from versions before 10.6, create system catalogs
            // added in 10.6
            bootingDictionary.upgradeMakeCatalog(
                    tc, DataDictionary.SYSSEQUENCES_CATALOG_NUM);
            bootingDictionary.upgradeMakeCatalog(
                    tc, DataDictionary.SYSPERMS_CATALOG_NUM);
        }

        if (fromMajorVersionNumber <= DataDictionary.DD_VERSION_DERBY_10_1)
        {
            // On ugrade from versions before 10.2, create system procedures
            // added in 10.2.
            bootingDictionary.create_10_2_system_procedures(
                tc,
                newlyCreatedRoutines,
                bootingDictionary.getSystemUtilSchemaDescriptor().getUUID());

			if (SanityManager.DEBUG)
            {
				SanityManager.ASSERT((aid != null), 
                    "Failed to get new Database Owner authorization");
            }

			// Change system schemas to be owned by aid
			bootingDictionary.updateSystemSchemaAuthorization(aid, tc);

        }

        if (fromMajorVersionNumber <= DataDictionary.DD_VERSION_DERBY_10_2)
        {
            // On ugrade from versions before 10.3, create system procedures
            // added in 10.3.
            bootingDictionary.create_10_3_system_procedures(tc, newlyCreatedRoutines );
        }

        if (fromMajorVersionNumber <= DataDictionary.DD_VERSION_DERBY_10_4)
        {
            // On upgrade from versions before 10.5, create system procedures
            // added in 10.5.
            bootingDictionary.create_10_5_system_procedures(tc, newlyCreatedRoutines);
        }

        //
        // Change the return type of SYSIBM.CLOBGETSUBSTRING if necessary. See
        // DERBY-4214. That function was added in 10.3 and the return type was
        // changed (but not upgraded) in 10.5. We can't distinguish
        // between databases which were originally created by 10.5 and databases
        // which were upgraded to 10.5.
        //
        if (
            ( fromMajorVersionNumber > DataDictionary.DD_VERSION_DERBY_10_2) &&
            ( fromMajorVersionNumber < DataDictionary.DD_VERSION_DERBY_10_6)
            )
        {
            bootingDictionary.upgradeCLOBGETSUBSTRING_10_6( tc );
        }

        //
        // Remove the bad permissions tuple for SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE if necessary.
        // See DERBY-4215. That procedure will have an extra permissions tuple
        // with a null GRANTOR field if the database was created by 10.0 and then
        // hard-upgraded to 10.2 or higher without an intermediate upgrade to 10.1.
        //
        if (
            ( fromMajorVersionNumber > DataDictionary.DD_VERSION_DERBY_10_1) &&
            ( fromMajorVersionNumber < DataDictionary.DD_VERSION_DERBY_10_6)
            )
        {
            bootingDictionary.upgradeSYSROUTINEPERMS_10_6( tc );
        }
        
        if (fromMajorVersionNumber <= DataDictionary.DD_VERSION_DERBY_10_9)
        {
            // On ugrade from versions before 10.9, create system procedures
            // added in 10.9.
            bootingDictionary.create_10_9_system_procedures( tc, newlyCreatedRoutines );
            
            // On upgrade from versions before 10.9, create system catalogs
            // added in 10.9
            bootingDictionary.upgradeMakeCatalog(tc, DataDictionary.SYSUSERS_CATALOG_NUM );

            // On upgrade from versions before 10.9, upgrade the way we store
            // jars: we now use UUID as part of the file name and sanitize the
            // sql (schema, schema object) parts of the file name to remove
            // path delimiters. ALso, we now use no schema subdirectories since
            // there is no chance of name collision with the UUID.
            bootingDictionary.upgradeJarStorage(tc);
        }

        // Grant PUBLIC access to some system routines
        bootingDictionary.grantPublicAccessToSystemRoutines(newlyCreatedRoutines, tc, aid);
	}

	/**
	 * Do any work needed for a minor revision change.
	 * For the data dictionary this is always invalidating
	 * stored prepared statements.  When we are done 
	 * with the upgrade, we always recompile all SPSes
	 * so the customer doesn't have to (and isn't going
	 * to get deadlocks because of the recomp).
	 *
	 * @param tc the xact
	 *
	 * @exception StandardException  Standard Derby error policy.
	 */
	private void handleMinorRevisionChange(TransactionController tc, DD_Version fromVersion, boolean softUpgradeRun) 
		throws StandardException
	{
		boolean isReadOnly = bootingDictionary.af.isReadOnly();

		if (!isReadOnly) {
			// Once a database is version 10.5 we will start updating metadata SPSes
			// on any version change,up or down.  This will ensure that metadata queries 
			// match the version we are using.  We don't want to do this for lower 
			// database versions because on reverting to the previous version the 
			// SPSes won't be restored.
			if (fromVersion.majorVersionNumber >= DataDictionary.DD_VERSION_DERBY_10_5)
				bootingDictionary.updateMetadataSPSes(tc);
			//Following make sure that the stored plans (including the ones for
			//triggers) will get cleared during upgrade and hence we will not
			//hold on to stale plans.
			bootingDictionary.clearSPSPlans();

			if (softUpgradeRun)
			{
				// log a version that will cause a minor revision change
				// for any subsequent re-boot, including an old Cloudscape version
				fromVersion.minorVersionNumber = 1; // see getJBMSMinorVersionNumber
			}
			else
			{
				// log the new version

				// and change the in-memory version.
				fromVersion.majorVersionNumber = majorVersionNumber;
				fromVersion.minorVersionNumber = minorVersionNumber;
			}

			tc.setProperty(DataDictionary.CORE_DATA_DICTIONARY_VERSION, fromVersion, true);
		}
		else
		{
			// For a readonly database where we need some kind of upgrade
			// (either minor release or soft upgrade) then since we cannot
			// invalidate all the procedures we need to indicate that
			// any procedure we read off disk is automatically invalid,
			// so we do not try to load the generated class.
			bootingDictionary.setReadOnlyUpgrade();
		}

	}

	/**
	  Remove the description of a System table from the data dictionary.
	  This does not delete the conglomerates that hold the catalog or
	  its indexes.
	  @param	tc TransactionController
	  @param    td Table descriptor for the catalog to drop. 
	  @exception StandardException  Standard Derby error policy.
	  */
	protected void
	dropSystemCatalogDescription(TransactionController tc, TableDescriptor td)
		throws StandardException
	{
		/* Drop the columns */
		bootingDictionary.dropAllColumnDescriptors(td.getUUID(), tc);

		/* Drop the conglomerate descriptors */
		bootingDictionary.dropAllConglomerateDescriptors(td, tc);

		/* Drop table descriptor */
		bootingDictionary.dropTableDescriptor( td, td.getSchemaDescriptor(), tc );
	}

	/**
 	 * Drop a System catalog.
	 *	@param	tc	TransactionController
	 *  @param  crf CatalogRowFactory for the catalog to drop.
	 *	@exception StandardException  Standard Derby error policy.
	 */
	protected void dropSystemCatalog(TransactionController tc,
							 CatalogRowFactory crf)
		throws StandardException
	{
		SchemaDescriptor		sd = bootingDictionary.getSystemSchemaDescriptor();
		TableDescriptor			td = bootingDictionary.getTableDescriptor(
											crf.getCatalogName(),
											sd, tc);
		ConglomerateDescriptor[]	cds = td.getConglomerateDescriptors();
        for (ConglomerateDescriptor cd : cds) {
            tc.dropConglomerate(cd.getConglomerateNumber());
        }
		dropSystemCatalogDescription(tc,td);
	}


	/**
	 * Populates a new system index from the base system table.
	 *
	 *	@param	tc						transaction controller
	 *	@param	heapConglomerateNumber	identifies system table to Store
	 *	@param	tabInfo					describes base system table
	 *	@param	indexNumber				index to populate
	 *
	 *
	 * @exception StandardException		Thrown on failure
	 */
	protected	void	fillIndex
	(
		TransactionController	tc,
		long					heapConglomerateNumber,
		TabInfoImpl					tabInfo,
		int						indexNumber
    )
		throws StandardException
	{
		long						indexConglomerateNumber = tabInfo.getIndexConglomerate( indexNumber );
		IndexRowGenerator			indexRowGenerator = tabInfo.getIndexRowGenerator( indexNumber );
		CatalogRowFactory			rowFactory = tabInfo.getCatalogRowFactory();
		ExecRow						heapRow = rowFactory.makeEmptyRow();
		ExecIndexRow				indexableRow = indexRowGenerator.getIndexRowTemplate();

		ScanController				heapScan =
			tc.openScan(
				heapConglomerateNumber,       // conglomerate to open
				false,                          // don't hold open across commit
				0,                              // for read
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_REPEATABLE_READ,
				(FormatableBitSet) null,                 // all fields as objects
				null,                           // start position - first row
				ScanController.GE,              // startSearchOperation
				null,                           //scanQualifier,
				null,                           //stop position-through last row
				ScanController.GT);             // stopSearchOperation

		RowLocation					heapLocation = 
            heapScan.newRowLocationTemplate();

		ConglomerateController		indexController = 
			tc.openConglomerate( 
				indexConglomerateNumber, 
                false,
				TransactionController.OPENMODE_FORUPDATE,
				TransactionController.MODE_TABLE,
				TransactionController.ISOLATION_REPEATABLE_READ);

		while ( heapScan.fetchNext(heapRow.getRowArray()) )
        {
 			heapScan.fetchLocation( heapLocation );

			indexRowGenerator.getIndexRow( heapRow, heapLocation, indexableRow, (FormatableBitSet) null );

			indexController.insert(indexableRow);
		}

		indexController.close();
		heapScan.close();
	}

	////////////////////////////////////////////////////////////////////////
	//
	//	FORMATABLE INTERFACE
	//
	////////////////////////////////////////////////////////////////////////
	/**
	 * Get the formatID which corresponds to this class.
	   Map to the 5.0 version identifier so that 5.0 will understand
	   this object when we write it out in soft upgrade mode.
	   CS 5.0 will de-serialize it correctly.
	   When we are writing out a 5.1 version number we write out
	   the 5.1 version just to ensure no problems.
	   
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{
		return majorVersionNumber == DataDictionary.DD_VERSION_CS_5_1 ?
			StoredFormatIds.DD_ARWEN_VERSION_ID : StoredFormatIds.DD_DB2J72_VERSION_ID;
	}
	/**
	 * Read this object from a stream of stored objects. Set
	 * the minor version.  Ignore the major version.  
	 *
	 * @param in read this.
	 *
	 * @exception IOException on error
	 */
	public void readExternal( ObjectInput in ) throws IOException
	{
		majorVersionNumber = in.readInt();
		minorVersionNumber = in.readInt();
	}

	/**
	 * Write this object to a stream of stored objects. Write
	 * out the minor version which is bumped across minor release.
	 * Just to be safe, write out the major version too.  This
	 * will allow us to do versioning of a specific Version impl
	 * in the future.
	 *
	 * @param out write bytes here.
	 *
	 * @exception IOException on error
	 */
	public void writeExternal( ObjectOutput out ) throws IOException
	{ 
		out.writeInt(majorVersionNumber);
		out.writeInt(minorVersionNumber);
	}
	/**
	 * Get the minor version from the JBMS product minor version/maint version.
	 * Bumps it up by 1 if production, or 0 if beta to ensure
	 * minor upgrade across beta.  Starts at 2 because of an
	 * old convention. We use this starting at 2 to allow soft upgrade to
	 * write a version of 1 with the old major number to ensure a minor upgrade
	   when reverting to an old version afer a soft upgrade. E.g run with 5.0.2,
	   then 5.2.1.1, then 5.0.2. Want to ensure 5.0.2 does the minor upgrade.
	 *
	 * @return the minor version

		For 5.0 and 5.1 the minor number was calculated as

		jbmsVersion.getMinorVersion()*100 +jbmsVersion.getMaintVersion() + (jbmsVersion.isBeta() ? 0 : 1) + 2

		5.0.22 => (0*100) + 22 + 2 =  24 - (5.0 has a unique major number)
		5.1.2  => (1*100) + 2 + 2  = 104 - (5.1 has a unique major number) 


		With the switch to the four part scheme in 5.2, the maint number now is in increments of one million,
		thus the above scheme could lead to duplicate numbers. Note that the major number may not change
		when the minor external release changes, e.g. 5.2 and 5.3 could share a DD_Version major number.

		5.2.1.100 => (2*100) + 1000100 + 2 = 1000302
		5.3.1.0   => (3*100) + 1000000 + 2 = 1000302

		

	 */
	private int getJBMSMinorVersionNumber() 
	{
		ProductVersionHolder jbmsVersion = Monitor.getMonitor().getEngineVersion();

		return jbmsVersion.getMinorVersion()*100 +jbmsVersion.getMaintVersion() + (jbmsVersion.isBeta() ? 0 : 1) + 2;
	}
	
	/**
	 * 
	 * Modifies the nullability of the system table corresponding
	 * to the received catalog number.
	 * 
	 * @param tc			TransactionController.
	 * @param catalogNum	The catalog number corresponding
	 *  to the table for which we will modify the nullability.
	 *  
	 *  OLD Cloudscape 5.1 upgrade code
	 *  If this corresponds to SYSALIASES, then the nullability of
	 *  the SYSALIASES.ALIASINFO column will be changed to true
	 *  (Beetle 4430).  If this corresponds to SYSSTATEMENTS,
	 *  the nullability of the SYSSTATEMENTS.LASTCOMPILED
	 *  column will be changed to true.
	 *
	 *  Derby upgrade code
	 *  If this corresponds to SYSSTATEMENTS, then the nullability of
	 *  the SYSSTATEMENTS.COMPILATION_SCHEMAID column will 
	 *  be changed to true.  If this corresponds to SYSVIEWS, the nullability
	 *  of the SYSVIEWS.COMPILATION_SCHEMAID column will be changed to true.
	 *  
	 * @exception StandardException   Thrown on error
	 */
	private void modifySysTableNullability(TransactionController tc, int catalogNum)
		throws StandardException
	{		
		TabInfoImpl ti = bootingDictionary.getNonCoreTIByNumber(catalogNum);
		CatalogRowFactory rowFactory = ti.getCatalogRowFactory();
		
		if (catalogNum == DataDictionaryImpl.SYSSTATEMENTS_CATALOG_NUM)
		{
			// SYSSTATEMENTS table ==> SYSSTATEMENTS_COMPILATION_SCHEMAID needs 
			// to be modified.
			bootingDictionary.upgradeFixSystemColumnDefinition(rowFactory,
				SYSSTATEMENTSRowFactory.SYSSTATEMENTS_COMPILATION_SCHEMAID, 
				tc);
		}
		else if (catalogNum == DataDictionaryImpl.SYSVIEWS_CATALOG_NUM)
		{
			// SYSVIEWS table ==> SYSVIEWS_COMPILATION_SCHEMAID needs 
			// to be modified.
			bootingDictionary.upgradeFixSystemColumnDefinition(rowFactory,
				SYSVIEWSRowFactory.SYSVIEWS_COMPILATION_SCHEMAID, 
				tc);
		}
		
		/* OLD Cloudscape 5.1 upgrade code. See applySafeChanges(). 
		if (catalogNum == DataDictionaryImpl.SYSALIASES_CATALOG_NUM) {
		// SYSALIASES table ==> ALIASINFO needs to be modified.
			bootingDictionary.upgrade_setNullability(rowFactory,
				SYSALIASESRowFactory.SYSALIASES_ALIASINFO, true, tc);
		}
		else if (catalogNum == DataDictionaryImpl.SYSSTATEMENTS_CATALOG_NUM) {
		// SYSSTATEMENTS table ==> LASTCOMPILED needs to be modified.
			bootingDictionary.upgrade_setNullability(rowFactory,
				SYSSTATEMENTSRowFactory.SYSSTATEMENTS_LASTCOMPILED, true, tc);
		}
		*/		
		
	}

	/**
		Check to see if a database has been upgraded to the required
		level in order to use a language feature.

		@param requiredMajorVersion Data Dictionary major version
		@param feature Non-null to throw an error, null to return the state of the version match.

		@return True if the database has been upgraded to the required level, false otherwise.
	*/
	boolean checkVersion(int requiredMajorVersion, String feature) throws StandardException {

		if (majorVersionNumber < requiredMajorVersion) {

			if (feature != null)
				throw StandardException.newException(SQLState.LANG_STATEMENT_UPGRADE_REQUIRED, feature,
					DD_Version.majorToString(majorVersionNumber),
					DD_Version.majorToString(requiredMajorVersion));

			return false;
		}

		return true;
	}

}
