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

package com.splicemachine.derby.impl.sql.execute.actions;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.catalog.AliasInfo;
import com.splicemachine.db.catalog.DependableFinder;
import com.splicemachine.db.catalog.TypeDescriptor;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.catalog.types.AggregateAliasInfo;
import com.splicemachine.db.catalog.types.RoutineAliasInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.depend.Dependent;
import com.splicemachine.db.iapi.sql.depend.Provider;
import com.splicemachine.db.iapi.sql.depend.ProviderInfo;
import com.splicemachine.db.iapi.sql.dictionary.AliasDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ColPermsDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptorList;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.DefaultDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DependencyDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.PermissionsDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.RoleClosureIterator;
import com.splicemachine.db.iapi.sql.dictionary.RoleGrantDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.StatementColumnPermission;
import com.splicemachine.db.iapi.sql.dictionary.StatementGenericPermission;
import com.splicemachine.db.iapi.sql.dictionary.StatementPermission;
import com.splicemachine.db.iapi.sql.dictionary.StatementRolePermission;
import com.splicemachine.db.iapi.sql.dictionary.StatementRoutinePermission;
import com.splicemachine.db.iapi.sql.dictionary.StatementSchemaPermission;
import com.splicemachine.db.iapi.sql.dictionary.StatementTablePermission;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.impl.sql.execute.ColumnInfo;
import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.db.shared.common.sanity.SanityManager;
import com.splicemachine.derby.stream.iapi.ScopeNamed;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.stream.Stream;
import com.splicemachine.stream.StreamException;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * Abstract class that has actions that are across
 * all DDL actions.
 *
 */
public abstract class DDLConstantOperation implements ConstantAction, ScopeNamed {
	private static final Logger LOG = Logger.getLogger(DDLConstantOperation.class);

    /**
	 * Get the schema descriptor for the schemaid.
	 *
	 * @param dd the data dictionary
	 * @param schemaId the schema id
	 * @param statementType string describing type of statement for error
	 *	reporting.  e.g. "ALTER STATEMENT"
	 *
	 * @return the schema descriptor
	 *
	 * @exception StandardException if schema is system schema
	 */
	static SchemaDescriptor getAndCheckSchemaDescriptor(DataDictionary dd,UUID schemaId,
						String statementType) throws StandardException {
		SpliceLogUtils.trace(LOG, "getAndCheckSchemaDescriptor with UUID %s and statementType %s",schemaId, statementType);
		return dd.getSchemaDescriptor(schemaId, null);
	}

	/**
	 * Get the schema descriptor in the creation of an object in
	   the passed in schema.
	 *
	 * @param dd the data dictionary
	 * @param activation activation
	 * @param schemaName name of the schema
	 *
	 * @return the schema descriptor
	 *
	 * @exception StandardException if the schema does not exist
	 */
	public static SchemaDescriptor getSchemaDescriptorForCreate(DataDictionary dd,
						Activation activation, String schemaName) throws StandardException {
		SpliceLogUtils.trace(LOG, "getSchemaDescriptorForCreate %s",schemaName);
		TransactionController tc = activation.getLanguageConnectionContext().getTransactionExecute();
		SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, tc, false);
		if (sd == null || sd.getUUID() == null) {
			CreateSchemaConstantOperation csca = new CreateSchemaConstantOperation(schemaName, null);
			try {
				csca.executeConstantAction(activation);
			} catch (StandardException se) {
				if (!se.getMessageId().equals(SQLState.LANG_OBJECT_ALREADY_EXISTS))
					throw se;
			}
			sd = dd.getSchemaDescriptor(schemaName, tc, true);
		}
		return sd;
	}

	protected String constructToString(String statementType,String objectName) {
		return statementType + objectName;
	}
	
	
	/**
	 *	This method saves dependencies of constraints on privileges in the  
	 *  dependency system. It gets called by CreateConstraintConstantAction.
	 *  Views and triggers and constraints run with definer's privileges. If 
	 *  one of the required privileges is revoked from the definer, the 
	 *  dependent view/trigger/constraint on that privilege will be dropped 
	 *  automatically. In order to implement this behavior, we need to save 
	 *  view/trigger/constraint dependencies on required privileges in the 
	 *  dependency system. Following method accomplishes that part of the 
	 *  equation for constraints only. The dependency collection for 
	 *  constraints is not same as for views and triggers and hence 
	 *  constraints are handled by this special method.
	 *
	 * 	Views and triggers can depend on many different kind of privileges
	 *  where as constraints only depend on REFERENCES privilege on a table
	 *  (FOREIGN KEY constraints) or EXECUTE privileges on one or more
	 *  functions (CHECK constraints).
	 *
	 *  Another difference is only one view or trigger can be defined by a
	 *  sql statement and hence all the dependencies collected for the sql
	 *  statement apply to the view or trigger in question. As for constraints,
	 *  one sql statement can defined multiple constraints and hence the 
	 *  all the privileges required by the statement are not necessarily
	 *  required by all the constraints defined by that sql statement. We need
	 *  to identify right privileges for right constraints for a given sql
	 *  statement. Because of these differences between constraints and views
	 *  (and triggers), there are 2 different methods in this class to save
	 *  their privileges in the dependency system.
	 *
	 *  For each required privilege, we now register a dependency on a role if
	 *  that role was required to find an applicable privilege.
	 *   
	 *  @param activation The execution environment for this constant action.
	 *  @param dependent Make this object depend on required privileges
	 *  @param refTableUUID Make sure we are looking for REFERENCES privilege 
	 * 		for right table
	 *  @param providers set of providers for this constraint
	 * @exception StandardException		Thrown on failure
	 */
	protected void storeConstraintDependenciesOnPrivileges(Activation activation,
		Dependent dependent,UUID refTableUUID, ProviderInfo[] providers) throws StandardException {
		SpliceLogUtils.trace(LOG, "storeConstraintDependenciesOnPrivileges %s",dependent);
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
        String currentUser = lcc.getCurrentUserId(activation);
		SettableBoolean roleDepAdded = new SettableBoolean();
        if (! currentUser.equals( dd.getAuthorizationDatabaseOwner()) ) {
			PermissionsDescriptor permDesc;
			// Now, it is time to add into dependency system the FOREIGN
			// constraint's dependency on REFERENCES privilege, or, if it is a
			// CHECK constraint, any EXECUTE or USAGE privileges. If the REFERENCES is
			// revoked from the constraint owner, the constraint will get
			// dropped automatically.
			List requiredPermissionsList = activation.getPreparedStatement().getRequiredPermissionsList();
			if (requiredPermissionsList != null && ! requiredPermissionsList.isEmpty()) {
				for(Iterator iter = requiredPermissionsList.iterator();iter.hasNext();) {
					StatementPermission statPerm = (StatementPermission) iter.next();
					//First check if we are dealing with a Table or 
					//Column level privilege. All the other privileges
					//are not required for a foreign key constraint.
					if (statPerm instanceof StatementTablePermission) {//It is a table/column level privilege
						StatementTablePermission statementTablePermission = 
							(StatementTablePermission) statPerm;
						//Check if we are dealing with REFERENCES privilege.
						//If not, move on to the next privilege in the
						//required privileges list
						if (statementTablePermission.getPrivType() != Authorizer.REFERENCES_PRIV)
							continue;
						//Next check is this REFERENCES privilege is 
						//on the same table as referenced by the foreign
						//key constraint? If not, move on to the next
						//privilege in the required privileges list
						if (!statementTablePermission.getTableUUID().equals(refTableUUID))
							continue;
					} else if (statPerm instanceof StatementSchemaPermission
						    || statPerm instanceof StatementRolePermission
                               || statPerm instanceof StatementGenericPermission ) {
						continue;
					} else {
						if (SanityManager.DEBUG) {
							SanityManager.ASSERT(
								statPerm instanceof StatementRoutinePermission,
								"only StatementRoutinePermission expected");
						}

						// skip if this permission concerns a function not
						// referenced by this constraint
						StatementRoutinePermission rp = (StatementRoutinePermission)statPerm;
						if (!inProviderSet(providers, rp.getRoutineUUID())) {
							continue;
						}
					}


					// We know that we are working with a REFERENCES, EXECUTE, or USAGE
					// privilege. Find all the PermissionDescriptors for this
					// privilege and make constraint depend on it through
					// dependency manager.  The REFERENCES privilege could be
					// defined at the table level or it could be defined at
					// individual column levels. In addition, individual column
					// REFERENCES privilege could be available at the user
					// level, PUBLIC or role level.  EXECUTE and USAGE privileges could be
					// available at the user level, PUBLIC or role level.
                    permDesc = statPerm.getPermissionDescriptor(currentUser, dd);

					if (permDesc == null)  {
						// No privilege exists for given user. The privilege
						// has to exist at at PUBLIC level....

						permDesc = statPerm.getPermissionDescriptor(Authorizer.PUBLIC_AUTHORIZATION_ID, dd);
						// .... or at the role level. Additionally, for column
						// level privileges, even if *some* were available at
						// the PUBLIC level others may be still be missing,
						// hence the call in the test below to
						// allColumnsCoveredByUserOrPUBLIC.
						boolean roleUsed = false;

						if (permDesc == null ||
							((permDesc instanceof ColPermsDescriptor) &&
                                 ! ((StatementColumnPermission)statPerm).
                                   allColumnsCoveredByUserOrPUBLIC(
                                       currentUser, dd))) {
							roleUsed = true;
							permDesc = findRoleUsage(activation, statPerm);
						}

						// If the user accessing the object is the owner of
						// that object, then no privilege tracking is needed
						// for the owner.
                        if (! permDesc.checkOwner(currentUser) ) {
                            dm.addDependency(dependent, permDesc,lcc.getContextManager());

							if (roleUsed) {
								// We had to rely on role, so track that
								// dependency, too.
								trackRoleDependency(activation, dependent, roleDepAdded);
							}
						}
					} else
						//if the object on which permission is required is owned by the
						//same user as the current user, then no need to keep that
						//object's privilege dependency in the dependency system
                    if (! permDesc.checkOwner(currentUser)) {
						dm.addDependency(dependent, permDesc, lcc.getContextManager());
						if (permDesc instanceof ColPermsDescriptor) {
							// The if statement above means we found a
							// REFERENCES privilege at column level for the
							// given authorizer. If this privilege doesn't
							// cover all the column , then there has to exisit
							// REFERENCES for the remaining columns at PUBLIC
							// level or at role level.  Get that permission
							// descriptor and save it in dependency system
							StatementColumnPermission
								statementColumnPermission = (
									StatementColumnPermission)statPerm;
							permDesc = statementColumnPermission.
                                getPUBLIClevelColPermsDescriptor(
                                    currentUser, dd);
							//Following if checks if some column level privileges
							//exist only at public level. If so, then the public
							//level column privilege dependency is added
							//into the dependency system
							if (permDesc != null &&
									permDesc.getObjectID() != null) {
								// User did not have all required column
								// permissions and at least one column is
								// covered by PUBLIC.
								dm.addDependency(dependent, permDesc,
												 lcc.getContextManager());
							}
							// Possibly, the current role has also been relied
							// upon.
							if (!statementColumnPermission.
                                    allColumnsCoveredByUserOrPUBLIC(
                                        currentUser, dd)) {
								// Role has been relied upon, so register a
								// dependency.
								trackRoleDependency
									(activation, dependent, roleDepAdded);
							}
						}
					}

					if (!(statPerm instanceof StatementRoutinePermission)) {
						//We have found the REFERENCES privilege for all the
						//columns in foreign key constraint and we don't
						//need to go through the rest of the privileges
						//for this sql statement.
						break;
					} else {
						// For EXECUTE privilege there may be several functions
						// referenced in the constraint, so continue looking.
					}
				}
			}
		}
		
	}	


	/**
	 * We have determined that the statement permission described by statPerm
	 * is not granted to the current user nor to PUBLIC, so it must be granted
	 * to the current role or one of the roles inherited by the current
	 * role. Find the relevant permission descriptor and return it.
	 *
	 * @return the permission descriptor that yielded the privilege
	 */
	private static PermissionsDescriptor findRoleUsage(Activation activation,
		 StatementPermission statPerm) throws StandardException {
		SpliceLogUtils.trace(LOG, "findRoleUsage %s",statPerm);
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		String role = lcc.getCurrentRoleId(activation);
		PermissionsDescriptor permDesc = null;
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(
				role != null,
				"Unexpected: current role is not set");
		}

		// determine how we got to be able use this role
//		rootGrant = dd.getRoleGrantDescriptor(role, currentUser, dbo);

//		if (rootGrant == null) {
//			rootGrant = dd.getRoleGrantDescriptor(
//				role,
//				Authorizer.PUBLIC_AUTHORIZATION_ID,
//				dbo);
//		}

		// If not found in current role, get transitive
		// closure of roles granted to current role and
		// iterate over it to see if permission has
		// been granted to any of the roles the current
		// role inherits.
		RoleClosureIterator rci = dd.createRoleClosureIterator
			(activation.getTransactionController(),role, true /* inverse relation*/);
		String graphGrant;
		while (permDesc == null &&
			   (graphGrant = rci.next()) != null) {
			permDesc = statPerm.getPermissionDescriptor(graphGrant, dd);
		}

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(
				permDesc != null,
				"Unexpected: Permission needs to be found via role");
		}
		return permDesc;
	}


	/**
	 * The statement permission needed for dependent has been found to rely on
	 * the current role. If not already done, register the dependency so that
	 * if the current role (or any of the roles it inherits) is revoked (or
	 * dropped), we can invalidate dependent.
	 *
	 * @param activation the current activation
	 * @param dependent the view, constraint or trigger that is dependent on the
	 *        current role for some privilege.
	 * @param roleDepAdded keeps track of whether a dependeny on the
	 *        current role has aleady been registered.
	 */
	private static void trackRoleDependency(Activation activation, Dependent dependent,
			SettableBoolean roleDepAdded) throws StandardException {
		SpliceLogUtils.trace(LOG, "trackRoleDependency with dependent %s and roleDepAdded %s",dependent,roleDepAdded);
		// We only register the dependency once, lest
		// we get duplicates in SYSDEPENDS (duplicates
		// are not healthy..invalidating more than once
		// fails for triggers at least).
		if (!roleDepAdded.get()) {
			LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
			DataDictionary dd = lcc.getDataDictionary();
			DependencyManager dm = dd.getDependencyManager();
			String role = lcc.getCurrentRoleId(activation);
			RoleGrantDescriptor rgd = dd.getRoleDefinitionDescriptor(role);
			dm.addDependency (dependent, rgd,lcc.getContextManager());
			roleDepAdded.set(true);
		}
	}

	/**
	 *	This method saves dependencies of views and triggers on privileges in  
	 *  the dependency system. It gets called by CreateViewConstantAction
	 *  and CreateTriggerConstantAction. Views and triggers and constraints
	 *  run with definer's privileges. If one of the required privileges is
	 *  revoked from the definer, the dependent view/trigger/constraint on
	 *  that privilege will be dropped automatically. In order to implement 
	 *  this behavior, we need to save view/trigger/constraint dependencies 
	 *  on required privileges in the dependency system. Following method 
	 *  accomplishes that part of the equation for views and triggers. The
	 *  dependency collection for constraints is not same as for views and
	 *  triggers and hence constraints are not covered by this method.
	 *  Views and triggers can depend on many different kind of privileges
	 *  where as constraints only depend on REFERENCES privilege on a table.
	 *  Another difference is only one view or trigger can be defined by a
	 *  sql statement and hence all the dependencies collected for the sql
	 *  statement apply to the view or trigger in question. As for constraints,
	 *  one sql statement can defined multiple constraints and hence the 
	 *  all the privileges required by the statement are not necessarily
	 *  required by all the constraints defined by that sql statement. We need
	 *  to identify right privileges for right constraints for a given sql
	 *  statement. Because of these differences between constraints and views
	 *  (and triggers), there are 2 different methods in this class to save
	 *  their privileges in the dependency system.
	 *
	 *  For each required privilege, we now register of a dependency on a role
	 *  if that role was required to find an applicable privilege.
	 *
	 *  @param activation The execution environment for this constant action.
	 *  @param dependent Make this object depend on required privileges
	 *
	 * @exception StandardException		Thrown on failure
	 */
	protected void storeViewTriggerDependenciesOnPrivileges( Activation activation, Dependent dependent)
			throws StandardException {
		SpliceLogUtils.trace(LOG, "storeViewTriggerDependenciesOnPrivileges with dependent %s",dependent);
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
		String dbo = dd.getAuthorizationDatabaseOwner();
        String currentUser = lcc.getCurrentUserId(activation);
		SettableBoolean roleDepAdded = new SettableBoolean();

		// If the Database Owner is creating this view/trigger, then no need to
		// collect any privilege dependencies because the Database Owner can
		// access any objects without any restrictions.
        if (! currentUser.equals(dbo)) {
			PermissionsDescriptor permDesc;
			List requiredPermissionsList = activation.getPreparedStatement().getRequiredPermissionsList();
			if (requiredPermissionsList != null && ! requiredPermissionsList.isEmpty()) {
				for(Iterator iter = requiredPermissionsList.iterator();iter.hasNext();) {
					StatementPermission statPerm = (StatementPermission) iter.next();
					//The schema ownership permission just needs to be checked 
					//at object creation time, to see if the object creator has 
					//permissions to create the object in the specified schema. 
					//But we don't need to add schema permission to list of 
					//permissions that the object is dependent on once it is 
					//created.
					//Also, StatementRolePermission should not occur here.
					if (statPerm instanceof StatementSchemaPermission ||
						statPerm instanceof StatementRolePermission) {

						if (SanityManager.DEBUG) {
							if (statPerm instanceof StatementRolePermission) {
								SanityManager.THROWASSERT(
									"Unexpected StatementRolePermission");
							}
						}

						continue;
					}

					//See if we can find the required privilege for given authorizer?
                    permDesc = statPerm.
                        getPermissionDescriptor(currentUser, dd);
					if (permDesc == null)//privilege not found for given authorizer 
					{
						//The if condition above means that required privilege does 
						//not exist at the user level. The privilege has to exist at 
						//PUBLIC level... ,
						permDesc = statPerm.getPermissionDescriptor(
							Authorizer.PUBLIC_AUTHORIZATION_ID, dd);

						boolean roleUsed = false;

						// .. or at role level
						if (permDesc == null ||
								((permDesc instanceof ColPermsDescriptor) &&
                                 ! ((StatementColumnPermission)statPerm).
                                     allColumnsCoveredByUserOrPUBLIC(
                                         currentUser, dd)) ) {
							roleUsed = true;
							permDesc = findRoleUsage(activation, statPerm);
						}

						//If the user accessing the object is the owner of that 
						//object, then no privilege tracking is needed for the
						//owner.
                        if (! permDesc.checkOwner(currentUser) ) {

							dm.addDependency(dependent, permDesc, lcc.getContextManager());

							// We had to rely on role, so track that
							// dependency, too.
							if (roleUsed) {
								trackRoleDependency
									(activation, dependent, roleDepAdded);
							}
						}
						continue;
					}
					//if the object on which permission is required is owned by the
					//same user as the current user, then no need to keep that
					//object's privilege dependency in the dependency system
                    if (! permDesc.checkOwner(currentUser) )
					{
						dm.addDependency(dependent, permDesc, lcc.getContextManager());	           							
						if (permDesc instanceof ColPermsDescriptor)
						{
							//For a given table, the table owner can give privileges
							//on some columns at individual user level and privileges
							//on some columns at PUBLIC level. Hence, when looking for
							//column level privileges, we need to look both at user
							//level as well as PUBLIC level(only if user level column
							//privileges do not cover all the columns accessed by this
							//object). We have finished adding dependency for user level 
							//columns, now we are checking if some required column 
							//level privileges are at PUBLIC level.
							//A specific eg of a view
							//user1
							//create table t11(c11 int, c12 int);
							//grant select(c11) on t1 to user2;
							//grant select(c12) on t1 to PUBLIC;
							//user2
							//create view v1 as select c11 from user1.t11 where c12=2;
							//For the view above, there are 2 column level privilege 
							//depencies, one for column c11 which exists directly
							//for user2 and one for column c12 which exists at PUBLIC level.
							StatementColumnPermission statementColumnPermission = (StatementColumnPermission) statPerm;
                            permDesc = statementColumnPermission.
                                getPUBLIClevelColPermsDescriptor(
                                    currentUser, dd);
							//Following if checks if some column level privileges
							//exist only at public level. If so, then the public
							//level column privilege, if any, dependency of
							//view is added into dependency system.

							if (permDesc != null &&
									permDesc.getObjectID() != null) {
								// User did not have all required column
								// permissions and at least one column is
								// covered by PUBLIC.
								dm.addDependency(dependent, permDesc,
												 lcc.getContextManager());
							} // else nothing found for PUBLIC..

							// Has the the current role has also been relied
							// upon?
							if (!statementColumnPermission.
                                    allColumnsCoveredByUserOrPUBLIC(
                                        currentUser, dd)) {
								trackRoleDependency
									(activation, dependent, roleDepAdded);
							}
						}
					}
				}
			}
		}
	}

	private boolean inProviderSet(ProviderInfo[] providers, UUID routineId) {
		if (providers == null) {
			return false;
		}

		for (int i = 0; i < providers.length; i++) {
			if (providers[i].getObjectId().equals(routineId)) {
				return true;
			}
		}
		return false;
	}

    /**
     * Add dependencies of a column on providers. These can arise if a generated column depends
     * on a user created function.
     */
    protected   void    addColumnDependencies
        (
         LanguageConnectionContext  lcc,
         DataDictionary     dd,
         TableDescriptor    td,
         ColumnInfo ci
         )
        throws StandardException
    {
        ProviderInfo[]  providers = ci.providers;

        if ( providers != null )
        {
            DependencyManager   dm = dd.getDependencyManager();
            ContextManager cm = lcc.getContextManager();
            int                         providerCount = providers.length;
            ColumnDescriptor    cd = td.getColumnDescriptor( ci.name );
            DefaultDescriptor   defDesc = cd.getDefaultDescriptor( dd );

            for ( int px = 0; px < providerCount; px++ )
            {
                ProviderInfo            pi = providers[ px ];
                DependableFinder finder = pi.getDependableFinder();
                UUID                        providerID = pi.getObjectId();
                Provider provider = (Provider) finder.getDependable( dd, providerID );

                dm.addDependency( defDesc, provider, cm );
            }   // end loop through providers
        }
    }

    /**
     * Adjust dependencies of a table on ANSI UDTs. We only add one dependency
     * between a table and a UDT. If the table already depends on the UDT, we don't add
     * a redundant dependency.
     */
    protected   void    adjustUDTDependencies
        (
         Activation					activation,
         ColumnInfo[]               columnInfos,
         boolean                    dropWholeTable
         )
        throws StandardException
    {
        if ( (!dropWholeTable) && (columnInfos == null) ) { return; }

		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		TransactionController tc = lcc.getTransactionExecute();
		DataDictionary dd = lcc.getDataDictionary();
		TableDescriptor td = activation.getDDLTableDescriptor();

        int changedColumnCount = columnInfos == null ? 0 : columnInfos.length;
        HashMap addUdtMap = new HashMap();
        HashMap dropUdtMap = new HashMap();
        HashSet addColumnNames = new HashSet();
        HashSet dropColumnNames = new HashSet();

        // first find all of the new ansi udts which the table must depend on
        // and the old ones which are candidates for removal
        for ( int i = 0; i < changedColumnCount; i++ )
        {
            ColumnInfo ci = columnInfos[ i ];

            // skip this column if it is not a UDT
            AliasDescriptor ad = dd.getAliasDescriptorForUDT( tc, columnInfos[ i ].dataType );
            if ( ad == null ) { continue; }

            String key = ad.getObjectID().toString();

            if ( ci.action == ColumnInfo.CREATE )
            {
                addColumnNames.add( ci.name);

                // no need to add the descriptor if it is already on the list
                if ( addUdtMap.get( key ) != null ) { continue; }

                addUdtMap.put( key, ad );
            }
            else if ( ci.action == ColumnInfo.DROP )
            {
                dropColumnNames.add( ci.name );
                dropUdtMap.put( key, ad );
            }
        }

        // nothing to do if there are no changed columns of udt type
        // and this is not a DROP TABLE command
        if ( (!dropWholeTable) && (addUdtMap.size() == 0) && (dropUdtMap.size() == 0) ) { return; }

        //
        // Now prune from the add list all udt descriptors for which we already have dependencies.
        // These are the udts for old columns. This supports the ALTER TABLE ADD COLUMN
        // case.
        //
        // Also prune from the drop list add udt descriptors which will still be
        // referenced by the remaining columns.
        //
        ColumnDescriptorList cdl = td.getColumnDescriptorList();
        int totalColumnCount = cdl.size();

        for ( int i = 0; i < totalColumnCount; i++ )
        {
            ColumnDescriptor cd = cdl.elementAt( i );

            // skip columns that are being added and dropped. we only want the untouched columns
            if (
                addColumnNames.contains( cd.getColumnName() ) ||
                dropColumnNames.contains( cd.getColumnName() )
                ) { continue; }

            // nothing to do if the old column isn't a UDT
            AliasDescriptor ad = dd.getAliasDescriptorForUDT( tc, cd.getType() );
            if ( ad == null ) { continue; }

            String key = ad.getObjectID().toString();

            // ha, it is a UDT.
            if ( dropWholeTable ) { dropUdtMap.put( key, ad ); }
            else
            {
                if ( addUdtMap.get( key ) != null ) { addUdtMap.remove( key ); }
                if ( dropUdtMap.get( key ) != null ) { dropUdtMap.remove( key ); }
            }
        }

        adjustUDTDependencies( lcc, dd, td, addUdtMap, dropUdtMap );
    }
    /**
     * Add and drop dependencies of an object on UDTs.
     *
     * @param lcc Interpreter's state variable for this session.
     * @param dd Metadata
     * @param dependent Object which depends on UDT
     * @param addUdtMap Map of UDTs for which dependencies should be added
     * @param dropUdtMap Map of UDT for which dependencies should be dropped
     */
    private   void    adjustUDTDependencies
        (
         LanguageConnectionContext  lcc,
         DataDictionary             dd,
         Dependent                  dependent,
         HashMap                    addUdtMap,
         HashMap                    dropUdtMap
         )
        throws StandardException
    {
        // again, nothing to do if there are no columns of udt type
        if ( (addUdtMap.size() == 0) && (dropUdtMap.size() == 0) ) { return; }

		TransactionController tc = lcc.getTransactionExecute();
        DependencyManager     dm = dd.getDependencyManager();
        ContextManager        cm = lcc.getContextManager();

        // add new dependencies
        Iterator            addIterator = addUdtMap.values().iterator();
        while( addIterator.hasNext() )
        {
            AliasDescriptor ad = (AliasDescriptor) addIterator.next();

            dm.addDependency( dependent, ad, cm );
        }

        // drop dependencies that are orphaned
        Iterator            dropIterator = dropUdtMap.values().iterator();
        while( dropIterator.hasNext() )
        {
            AliasDescriptor ad = (AliasDescriptor) dropIterator.next();

            DependencyDescriptor dependency = new DependencyDescriptor( dependent, ad );

            dd.dropStoredDependency( dependency, tc );
        }
    }

    /**
     * Add and drop dependencies of a routine on UDTs.
     *
     * @param lcc Interpreter's state variable for this session.
     * @param dd Metadata
     * @param ad Alias descriptor for the routine
     * @param adding True if we are adding dependencies, false if we're dropping them
     */
    protected   void    adjustUDTDependencies
        (
         LanguageConnectionContext  lcc,
         DataDictionary             dd,
         AliasDescriptor            ad,
         boolean                    adding
         )
        throws StandardException
    {
        RoutineAliasInfo routineInfo = null;
        AggregateAliasInfo aggInfo = null;

        // nothing to do if this is not a routine
        switch ( ad.getAliasType() )
        {
        case AliasInfo.ALIAS_TYPE_AGGREGATE_AS_CHAR:
            aggInfo = (AggregateAliasInfo) ad.getAliasInfo();
            break;

		case AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR:
		case AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR:
            routineInfo = (RoutineAliasInfo) ad.getAliasInfo();
            break;

        default: return;
        }
        
		TransactionController tc = lcc.getTransactionExecute();
        HashMap               addUdtMap = new HashMap();
        HashMap               dropUdtMap = new HashMap();
        HashMap               udtMap = adding ? addUdtMap : dropUdtMap;
        TypeDescriptor rawReturnType = aggInfo!= null ? aggInfo.getReturnType() : routineInfo.getReturnType();

        if ( rawReturnType != null )
        {
            AliasDescriptor       returnTypeAD = dd.getAliasDescriptorForUDT
                ( tc, DataTypeDescriptor.getType(rawReturnType) );

            if ( returnTypeAD != null ) { udtMap.put( returnTypeAD.getObjectID().toString(), returnTypeAD ); }
        }

        // table functions can have udt columns. track those dependencies.
        if ( (rawReturnType != null) && rawReturnType.isRowMultiSet() )
        {
            TypeDescriptor[] columnTypes = rawReturnType.getRowTypes();
            int columnCount = columnTypes.length;

            for ( int i = 0; i < columnCount; i++ )
            {
                AliasDescriptor       columnTypeAD = dd.getAliasDescriptorForUDT
                    ( tc, DataTypeDescriptor.getType( columnTypes[ i ] ) );

                if ( columnTypeAD != null ) { udtMap.put( columnTypeAD.getObjectID().toString(), columnTypeAD ); }
            }
        }

        TypeDescriptor[]      paramTypes = aggInfo != null ?
                new TypeDescriptor[] { aggInfo.getForType() } : routineInfo.getParameterTypes();

        if ( paramTypes != null )
        {
            int paramCount = paramTypes.length;
            for ( int i = 0; i < paramCount; i++ )
            {
                AliasDescriptor       paramType = dd.getAliasDescriptorForUDT
                    ( tc, DataTypeDescriptor.getType( paramTypes[ i ] ) );

                if ( paramType != null ) { udtMap.put( paramType.getObjectID().toString(), paramType ); }
            }
        }

        adjustUDTDependencies( lcc, dd, ad, addUdtMap, dropUdtMap );
    }
    
	/**
	 * Mutable Boolean wrapper, initially false
	 */
	private static class SettableBoolean {
		boolean value;

		SettableBoolean() {
			value = false;
		}

		void set(boolean b) {
			value = b;
		}

		boolean get() {
			return value;
		}
	}

	/**
	 * @return list of tables affected by this DDL operation that have to be forbidden to write by concurrent transactions. 
	 */
    protected List<String> getBlockedTables() {
        return Collections.emptyList();
    }

    /**
     * Waits for concurrent transactions that started before the tentative
     * change completed.
     *
     * Performs an exponential backoff until a configurable timeout triggers,
     * then returns the list of transactions still running. The caller has to
     * forbid those transactions to ever write to the tables subject to the DDL
     * change.
     *
     * @param maximum
     *            wait for all transactions started before this one. It should
     *            be the transaction created just after the tentative change
     *            committed.
     * @param userTxn the <em>user-level</em> transaction of the ddl operation. It is important
     *                that it be the user-level, otherwise some child transactions may be treated
     *                as active when they are not actually active.
     * @return list of transactions still running after timeout
     * @throws IOException
     */
    public long waitForConcurrentTransactions(Txn maximum, TxnView userTxn,long tableConglomId) throws IOException {
        if (!waitsForConcurrentTransactions()) {
            return -1l;
        }
        byte[] conglomBytes = Bytes.toBytes(Long.toString(tableConglomId));

        ActiveTransactionReader transactionReader = new ActiveTransactionReader(0l,maximum.getTxnId(),conglomBytes);
		SConfiguration config =EngineDriver.driver().getConfiguration();
        long waitTime = config.getDdlDrainingInitialWait();
        long maxWait = config.getDdlDrainingMaximumWait();
        long scale = 2; //the scale factor for the exponential backoff
        long timeAvailable = maxWait;
        long activeTxnId = -1l;
        do{
            try(Stream<TxnView> activeTxns = transactionReader.getActiveTransactions()){
                TxnView txn;
                while((txn = activeTxns.next())!=null){
                    if(!txn.descendsFrom(userTxn)){
                        activeTxnId = txn.getTxnId();
                    }
                }
            } catch (StreamException e) {
                throw new IOException(e.getCause());
            }
            if(activeTxnId<0) return activeTxnId;
            /*
             * It is possible for a sleep to pick up before the
             * waitTime is expired. Therefore, we measure that actual
             * time spent and use that for our time remaining period
             * instead.
             */
            long start = System.currentTimeMillis();
            try {
                Thread.sleep(waitTime);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
            long stop = System.currentTimeMillis();
            timeAvailable-=(stop-start);
            /*
             * We want to exponentially back off, but only to the limit imposed on us. Once
             * our backoff exceeds that limit, we want to just defer to that limit directly.
             */
            waitTime = Math.min(timeAvailable,scale*waitTime);
        } while(timeAvailable>0);

        if (activeTxnId>=0) {
            LOG.warn(String.format("Running DDL statement %s. There are transaction still active: %d", toString(), activeTxnId));
        }
        return activeTxnId;
    }

	/**
	 * Declares whether this DDL operation has to wait for the draining of concurrent transactions or not
	 * @return true if it has to wait for the draining of concurrent transactions
	 */
    protected boolean waitsForConcurrentTransactions() {
        return false;
    }

	public String getScopeName() {
		return StringUtils.join(
			StringUtils.splitByCharacterTypeCamelCase(
				this.getClass().getSimpleName().
					replace("ConstantOperation", "")), ' ');
	}
}

