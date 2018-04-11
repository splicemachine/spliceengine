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
 * All such Splice Machine modifications are Copyright 2012 - 2018 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.sql.dictionary;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.execute.ExecPreparedStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * This class describes a column permission used (required) by a statement.
 */

public class StatementColumnPermission extends StatementTablePermission
{
	private FormatableBitSet columns;

	/**
	 * Constructor for StatementColumnPermission. Creates an instance of column permission requested
	 * for the given access.
	 * 
	 * @param tableUUID	UUID of the table
	 * @param privType	Access privilege requested
	 * @param columns	List of columns
	 *
	 */
	public StatementColumnPermission(UUID schemaUUID, UUID tableUUID, int privType, FormatableBitSet columns)
	{
		super(schemaUUID, tableUUID, privType);
		this.columns = columns;
	}

	/**
	 * Return list of columns that need access
	 *
	 * @return	FormatableBitSet of columns
	 */
	public FormatableBitSet getColumns()
	{
		return columns;
	}

	/**
	 * Method to check if another instance of column access descriptor matches this.
	 * Used to ensure only one access descriptor for a table/columns of given privilege is created.
	 *
	 * @param obj	Another instance of StatementPermission
	 *
	 * @return	true if match
	 */
	public boolean equals( Object obj)
	{
		if( obj instanceof StatementColumnPermission)
		{
			StatementColumnPermission other = (StatementColumnPermission) obj;
			if( ! columns.equals( other.columns))
				return false;
			return super.equals( obj);
		}
		return false;
	}
	
	/**
	 * @see StatementPermission#check
	 */
	public void check( LanguageConnectionContext lcc,
					   boolean forGrant,
					   Activation activation)
		throws StandardException
	{
		DataDictionary dd = lcc.getDataDictionary();
		ExecPreparedStatement ps = activation.getPreparedStatement();

        if (hasPermissionOnTable(lcc, activation, forGrant, ps)) {
			return;
		}

        String currentUserId = lcc.getCurrentUserId(activation);
		String currentGroupuser = lcc.getCurrentGroupUser(activation);

			FormatableBitSet permittedColumns = null;
		if( ! forGrant)
		{
			permittedColumns = addPermittedColumns( dd,
													false /* non-grantable permissions */,
													Authorizer.PUBLIC_AUTHORIZATION_ID,
													permittedColumns);
			permittedColumns = addPermittedColumns( dd,
													false /* non-grantable permissions */,
                                                    currentUserId,
													permittedColumns);
			if (currentGroupuser != null) {
				permittedColumns = addPermittedColumns( dd,
						false /* non-grantable permissions */,
						currentGroupuser,
						permittedColumns);
			}
		}
		permittedColumns = addPermittedColumns( dd,
												true /* grantable permissions */,
												Authorizer.PUBLIC_AUTHORIZATION_ID,
												permittedColumns);
		permittedColumns = addPermittedColumns( dd,
												true /* grantable permissions */,
                                                currentUserId,
												permittedColumns);
		if (currentGroupuser != null) {
			permittedColumns = addPermittedColumns( dd,
					true /* grantable permissions */,
					currentGroupuser,
					permittedColumns);
		}
		
		//DERBY-4191
		//If we are looking for select privilege on ANY column,
		//then we can quit as soon as we find some column with select
		//privilege. This is needed for queries like
		//select count(*) from t1
		//select count(1) from t1
		//select 1 from t1
		//select t1.c1 from t1, t2
		if (privType == Authorizer.MIN_SELECT_PRIV && permittedColumns != null)
			return;

		FormatableBitSet unresolvedColumns = (FormatableBitSet)columns.clone();

		for (int i = unresolvedColumns.anySetBit();
			 i >= 0;
			 i = unresolvedColumns.anySetBit(i)) {

			if (permittedColumns != null && permittedColumns.get(i)) {
				// column i (zero-based here) accounted for:
				unresolvedColumns.clear(i);
			}
		}

		if (unresolvedColumns.anySetBit() < 0) {
			// all ok
			return;
		}

		// If columns are still unauthorized, look to role closure for
		// resolution.
		List<String> currentRoles = lcc.getCurrentRoles(activation);
		List<String> rolesToRemove = new ArrayList<>();
		String lastRole = null;
		for (String role : currentRoles) {
			// Check that role is still granted to current user or
			// to PUBLIC: A revoked role which is current for this
			// session, is lazily set to none when it is attempted
			// used.
			String dbo = dd.getAuthorizationDatabaseOwner();
			RoleGrantDescriptor rd = dd.getRoleGrantDescriptor(role, currentUserId, dbo);

			if (rd == null) {
				rd = dd.getRoleGrantDescriptor
					(role,
					 Authorizer.PUBLIC_AUTHORIZATION_ID,
					 dbo);
			}
			if (rd == null && currentGroupuser != null) {
				rd = dd.getRoleGrantDescriptor(role, currentGroupuser, dbo);
			}

			if (rd == null) {
				// we have lost the right to set this role, so we can't
				// make use of any permission granted to it or its ancestors.
				rolesToRemove.add(role);
			} else {
				lastRole = role;
				// The current role is OK, so we can make use of
				// any permission granted to it.
				//
				// Look at the current role and, if necessary, the transitive
				// closure of roles granted to current role to see if
				// permission has been granted to any of the applicable roles.

				RoleClosureIterator rci =
					dd.createRoleClosureIterator
					(activation.getTransactionController(),
					 role, true /* inverse relation*/);

				String r;

				while (unresolvedColumns.anySetBit() >= 0 &&
					   (r = rci.next()) != null ) {
					//The user does not have needed privilege directly 
					//granted to it, so let's see if he has that privilege
					//available to him/her through his roles.
					permittedColumns = tryRole(lcc, dd,	forGrant, r);
					//DERBY-4191
					//If we are looking for select privilege on ANY column,
					//then we can quit as soon as we find some column with select
					//privilege through this role. This is needed for queries like
					//select count(*) from t1
					//select count(1) from t1
					//select 1 from t1
					//select t1.c1 from t1, t2
					if (privType == Authorizer.MIN_SELECT_PRIV && permittedColumns != null) {
						DependencyManager dm = dd.getDependencyManager();
						RoleGrantDescriptor rgd =
							dd.getRoleDefinitionDescriptor(role);
						ContextManager cm = lcc.getContextManager();

						dm.addDependency(ps, rgd, cm);
						dm.addDependency(activation, rgd, cm);
						return;
					}

					//Use the privileges obtained through the role to satisfy
					//the column level privileges we need. If all the remaining
					//column level privileges are satisfied through this role,
					//we will quit out of this while loop
					for(int i = unresolvedColumns.anySetBit();
						i >= 0;
						i = unresolvedColumns.anySetBit(i)) {

						if(permittedColumns != null && permittedColumns.get(i)) {
							unresolvedColumns.clear(i);
						}
					}
				}
				//if we get the permission we want, we don't need to look further
				if (unresolvedColumns.anySetBit() < 0)
					break;
			}
		}
		lcc.removeRoles(activation, rolesToRemove);

		TableDescriptor td = getTableDescriptor(dd);
		//if we are still here, then that means that we didn't find any select
		//privilege on the table or any column in the table
		if (td.getTableType() == TableDescriptor.VIEW_TYPE) {
			ViewDescriptor vd = dd.getViewDescriptor(td);
			vd.getViewText();
		}
		if (privType == Authorizer.MIN_SELECT_PRIV)
			throw StandardException.newException( forGrant ? SQLState.AUTH_NO_TABLE_PERMISSION_FOR_GRANT
					  : SQLState.AUTH_NO_TABLE_PERMISSION,
                      currentUserId,
					  getPrivName(),
					  td.getSchemaName(),
					  td.getName());

		int remains = unresolvedColumns.anySetBit();

		if (remains >= 0) {
			// No permission on this column.
			ColumnDescriptor cd = td.getColumnDescriptor(remains + 1);

			if(cd == null) {
				throw StandardException.newException(
					SQLState.AUTH_INTERNAL_BAD_UUID, "column");
			} else {
				throw StandardException.newException(
					(forGrant
					 ? SQLState.AUTH_NO_COLUMN_PERMISSION_FOR_GRANT
					 : SQLState.AUTH_NO_COLUMN_PERMISSION),
                    currentUserId,
					getPrivName(),
					cd.getColumnName(),
					td.getSchemaName(),
					td.getName());
			}
		} else {
			// We found and successfully applied a (set of) role to resolve the
			// (remaining) required permissions.
			//
			// Also add a dependency on the roles (qua provider), so
			// that if the roles are no longer available to the current
			// user (e.g. grant is revoked, role is dropped,
			// another role has been set), we are able to
			// invalidate the ps or activation (the latter is used
			// if the current role changes).
			DependencyManager dm = dd.getDependencyManager();
			ContextManager cm = lcc.getContextManager();
			// get the currentRoles again, as some may have been removed
			// during the loop above due to the loss of the right to set this role
			// we need to set dependency for all roles checked so far, as the privileges of columns
			// may come from multiple roles
			currentRoles = lcc.getCurrentRoles(activation);
			for (String role: currentRoles) {
				RoleGrantDescriptor rgd =
						dd.getRoleDefinitionDescriptor(role);
				dm.addDependency(ps, rgd, cm);
				dm.addDependency(activation, rgd, cm);
				if (role.equals(lastRole))
					break;
			}
		}

	} // end of check

	/**
	 * Add one user's set of permitted columns to a list of permitted columns.
	 */
	private FormatableBitSet addPermittedColumns( DataDictionary dd,
												  boolean forGrant,
												  String authorizationId,
												  FormatableBitSet permittedColumns)
		throws StandardException
	{
		if( permittedColumns != null && permittedColumns.getNumBitsSet() == permittedColumns.size())
			return permittedColumns;
		ColPermsDescriptor perms = dd.getColumnPermissions( tableUUID, privType, false, authorizationId);
		if( perms != null)
		{
			if( permittedColumns == null)
				return perms.getColumns();
			permittedColumns.or( perms.getColumns());
		}
		return permittedColumns;
	} // end of addPermittedColumns

	/**
	 * @see StatementPermission#getPermissionDescriptor
	 */
	public PermissionsDescriptor getPermissionDescriptor(String authid, DataDictionary dd)
	throws StandardException
	{
		//If table permission found for authorizationid, then simply return that
		if (oneAuthHasPermissionOnTable( dd, authid, false) == AUTHORIZED)
			return dd.getTablePermissions(tableUUID, authid);
		//If table permission found for PUBLIC, then simply return that
		if (oneAuthHasPermissionOnTable( dd, Authorizer.PUBLIC_AUTHORIZATION_ID, false) == AUTHORIZED)
			return dd.getTablePermissions(tableUUID, Authorizer.PUBLIC_AUTHORIZATION_ID);

		if (oneAuthHasPermissionOnSchema( dd, authid, false) == AUTHORIZED)
			return dd.getSchemaPermissions(schemaUUID, authid);

		if (oneAuthHasPermissionOnSchema( dd, Authorizer.PUBLIC_AUTHORIZATION_ID, false) == AUTHORIZED)
			return dd.getSchemaPermissions(schemaUUID, authid);
		
		//If table level permission not found, then we have to find permissions 
		//at column level. Look for column level permission for the passed 
		//authorizer. If found any of the required column level permissions,
		//return the permission descriptor for it.
		ColPermsDescriptor colsPermsDesc = dd.getColumnPermissions(tableUUID, privType, false, authid);
		if( colsPermsDesc != null)
		{
			if( colsPermsDesc.getColumns() != null){
				FormatableBitSet permittedColumns = colsPermsDesc.getColumns();
				for( int i = columns.anySetBit(); i >= 0; i = columns.anySetBit( i))
				{
					if(permittedColumns.get(i))
						return colsPermsDesc;
				}
			}
		}
		return null;
	}
	
	/**
	 * This method gets called in execution phase after it is established that 
	 * all the required privileges exist for the given sql. This method gets 
	 * called by create view/trigger/constraint to record their dependency on 
	 * various privileges.
	 * Special code is required to track column level privileges.
	 * It is possible that some column level privileges are available to the
	 * passed authorizer id but the rest required column level privileges
	 * are available at PUBLIC level. In this method, we check if all the
	 * required column level privileges are found for the passed authorizer.
	 * If yes, then simply return null, indicating that no dependency is 
	 * required at PUBLIC level, because all the required privileges were found
	 * at the user level. But if some column level privileges are not
	 * available at user level, then they have to exist at the PUBLIC
	 * level when this method gets called.  
	 */
	public PermissionsDescriptor getPUBLIClevelColPermsDescriptor(String authid, DataDictionary dd)
	throws StandardException
	{
		ColPermsDescriptor colsPermsDesc = dd.getColumnPermissions(tableUUID, privType, false, authid);
		FormatableBitSet permittedColumns = colsPermsDesc.getColumns();
		boolean allColumnsCoveredByUserLevelPrivilege = true;
		for( int i = columns.anySetBit(); i >= 0 && allColumnsCoveredByUserLevelPrivilege; i = columns.anySetBit( i))
		{
			if(permittedColumns.get(i))
				continue;
			else
				allColumnsCoveredByUserLevelPrivilege = false;
		}
		if (allColumnsCoveredByUserLevelPrivilege)
			return null;
		else
			return (dd.getColumnPermissions(tableUUID, privType, false, Authorizer.PUBLIC_AUTHORIZATION_ID));	
	}

	/**
	 * Returns false if the current role is necessary to cover
	 * the necessary permission(s).
	 * @param authid authentication id of the current user
	 * @param dd data dictionary
	 *
	 * @return false if the current role is required
	 */
	public boolean allColumnsCoveredByUserOrPUBLIC(String authid,
												   DataDictionary dd)
			throws StandardException {

		ColPermsDescriptor colsPermsDesc =
			dd.getColumnPermissions(tableUUID, privType, false, authid);
		FormatableBitSet permittedColumns = colsPermsDesc.getColumns();
		FormatableBitSet unresolvedColumns = (FormatableBitSet)columns.clone();
		boolean result = true;

		if (permittedColumns != null) { // else none at user level
			for(int i = unresolvedColumns.anySetBit();
				i >= 0;
				i = unresolvedColumns.anySetBit(i)) {

				if(permittedColumns.get(i)) {
					unresolvedColumns.clear(i);
				}
			}
		}


		if (unresolvedColumns.anySetBit() >= 0) {
			colsPermsDesc =
				dd.getColumnPermissions(
					tableUUID, privType, false,
					Authorizer.PUBLIC_AUTHORIZATION_ID);
			permittedColumns = colsPermsDesc.getColumns();

			if (permittedColumns != null) { // else none at public level
				for(int i = unresolvedColumns.anySetBit();
					i >= 0;
					i = unresolvedColumns.anySetBit(i)) {

					if(permittedColumns.get(i)) {
						unresolvedColumns.clear(i);
					}
				}
			}

			if (unresolvedColumns.anySetBit() >= 0) {
				// even after trying all grants to user and public there
				// are unresolved columns so role must have been used.
				result = false;
			}
		}

		return result;
	}


	/**
	 * Try to use the supplied role r to see what column privileges are we 
	 * entitled to. 
	 *
	 * @param lcc language connection context
	 * @param dd  data dictionary
	 * @param forGrant true of a GRANTable permission is sought
	 * @param r the role to inspect to see if it can supply the required
	 *          privileges
	 * return the set of columns on which we have privileges through this role
	 */
	private FormatableBitSet tryRole(LanguageConnectionContext lcc,
									 DataDictionary dd,
									 boolean forGrant,
									 String r)
			throws StandardException {

		FormatableBitSet permittedColumns = null;

		if (! forGrant) {
			// This is a weaker permission than GRANTable, so only applicable
			// if grantable is not required.
			permittedColumns = addPermittedColumns(dd, false, r, null);
		}

		// if grantable is given, applicable in both cases, so use union
		permittedColumns = addPermittedColumns(dd, true, r, permittedColumns);
		return permittedColumns;
	}


	public String toString()
	{
		return "StatementColumnPermission: " + getPrivName() + " " +
			tableUUID + " columns: " + columns;
	}

	@Override
	public Type getType() {
		return Type.COLUMN;
	}
}
