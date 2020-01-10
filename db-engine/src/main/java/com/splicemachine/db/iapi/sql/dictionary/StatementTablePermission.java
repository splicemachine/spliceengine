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

package com.splicemachine.db.iapi.sql.dictionary;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecPreparedStatement;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.services.context.ContextManager;

import java.util.ArrayList;
import java.util.List;

/**
 * This class describes a table permission required by a statement.
 */

public class StatementTablePermission extends StatementSchemaPermission
{
	UUID tableUUID;

	/**
	 * Constructor for StatementTablePermission. Creates an instance of
	 * table permission requested for the given access.
	 * 
	 * @param tableUUID	UUID of the table
	 * @param privType	Access privilege requested
	 *
	 */
	public StatementTablePermission(UUID tableUUID, int privType)
	{
		super(null,privType);
		this.tableUUID = tableUUID;
		this.privType = privType;
	}


	public StatementTablePermission(UUID schemaUUID, UUID tableUUID, int privType)
	{
		super(schemaUUID,privType);
		this.tableUUID = tableUUID;

	}

	/**
	 * Return privilege access requested for this access descriptor
	 *
	 * @return	Privilege access
	 */
	public int getPrivType()
	{
		return privType;
	}

	/**
	 * Return table UUID for this access descriptor
	 *
	 * @return	Table UUID
	 */
	public UUID getTableUUID()
	{
		return tableUUID;
	}

	/**
	 * Routine to check if another instance of access descriptor matches this.
	 * Used to ensure only one access descriptor for a table of given privilege is created.
	 * Otherwise, every column reference from a table may create a descriptor for that table.
	 *
	 * @param obj	Another instance of StatementPermission
	 *
	 * @return	true if match
	 */
	public boolean equals( Object obj)
	{
		if( obj == null)
			return false;
		if( getClass().equals( obj.getClass()))
		{
			StatementTablePermission other = (StatementTablePermission) obj;
			return privType == other.privType && tableUUID.equals( other.tableUUID);
		}
		return false;
	} // end of equals

	/**
	 * Return hash code for this instance
	 *
	 * @return	Hashcode
	 *
	 */
	public int hashCode()
	{
		return privType + tableUUID.hashCode();
	}
	
	/**
	 * @see StatementPermission#check
	 */
	public void check( LanguageConnectionContext lcc,
					   boolean forGrant,
					   Activation activation)
		throws StandardException
	{
		ExecPreparedStatement ps = activation.getPreparedStatement();

        if (!hasPermissionOnTable(lcc, activation, forGrant, ps)) {
		    DataDictionary dd = lcc.getDataDictionary();
			TableDescriptor td = getTableDescriptor( dd);
            throw StandardException.newException(
                (forGrant ? SQLState.AUTH_NO_TABLE_PERMISSION_FOR_GRANT
                 : SQLState.AUTH_NO_TABLE_PERMISSION),
                lcc.getCurrentUserId(activation),
                getPrivName(),
                td.getSchemaName(),
                td.getName());
		}
	} // end of check

	protected TableDescriptor getTableDescriptor(DataDictionary dd)  throws StandardException
	{
		TableDescriptor td = dd.getTableDescriptor( tableUUID);
		if( td == null)
			throw StandardException.newException(SQLState.AUTH_INTERNAL_BAD_UUID, "table");
		return td;
	} // end of getTableDescriptor

	/**
	 * Check if current session has permission on the table (current user,
	 * PUBLIC or role) and, if applicable, register a dependency of ps on the
	 * current role.
	 *
	 * @param lcc the current language connection context
	 * @param activation the activation of ps
	 * @param forGrant true if FOR GRANT is required
	 * @param ps the prepared statement for which we are checking necessary
	 *        privileges
	 */
	protected boolean hasPermissionOnTable(LanguageConnectionContext lcc,
										   Activation activation,
										   boolean forGrant,
										   ExecPreparedStatement ps)
		throws StandardException
	{
		DataDictionary dd = lcc.getDataDictionary();
        String currentUserId = lcc.getCurrentUserId(activation);
        List<String> currentGroupuserlist = lcc.getCurrentGroupUser(activation);

		// Let check the table level first
		int authorization =
				oneAuthHasPermissionOnTable(dd, Authorizer.PUBLIC_AUTHORIZATION_ID, forGrant);

		if(authorization == NONE || authorization == UNAUTHORIZED ) {
			authorization = oneAuthHasPermissionOnTable(dd, currentUserId, forGrant);
		}
		if((authorization == NONE || authorization == UNAUTHORIZED) && currentGroupuserlist != null ) {
			for (String currentGroupuser : currentGroupuserlist) {
				authorization = oneAuthHasPermissionOnTable(dd, currentGroupuser, forGrant);
				if (authorization == AUTHORIZED)
					break;
			}

		}

		// Let see if we have schema level privileges. We overwrite only it there is no privileges for tables
		if(authorization == NONE){
			int authorizationSchema =
					oneAuthHasPermissionOnSchema(dd, Authorizer.PUBLIC_AUTHORIZATION_ID, forGrant);
			// if there is no privileges for the special user PUBLIC or if it is unauthorized, then :
			if(authorizationSchema == NONE || authorizationSchema == UNAUTHORIZED ) {
				authorizationSchema = oneAuthHasPermissionOnSchema(dd, currentUserId, forGrant);
			}
			if((authorizationSchema == NONE || authorizationSchema == UNAUTHORIZED ) && currentGroupuserlist != null ) {
				for (String currentGroupuser : currentGroupuserlist) {
					authorizationSchema = oneAuthHasPermissionOnSchema(dd, currentGroupuser, forGrant);
					if (authorizationSchema == AUTHORIZED)
						break;
				}
			}

			// we overwrite the authorization type with what we found for schemas
			authorization = authorizationSchema;
		}


		if (authorization == NONE) {
			// Since no permission exists for the current user or PUBLIC,
			// check if a permission exists for the current role (if set).
			List<String> currentRoles = lcc.getCurrentRoles(activation);
			List<String> rolesToRemove = new ArrayList<>();
			for (String role : currentRoles) {
				// Check that role is still granted to current user or
				// to PUBLIC: A revoked role which is current for this
				// session, is lazily set to none when it is attempted
				// used.
				RoleGrantDescriptor rd = dd.getRoleGrantDescriptor
						(role, currentUserId);

				if (rd == null) {
					rd = dd.getRoleGrantDescriptor(
							role,
							Authorizer.PUBLIC_AUTHORIZATION_ID);
				}
				if (rd == null && currentGroupuserlist != null) {
					for (String currentGroupuser : currentGroupuserlist) {
						rd = dd.getRoleGrantDescriptor(role, currentGroupuser);
						if (rd != null)
							break;
					}
				}

				if (rd == null) {
					// We have lost the right to set this role, so we can't
					// make use of any permission granted to it or its
					// ancestors.
					rolesToRemove.add(role);
				} else {
					// The current role is OK, so we can make use of
					// any permission granted to it.
					//
					// Look at the current role and, if necessary, the
					// transitive closure of roles granted to current role to
					// see if permission has been granted to any of the
					// applicable roles.

					RoleClosureIterator rci =
							dd.createRoleClosureIterator
									(activation.getTransactionController(),
											role, true /* inverse relation*/);

					String r;

					while ((authorization != AUTHORIZED) && (r = rci.next()) != null) {
						authorization = oneAuthHasPermissionOnTable
								(dd, r, forGrant);

						if (authorization == NONE) {
							authorization = oneAuthHasPermissionOnSchema
									(dd, r, forGrant);
						}
					}

					if (authorization == AUTHORIZED) {
						// Also add a dependency on the role (qua provider), so
						// that if role is no longer available to the current
						// user (e.g. grant is revoked, role is dropped,
						// another role has been set), we are able to
						// invalidate the ps or activation (the latter is used
						// if the current role changes).
						DependencyManager dm = dd.getDependencyManager();
						RoleGrantDescriptor rgd =
								dd.getRoleDefinitionDescriptor(role);
						ContextManager cm = lcc.getContextManager();

						dm.addDependency(ps, rgd, cm);
						dm.addDependency(activation, rgd, cm);
						break;
					}
				}
			}
			lcc.removeRoles(activation, rolesToRemove);
		}
		return authorization == AUTHORIZED;
	}



	protected int oneAuthHasPermissionOnTable(DataDictionary dd, String authorizationId, boolean forGrant)
		throws StandardException
	{
		TablePermsDescriptor perms = dd.getTablePermissions( tableUUID, authorizationId);
		if( perms == null)
			return NONE;
		
		String priv = null;
			
		switch( privType)
		{
		case Authorizer.SELECT_PRIV:
		case Authorizer.MIN_SELECT_PRIV:
			priv = perms.getSelectPriv();
			break;
		case Authorizer.UPDATE_PRIV:
			priv = perms.getUpdatePriv();
			break;
		case Authorizer.REFERENCES_PRIV:
			priv = perms.getReferencesPriv();
			break;
		case Authorizer.INSERT_PRIV:
			priv = perms.getInsertPriv();
			break;
		case Authorizer.DELETE_PRIV:
			priv = perms.getDeletePriv();
			break;
		case Authorizer.TRIGGER_PRIV:
			priv = perms.getTriggerPriv();
			break;
		}

		return "Y".equals(priv) || (!forGrant) && "y".equals( priv) ? AUTHORIZED : UNAUTHORIZED;
	} // end of hasPermissionOnTable

	protected int oneAuthHasPermissionOnSchema(DataDictionary dd, String authorizationId, boolean forGrant)
			throws StandardException
	{
		SchemaPermsDescriptor perms = dd.getSchemaPermissions( schemaUUID, authorizationId);
		if( perms == null)
			return NONE;

		String priv = null;

		switch( privType)
		{
			case Authorizer.SELECT_PRIV:
			case Authorizer.MIN_SELECT_PRIV:
				priv = perms.getSelectPriv();
				break;
			case Authorizer.UPDATE_PRIV:
				priv = perms.getUpdatePriv();
				break;
			case Authorizer.REFERENCES_PRIV:
				priv = perms.getReferencesPriv();
				break;
			case Authorizer.INSERT_PRIV:
				priv = perms.getInsertPriv();
				break;
			case Authorizer.DELETE_PRIV:
				priv = perms.getDeletePriv();
				break;
			case Authorizer.TRIGGER_PRIV:
				priv = perms.getTriggerPriv();
				break;
		}

		return "Y".equals(priv) || (!forGrant) && "y".equals( priv) ? AUTHORIZED : UNAUTHORIZED;
	} // end of hasPermissionOnSchema


	/**
	 * @see StatementPermission#getPermissionDescriptor
	 */
	public PermissionsDescriptor getPermissionDescriptor(String authid, DataDictionary dd)
	throws StandardException
	{
		//if the required type of privilege exists for the given authorizer,
		//then pass the permission descriptor for it.
		if (oneAuthHasPermissionOnTable( dd, authid, false) == AUTHORIZED)
			return dd.getTablePermissions(tableUUID, authid);
		else return null;
	}

	/**
	 * Return privilege needed for this access as string
	 *
	 * @return	privilege string
	 */
	public String getPrivName( )
	{
		switch( privType)
		{
		case Authorizer.SELECT_PRIV:
		case Authorizer.MIN_SELECT_PRIV:
			return "SELECT";
		case Authorizer.UPDATE_PRIV:
			return "UPDATE";
		case Authorizer.REFERENCES_PRIV:
			return "REFERENCES";
		case Authorizer.INSERT_PRIV:
			return "INSERT";
		case Authorizer.DELETE_PRIV:
			return "DELETE";
		case Authorizer.TRIGGER_PRIV:
			return "TRIGGER";
		}
		return "?";
	} // end of getPrivName

	public UUID getSchemaUUID() {
		return schemaUUID;
	}

	public String toString()
	{
		return "StatementTablePermission: " + getPrivName() + " " + tableUUID;
	}

	@Override
	public Type getType() {
		return Type.TABLE;
	}

}
