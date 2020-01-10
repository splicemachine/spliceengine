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
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.execute.ExecPreparedStatement;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.shared.common.reference.AuditEventType;
import com.splicemachine.utils.StringUtils;
import org.apache.log4j.Logger;

import java.sql.CallableStatement;
import java.util.ArrayList;
import java.util.List;

/**
 * This class describes a permission require by a statement.
 */

public abstract class StatementPermission {
	public enum Type {
		COLUMN,
		TABLE,
		GENERIC,
		SCHEMA,
		ROUTINE,
		ROLE;
	}


	public static final int UNAUTHORIZED = 0;
	public static final int AUTHORIZED = 1;
	public static final int NONE = 2;

	private static final Logger AUDITLOG=Logger.getLogger("splice-audit");

	/**
	 * Restrict implementations to this package to reduce
	 * risk of external code spoofing the GRANT/REVOKE system
	 * by providing its own fake implementations.
	 *
	 */
	StatementPermission()
	{
	}
	/**
	 * @param lcc				LanguageConnectionContext
	 * @param forGrant
	 * @param activation        activation for statement needing check
	 *
	 * @exception StandardException if the permission has not been granted
	 */
	public abstract void check( LanguageConnectionContext lcc,
								boolean forGrant,
								Activation activation) throws StandardException;

	/**
	 * 
	 * Get the PermissionsDescriptor for the passed authorization id for this
	 * object. This method gets called during the execution phase of create 
	 * view/constraint/trigger. The return value of this method is saved in
	 * dependency system to keep track of views/constraints/triggers 
	 * dependencies on required permissions. This happens in execution phase 
	 * after it has been established that passed authorization id has all the 
	 * permissions it needs to create that view/constraint/trigger. Which means 
	 * that we can only get to writing into dependency system once all the required 
	 * privileges are confirmed. 
	 *   
	 * @param authid	AuthorizationId
	 * @param dd	DataDictionary
	 * 
	 * @return PermissionsDescriptor	The PermissionsDescriptor for the passed
	 *  authorization id on this object
	 * 
	 * @exception StandardException
	 */
	public abstract PermissionsDescriptor getPermissionDescriptor(String authid, DataDictionary dd)
	throws StandardException;

    /**
     * Return true if the passed in permission matches the one required by this
     * StatementPermission.
     */
    public boolean isCorrectPermission( PermissionsDescriptor pd ) throws StandardException
    { return false; }

    /**
     * Get the privileged object associated with this permission.
     */
    public PrivilegedSQLObject getPrivilegedObject( DataDictionary dd ) throws StandardException
    { return null; }

    /**
     * Get the type of the privileged object.
     */
    public String getObjectType()
    { return null; }

    /**
     * Generic logic called by check() for USAGE and EXECUTE privileges. Throws
     * an exception if the correct permission cannot be found.
     */
	public void genericCheck
        (
         LanguageConnectionContext lcc,
         boolean forGrant,
         Activation activation,
         String privilegeType )
        throws StandardException
	{
		DataDictionary dd = lcc.getDataDictionary();
		TransactionController tc = lcc.getTransactionExecute();
		ExecPreparedStatement ps = activation.getPreparedStatement();
		List<String> currentGroupuserlist = lcc.getCurrentGroupUser(activation);

        PermissionsDescriptor perm =
            getPermissionDescriptor( lcc.getCurrentUserId(activation), dd );
		if( !isCorrectPermission( perm ) ) { perm = getPermissionDescriptor(Authorizer.PUBLIC_AUTHORIZATION_ID, dd ); }

		if( !isCorrectPermission( perm ) && currentGroupuserlist != null ) {
			for (String currentGroupuser : currentGroupuserlist) {
				perm = getPermissionDescriptor(currentGroupuser, dd );
				// if the groupuser has the correct permission, we're done
				if ( isCorrectPermission( perm ) ) { return; }
			}
		}
        // if the user/groupuser has the correct permission, we're done
		if ( isCorrectPermission( perm ) ) { return; }

			boolean resolved = false;

		// Since no permission exists for the current user or PUBLIC,
		// check if a permission exists for the current role (if set).
		List<String> currentRoles = lcc.getCurrentRoles(activation);
		List<String> rolesToRemove = new ArrayList<>();
		for (String role : currentRoles) {
			// Check that role is still granted to current user or
			// to PUBLIC: A revoked role which is current for this
			// session, is lazily set to none when it is attemped
			// used.
			RoleGrantDescriptor rd = dd.getRoleGrantDescriptor
                (role, lcc.getCurrentUserId(activation));

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
					 role, true );

				String r;
				while (!resolved && (r = rci.next()) != null)
                {
					perm = getPermissionDescriptor( r, dd );

					if ( isCorrectPermission( perm ) ) { resolved = true; }
				}
			}

			if (resolved ) {
				// Also add a dependency on the role (qua provider), so that if
				// role is no longer available to the current user (e.g. grant
				// is revoked, role is dropped, another role has been set), we
				// are able to invalidate the ps or activation (the latter is
				// used if the current role changes).
				DependencyManager dm = dd.getDependencyManager();
				RoleGrantDescriptor rgd = dd.getRoleDefinitionDescriptor(role);
				ContextManager cm = lcc.getContextManager();
				dm.addDependency(ps, rgd, cm);
				dm.addDependency(activation, rgd, cm);
				break;
			}
		}
		lcc.removeRoles(activation, rolesToRemove);

		if (!resolved)
        {
            PrivilegedSQLObject pso = getPrivilegedObject( dd );

			try {
				if (pso == null) {
					throw StandardException.newException
							(SQLState.AUTH_INTERNAL_BAD_UUID, getObjectType());
				}

				SchemaDescriptor sd = pso.getSchemaDescriptor();
				if (sd == null) {
					throw StandardException.newException(
							SQLState.AUTH_INTERNAL_BAD_UUID, "SCHEMA");
				}

				throw StandardException.newException(
						(forGrant
								? SQLState.AUTH_NO_GENERIC_PERMISSION_FOR_GRANT
								: SQLState.AUTH_NO_GENERIC_PERMISSION),
						lcc.getCurrentUserId(activation),
						privilegeType,
						getObjectType(),
						sd.getSchemaName(),
						pso.getName());
			}
			catch(StandardException se){
				if (AUDITLOG.isInfoEnabled()
						&& privilegeType.equalsIgnoreCase("EXECUTE")
						&& (!se.getMessageId().equals(SQLState.AUTH_INTERNAL_BAD_UUID))){

					String userid = lcc.getCurrentUserId(activation);
					boolean status = false;
					String ip = lcc.getClientIPAddress();
					String statement = lcc.getStatementContext().getStatementText();
					String procedure = pso.getName();
					String schema = pso.getSchemaDescriptor().getSchemaName();

					// Check whether the procedure in the audit procedure list
					for (AuditEventType t : AuditEventType.values())
					{
						String[] p = t.getProcedure().split("\\.");
						if (p.length == 2 && schema.equals(p[0]) && procedure.equals(p[1])) {
							AUDITLOG.info(StringUtils.logSpliceAuditEvent(userid,t.name(),status,ip,statement,se.getMessage()));
							break;
						}
					}
				}
				throw se;
			}
		}

	} // end of genericCheck


	public abstract Type getType() ;
}
