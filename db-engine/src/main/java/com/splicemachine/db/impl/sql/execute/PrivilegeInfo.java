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

package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.iapi.sql.dictionary.PermissionsDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.reference.SQLState;

import com.splicemachine.db.iapi.error.StandardException;

import java.util.List;

public abstract class PrivilegeInfo
{

	/**
	 *	This is the guts of the Execution-time logic for GRANT/REVOKE
	 *
	 * @param activation
	 * @param grant true if grant, false if revoke
	 * @param grantees a list of authorization ids (strings)
	 *
	 * @exception StandardException		Thrown on failure
	 */
	abstract public List<PermissionsDescriptor> executeGrantRevoke( Activation activation,
											 boolean grant,
											 List grantees)
		throws StandardException;

	/**
	 * Determines whether a user is the owner of an object
	 * (table, function, or procedure). Note that Database Owner can access
	 * database objects without needing to be their owner
	 *
	 * @param user					authorizationId of current user
	 * @param groupuserlist         groupId list of current user
	 * @param sd					SchemaDescriptor
	 * @param dd					DataDictionary
	 *
	 * @exception StandardException if user does not own the object
	 */
	protected void checkOwnership( String user,
								   List<String> groupuserlist,
								   SchemaDescriptor sd,
								   DataDictionary dd)
		throws StandardException
	{
		if (!user.equals(sd.getAuthorizationId()) &&
				!user.equals(dd.getAuthorizationDatabaseOwner())) {
			if (groupuserlist != null) {
				if (!groupuserlist.contains(sd.getAuthorizationId())
						&& !groupuserlist.contains(dd.getAuthorizationDatabaseOwner()))
				throw StandardException.newException(SQLState.AUTH_NOT_OWNER, user, sd.getSchemaName());
			} else {
				throw StandardException.newException(SQLState.AUTH_NOT_OWNER, user, sd.getSchemaName());
			}
		}
	}
	
	/**
	 * This method adds a warning if a revoke statement has not revoked 
	 * any privileges from a grantee.
	 * 
	 * @param activation
	 * @param grant true if grant, false if revoke
	 * @param privileges_revoked true, if at least one privilege has been 
	 * 							revoked from a grantee, false otherwise
	 * @param grantee authorization id of the user
	 */
	protected void addWarningIfPrivilegeNotRevoked( Activation activation,
													boolean grant,
													boolean privileges_revoked,
													String grantee) 
	{
		if(!grant && !privileges_revoked)
			activation.addWarning(StandardException.newWarning
					(SQLState.LANG_PRIVILEGE_NOT_REVOKED, grantee));
	}
}
