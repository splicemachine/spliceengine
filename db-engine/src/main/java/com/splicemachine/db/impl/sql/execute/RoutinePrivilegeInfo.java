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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.error.StandardException;
import org.spark_project.guava.collect.Lists;
import java.util.Iterator;
import java.util.List;

public class RoutinePrivilegeInfo extends PrivilegeInfo
{
	private AliasDescriptor aliasDescriptor;

	public RoutinePrivilegeInfo( AliasDescriptor aliasDescriptor)
	{
		this.aliasDescriptor = aliasDescriptor;
	}
	
	/**
	 *	This is the guts of the Execution-time logic for GRANT/REVOKE of a routine execute privilege
	 *
	 * @param activation
	 * @param grant true if grant, false if revoke
	 * @param grantees a list of authorization ids (strings)
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public List<PermissionsDescriptor> executeGrantRevoke( Activation activation,
									boolean grant,
									List grantees)
		throws StandardException
	{
		// Check that the current user has permission to grant the privileges.
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
        String currentUser = lcc.getCurrentUserId(activation);
		TransactionController tc = lcc.getTransactionExecute();

		List<PermissionsDescriptor> result = Lists.newArrayList();
        // Check that the current user has permission to grant the privileges.
		checkOwnership( currentUser,
						dd.getSchemaDescriptor( aliasDescriptor.getSchemaUUID(), tc),
						dd);
		
		DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();

		RoutinePermsDescriptor routinePermsDesc = ddg.newRoutinePermsDescriptor( aliasDescriptor, currentUser);

		dd.startWriting(lcc);
		for( Iterator itr = grantees.iterator(); itr.hasNext();)
		{
			// Keep track to see if any privileges are revoked by a revoke 
			// statement. If a privilege is not revoked, we need to raise a
			// warning.
			boolean privileges_revoked = false;
			String grantee = (String) itr.next();
			if (dd.addRemovePermissionsDescriptor( grant, routinePermsDesc, grantee, tc)) 
			{
				privileges_revoked = true;	
				//Derby currently supports only restrict form of revoke execute
				//privilege and that is why, we are sending invalidation action 
				//as REVOKE_PRIVILEGE_RESTRICT rather than REVOKE_PRIVILEGE
				dd.getDependencyManager().invalidateFor
					(routinePermsDesc,
					 DependencyManager.REVOKE_PRIVILEGE_RESTRICT, lcc);

				// When revoking a privilege from a Routine we need to
				// invalidate all GPSs refering to it. But GPSs aren't
				// Dependents of RoutinePermsDescr, but of the
				// AliasDescriptor itself, so we must send
				// INTERNAL_RECOMPILE_REQUEST to the AliasDescriptor's
				// Dependents.
				dd.getDependencyManager().invalidateFor
					(aliasDescriptor,
					 DependencyManager.INTERNAL_RECOMPILE_REQUEST, lcc);

                RoutinePermsDescriptor routinePermsDescriptor =
                        new RoutinePermsDescriptor(dd, routinePermsDesc.getGrantee(), routinePermsDesc.getGrantor(),
                                routinePermsDesc.getRoutineUUID());
                routinePermsDescriptor.setUUID(routinePermsDesc.getUUID());
                result.add(routinePermsDescriptor);
			}
			
			addWarningIfPrivilegeNotRevoked(activation, grant, privileges_revoked, grantee);
		}
        return result;
	} // end of executeConstantAction
}
