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

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.store.access.TransactionController;
import splice.com.google.common.collect.Lists;

import java.util.List;

public class GenericPrivilegeInfo extends PrivilegeInfo
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private PrivilegedSQLObject _tupleDescriptor;
    private String              _privilege;
    private boolean             _restrict;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Construct from the object which is protected by privileges.
     *
     * @param tupleDescriptor The object which is being protected
     * @param privilege Kind of privilege (e.g., PermDescriptor.USAGE_PRIV)
     * @param restrict True if this is a REVOKE RESTRICT action
     */
	public GenericPrivilegeInfo( PrivilegedSQLObject tupleDescriptor, String privilege, boolean restrict )
	{
		_tupleDescriptor = tupleDescriptor;
        _privilege = privilege;
        _restrict = restrict;
	}
	
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // PrivilegeInfo BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

	/**
	 *	This is the guts of the Execution-time logic for GRANT/REVOKE generic privileges.
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
        SchemaDescriptor sd = _tupleDescriptor.getSchemaDescriptor();
        UUID objectID = _tupleDescriptor.getUUID();
        String objectTypeName = _tupleDescriptor.getObjectTypeName();
		List<PermissionsDescriptor> result = Lists.newArrayList();
		// Check that the current user has permission to grant the privileges.
		List<String> groupuserlist = lcc.getCurrentGroupUser(activation);
		checkOwnership( currentUser, groupuserlist, sd, dd );
		
		DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();

		PermDescriptor permDesc = ddg.newPermDescriptor
            ( null, objectTypeName, objectID, _privilege, currentUser, null, false );

		dd.startWriting(lcc);
        for (Object grantee1 : grantees) {
            // Keep track to see if any privileges are revoked by a revoke
            // statement. If a privilege is not revoked, we need to raise a
            // warning.
            boolean privileges_revoked = false;
            String grantee = (String) grantee1;
			DataDictionary.PermissionOperation action = dd.addRemovePermissionsDescriptor( grant, permDesc, grantee, tc);
            if (action == DataDictionary.PermissionOperation.REMOVE){
				//
				// We fall in here if we are performing REVOKE.
				//
				privileges_revoked = true;
				int invalidationType = _restrict ? DependencyManager.REVOKE_PRIVILEGE_RESTRICT : DependencyManager.REVOKE_PRIVILEGE;

				dd.getDependencyManager().invalidateFor(permDesc, invalidationType, lcc);

				// Now invalidate all GPSs refering to the object.
				dd.getDependencyManager().invalidateFor(_tupleDescriptor, invalidationType, lcc);
			}

			if (action != DataDictionary.PermissionOperation.NOCHANGE) {
                PermDescriptor permDescriptor = ddg.newPermDescriptor
                        (permDesc.getUUID(), objectTypeName, objectID, _privilege, currentUser, grantee, false);
                result.add(permDescriptor);

            }
            addWarningIfPrivilegeNotRevoked(activation, grant, privileges_revoked, grantee);
        }
        return result;
	} // end of executeGrantRevoke

    public boolean isRestrict() {
        return _restrict;
    }
}
