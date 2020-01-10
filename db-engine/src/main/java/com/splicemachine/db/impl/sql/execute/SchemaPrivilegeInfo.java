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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.store.access.TransactionController;

import java.util.ArrayList;
import java.util.List;

public class SchemaPrivilegeInfo extends BasicPrivilegeInfo
{


	protected SchemaDescriptor sd;


	public SchemaPrivilegeInfo(SchemaDescriptor sd,
							   boolean[] actionAllowed,
							   FormatableBitSet[] columnBitSets,
							   List descriptorList)
	{
		this.actionAllowed = actionAllowed;
		this.columnBitSets = columnBitSets;
		this.sd = sd;
		this.descriptorList = descriptorList;
	}

	@Override
	public List<PermissionsDescriptor> executeGrantRevoke( Activation activation,
														   boolean grant,
														   List grantees)
			throws StandardException {
		List<PermissionsDescriptor> result = new ArrayList<>();
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		String currentUser = lcc.getCurrentUserId(activation);
		TransactionController tc = lcc.getTransactionExecute();
		List<String> groupuserlist = lcc.getCurrentGroupUser(activation);

		// Check that the current user has permission to grant the privileges.
		checkOwnership( currentUser, groupuserlist, sd, dd, lcc, grant);

		DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();
		SchemaPermsDescriptor schemaPermsDesc =
				ddg.newSchemaPermsDescriptor( sd,
						getPermString( SELECT_ACTION, false),
						getPermString( DELETE_ACTION, false),
						getPermString( INSERT_ACTION, false),
						getPermString( UPDATE_ACTION, false),
						getPermString( REFERENCES_ACTION, false),
						getPermString( TRIGGER_ACTION, false),
						getPermString( MODIFY_ACTION, false),
						getPermString( ACCESS_ACTION, false),
						currentUser);


		dd.startWriting(lcc);
        for (Object grantee1 : grantees) {
            // Keep track to see if any privileges are revoked by a revoke
            // statement. If a privilege is not revoked, we need to raise a
            // warning.
            boolean privileges_revoked = false;

            String grantee = (String) grantee1;
            if (schemaPermsDesc != null) {
				DataDictionary.PermissionOperation action = dd.addRemovePermissionsDescriptor(grant, schemaPermsDesc, grantee, tc);
                if (action == DataDictionary.PermissionOperation.REMOVE) {
					privileges_revoked = true;
					dd.getDependencyManager().invalidateFor
							(schemaPermsDesc,
									DependencyManager.REVOKE_PRIVILEGE, lcc);

					// When revoking a privilege from a Table we need to
					// invalidate all GPSs refering to it. But GPSs aren't
					// Dependents of SchemaPermsDescr, but of the
					// SchemaDescriptor itself, so we must send
					// INTERNAL_RECOMPILE_REQUEST to the SchemaDescriptor's
					// Dependents.
					dd.getDependencyManager().invalidateFor
							(sd, DependencyManager.INTERNAL_RECOMPILE_REQUEST, lcc);
				}
				if (action != DataDictionary.PermissionOperation.NOCHANGE) {
                    SchemaPermsDescriptor schemaPermsDescriptor =
                            new SchemaPermsDescriptor(dd, schemaPermsDesc.getGrantee(),
                                    schemaPermsDesc.getGrantor(), schemaPermsDesc.getSchemaUUID(),
                                    schemaPermsDesc.getSelectPriv(), schemaPermsDesc.getDeletePriv(),
                                    schemaPermsDesc.getInsertPriv(), schemaPermsDesc.getUpdatePriv(),
                                    schemaPermsDesc.getReferencesPriv(), schemaPermsDesc.getTriggerPriv(),
									schemaPermsDesc.getModifyPriv(), schemaPermsDesc.getAccessPriv());
                    schemaPermsDescriptor.setUUID(schemaPermsDesc.getUUID());
                    result.add(schemaPermsDescriptor);
                }
            }


            addWarningIfPrivilegeNotRevoked(activation, grant, privileges_revoked, grantee);
        }
		return result;
	}

}

