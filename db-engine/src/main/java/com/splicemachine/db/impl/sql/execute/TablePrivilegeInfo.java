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

import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import org.spark_project.guava.collect.Lists;
import java.util.List;
import java.util.Iterator;

public class TablePrivilegeInfo extends BasicPrivilegeInfo
{

	private TableDescriptor td;
	protected SchemaDescriptor sd;
	
	/**
	 * @param actionAllowed actionAllowed[action] is true if action is in the privilege set.
	 */
	public TablePrivilegeInfo( TableDescriptor td,
							   boolean[] actionAllowed,
							   FormatableBitSet[] columnBitSets,
							   List descriptorList)
	{
		this.actionAllowed = actionAllowed;
		this.columnBitSets = columnBitSets;
		this.td = td;
		this.descriptorList = descriptorList;
	}
	

	

	
	/**
	 *	This is the guts of the Execution-time logic for GRANT/REVOKE of a table privilege
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
		List<PermissionsDescriptor> result = Lists.newArrayList();
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
        String currentUser = lcc.getCurrentUserId(activation);
		TransactionController tc = lcc.getTransactionExecute();
		SchemaDescriptor sd = td.getSchemaDescriptor();
		
		// Check that the current user has permission to grant the privileges.
		checkOwnership( currentUser, td, sd, dd, lcc, grant);
		
		DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();

		TablePermsDescriptor tablePermsDesc =
		  ddg.newTablePermsDescriptor( td,
									   getPermString( SELECT_ACTION, false),
									   getPermString( DELETE_ACTION, false),
									   getPermString( INSERT_ACTION, false),
									   getPermString( UPDATE_ACTION, false),
									   getPermString( REFERENCES_ACTION, false),
									   getPermString( TRIGGER_ACTION, false),
									   currentUser);
			
		ColPermsDescriptor[] colPermsDescs = new ColPermsDescriptor[ columnBitSets.length];
		for( int i = 0; i < columnBitSets.length; i++)
		{
			if( columnBitSets[i] != null ||
				// If it is a revoke and no column list is specified then revoke all column permissions.
				// A null column bitSet in a ColPermsDescriptor indicates that all the column permissions
				// should be removed.
				(!grant) && hasColumnPermissions(i) && actionAllowed[i]
				)
			{
				colPermsDescs[i] = ddg.newColPermsDescriptor( td,
															  getActionString(i, false),
															  columnBitSets[i],
															  currentUser);
			}
		}


		dd.startWriting(lcc);
		// Add or remove the privileges to/from the SYS.SYSTABLEPERMS and SYS.SYSCOLPERMS tables
		for( Iterator itr = grantees.iterator(); itr.hasNext();)
		{
			// Keep track to see if any privileges are revoked by a revoke 
			// statement. If a privilege is not revoked, we need to raise a 
			// warning. For table privileges, we do not check if privilege for 
			// a specific action has been revoked or not. Also, we do not check
			// privileges for specific columns. If at least one privilege has 
			// been revoked, we do not raise a warning. This has to be refined 
			// further to check for specific actions/columns and raise warning 
			// if any privilege has not been revoked.
			boolean privileges_revoked = false;
						
			String grantee = (String) itr.next();
			if( tablePermsDesc != null)
			{
				if (dd.addRemovePermissionsDescriptor( grant, tablePermsDesc, grantee, tc))
				{
					privileges_revoked = true;
					dd.getDependencyManager().invalidateFor
						(tablePermsDesc,
						 DependencyManager.REVOKE_PRIVILEGE, lcc);

					// When revoking a privilege from a Table we need to
					// invalidate all GPSs refering to it. But GPSs aren't
					// Dependents of TablePermsDescr, but of the
					// TableDescriptor itself, so we must send
					// INTERNAL_RECOMPILE_REQUEST to the TableDescriptor's
					// Dependents.
					dd.getDependencyManager().invalidateFor
						(td, DependencyManager.INTERNAL_RECOMPILE_REQUEST, lcc);
                    TablePermsDescriptor tablePermsDescriptor =
                            new TablePermsDescriptor(dd, tablePermsDesc.getGrantee(),
                                    tablePermsDesc.getGrantor(), tablePermsDesc.getTableUUID(),
                                    tablePermsDesc.getSelectPriv(), tablePermsDesc.getDeletePriv(),
                                    tablePermsDesc.getInsertPriv(), tablePermsDesc.getUpdatePriv(),
                                    tablePermsDesc.getReferencesPriv(), tablePermsDesc.getTriggerPriv());
                    tablePermsDescriptor.setUUID(tablePermsDesc.getUUID());
                    result.add(tablePermsDescriptor);
				}
			}
			for( int i = 0; i < columnBitSets.length; i++)
			{
				if( colPermsDescs[i] != null)
				{
					if (dd.addRemovePermissionsDescriptor( grant, colPermsDescs[i], grantee, tc)) 
					{
						privileges_revoked = true;
						dd.getDependencyManager().invalidateFor(colPermsDescs[i], DependencyManager.REVOKE_PRIVILEGE, lcc);
						// When revoking a privilege from a Table we need to
						// invalidate all GPSs refering to it. But GPSs aren't
						// Dependents of colPermsDescs[i], but of the
						// TableDescriptor itself, so we must send
						// INTERNAL_RECOMPILE_REQUEST to the TableDescriptor's
						// Dependents.
						dd.getDependencyManager().invalidateFor
							(td,
							 DependencyManager.INTERNAL_RECOMPILE_REQUEST,
							 lcc);
                        ColPermsDescriptor colPermsDescriptor =
                                new ColPermsDescriptor(dd, grantee, colPermsDescs[i].getGrantor(),
                                        colPermsDescs[i].getTableUUID(), colPermsDescs[i].getType(),
                                        colPermsDescs[i].getColumns());
                        colPermsDescriptor.setUUID(colPermsDescs[i].getUUID());
                        result.add(colPermsDescriptor);
					}
				}
			}
			
			addWarningIfPrivilegeNotRevoked(activation, grant, privileges_revoked, grantee);
		}
        return result;
	} // end of executeConstantAction



	private boolean hasColumnPermissions( int action)
	{
		return action == SELECT_ACTION || action == UPDATE_ACTION || action == REFERENCES_ACTION;
	}
}
