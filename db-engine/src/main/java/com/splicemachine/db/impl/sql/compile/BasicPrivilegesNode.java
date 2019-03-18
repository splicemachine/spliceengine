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
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.depend.Provider;
import com.splicemachine.db.iapi.sql.depend.ProviderInfo;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.impl.sql.execute.PrivilegeInfo;
import com.splicemachine.db.impl.sql.execute.SchemaPrivilegeInfo;
import com.splicemachine.db.impl.sql.execute.TablePrivilegeInfo;

import java.util.ArrayList;
import java.util.List;

import static com.splicemachine.db.impl.sql.execute.BasicPrivilegeInfo.ACCESS_ACTION;
import static com.splicemachine.db.impl.sql.execute.BasicPrivilegeInfo.MODIFY_ACTION;
import static com.splicemachine.db.impl.sql.execute.BasicPrivilegeInfo.SELECT_ACTION;

/**
 * This class represents a set of privileges on one table and schema.
 */
public class BasicPrivilegesNode extends QueryTreeNode
{
	private boolean[] actionAllowed = new boolean[ TablePrivilegeInfo.ACTION_COUNT];
	private ResultColumnList[] columnLists = new ResultColumnList[ TablePrivilegeInfo.ACTION_COUNT];
	private FormatableBitSet[] columnBitSets = new FormatableBitSet[ TablePrivilegeInfo.ACTION_COUNT];
	private TableDescriptor td;  
	private SchemaDescriptor sd;
	private List descriptorList;
	private boolean isAll = false;
	
	/**
	 * Add all actions
	 */
	public void addAll()
	{
		for( int i = 0; i < TablePrivilegeInfo.ACTION_COUNT; i++)
		{
			actionAllowed[i] = true;
			columnLists[i] = null;
		}
		isAll = true;
	} // end of addAll

	/**
	 * Add one action to the privileges for this table
	 *
	 * @param action The action type
	 * @param privilegeColumnList The set of privilege columns. Null for all columns
	 *
	 * @exception StandardException standard error policy.
	 */
	public void addAction( int action, ResultColumnList privilegeColumnList)
	{
		actionAllowed[ action] = true;
		if( privilegeColumnList == null)
			columnLists[ action] = null;
		else if( columnLists[ action] == null)
			columnLists[ action] = privilegeColumnList;
		else
			columnLists[ action].appendResultColumns( privilegeColumnList, false);
	} // end of addAction

	public void removeAction(int action) {
		actionAllowed[action] = false;
		if (columnLists[action] != null)
			columnLists[action] = null;
		return;
	}
	/**
	 * Bind.
	 *
	 * @param td The table descriptor
	 * @param isGrant grant if true; revoke if false
	 */
	public void bind( TableDescriptor td, boolean isGrant) throws StandardException
	{
		this.td = td;
			
		for( int action = 0; action < TablePrivilegeInfo.ACTION_COUNT; action++)
		{
			if( columnLists[ action] != null) {
				if (columnLists[action].size() == 1) {
					ResultColumn rc = columnLists[action].elementAt(0);
					if (rc instanceof AllResultColumn) {
						columnLists[action].remove(0);

						//expand asterisk to all columns of the table
						ColumnDescriptorList cdl = td.getColumnDescriptorList();
						for (ColumnDescriptor cd: cdl) {
							ResultColumn newRC = (ResultColumn) getNodeFactory().getNode(
									C_NodeTypes.RESULT_COLUMN,
									cd.getColumnName(),
									null,
									getContextManager());
							columnLists[action].addResultColumn(newRC);
						}
					}
				}

				columnBitSets[action] = columnLists[action].bindResultColumnsByName(td, (DMLStatementNode) null);
			}

			// Prevent granting non-SELECT privileges to views
			if (td.getTableType() == TableDescriptor.VIEW_TYPE && action != SELECT_ACTION)
				if (actionAllowed[action])
					throw StandardException.newException(SQLState.AUTH_GRANT_REVOKE_NOT_ALLOWED,
									td.getQualifiedName());
		}
		
		if (isGrant && td.getTableType() == TableDescriptor.VIEW_TYPE)
		{
			bindPrivilegesForView(td);
		}
	}

	/**
	 * Bind for schemas
	 * @param sd
	 * @param isGrant
	 * @throws StandardException
	 */

	public void bind(SchemaDescriptor sd, boolean isGrant) throws StandardException
	{
		this.sd = sd;

		for( int action = 0; action < TablePrivilegeInfo.ACTION_COUNT; action++)
		{
			if( columnLists[ action] != null)
				columnBitSets[action] = columnLists[ action].bindResultColumnsByName( td, (DMLStatementNode) null);

		}


	}
	
	/**
	 * @return PrivilegeInfo for this node with a schema info
	 */
	public PrivilegeInfo makeSchemaPrivilegeInfo()
	{
		return new SchemaPrivilegeInfo(  sd, actionAllowed, columnBitSets,
				descriptorList);
	}
	/**
	 * @return PrivilegeInfo for this node with a table Info
	 */

	public PrivilegeInfo makePrivilegeInfo()
	{
		return new TablePrivilegeInfo(td, actionAllowed, columnBitSets,
				descriptorList);
	}
	
	/**
	 *  Retrieve all the underlying stored dependencies such as table(s), 
	 *  view(s) and routine(s) descriptors which the view depends on.
	 *  This information is then passed to the runtime to determine if
	 *  the privilege is grantable to the grantees by this grantor at
	 *  execution time.
	 *  
	 *  Go through the providers regardless who the grantor is since 
	 *  the statement cache may be in effect.
	 *  
	 * @param td the TableDescriptor to check
	 *
	 * @exception StandardException standard error policy.
	 */
	private void bindPrivilegesForView ( TableDescriptor td) 
		throws StandardException
	{
		LanguageConnectionContext lcc = getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		ViewDescriptor vd = dd.getViewDescriptor(td);
		DependencyManager dm = dd.getDependencyManager();
		ProviderInfo[] pis = dm.getPersistentProviderInfos(vd);
		this.descriptorList = new ArrayList();
					
		int siz = pis.length;
        for (ProviderInfo pi : pis) {
            Provider provider = (Provider) pi.getDependableFinder().getDependable(dd, pi.getObjectId());

            if (provider instanceof TableDescriptor ||
                    provider instanceof ViewDescriptor ||
                    provider instanceof AliasDescriptor) {
                descriptorList.add(provider);
            }
        }
	}

	public boolean modifyActionAllowed() {
		return actionAllowed[MODIFY_ACTION];
	}

	public boolean accessActionAllowed() {
		return actionAllowed[ACCESS_ACTION];
	}

	public boolean selectActionAllowed() {
		return actionAllowed[SELECT_ACTION];
	}

	public boolean getIsAll() {
		return isAll;
	}
}
	
