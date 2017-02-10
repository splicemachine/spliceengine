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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.store.access.TransactionController;

import java.util.List;

/**
 * A set of of logic use by both table and schema permissions
 * The structure of the table for table and schema permissions are similar
 * so we regroup the share functionality in that class.
 *
 */

public abstract class BasicPrivilegeInfo extends PrivilegeInfo {
	// Action types
	public static final int SELECT_ACTION = 0;
	public static final int DELETE_ACTION = 1;
	public static final int INSERT_ACTION = 2;
	public static final int UPDATE_ACTION = 3;
	public static final int REFERENCES_ACTION = 4;
	public static final int TRIGGER_ACTION = 5;
	public static final int ACTION_COUNT = 6;

	protected static final String YES_WITH_GRANT_OPTION = "Y";
	protected static final String YES_WITHOUT_GRANT_OPTION = "y";
	protected static final String NO = "N";

	protected static final String[][] actionString =
			{{"s", "S"}, {"d", "D"}, {"i", "I"}, {"u", "U"}, {"r", "R"}, {"t", "T"}};

	protected boolean[] actionAllowed;
	protected FormatableBitSet[] columnBitSets;
	protected List descriptorList;


	protected String getPermString(int action, boolean forGrantOption) {
		if (actionAllowed[action] && columnBitSets[action] == null)
			return forGrantOption ? YES_WITH_GRANT_OPTION : YES_WITHOUT_GRANT_OPTION;
		else
			return NO;
	} // end of getPermString

	protected String getActionString(int action, boolean forGrantOption) {
		return actionString[action][forGrantOption ? 1 : 0];
	}


	/**
	 * Determine the schema for a particular table and then
	 * determine is the user is the owner.
	 * There is some specific logic to determine the schema depending
	 * of the nature of the table
	 * @param user
	 * @param td
	 * @param sd
	 * @param dd
	 * @param lcc
	 * @param grant
	 * @throws StandardException
	 */
	protected void checkOwnership(String user,
								  TableDescriptor td,
								  SchemaDescriptor sd,
								  DataDictionary dd,
								  LanguageConnectionContext lcc,
								  boolean grant)
			throws StandardException {
		super.checkOwnership(user, sd, dd);

		// additional check specific to this subclass
		if (grant) {
			checkPrivileges(user, td, sd, dd, lcc);
		}
	}

	/**
	 * Determines if the user is the ownership of the schema
	 * Throws a exception if the user is not
	 * @see BasicPrivilegeInfo#checkOwnership(String, SchemaDescriptor, DataDictionary, LanguageConnectionContext, boolean)
	 *
	 * Basically the same but we set the table descriptor to null
	 * @param user
	 * @param sd
	 * @param dd
	 * @param lcc
	 * @param grant
	 * @throws StandardException
	 */

	protected void checkOwnership(String user,
								  SchemaDescriptor sd,
								  DataDictionary dd,
								  LanguageConnectionContext lcc,
								  boolean grant)
			throws StandardException{
		checkOwnership(user,null,sd,dd,lcc,grant);
	}


	/**
	 * Determines if the privilege is grantable by this grantor
	 * for the given view.
	 * <p>
	 * Note that the database owner can access database objects
	 * without needing to be their owner.  This method should only
	 * be called if it is a GRANT.
	 *
	 * @param user authorizationId of current user
	 * @param td   TableDescriptor to be checked against
	 * @param sd   SchemaDescriptor
	 * @param dd   DataDictionary
	 * @param lcc  LanguageConnectionContext
	 * @throws StandardException if user does not have permission to grant
	 */
	protected void checkPrivileges(String user,
								   TableDescriptor td,
								   SchemaDescriptor sd,
								   DataDictionary dd,
								   LanguageConnectionContext lcc)
			throws StandardException {
		if (user.equals(dd.getAuthorizationDatabaseOwner())) return;

		if (td == null) return;
		//  check view specific
		if (td.getTableType() == TableDescriptor.VIEW_TYPE) {
			if (descriptorList != null) {
				TransactionController tc = lcc.getTransactionExecute();
				int siz = descriptorList.size();
				for (int i = 0; i < siz; i++) {
					TupleDescriptor p;
					SchemaDescriptor s = null;

					p = (TupleDescriptor) descriptorList.get(i);
					if (p instanceof TableDescriptor) {
						TableDescriptor t = (TableDescriptor) p;
						s = t.getSchemaDescriptor();
					} else if (p instanceof ViewDescriptor) {
						ViewDescriptor v = (ViewDescriptor) p;
						s = dd.getSchemaDescriptor(v.getCompSchemaId(), tc);
					} else if (p instanceof AliasDescriptor) {
						AliasDescriptor a = (AliasDescriptor) p;
						s = dd.getSchemaDescriptor(a.getSchemaUUID(), tc);
					}

					if (s != null && !user.equals(s.getAuthorizationId())) {
						throw StandardException.newException(
								SQLState.AUTH_NO_OBJECT_PERMISSION,
								user,
								"grant",
								sd.getSchemaName(),
								td.getName());
					}

					// FUTURE: if object is not own by grantor then check if
					//         the grantor have grant option.
				}
			}
		}


	}


}


