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

package com.splicemachine.db.impl.sql.conn;

import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.util.IdUtil;
import com.splicemachine.db.iapi.util.StringUtil;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.services.property.PropertyUtil;
import com.splicemachine.db.iapi.services.property.PersistentSet;
import com.splicemachine.db.catalog.types.RoutineAliasInfo;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.StatementPermission;
import java.util.List;
import java.util.Iterator;

class GenericAuthorizer
implements Authorizer
{
	//
	//Enumerations for user access levels.
	private static final int NO_ACCESS = 0;
	private static final int READ_ACCESS = 1;
	private static final int FULL_ACCESS = 2;
	
	//
	//Configurable userAccessLevel - derived from Database level
	//access control lists + database boot time controls.
	private int userAccessLevel;

	//
	//Connection's readOnly status
    boolean readOnlyConnection;

	private final LanguageConnectionContext lcc;
	
    GenericAuthorizer(LanguageConnectionContext lcc)
		 throws StandardException
	{
		this.lcc = lcc;

		refresh();
	}

	/*
	  Return true if the connection must remain readOnly
	  */
	private boolean connectionMustRemainReadOnly()
	{
		if (lcc.getDatabase().isReadOnly() ||
			(userAccessLevel==READ_ACCESS))
			return true;
		else
			return false;
	}

	/**
	  Used for operations that do not involve tables or routines.
     
	  @see Authorizer#authorize
	  @exception StandardException Thrown if the operation is not allowed
	*/
	public void authorize( int operation) throws StandardException
	{
		authorize( (Activation) null, operation);
	}

	/**
	  @see Authorizer#authorize
	  @exception StandardException Thrown if the operation is not allowed
	 */
	public void authorize( Activation activation, int operation) throws StandardException
	{
		int sqlAllowed = lcc.getStatementContext().getSQLAllowed();

		switch (operation)
		{
		case Authorizer.SQL_ARBITARY_OP:
		case Authorizer.SQL_CALL_OP:
			if (sqlAllowed == RoutineAliasInfo.NO_SQL)
				throw externalRoutineException(operation, sqlAllowed);
			break;
		case Authorizer.SQL_SELECT_OP:
			if (sqlAllowed > RoutineAliasInfo.READS_SQL_DATA)
				throw externalRoutineException(operation, sqlAllowed);
			break;

		// SQL write operations
		case Authorizer.SQL_WRITE_OP:
		case Authorizer.PROPERTY_WRITE_OP:
			if (isReadOnlyConnection())
				throw StandardException.newException(SQLState.AUTH_WRITE_WITH_READ_ONLY_CONNECTION);
			if (sqlAllowed > RoutineAliasInfo.MODIFIES_SQL_DATA)
				throw externalRoutineException(operation, sqlAllowed);
			break;

		// SQL DDL operations
		case Authorizer.JAR_WRITE_OP:
		case Authorizer.SQL_DDL_OP:
 			if (isReadOnlyConnection())
				throw StandardException.newException(SQLState.AUTH_DDL_WITH_READ_ONLY_CONNECTION);

			if (sqlAllowed > RoutineAliasInfo.MODIFIES_SQL_DATA)
				throw externalRoutineException(operation, sqlAllowed);
			break;

		default:
			if (SanityManager.DEBUG)
				SanityManager.THROWASSERT("Bad operation code "+operation);
		}
        if( activation != null)
        {
            List requiredPermissionsList = activation.getPreparedStatement().getRequiredPermissionsList();
            DataDictionary dd = lcc.getDataDictionary();

            // Database Owner can access any object. Ignore 
            // requiredPermissionsList for Database Owner
            if( requiredPermissionsList != null    && 
                !requiredPermissionsList.isEmpty() && 
                !lcc.getCurrentUserId(activation).equals(
                    dd.getAuthorizationDatabaseOwner()))
            {

                 /*
                  * The system may need to read the permission descriptor(s) 
                  * from the system table(s) if they are not available in the 
                  * permission cache.  So start an internal read-only nested 
                  * transaction for this.
                  * 
                  * The reason to use a nested transaction here is to not hold
                  * locks on system tables on a user transaction.  e.g.:  when
                  * attempting to revoke an user, the statement may time out
                  * since the user-to-be-revoked transaction may have acquired 
                  * shared locks on the permission system tables; hence, this
                  * may not be desirable.  
                  * 
                  * All locks acquired by StatementPermission object's check()
                  * method will be released when the system ends the nested 
                  * transaction.
                  * 
                  * In Derby, the locks from read nested transactions come from
                  * the same space as the parent transaction; hence, they do not
                  * conflict with parent locks.
                  */  
                lcc.beginNestedTransaction(true);
            	
                try 
                {
                    try 
                    {
                    	// perform the permission checking
                        for (Iterator iter = requiredPermissionsList.iterator(); 
                            iter.hasNext();) 
                        {
                            ((StatementPermission) iter.next()).check
                                (lcc, false, activation);
                        }
                    } 
                    finally
                    {
                    }
                } 
                finally 
                {
                	// make sure we commit; otherwise, we will end up with 
                	// mismatch nested level in the language connection context.
                    lcc.commitNestedTransaction();
                }
            }
        }
    }

	private static StandardException externalRoutineException(int operation, int sqlAllowed) {

		String sqlState;
		if (sqlAllowed == RoutineAliasInfo.READS_SQL_DATA)
			sqlState = SQLState.EXTERNAL_ROUTINE_NO_MODIFIES_SQL;
		else if (sqlAllowed == RoutineAliasInfo.CONTAINS_SQL)
		{
			switch (operation)
			{
			case Authorizer.SQL_WRITE_OP:
			case Authorizer.PROPERTY_WRITE_OP:
			case Authorizer.JAR_WRITE_OP:
			case Authorizer.SQL_DDL_OP:
				sqlState = SQLState.EXTERNAL_ROUTINE_NO_MODIFIES_SQL;
				break;
			default:
				sqlState = SQLState.EXTERNAL_ROUTINE_NO_READS_SQL;
				break;
			}
		}
		else
			sqlState = SQLState.EXTERNAL_ROUTINE_NO_SQL;

		return StandardException.newException(sqlState);
	}
	

	private void getUserAccessLevel() throws StandardException
	{
		userAccessLevel = NO_ACCESS;
		if (userOnAccessList(Property.FULL_ACCESS_USERS_PROPERTY))
			userAccessLevel = FULL_ACCESS;

		if (userAccessLevel == NO_ACCESS &&
			userOnAccessList(Property.READ_ONLY_ACCESS_USERS_PROPERTY))
			userAccessLevel = READ_ACCESS;

		if (userAccessLevel == NO_ACCESS)
			userAccessLevel = getDefaultAccessLevel();
	}

	private int getDefaultAccessLevel() throws StandardException
	{
		PersistentSet tc = lcc.getTransactionExecute();

		String modeS = (String)
			PropertyUtil.getServiceProperty(
									tc,
									Property.DEFAULT_CONNECTION_MODE_PROPERTY);
		if (modeS == null)
			return FULL_ACCESS;
		else if(StringUtil.SQLEqualsIgnoreCase(modeS, Property.NO_ACCESS))
			return NO_ACCESS;
		else if(StringUtil.SQLEqualsIgnoreCase(modeS, Property.READ_ONLY_ACCESS))
			return READ_ACCESS;
		else if(StringUtil.SQLEqualsIgnoreCase(modeS, Property.FULL_ACCESS))
			return FULL_ACCESS;
		else
		{
			if (SanityManager.DEBUG)
				SanityManager.THROWASSERT("Invalid value for property "+
										  Property.DEFAULT_CONNECTION_MODE_PROPERTY+
										  " "+
										  modeS);
 			return FULL_ACCESS;
		}
	}

	private boolean userOnAccessList(String listName) throws StandardException {
		PersistentSet tc = lcc.getTransactionExecute();
		//String listS = (String) PropertyUtil.getServiceProperty(tc, listName);
		// XXX = TODO JLEACH: Fix This...
		String listS = null;
        return IdUtil.idOnList(lcc.getSessionUserId(),listS);
	}

	/**
	  @see Authorizer#isReadOnlyConnection
	 */
	public boolean isReadOnlyConnection()
	{
		return readOnlyConnection;
	}

	/**
	  @see Authorizer#isReadOnlyConnection
	  @exception StandardException Thrown if the operation is not allowed
	 */
	public void setReadOnlyConnection(boolean on, boolean authorize)
		 throws StandardException
	{
		if (authorize && !on) {
			if (connectionMustRemainReadOnly())
				throw StandardException.newException(SQLState.AUTH_CANNOT_SET_READ_WRITE);
		}
		readOnlyConnection = on;
	}

	/**
	  @see Authorizer#refresh
	  @exception StandardException Thrown if the operation is not allowed
	  */
	public void refresh() throws StandardException
	{
		getUserAccessLevel();
		if (!readOnlyConnection)
			readOnlyConnection = connectionMustRemainReadOnly();

		// Is a connection allowed.
		if (userAccessLevel == NO_ACCESS)
			throw StandardException.newException(SQLState.AUTH_DATABASE_CONNECTION_REFUSED);
	}
	
}
