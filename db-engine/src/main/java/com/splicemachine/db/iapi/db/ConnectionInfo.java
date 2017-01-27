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

package com.splicemachine.db.iapi.db;

import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.conn.ConnectionUtil;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.error.PublicAPI;
import java.sql.SQLException;

/**
 *
 * ConnectionInfo class provides static methods for getting information 
 * related to a JDBC connection.
 * 
 * When called from within the query language,
 * each method returns information about the connection from which it was called.
 * <p>
 * Use the methods of this class only within an SQL-J statement; do not call 
 * them directly. 
 */
 
public abstract class ConnectionInfo
{

	/** no requirement for a constructor */
	private ConnectionInfo() {}

	/**
	 * Get the last autoincrement value inserted into the column by 
	 * a statement in this connection.

	 <BR><B> In JDBC 3.0 an application should use the standard methods provided by
	 JDBC 3.0 to obtain generated key values. See java.sql.Statement.getGeneratedKeys().</B>
	 * 
	 * @param 	schemaName		Name of the schema.
	 * @param	tableName		Name of the table.
	 * @param 	columnName		Name of the column.
	 * 
	 * @return  the last value to be inserted into the named autoincrement
	 * column by this connection. Returns null if this connection has never
	 * inserted into this column.
	 *
	 * @exception SQLException if the current connection could not be
	 * 			  established properly.
	 */
	public static Long lastAutoincrementValue(String schemaName, 
											  String tableName,
											  String columnName)
			throws SQLException								  
	{
		// a static method can manipulate lcc?
		LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
		return lcc.lastAutoincrementValue(schemaName, tableName, columnName);
	}	
	
	/**
	 * <B>INTERNAL USE ONLY</B>
	 * (<B>THIS METHOD MAY BE REMOVED IN A FUTURE RELEASE</B>.)
	 * @throws SQLException on error
	 **/
	public static long nextAutoincrementValue(String schemaName,
											  String tableName,
											  String columnName
											  )
	     throws SQLException
	{
		LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
		try
		{ 
			return 
				lcc.nextAutoincrementValue(schemaName, tableName, columnName);
		}
		catch (StandardException se)
		{ 
			throw PublicAPI.wrapStandardException(se);
		}
	}	
}

