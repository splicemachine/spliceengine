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

package com.splicemachine.db.iapi.db;

import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.conn.ConnectionUtil;
import com.splicemachine.db.impl.sql.execute.TriggerExecutionContext;

import java.sql.SQLException;

/**
 *  <P>
 *  Callers of these methods must be within the context of a
 *  Derby statement execution otherwise a SQLException will be thrown.
 *  <BR>
 *  There are two basic ways to call these methods.
 *  <OL>
 *  <LI>
 *  Within a SQL statement.
 *  <PRE>
 *		-- checkpoint the database
 *		CALL com.splicemachine.db.iapi.db.Factory::
 *				getDatabaseOfConnection().checkpoint();
 *  </PRE>
 *  <LI>
 *  In a server-side JDBC method.
 *  <PRE>
 *		import com.splicemachine.db.iapi.db.*;
 *
 *		...
 *
 *	// checkpoint the database
 *	    Database db = Factory.getDatabaseOfConnection();
 *		db.checkpoint();
 *
 *  </PRE>
 *  </OL>
  This class can only be used within an SQL-J statement, a Java procedure or a server side Java method.
  <p>This class can be accessed using the class alias <code> FACTORY </code> in SQL-J statements.
 */

public class Factory
{


	/**
	<P>
	Returns the Database object associated with the current connection.
		@exception SQLException Not in a connection context.
	**/
	public static com.splicemachine.db.database.Database getDatabaseOfConnection()
		throws SQLException
	{
		// Get the current language connection context.  This is associated
		// with the current database.
		LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
		return lcc.getDatabase();
	}

	/** 
	 * Get the TriggerExecutionContext for the current connection
	 * of the connection.
	 *
	 * @return the TriggerExecutionContext if called from the context
	 * of a trigger; otherwise, null.

		@exception SQLException Not in a connection or trigger context.
	 */
	public static TriggerExecutionContext getTriggerExecutionContext()
		throws SQLException
	{
		LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
		return lcc.getTriggerExecutionContext();
	}
}
