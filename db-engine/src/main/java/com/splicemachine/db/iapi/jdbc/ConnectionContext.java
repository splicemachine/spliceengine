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

package com.splicemachine.db.iapi.jdbc;

import com.splicemachine.db.iapi.sql.ResultSet;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Interface-ized from EmbedConnectionContext.  Some basic
 * connection attributes that can be obtained from jdbc.
 *
 */
public interface ConnectionContext 
{
	public static final String CONTEXT_ID = "JDBC_ConnectionContext";

	/**
		Get a new connection object equivalent to the call
		<PRE>
		DriverManager.getConnection("jdbc:default:connection");
		</PRE>

		@exception SQLException Parent connection has been closed.
	*/
	public Connection getNestedConnection(boolean internal) throws SQLException;

	/**
	 * Get a jdbc ResultSet based on the execution ResultSet.
	 *
	 * @param executionResultSet	a result set as gotten from execution
	 *	
	 * @exception java.sql.SQLException	on error
	 */	
	public java.sql.ResultSet getResultSet
	(
		ResultSet 				executionResultSet
	) throws java.sql.SQLException;
    
    /**
     * Process the resultSet as a dynamic result for closure.
     * The result set will have been created in a Java procedure.
     * If the ResultSet is a valid dynamic ResultSet for
     * this connection, then it is set up as a dynamic result
     * which includes:
     * <UL>
     * <LI> breaking its link with the JDBC connection
     * that created it, since there is a good chance that connection
     * was closed explicitly by the Java procedure.
     * <LI> marking its activation as single use to ensure the
     * close of the ResultSet will close the activation.
     * </UL>
     * <P>
     * If the result set a valid dynamic result then false will
     * be returned and no action made against it.
     * 
     * @param resultSet ResultSet to process.
     * @return True if this ResultSet was created by this connection
     * and the result set is open. False otherwise.
     */
    public boolean processInaccessibleDynamicResult(java.sql.ResultSet resultSet);
}
