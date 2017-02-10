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

import java.sql.*;

/**
	Provides control over a BrokeredStatement, BrokeredPreparedStatement or BrokeredCallableStatement
*/
public interface BrokeredStatementControl
{
	/**
		Can cursors be held across commits.
        Returns the holdability that should be
        used which may be different from the passed
        in holdabilty.
	*/
	public int checkHoldCursors(int holdability) throws SQLException;

	/**
	 * Close the real JDBC Statement when this is controlling a Statement.
	 * @throws SQLException
	 */
	public void closeRealStatement() throws SQLException;
	
	/**
	 * Close the real JDBC CallableStatement when this is controlling a
	 * CallableStatement. 
	 * @throws SQLException
	 */
	public void closeRealCallableStatement() throws SQLException;
	
	/**
	 * Close the real JDBC CallableStatement when this is controlling a
	 * PreparedStatement. 
	 * @throws SQLException
	 */
	public void closeRealPreparedStatement() throws SQLException;
	
	/**
		Return the real JDBC statement for the brokered statement
		when this is controlling a Statement.
	*/
	public Statement	getRealStatement() throws SQLException;

	/**
		Return the real JDBC PreparedStatement for the brokered statement
		when this is controlling a PreparedStatement.
	*/
	public PreparedStatement	getRealPreparedStatement() throws SQLException;


	/**
		Return the real JDBC CallableStatement for the brokered statement
		when this is controlling a CallableStatement.
	*/
	public CallableStatement	getRealCallableStatement() throws SQLException;

	/**
		Optionally wrap a returned ResultSet in another ResultSet.
        @param s Statement that created the ResultSet.
	*/
	public ResultSet	wrapResultSet(Statement s, ResultSet rs);

    /**
     * Return the exception factory for the underlying connection.
     * @return an exception factory instance
     */
    public ExceptionFactory getExceptionFactory();
}
