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

package com.splicemachine.db.iapi.jdbc;

import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.CallableStatement;

/**
	Provides control over a BrokeredConnection
*/
public interface BrokeredConnectionControl
{
	/**
		Return the real JDBC connection for the brokered connection.
	*/
	EngineConnection	getRealConnection() throws SQLException;

	/**
		Notify the control class that a SQLException was thrown
		during a call on one of the brokered connection's methods.
	*/
	void notifyException(SQLException sqle);


	/**
		Allow control over setting auto commit mode.
	*/
	void checkAutoCommit(boolean autoCommit) throws SQLException;

	/**
		Allow control over creating a Savepoint (JDBC 3.0)
	*/
	void checkSavepoint() throws SQLException;

	/**
		Allow control over calling rollback.
	*/
	void checkRollback() throws SQLException;

	/**
		Allow control over calling commit.
	*/
	void checkCommit() throws SQLException;

    /**
     * Check if the brokered connection can be closed.
     *
     * @throws SQLException if it is not allowed to call close on the brokered
     * connection
     */
	void checkClose() throws SQLException;

	/**
		Can cursors be held across commits.
        @param downgrade true to downgrade the holdability,
        false to throw an exception.
	*/
	int checkHoldCursors(int holdability, boolean downgrade)
        throws SQLException;

	/**
		Returns true if isolation level has been set using JDBC/SQL.
	*/
	boolean isIsolationLevelSetUsingSQLorJDBC() throws SQLException;
	/**
		Reset the isolation level flag used to keep state in 
		BrokeredConnection. It will get set to true when isolation level 
		is set using JDBC/SQL. It will get reset to false at the start
		and the end of a global transaction.
	*/
	void resetIsolationLevelFlag() throws SQLException;

    /**
     * Is this a global transaction
     * @return true if this is a global XA transaction
     */
	boolean isInGlobalTransaction();

	/**
		Close called on BrokeredConnection. If this call
		returns true then getRealConnection().close() will be called.
	*/
	boolean closingConnection() throws SQLException;

	/**
		Optionally wrap a Statement with another Statement.
	*/
	Statement wrapStatement(Statement realStatement) throws SQLException;

	/**
		Optionally wrap a PreparedStatement with another PreparedStatement.
	*/
	PreparedStatement wrapStatement(PreparedStatement realStatement, String sql, Object generateKeys)  throws SQLException;

	/**
		Optionally wrap a CallableStatement with an CallableStatement.
	*/
	CallableStatement wrapStatement(CallableStatement realStatement, String sql) throws SQLException;
        
        /**
         * Close called on the associated PreparedStatement object
         * @param statement PreparedStatement object on which the close event 
         * occurred     
         */
		void onStatementClose(PreparedStatement statement);
        
        /**
         * Error occurred on associated PreparedStatement object
         * @param statement PreparedStatement object on which the 
         * error occured
         * @param sqle      The SQLExeption that caused the error
         */
		void onStatementErrorOccurred(PreparedStatement statement, SQLException sqle);
        
}
