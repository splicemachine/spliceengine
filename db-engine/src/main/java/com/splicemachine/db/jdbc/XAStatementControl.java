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

package com.splicemachine.db.jdbc;

import com.splicemachine.db.iapi.jdbc.BrokeredConnection;
import com.splicemachine.db.iapi.jdbc.BrokeredStatementControl;
import com.splicemachine.db.iapi.jdbc.BrokeredStatement;
import com.splicemachine.db.iapi.jdbc.BrokeredPreparedStatement;
import com.splicemachine.db.iapi.jdbc.BrokeredCallableStatement;
import com.splicemachine.db.iapi.jdbc.ExceptionFactory;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedResultSet;
import com.splicemachine.db.impl.jdbc.EmbedStatement;
import com.splicemachine.db.impl.jdbc.EmbedPreparedStatement;

import java.sql.*;

/**
	The Statement returned by an Connection returned by a XAConnection
	needs to float across the underlying real connections. We do this by implementing
	a wrapper statement.
*/
final class XAStatementControl implements BrokeredStatementControl {

	/**
	*/
	private final EmbedXAConnection	xaConnection;
	private final BrokeredConnection	applicationConnection;
	BrokeredStatement		applicationStatement;
	private EmbedConnection	realConnection;
	private Statement			realStatement;
	private PreparedStatement	realPreparedStatement;
	private CallableStatement	realCallableStatement;

	private XAStatementControl(EmbedXAConnection xaConnection) {
		this.xaConnection = xaConnection;
		this.realConnection = xaConnection.realConnection;
		this.applicationConnection = xaConnection.currentConnectionHandle;
	}

	XAStatementControl(EmbedXAConnection xaConnection, 
                                Statement realStatement) throws SQLException {
		this(xaConnection);
		this.realStatement = realStatement;
		this.applicationStatement = applicationConnection.newBrokeredStatement(this);
        
        ((EmbedStatement) realStatement).setApplicationStatement(
                applicationStatement);
	}
	XAStatementControl(EmbedXAConnection xaConnection, 
                PreparedStatement realPreparedStatement, 
                String sql, Object generatedKeys) throws SQLException {            
		this(xaConnection);
		this.realPreparedStatement = realPreparedStatement;
		this.applicationStatement = applicationConnection.newBrokeredStatement(this, sql, generatedKeys);
        ((EmbedStatement) realPreparedStatement).setApplicationStatement(
                applicationStatement);
	}
	XAStatementControl(EmbedXAConnection xaConnection, 
                CallableStatement realCallableStatement, 
                String sql) throws SQLException {
		this(xaConnection);
		this.realCallableStatement = realCallableStatement;
		this.applicationStatement = applicationConnection.newBrokeredStatement(this, sql);
        ((EmbedStatement) realCallableStatement).setApplicationStatement(
                applicationStatement);
	}

	/**
	 * Close the realStatement within this control. 
	 */
	public void closeRealStatement() throws SQLException {
		realStatement.close();
	}
	
	/**
	 * Close the realCallableStatement within this control. 
	 */
	public void closeRealCallableStatement() throws SQLException {
		realCallableStatement.close();
	}
	
	/**
	 * Close the realPreparedStatement within this control. 
	 */
	public void closeRealPreparedStatement() throws SQLException {
		realPreparedStatement.close();
	}
	
	public Statement getRealStatement() throws SQLException {

		// 
		if (applicationConnection == xaConnection.currentConnectionHandle) {

			// Application connection is the same.
			if (realConnection == xaConnection.realConnection)
				return realStatement;

			// If we switched back to a local connection, and the first access is through
			// a non-connection object (e.g. statement then realConnection will be null)
			if (xaConnection.realConnection == null) {
				// force the connection
				xaConnection.getRealConnection();
			}

			// underlying connection has changed.
			// create new Statement
			Statement newStatement = applicationStatement.createDuplicateStatement(xaConnection.realConnection, realStatement);
			((EmbedStatement) realStatement).transferBatch((EmbedStatement) newStatement);
 
			try {
				realStatement.close();
			} catch (SQLException ignored) {
			}

			realStatement = newStatement;
			realConnection = xaConnection.realConnection;
            ((EmbedStatement) realStatement).setApplicationStatement(
                    applicationStatement);
		}
		else {
			// application connection is different, therefore the outer application
			// statement is closed, so just return the realStatement. It should be
			// closed by virtue of its application connection being closed.
		}
		return realStatement;
	}

	public PreparedStatement getRealPreparedStatement() throws SQLException {
		// 
		if (applicationConnection == xaConnection.currentConnectionHandle) {
			// Application connection is the same.
			if (realConnection == xaConnection.realConnection)
				return realPreparedStatement;

			// If we switched back to a local connection, and the first access is through
			// a non-connection object (e.g. statement then realConnection will be null)
			if (xaConnection.realConnection == null) {
				// force the connection
				xaConnection.getRealConnection();
			}

			// underlying connection has changed.
			// create new PreparedStatement
			PreparedStatement newPreparedStatement =
				((BrokeredPreparedStatement) applicationStatement).createDuplicateStatement(xaConnection.realConnection, realPreparedStatement);


			// ((EmbedStatement) realPreparedStatement).transferBatch((EmbedStatement) newPreparedStatement);
			((EmbedPreparedStatement) realPreparedStatement).transferParameters((EmbedPreparedStatement) newPreparedStatement);

			try {
				realPreparedStatement.close();
			} catch (SQLException ignored) {
			}

			realPreparedStatement = newPreparedStatement;
			realConnection = xaConnection.realConnection;
            ((EmbedStatement) realPreparedStatement).setApplicationStatement(
                        applicationStatement);
		}
		else {
			// application connection is different, therefore the outer application
			// statement is closed, so just return the realStatement. It should be
			// closed by virtue of its application connection being closed.
		}
		return realPreparedStatement;
	}

	public CallableStatement getRealCallableStatement() throws SQLException {
		if (applicationConnection == xaConnection.currentConnectionHandle) {
			// Application connection is the same.
			if (realConnection == xaConnection.realConnection)
				return realCallableStatement;

			// If we switched back to a local connection, and the first access is through
			// a non-connection object (e.g. statement then realConnection will be null)
			if (xaConnection.realConnection == null) {
				// force the connection
				xaConnection.getRealConnection();
			}

			// underlying connection has changed.
			// create new PreparedStatement
			CallableStatement newCallableStatement =
				((BrokeredCallableStatement) applicationStatement).createDuplicateStatement(xaConnection.realConnection, realCallableStatement);

			((EmbedStatement) realCallableStatement).transferBatch((EmbedStatement) newCallableStatement);

			try {
				realCallableStatement.close();
			} catch (SQLException ignored) {
			}

			realCallableStatement = newCallableStatement;
			realConnection = xaConnection.realConnection;
            ((EmbedStatement) realCallableStatement).setApplicationStatement(
                    applicationStatement);
		}
		else {
			// application connection is different, therefore the outer application
			// statement is closed, so just return the realStatement. It should be
			// closed by virtue of its application connection being closed.
		}
		return realCallableStatement;
	}

    /**
     * Don't need to wrap the ResultSet but do need to update its
     * application Statement reference to be the one the application
     * used to create the ResultSet.
     */
	public ResultSet wrapResultSet(Statement s, ResultSet rs) {
        if (rs != null)
            ((EmbedResultSet) rs).setApplicationStatement(s);
		return rs;
	}

	/**
		Can cursors be held across commits.
	*/
	public int checkHoldCursors(int holdability) throws SQLException {
		return xaConnection.checkHoldCursors(holdability, true);
 	}

    /**
     * Return the exception factory for the underlying connection.
     */
    public ExceptionFactory getExceptionFactory() {
        return applicationConnection.getExceptionFactory();
    }
}
