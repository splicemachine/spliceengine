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

//depot/main/java/com.splicemachine.db.impl.jdbc/EmbedConnectionContext.java#24 - edit change 16899 (text)
package com.splicemachine.db.impl.jdbc;

// This is the recommended super-class for all contexts.
import com.splicemachine.db.iapi.services.context.ContextImpl;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.sql.conn.StatementContext;
import com.splicemachine.db.iapi.jdbc.ConnectionContext;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.ResultSet;

import com.splicemachine.db.iapi.error.ExceptionSeverity;
import java.sql.SQLException;

/**
 */
public class EmbedConnectionContext extends ContextImpl
		implements ConnectionContext
{

	/**
		We hold a soft reference to the connection so that when the application
		releases its reference to the Connection without closing it, its finalize
		method will be called, which will then close the connection. If a direct
		reference is used here, such a Connection will never be closed or garbage
		collected as modules hold onto the ContextManager and thus there would
		be a direct reference through this object.
	*/
	private java.lang.ref.SoftReference	connRef;


	public EmbedConnectionContext(ContextManager cm, EmbedConnection conn) {
		super(cm, ConnectionContext.CONTEXT_ID);

		connRef = new java.lang.ref.SoftReference(conn);
	}

	public void cleanupOnError(Throwable error) {

		if (connRef == null)
			return;

		EmbedConnection conn = (EmbedConnection) connRef.get();

		if (error instanceof StandardException) {

			StandardException se = (StandardException) error;
			if (se.getSeverity() < ExceptionSeverity.SESSION_SEVERITY) {

				// any error in auto commit mode that does not close the
				// session will cause a rollback, thus remvoing the need
				// for any commit. We could check this flag under autoCommit
				// being true but the flag is ignored when autoCommit is false
				// so why add the extra check
				if (conn != null) {
					conn.needCommit = false;
				}
				return;
			}
		}

		// This may be a transaction without connection.
		if (conn != null)
			conn.setInactive(); // make the connection inactive & empty

		connRef = null;
		popMe();
	}

	//public java.sql.Connection getEmbedConnection()
	//{
	///	return conn;
	//}

	/**
		Get a connection equivalent to the call
		<PRE>
		DriverManager.getConnection("jdbc:default:connection");
		</PRE>
	*/
	public java.sql.Connection getNestedConnection(boolean internal) throws SQLException {

		EmbedConnection conn = (EmbedConnection) connRef.get();

		if ((conn == null) || conn.isClosed())
			throw Util.noCurrentConnection();

		if (!internal) {
			StatementContext sc = conn.getLanguageConnection().getStatementContext();
			if ((sc == null) || (sc.getSQLAllowed() < com.splicemachine.db.catalog.types.RoutineAliasInfo.MODIFIES_SQL_DATA))
				throw Util.noCurrentConnection();
		}

		return conn.getLocalDriver().getNewNestedConnection(conn);
	}

	/**
	 * Get a jdbc ResultSet based on the execution ResultSet.
	 *
	 * @param executionResultSet	a result set as gotten from execution
	 *	
	 */	
	public java.sql.ResultSet getResultSet
	(
		ResultSet 				executionResultSet
	) throws SQLException
	{
		EmbedConnection conn = (EmbedConnection) connRef.get();

		return conn.getLocalDriver().newEmbedResultSet(conn, executionResultSet,
							false, (EmbedStatement) null, true);
	}

    /**
     * Process a ResultSet from a procedure to be a dynamic result,
     * but one that will be closed due to it being inaccessible. We cannot simply
     * close the ResultSet as it the nested connection that created
     * it might be closed, leading to its close method being a no-op.
     * This performs all the conversion (linking the ResultSet
     * to a valid connection) required but does not close
     * the ResultSet.
     * 
     *   @see EmbedStatement#processDynamicResult(EmbedConnection, java.sql.ResultSet, EmbedStatement)
     */
    public boolean processInaccessibleDynamicResult(java.sql.ResultSet resultSet) {
        EmbedConnection conn = (EmbedConnection) connRef.get();
        if (conn == null)
            return false;
        
        // Pass in null as the Statement to own the ResultSet since
        // we don't have one since the dynamic result will be inaccessible.
        return EmbedStatement.processDynamicResult(conn, resultSet, null) != null;
    }
}
