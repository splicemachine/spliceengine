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

package com.splicemachine.db.impl.jdbc;

import com.splicemachine.db.jdbc.InternalDriver;
import com.splicemachine.db.iapi.util.InterruptStatus;

import java.sql.SQLException;

/**
	Any class in the embedded JDBC driver (ie this package) that needs to
	refer back to the EmbedConnection object extends this class.
*/

abstract class ConnectionChild {

	/*
	** Local connection is the current EmbedConnection
	** object that we use for all our work.
	*/
	EmbedConnection localConn;

	/**	
		Factory for JDBC objects to be created.
	*/
	final InternalDriver factory;

	/**
		Calendar for data operations.
	*/
	private java.util.Calendar cal;


	ConnectionChild(EmbedConnection conn) {
		super();
		localConn = conn;
		factory = conn.getLocalDriver();
	}

	/**
		Return a reference to the EmbedConnection
	*/
	final EmbedConnection getEmbedConnection() {
		return localConn;
	}

	/**
	 * Return an object to be used for connection
	 * synchronization.
	 */
	final Object getConnectionSynchronization()
	{
		return localConn.getConnectionSynchronization();
	}

	/**
		Handle any exception.
		@see EmbedConnection#handleException
		@exception SQLException thrown if can't handle
	*/
	final SQLException handleException(Throwable t)
			throws SQLException {
		return localConn.handleException(t);
	}

	/**
		If Autocommit is on, note that a commit is needed.
		@see EmbedConnection#needCommit
	 */
	final void needCommit() {
		localConn.needCommit();
	}

	/**
		Perform a commit if one is needed.
		@see EmbedConnection#commitIfNeeded
		@exception SQLException thrown on failure
	 */
	final void commitIfNeeded() throws SQLException {
		//System.out.println(this + " <> " + localConn.getClass());
		//new Throwable("cin").printStackTrace(System.out);
		localConn.commitIfNeeded();
	}

	/**
		Perform a commit if autocommit is enabled.
		@see EmbedConnection#commitIfNeeded
		@exception SQLException thrown on failure
	 */
	final void commitIfAutoCommit() throws SQLException {
		//System.out.println(this + " <> " + localConn.getClass());
		//new Throwable("cin").printStackTrace(System.out);
		localConn.commitIfAutoCommit();
	}

	/**
		Setup the context stack (a.k.a. context manager)
		for this connection.
		@see EmbedConnection#setupContextStack
		@exception SQLException thrown on failure
	 */
	final void setupContextStack() throws SQLException {
		localConn.setupContextStack();
	}

	/**
		Setup the context stack (a.k.a. context manager)
		for this connection.
		@see EmbedConnection#restoreContextStack
		@exception SQLException thrown on failure
	 */
	final void restoreContextStack() throws SQLException {
		localConn.restoreContextStack();
	}

	/**
		Get and save a unique calendar object for this JDBC object.
		No need to synchronize because multiple threads should not
		be using a single JDBC object. Even if they do there is only
		a small window where each would get its own Calendar for a
		single call.
	*/
	java.util.Calendar getCal() {
		if (cal == null)
			cal = new java.util.GregorianCalendar();
		return cal;
	}

	SQLException newSQLException(String messageId) {
		return localConn.newSQLException(messageId);
	}
	SQLException newSQLException(String messageId, Object arg1) {
		return localConn.newSQLException(messageId, arg1);
	}
	SQLException newSQLException(String messageId, Object arg1, Object arg2) {
		return localConn.newSQLException(messageId, arg1, arg2);
	}

    protected static void restoreIntrFlagIfSeen(
        boolean pushStack, EmbedConnection ec) {

        if (pushStack) {
            InterruptStatus.restoreIntrFlagIfSeen(ec.getLanguageConnection());
        } else {
            // no lcc if connection is closed:
            InterruptStatus.restoreIntrFlagIfSeen();
        }
    }
}


