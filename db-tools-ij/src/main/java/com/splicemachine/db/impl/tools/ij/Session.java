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

package com.splicemachine.db.impl.tools.ij;

import com.splicemachine.db.iapi.tools.i18n.LocalizedOutput;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Hashtable;

/**
	Session holds the objects local to a particular database session,
	which starts with a connection and is all other JDBC
	stuff used on that connection, along with some ij state
	that is connection-based as well.

	This is separated out to localize database objects and
	also group objects by session.

 */
class Session {
	static final String DEFAULT_NAME="CONNECTION";

	boolean singleSession = true;
	Connection conn = null;
	String tag, name;
	Hashtable prepStmts = new Hashtable();
	Hashtable cursorStmts = new Hashtable();
	Hashtable cursors = new Hashtable();
	Hashtable asyncStmts = new Hashtable();
	boolean isJCC= false;      // Is this the IBM UNIVERSAL DRIVER.
	boolean isDNC = false;     // Is this the Derby Network Client JDBC Driver
	boolean isEmbeddedDerby = false; // Is this the Derby Embedded JDBC Driver

	Session(Connection newConn, String newTag, String newName) {
		conn = newConn;
		tag = newTag;
		name = newName;

		try
		{
			isJCC = conn.getMetaData().getDriverName().startsWith("IBM DB2 JDBC Universal Driver");
			isDNC = conn.getMetaData().getDriverName().startsWith("Apache Derby Network Client");
			isEmbeddedDerby = conn.getMetaData().getDriverName().
				startsWith("Apache Derby Embedded JDBC Driver");
		}
		catch (SQLException se)
		{
			// if there is a problem getting the driver name we will
			// assume it is not JCC or DNC.
		}
	}

	Connection getConnection() {
		// CHECK: should never be null
		return conn;
	}

	boolean getIsJCC()
	{
		return isJCC;
	}

	boolean getIsDNC()
	{
		return isDNC;
	}

	boolean getIsEmbeddedDerby()
	{
		return isEmbeddedDerby;
	}

	String getName() {
		return name;
	}

	Object addPreparedStatement(String name, PreparedStatement ps) {
		return prepStmts.put(name,ps);
	}

	Object addCursorStatement(String name, Statement s) {
		return cursorStmts.put(name, s);
	}

	Object addCursor(String name, ResultSet rs) {
		return cursors.put(name, rs);
	}

	Object addAsyncStatement(String name, AsyncStatement s) {
		return asyncStmts.put(name, s);
	}

	PreparedStatement getPreparedStatement(String name) {
		return (PreparedStatement) prepStmts.get(name);
	}

	Statement getCursorStatement(String name) {
		return (Statement)cursorStmts.get(name);
	}

	ResultSet getCursor(String name) {
		return (ResultSet)cursors.get(name);
	}

	AsyncStatement getAsyncStatement(String name) {
		return (AsyncStatement)asyncStmts.get(name);
	}

	boolean removePreparedStatement(String name) {
		return prepStmts.remove(name)!=null;
	}

	boolean removeCursorStatement(String name) {
		return cursorStmts.remove(name)!=null;
	}

	boolean removeCursor(String name) {
		return cursors.remove(name)!=null;
	}

    void doPrompt(boolean newStatement, LocalizedOutput out, boolean multiSessions) {
		// check if tag should be increased...
		if (multiSessions && singleSession) {
			singleSession = false;

			if (tag == null) tag = "("+name+")";
			else tag = tag.substring(0,tag.length()-1)+":"+name+")";
		}

		// check if tag should be reduced...
		if (!multiSessions && !singleSession) {
			singleSession = true;

			if (tag == null) {}
			else if (tag.length() == name.length()+2) tag = null;
			else tag = tag.substring(0,tag.length()-2-name.length())+")";
		}

		utilMain.doPrompt(newStatement, out, tag);
	}

	void close() throws SQLException {

		if (!conn.isClosed())
		{
			if  (!conn.getAutoCommit() && name != null && ! name.startsWith("XA"))
				conn.rollback();
			conn.close();
		}

		prepStmts.clear(); // should we check & close them individually?
		cursorStmts.clear();
		cursors.clear();
		asyncStmts.clear();

		conn = null;
	}

}
