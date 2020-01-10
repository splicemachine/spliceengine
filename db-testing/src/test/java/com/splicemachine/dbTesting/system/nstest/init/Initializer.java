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

package com.splicemachine.dbTesting.system.nstest.init;

import java.sql.SQLException;
import java.sql.DriverManager;
import java.sql.Connection;

import com.splicemachine.dbTesting.system.nstest.NsTest;
import com.splicemachine.dbTesting.system.nstest.utils.DbUtil;

/**
 * Initializer: Main Class that populates the tables needed for the test
 */
public class Initializer {

	private String thread_id;

	private DbUtil dbutil;

	public Initializer(String name) {
		this.thread_id = name;
		dbutil = new DbUtil(this.thread_id);
	}

	// This starts the acutal inserts
	public void startInserts() {

		Connection conn = null;
		int insertsRemaining = NsTest.MAX_INITIAL_ROWS;

		// The JDBC driver should have been loaded by nstest.java at this
		// point, we just need to get a connection to the database
		try {

			System.out.println(thread_id
					+ " is getting a connection to the database...");

			if (NsTest.embeddedMode) {
				conn = DriverManager.getConnection(NsTest.embedDbURL,
						NsTest.prop);
			} else {
				if(NsTest.driver_type.equalsIgnoreCase("DerbyClient")) {
					System.out.println("-->Using db client url");
					conn = DriverManager.getConnection(NsTest.clientDbURL,
							NsTest.prop);
				}
			}
		} catch (Exception e) {
			System.out.println("FAIL: " + thread_id
					+ " could not get the database connection");
			printException("getting database connection in startInserts()", e);
		}

		// add one to the statistics of client side connections made per jvm
		NsTest.addStats(NsTest.CONNECTIONS_MADE, 1);
		System.out.println("Connection number: " + NsTest.numConnections);

		// set autocommit to false to keep transaction control in your hand
		if (NsTest.AUTO_COMMIT_OFF) {
			try {

				conn.setAutoCommit(false);
			} catch (Exception e) {
				System.out.println("FAIL: " + thread_id
						+ "'s setAutoCommit() failed:");
				printException("setAutoCommit() in Initializer", e);
			}
		}

		while (insertsRemaining-- >= 0) {
			try {
				int numInserts = dbutil.add_one_row(conn, thread_id);
				//System.out.println("Intializer.java: exited add_one_row: "
				//		+ numInserts + " rows");
			} catch (Exception e) {
				System.out.println(" FAIL: " + thread_id
						+ " unexpected exception:");
				printException("add_one_row() in Initializer", e);
				break;
			}
		}// end of while(insertsRemaning-- > 0)

		// commit the huge bulk Insert!
		if (NsTest.AUTO_COMMIT_OFF) {
			try {
				conn.commit();
			} catch (Exception e) {
				System.out
						.println("FAIL: " + thread_id + "'s commit() failed:");
				printException("commit in Initializer", e);
			}
		}

	}// end of startInserts()

	// ** This method abstracts exception message printing for all exception
	// messages. You may want to change
	// ****it if more detailed exception messages are desired.
	// ***Method is synchronized so that the output file will contain sensible
	// stack traces that are not
	// ****mixed but rather one exception printed at a time
	public synchronized void printException(String where, Exception e) {
		if (e instanceof SQLException) {
			SQLException se = (SQLException) e;

			if (se.getSQLState().equals("40001"))
				System.out.println("deadlocked detected");
			if (se.getSQLState().equals("40XL1"))
				System.out.println(" lock timeout exception");
			if (se.getSQLState().equals("23500"))
				System.out.println(" duplicate key violation");
			if (se.getNextException() != null) {
				String m = se.getNextException().getSQLState();
				System.out.println(se.getNextException().getMessage()
						+ " SQLSTATE: " + m);
			}
		}
		if (e.getMessage() == null) {
			e.printStackTrace(System.out);
		}
		System.out.println("During - " + where
				+ ", the exception thrown was : " + e.getMessage());
	}

}
