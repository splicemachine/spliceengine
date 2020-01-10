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

package com.splicemachine.dbTesting.functionTests.tests.jdbcapi;

import java.sql.*;

import junit.framework.Test;
import junit.framework.TestSuite;
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.DatabasePropertyTestSetup;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * Tests relative scrolling of a resultset. This is the JUnit conversion of
 * jdbcapi/testRelative test.
 */
public class RelativeTest extends BaseJDBCTestCase {

	public RelativeTest(String name) {
		super(name);
	}

	/**
	 * Test relative scrolling of ResultSet with concurrency set to
	 * CONCUR_READ_ONLY.
	 */
	public void testScrolling_CONCUR_READ_ONLY() throws SQLException {
		int concurrency = ResultSet.CONCUR_READ_ONLY;
		Statement stmt1 = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
				concurrency);
		ResultSet rs = stmt1.executeQuery("select * from testRelative");

		rs.next(); // First Record
		assertEquals("work1", rs.getString("name"));
		rs.relative(2);
		assertEquals("work3", rs.getString("name"));
		assertEquals(false, rs.isFirst());
		assertEquals(false, rs.isLast());
		assertEquals(false, rs.isAfterLast());
		rs.relative(-2);
		assertEquals("work1", rs.getString("name"));

		rs.relative(10);
		try {
			/*
			 * Attempting to move beyond the first/last row in the result set
			 * positions the cursor before/after the the first/last row.
			 * Therefore, attempting to get value will throw an exception.
			 */
			rs.getString("name");
			fail("FAIL - Attempting to read from an invalid row should have " +
					"thrown an exception");
		} catch (SQLException sqle) {
			/**
			 * sets the expected sql state for the expected exceptions,
			 * according to return value of usingDerbyNetClient().
			 */
			String NO_CURRENT_ROW_SQL_STATE = "";
			if (usingDerbyNetClient()) {
				NO_CURRENT_ROW_SQL_STATE = "XJ121";
			} else {
				NO_CURRENT_ROW_SQL_STATE = "24000";
			}
			assertSQLState(NO_CURRENT_ROW_SQL_STATE, sqle);
		}
	}

	/**
	 * Test relative scrolling of ResultSet with concurrency set to
	 * CONCUR_UPDATABLE.
	 */
	public void testScrolling_CONCUR_UPDATABLE() throws SQLException {
		int concurrency = ResultSet.CONCUR_UPDATABLE;
		Statement stmt1 = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
				concurrency);
		ResultSet rs = stmt1.executeQuery("select * from testRelative");

		rs.next(); // First Record
		assertEquals("work1", rs.getString("name"));
		rs.relative(2);
		assertEquals("work3", rs.getString("name"));
		assertEquals(false, rs.isFirst());
		assertEquals(false, rs.isLast());
		assertEquals(false, rs.isAfterLast());
		rs.relative(-2);
		assertEquals("work1", rs.getString("name"));

		rs.relative(10);
		try {
			/*
			 * Attempting to move beyond the first/last row in the result set
			 * positions the cursor before/after the the first/last row.
			 * Therefore, attempting to get value now will throw an exception.
			 */
			rs.getString("name");
			fail("FAIL - Attempting to read from an invalid row should have " +
				"thrown an exception");
		} catch (SQLException sqle) {
			/**
			 * sets the expected sql state for the expected exceptions,
			 * according to return value of usingDerbyNetClient().
			 */
			String NO_CURRENT_ROW_SQL_STATE = "";
			if (usingDerbyNetClient()) {
				NO_CURRENT_ROW_SQL_STATE = "XJ121";
			} else {
				NO_CURRENT_ROW_SQL_STATE = "24000";
			}
			assertSQLState(NO_CURRENT_ROW_SQL_STATE, sqle);
		}
	}

	/**
	 * Runs the test fixtures in embedded and client.
	 * 
	 * @return test suite
	 */
	public static Test suite() {
		TestSuite suite = new TestSuite("RelativeTest");
		suite.addTest(baseSuite("RelativeTest:embedded"));
		suite.addTest(TestConfiguration
				.clientServerDecorator(baseSuite("RelativeTest:client")));
		return suite;
	}

	/**
	 * Base suite of tests that will run in both embedded and client.
	 * 
	 * @param name
	 *            Name for the suite.
	 */
	private static Test baseSuite(String name) {
		TestSuite suite = new TestSuite(name);
		suite.addTestSuite(RelativeTest.class);
		return new CleanDatabaseTestSetup(DatabasePropertyTestSetup
				.setLockTimeouts(suite, 2, 4)) {

			/**
			 * Creates the tables used in the test cases.
			 * 
			 * @exception SQLException
			 *                if a database error occurs
			 */
			protected void decorateSQL(Statement stmt) throws SQLException {
				stmt.execute("create table testRelative("
						+ "name varchar(10), i int)");

				stmt.execute("insert into testRelative values ("
						+ "'work1', NULL)");
				stmt.execute("insert into testRelative values ("
						+ "'work2', NULL)");
				stmt.execute("insert into testRelative values ("
						+ "'work3', NULL)");
				stmt.execute("insert into testRelative values ("
						+ "'work4', NULL)");
			}
		};
	}
}
