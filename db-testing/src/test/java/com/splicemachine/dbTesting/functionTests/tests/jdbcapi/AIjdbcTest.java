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
package com.splicemachine.dbTesting.functionTests.tests.jdbcapi;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * Test execution of JDBC method, isAutoincrement.
 */
public class AIjdbcTest extends BaseJDBCTestCase {

	/**
	 * Basic constructor.
	 */
	public AIjdbcTest(String name) {
		super(name);
	}
	
	/**
	 * Returns the implemented tests.
	 * 
	 * @return An instance of <code>Test</code> with the implemented tests to
	 *         run.
	 */
	public static Test suite() {
		return new CleanDatabaseTestSetup(TestConfiguration.defaultSuite(AIjdbcTest.class, false)) {
			protected void decorateSQL(Statement stmt) throws SQLException {
				stmt.execute("create table tab1 (x int, y int generated always as identity,z char(2))");
				stmt.execute("create view tab1_view (a,b) as select y,y+1 from tab1");
			}
		};
	}
	
	/**
	 * Sets the auto commit to false.
	 */
	protected void initializeConnection(Connection conn) throws SQLException {
		conn.setAutoCommit(false);
	}
	
	/**
	 * Select from base table.
	 * 
	 * @throws SQLException
	 */
	public void testSelect() throws SQLException {
		Statement s = createStatement();
		ResultSet rs;
		ResultSetMetaData rsmd;
		
		rs = s.executeQuery("select x,z from tab1");
		rsmd = rs.getMetaData();
		
		assertFalse("Column count doesn't match.", rsmd.getColumnCount() != 2);
		assertFalse("Column 1 is NOT ai.", rsmd.isAutoIncrement(1));
		assertFalse("Column 2 is NOT ai.", rsmd.isAutoIncrement(2));
		
		rs.close();
		
		rs = s.executeQuery("select y, x,z from tab1");
		rsmd = rs.getMetaData();
		
		assertFalse("Column count doesn't match.", rsmd.getColumnCount() != 3);
		assertFalse("Column 1 IS ai.", !rsmd.isAutoIncrement(1));
		assertFalse("Column 2 is NOT ai.", rsmd.isAutoIncrement(2));
		assertFalse("Column 3 is NOT ai.", rsmd.isAutoIncrement(3));
		
		rs.close();
		s.close();
	}

	/**
	 * Select from view.
	 * 
	 * @throws SQLException
	 */
	public void testSelectView() throws SQLException {
		Statement s = createStatement();
		ResultSet rs;
		ResultSetMetaData rsmd;
		
		rs = s.executeQuery("select * from tab1_view");
		rsmd = rs.getMetaData();
		
		assertFalse("Column count doesn't match.", rsmd.getColumnCount() != 2);
		assertFalse("Column 1 IS ai.", !rsmd.isAutoIncrement(1));
		assertFalse("Column 1 is NOT ai.", rsmd.isAutoIncrement(2));
		
		rs.close();
		s.close();
	}
}
