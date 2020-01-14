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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.JDBC;

public class metadataMultiConnTest extends BaseJDBCTestCase {

	
	public metadataMultiConnTest(String name) {
		super(name);
	}

	public static Test suite() {
		return new TestSuite(metadataMultiConnTest.class);
	}


	public void testMetadataMultiConn() throws SQLException {

		Connection conn1 = openDefaultConnection();
		metadataCalls(conn1);

		Connection conn2 = openDefaultConnection();
		metadataCalls(conn2);

		Connection conn3 = openDefaultConnection();
		metadataCalls(conn3);

		conn1.commit();
		conn2.commit();

		checkConsistencyOfAllTables(conn3);
	}

	public void metadataCalls(Connection conn) throws SQLException {
		DatabaseMetaData dmd = conn.getMetaData();
		getTypeInfo(dmd);
		getTables(dmd);
		getColumnInfo(dmd);
		getPrimaryKeys(dmd);
		getExportedKeys(dmd);
	}

	public void getTypeInfo(DatabaseMetaData dmd) throws SQLException {
		ResultSet rs = dmd.getTypeInfo();
		JDBC.assertDrainResults(rs);
	}

	public void getTables(DatabaseMetaData dmd) throws SQLException {
		String types[] = new String[1];
		types[0] = "TABLE";
		ResultSet rs = dmd.getTables(null, null, null, types);
		JDBC.assertDrainResults(rs);
	}

	public void getColumnInfo(DatabaseMetaData dmd) throws SQLException {
		ResultSet rs = dmd.getColumns(null, null, "%", "%");
		JDBC.assertDrainResults(rs);
	}

	public void getPrimaryKeys(DatabaseMetaData dmd) throws SQLException {
		ResultSet rs = dmd.getPrimaryKeys(null, null, "%");
		JDBC.assertDrainResults(rs);
	}

	public void getExportedKeys(DatabaseMetaData dmd) throws SQLException {
		ResultSet rs = dmd.getExportedKeys(null, null, "%");
		JDBC.assertDrainResults(rs);
	}


	public void checkConsistencyOfAllTables(Connection conn)
			throws SQLException {

		Statement s = conn.createStatement();
		ResultSet rs = s.executeQuery("SELECT schemaname, tablename, "
				+ "SYSCS_UTIL.SYSCS_CHECK_TABLE(schemaname, tablename) "
				+ "FROM sys.sysschemas s, sys.systables t "
				+ "WHERE s.schemaid = t.schemaid");
		while (rs.next()) {
			assertTrue(rs.getBoolean(3));
		}
		rs.close();
		s.close();
	}
}
