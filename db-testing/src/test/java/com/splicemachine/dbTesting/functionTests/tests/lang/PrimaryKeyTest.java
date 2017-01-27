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

package com.splicemachine.dbTesting.functionTests.tests.lang;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.JDBC;

public class PrimaryKeyTest extends BaseJDBCTestCase
{	
	public PrimaryKeyTest(String name)      {
		super(name);
	}
	public static Test suite() {
		return new TestSuite(PrimaryKeyTest.class);
	}
	protected void setUp() throws Exception {
		super.setUp();
		getConnection().setAutoCommit(false);
	}
	protected void tearDown() throws Exception {
		super.tearDown();
	}

					/************ NEGATIVE TESTS ************/	
	/**
 	 * 
 	 * Tests that A table can't contain two primary keys.
 	 *
 	 *  @exception SQLException 
 	 */ 	
	public void testDuplicatePrimaryKey() throws SQLException {
		Statement s = createStatement();
		//duplicate primary keys
		assertStatementError("42X90" , s , "create table neg (c1 int not null primary key, c2 int, c3 int not null constraint asdf primary key)");
		assertStatementError("42X90" , s , "create table neg (c1 int not null primary key, c2 int not null, primary key(c1, c2))");
	}
	/**
	 * Tests the Invalid constraint Definations.
	 *
	 *  @exception SQLException 
	 */ 
	public void testInvalidConstaintDefs() throws SQLException {
		Statement s = createStatement();
		//-- duplicate constraint names
		assertStatementError("42X91" , s , "create table neg (c1 int not null constraint asdf primary key, c2 int, c3 int constraint asdf unique)");
		//-- duplicate column names in same constraint column list
		assertStatementError("42X92" , s , "create table neg (c1 int not null, c2 int not null, primary key(c1, c2, c1))");
		//-- non-existant columns in constraint column list
		assertStatementError("42X93" , s , "create table neg (c1 int not null, c2 int not null, primary key(c1, c2, cx))");
	}
	/**
	 *  Tests Invalid constraint schema name
	 *
	 *
	 *   @exception SQLException 
	 */
	public void testInvalidConstraintSchemaNames() throws SQLException
	{
		Statement s = createStatement();
		// constraint names are required to be in the same schema as the table on which they are constrained.
		assertStatementError("42X85" , s , "create table neg (c1 int not null, c2 int not null, constraint bar.pkneg primary key(c1, c2))");
		assertStatementError("42X85" , s , "create table neg (c1 int not null, c2 int not null, constraint sys.pkneg primary key(c1, c2))");
		assertStatementError("42X85" , s , "create table neg (c1 int not null constraint bar.pkneg primary key, c2 int)");
		assertStatementError("42X85" , s , "create table neg (c1 int not null constraint sys.pkneg primary key, c2 int)");
	}
	/**
	 * Constraint Names must be Unique with in a schema
	 *
	 * @exception SQLException 
	 */
	public void testDuplicateConstraintNames() throws SQLException {
		Statement s = createStatement();
		assertUpdateCount(s , 0 , "create table neg1(c1 int not null constraint asdf primary key)");
		//-- constraint names must be unique within a schema
		assertStatementError("X0Y32" , s , "create table neg2(c1 int not null constraint asdf primary key)");
		assertUpdateCount(s , 0 , "drop table neg1");
		assertUpdateCount(s , 0 , "create table neg2(c1 int not null constraint asdf primary key)");
		assertUpdateCount(s , 0 , "drop table neg2");

		//-- again with explict schema names, should fail
		assertUpdateCount(s , 0 , "create table neg1(c1 int not null constraint app.asdf primary key)");
		assertStatementError("X0Y32" , s , "create table neg2(c1 int not null constraint app.asdf primary key)");

		//-- again with mixing schema names
		assertStatementError("X0Y32" , s , "create table neg1(c1 int not null constraint asdf primary key)");
		assertStatementError("X0Y32" , s , "create table neg2(c1 int not null constraint app.asdf primary key)");
		assertUpdateCount(s , 0 , "drop table neg1");
		assertUpdateCount(s , 0 , "create table neg2(c1 int not null constraint app.asdf primary key)");
	}
	/**
	 * Tests that primary and Unique key constraint cannot be Explicitely Nullable.
	 *
	 *  @exception SQLException 
	 */ 
    public void testExplicitNullabilityOfConstraints() throws SQLException {
		Statement s = createStatement();
		//-- primary key cannot be explicitly nullable
		assertStatementError("42X01" , s , "create table neg2(c1 int null constraint asdf primary key)");
		assertStatementError("42X01" , s , "create table neg2(c1 int null, c2 int, constraint asdf primary key(c1, c2))");
		//-- test that a unique key can not be explicitly nullable
		assertStatementError("42X01" , s , "create table neg1(c1 int null unique)");
		assertStatementError("42X01" , s , "create table neg1(c1 int null, c2 int, constraint asdf unique(c1))");
	}
			
					/************* POSITIVE TESTS ************/
	/** 
	 * Tests that If a column is a part of Primary Key then it cann't contain NULL values.
	 * And also Unique key cann't contain nulls.
	 *
	 *  @exception SQLException 
	 */ 
	public void testKeyConstraintsImpliesNotNull() throws SQLException {
		//-- verify that a primary key implies not null
		Statement s = createStatement();
		assertUpdateCount(s , 0 , "create table pos1 (c1 int primary key)");
		assertUpdateCount(s , 1 , "insert into pos1(c1) values(1)");
		assertStatementError("23505" , s , "insert into pos1(c1) values(1)");
		assertStatementError("23502" , s , "insert into pos1(c1) values(null)");
		assertUpdateCount(s , 0 , "drop table pos1");
	}
	/**
	 * Tests that we can combile key constraints with not null.
	 *
	 *  @exception SQLException 
	 */ 
	public void testConstraintwithNotNull() throws SQLException
	{
		Statement s = createStatement();
		//-- verify that you can combine not null and unique/primary key constraints
		assertUpdateCount(s , 0 , "create table pos1 (c1 int not null unique, c2 int not null primary key)");
		assertStatementError("23502" , s , "insert into pos1 (c1) values (null)");
		assertStatementError("23502" , s , "insert into pos1 (c2) values (null)");
		assertUpdateCount(s , 0 , "drop table pos1");

		//-- verify that you can combine multiple column constraints
		ResultSet rs1 = s.executeQuery("select count(*) from sys.sysconstraints");
		JDBC.assertSingleValueResultSet(rs1 , "0");

		rs1 = s.executeQuery("select count(*) from sys.syskeys");
		JDBC.assertSingleValueResultSet(rs1 , "0");

		//-- we will be adding 6 rows to both sysconstraints and syskeys
		assertUpdateCount(s , 0 , "create table pos1 (c1 int not null unique, c2 int not null primary key)");
		assertStatementError("23502" , s , "insert into pos1 (c1) values (null)");
		assertStatementError("23502" , s , "insert into pos1 (c2) values (null)");
		assertStatementError("23505" , s , "insert into pos1 values (1, 1), (1, 2)");
		assertStatementError("23505" , s , "insert into pos1 values (1, 1), (2, 1)");

		rs1 = s.executeQuery("select count(*) from sys.sysconstraints");
		JDBC.assertSingleValueResultSet(rs1 , "2");

		rs1 = s.executeQuery("select count(*) from sys.syskeys");
		JDBC.assertSingleValueResultSet(rs1 , "2");

		assertUpdateCount(s , 0 , "drop table pos1");
	}
	/**
	 * tests that we can Delete from Primary Key
	 *
	 *  @exception SQLException 
	 */ 
	public void testDeleteFromPrimaryKey() throws SQLException {
		Statement s = createStatement();
		//-- verify that you can delete from a primary key
		assertUpdateCount(s , 0 , "create table pos1 (c1 int not null, c2 int not null, primary key(c2, c1))");
		assertUpdateCount(s , 1 , "insert into pos1 values (1, 2)");
		ResultSet rs = s.executeQuery("select count(*) from pos1");
		JDBC.assertSingleValueResultSet(rs , "1");

		s.executeUpdate("delete from pos1");

		rs = s.executeQuery("select count(*) from pos1");
		JDBC.assertSingleValueResultSet(rs , "0");
		assertUpdateCount(s , 0 , "drop table pos1");

	}
	/**
	 * verify the consistency of the indexes on the system catalogs
	 *
	 *  @exception SQLException 
	 */ 
	public void testCatalog() throws SQLException {
		Statement s = createStatement();
		assertUpdateCount(s , 0 , "create table pos1(c1 int primary key)");
		ResultSet rs = s.executeQuery("select tablename, SYSCS_UTIL.SYSCS_CHECK_TABLE('SYS', tablename) from sys.systables where CAST(tabletype AS CHAR(1)) = 'S'  and CAST(tablename AS VARCHAR(128)) != 'SYSDUMMY1' order by tablename");
                String[][] expectedCheckTables = new String[][]
                       {
                        {"SYSALIASES","1"},
                        {"SYSCHECKS","1"},
                        {"SYSCOLPERMS","1"},
                        {"SYSCOLUMNS","1"},
                        {"SYSCONGLOMERATES","1"},
                        {"SYSCONSTRAINTS","1"},
                        {"SYSDEPENDS","1"},
                        {"SYSFILES","1"},
                        {"SYSFOREIGNKEYS","1"},
                        {"SYSKEYS","1"},
                        {"SYSPERMS", "1"},
						{"SYSROLES", "1"},
                        {"SYSROUTINEPERMS","1"},
                        {"SYSSCHEMAS","1"},
                        {"SYSSEQUENCES", "1"},
                        {"SYSSTATEMENTS","1"},
                        {"SYSSTATISTICS","1"},
                        {"SYSTABLEPERMS","1"},
                        {"SYSTABLES","1"},
                        {"SYSTRIGGERS","1"},
                        {"SYSUSERS","1"},
                        {"SYSVIEWS","1"},
                       };
                JDBC.assertFullResultSet(rs,expectedCheckTables); 
		//-- drop tables
		assertUpdateCount(s , 0 , "drop table pos1");
		//-- verify it again
                rs = s.executeQuery("select tablename, SYSCS_UTIL.SYSCS_CHECK_TABLE('SYS', tablename) from sys.systables where CAST(tabletype AS CHAR(1)) = 'S'  and CAST(tablename AS VARCHAR(128)) != 'SYSDUMMY1' order by tablename");
                JDBC.assertFullResultSet(rs, expectedCheckTables);
	}
	/**
	 * Testing The Bug5420
	 *
	 *  @exception SQLException 
	 */ 
	public void testBug5420() throws SQLException
	{
		Statement s = createStatement();
		//-- bug 5420 - constraint names in new schemas.
		assertUpdateCount(s , 0 , "create table B5420_1.t1 (c1 int not null primary key)");
		assertUpdateCount(s , 0 , "create table B5420_2.t2 (c2 int not null constraint c2pk primary key)");
		//-- two part constraint names are not allowed
		assertUpdateCount(s , 0 , "create table B5420_3.t3 (c3 int not null constraint B5420_3.c3pk primary key)");
		assertUpdateCount(s , 0 , "create table B5420_4.t4 (c4 int not null, primary key (c4))");
		assertUpdateCount(s , 0 , "create table B5420_5.t5 (c5 int not null, constraint c5pk primary key (c5))");
		//-- two part constraint names are not allowed
		assertUpdateCount(s , 0 , "create table B5420_6.t6 (c6 int not null, constraint B5420_6.c6pk primary key (c6))");
		ResultSet rs = s.executeQuery("	SELECT CAST (S.SCHEMANAME AS VARCHAR(12)), CAST (C.CONSTRAINTNAME AS VARCHAR(36)), CAST (T.TABLENAME AS VARCHAR(12)) FROM SYS.SYSCONSTRAINTS C , SYS.SYSTABLES T, SYS.SYSSCHEMAS S WHERE C.SCHEMAID = S.SCHEMAID AND C.TABLEID = T.TABLEID AND T.SCHEMAID = S.SCHEMAID AND CAST(S.SCHEMANAME AS VARCHAR(128)) LIKE 'B5420_%' ORDER BY 1,2,3");
		rs.next();
		ResultSetMetaData rsmd = rs.getMetaData();
		assertEquals(3 , rsmd.getColumnCount());
		int rows = 0;
		do
		{
			rows++;
		}while(rs.next());
		assertEquals(6 , rows);
		//-- clean up
		assertUpdateCount(s , 0 , "drop table B5420_1.t1");
		assertUpdateCount(s , 0 , "drop table B5420_2.t2");
		assertUpdateCount(s , 0 , "drop table B5420_3.t3");
		assertUpdateCount(s , 0 , "drop table B5420_4.t4");
		assertUpdateCount(s , 0 , "drop table B5420_5.t5");
		assertUpdateCount(s , 0 , "drop table B5420_6.t6");
	}
}

