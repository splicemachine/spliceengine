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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import junit.framework.Test;
import junit.framework.TestSuite;
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.TestConfiguration;
	/**
	 * This tests unary minus and unary plus as dynamic parameters in PreparedStatements.
	 */

public class UnaryArithmeticParameterTest extends BaseJDBCTestCase {
	/**
     * Public constructor required for running test as standalone JUnit.
     */
	public UnaryArithmeticParameterTest(String name) {
		super(name);
	}
	/**
     * Create a suite of tests.
     */
	public static Test suite() {
		// This test should not get run under jdk13 because it uses ParameterMetaData calls
		// to ensure that the parameter type for dynamic parameter 
		// for unary minus and plus get set correctly from the context in which they are used.
		if ( !JDBC.vmSupportsJDBC3())
            return new TestSuite("empty UnaryArithmeticParameterTest - supported only from JDBC3.0 and above");
        else{
            	return   TestConfiguration.defaultSuite(UnaryArithmeticParameterTest.class);
        }
	}
	 /**
     * Set the fixture up with tables 
     */
	protected void setUp() throws SQLException {
		getConnection().setAutoCommit(false);
		Statement stmt = createStatement();
		stmt.executeUpdate("create table t1 (c11 int, c12 smallint, c13 double, c14 char(3))");
		stmt.execute("insert into t1 values(1,1,1.1,'abc')");
		stmt.execute("insert into t1 values(-1,-1,-1.0,'def')");
		stmt.execute("create table t2 (c21 int)");
		stmt.execute("insert into t2 values (-1),(-2)");
		stmt.close();
	}
	/**
     * Tear-down the fixture by removing the tables
     */
	protected void tearDown() throws Exception {
		Statement stmt = createStatement();
		stmt.executeUpdate("drop table t1");
		stmt.executeUpdate("drop table t2");
		stmt.close();
		commit();
		super.tearDown();
	}
	/**
	 * Test using parameters with unary minus and unary plus
	 * @throws Exception
	 */
		public void testParametersForUnaryMinusAndPlus() throws Exception {
			PreparedStatement ps = prepareStatement("insert into t1 values(?,+?,-?,?)");
			ps.setInt(1,3);
			ps.setInt(2,3);
			ps.setDouble(3,-3.1);
			ps.setString(4,"ghi");
			int[] expectedTypes={Types.INTEGER,Types.SMALLINT, Types.DOUBLE,Types.CHAR};
			JDBC.assertParameterTypes(ps,expectedTypes);
			ps.executeUpdate();
			ps.setInt(1,-1);
			ps.setInt(2,-1);
			ps.setDouble(3,1.0);
			ps.setString(4,"jkl");
			ps.executeUpdate();
			
			ps = prepareStatement("select * from t1 where -? in (select c21 from t2)");
			ps.setInt(1,1);
			expectedTypes=new int[] {Types.INTEGER};
			JDBC.assertParameterTypes(ps,expectedTypes);
			Object[][] expectedRows = new Object[][]{{new Integer(1),new Integer(1),new Double(1.1),new String("abc")},
										{new Integer(-1),new Integer(-1),new Double(-1.0),new String("def")},
										{new Integer(3),new Integer(3),new Double(3.1),new String("ghi")},
										{new Integer(-1),new Integer(-1),new Double(-1.0),new String("jkl")}};
			JDBC.assertFullResultSet(ps.executeQuery(), expectedRows, false);
			ps =prepareStatement("select * from t1 where c11 = -? and c12 = +? and c13 = ?");
			ps.setInt(1,-1);
			ps.setInt(2,1);
			ps.setDouble(3,1.1);
			expectedTypes= new int[]{Types.INTEGER, Types.SMALLINT,Types.DOUBLE};
			JDBC.assertParameterTypes(ps,expectedTypes);
			expectedRows = new Object[][]{{new Integer(1),new Integer(1),new Double(1.1),new String("abc")}};
			JDBC.assertFullResultSet(ps.executeQuery(), expectedRows, false);
			
			ps.setShort(1,(short) 1);
			ps.setInt(2,-1);		
			ps.setInt(3,-1);
			expectedRows = new Object[][]{{new Integer(-1),new Integer(-1),new Double(-1.0),new String("def")},
										{new Integer(-1),new Integer(-1),new Double(-1.0),new String("jkl")}};
			JDBC.assertFullResultSet(ps.executeQuery(), expectedRows, false);
			ps.close();
			}
		/**
		 * Tests ABS function with Unary plus and Unary minus
		 * @throws Exception
		 */
		public void testABSWithUnaryMinusAndPlus() throws Exception {
			Statement s = createStatement();
			s.execute("CREATE FUNCTION ABS_FUNCT(P1 INT) RETURNS INT CALLED ON NULL INPUT EXTERNAL NAME 'java.lang.Math.abs' LANGUAGE JAVA PARAMETER STYLE JAVA");
			PreparedStatement ps = prepareStatement("select * from t1 where -? = abs_funct(+?)");
			ps.setInt(1,-1);
			ps.setInt(2,1);
			int[] expectedTypes= new int[]{Types.INTEGER,Types.INTEGER};
			JDBC.assertParameterTypes(ps,expectedTypes);
			Object[][] expectedRows = new Object[][]{{new Integer(1),new Integer(1),new Double(1.1),new String("abc")},
										{new Integer(-1),new Integer(-1),new Double(-1.0),new String("def")}};
			JDBC.assertFullResultSet(ps.executeQuery(), expectedRows, false);
			ps.close();
			s.execute("Drop function ABS_FUNCT" );
			s.close();
		}	
		/**
		 * Tests MAX function with Unary plus and Unary minus
		 * @throws Exception
		 */
		public void testMAXWithUnaryMinusAndPlus() throws Exception{
			Statement s = createStatement();
			s.execute("CREATE FUNCTION MAX_CNI(P1 INT, P2 INT) RETURNS INT CALLED ON NULL INPUT EXTERNAL NAME 'java.lang.Math.max' LANGUAGE JAVA PARAMETER STYLE JAVA");
			PreparedStatement ps = prepareStatement("select * from t1 where -? = max_cni(-5,-1)");
			ps.setInt(1,1);
			int[] expectedTypes= new int[]{Types.INTEGER};
			JDBC.assertParameterTypes(ps,expectedTypes);
			Object[][] expectedRows = new Object[][]{{new Integer(1),new Integer(1),new Double(1.1),new String("abc")},
					{new Integer(-1),new Integer(-1),new Double(-1.0),new String("def")}};
			JDBC.assertFullResultSet(ps.executeQuery(), expectedRows, false);
			
			ps = prepareStatement("select * from t1 where -? = max_cni(-?,+?)");
			ps.setInt(1,-1);
			ps.setInt(2,1);
			ps.setInt(3,1);
			expectedTypes= new int[]{Types.INTEGER,Types.INTEGER,Types.INTEGER};
			JDBC.assertParameterTypes(ps,expectedTypes);
			JDBC.assertFullResultSet(ps.executeQuery(), expectedRows, false);

			//Try the function again. But use, use sqrt(+?) & abs(-?) functions to send params
			ps = prepareStatement("select * from t1 where -? = max_cni(abs(-?), sqrt(+?))");
			ps.setInt(1,-2);
			ps.setInt(2,1);
			ps.setInt(3,4);
			expectedTypes=new int[]{Types.INTEGER,Types.DOUBLE,Types.DOUBLE};
			JDBC.assertParameterTypes(ps,expectedTypes);
			JDBC.assertFullResultSet(ps.executeQuery(), expectedRows, false);
			ps.close();
			s.execute("Drop function MAX_CNI" );
			s.close();
			}
		/**
		 * Tests BETWEEN with unary minus and unary plus
		 * @throws Exception
		 */
		public void testBETWEENWithUnaryMinusAndPlus() throws Exception{
			PreparedStatement ps = prepareStatement("select * from t1 where c11 between -? and +?");
			ps.setInt(1,-1);
			ps.setInt(2,1);
			int[] expectedTypes= new int[]{Types.INTEGER,Types.INTEGER};
			JDBC.assertParameterTypes(ps,expectedTypes);
			Object[][] expectedRows = new Object[][]{{new Integer(1),new Integer(1),new Double(1.1),new String("abc")}};
			JDBC.assertFullResultSet(ps.executeQuery(), expectedRows, false);
			ps.close();
		}
		/**
		 * Tests NOT IN with unary minus and unary plus
		 * @throws Exception
		 */
		public void testNOTINWithUnaryMinusAndPlus() throws Exception{
			PreparedStatement ps = prepareStatement("select * from t1 where +? not in (-?, +?, 2, ?)");
			ps.setInt(1,-11);
			ps.setInt(2,1);
			ps.setInt(3,1);
			ps.setInt(4,4);
			int[] expectedTypes= new int[]{Types.INTEGER,Types.INTEGER, Types.INTEGER,Types.INTEGER};
			JDBC.assertParameterTypes(ps,expectedTypes);
			Object[][] expectedRows = new Object[][]{{new Integer(1),new Integer(1),new Double(1.1),new String("abc")},
										{new Integer(-1),new Integer(-1),new Double(-1.0),new String("def")}};
			JDBC.assertFullResultSet(ps.executeQuery(), expectedRows, false);
						
			ps = prepareStatement("select * from t1 where -? not in (select c21+? from t2)");
			ps.setInt(1,1);
			ps.setInt(2,2);
			expectedTypes = new int[]{Types.INTEGER,Types.INTEGER};
			JDBC.assertParameterTypes(ps,expectedTypes);
			JDBC.assertFullResultSet(ps.executeQuery(), expectedRows, false);
			ps.close();
		}
		/**
		 * Tests operators with Unary plus and unary Minus
		 * @throws Exception
		 */
		public void testOperatorsWithUnaryMinusAndPlus() throws Exception{
			PreparedStatement ps = prepareStatement("select * from t1 where +? < c12");
			ps.setInt(1,0);
			int[] expectedTypes=new int[]{Types.SMALLINT};
			JDBC.assertParameterTypes(ps,expectedTypes);
			Object[][] expectedRows = new Object[][]{{new Integer(1),new Integer(1),new Double(1.1),new String("abc")}};
			JDBC.assertFullResultSet(ps.executeQuery(), expectedRows, false);

			ps = prepareStatement("select * from t1 where -? = c11 + ?");
			ps.setInt(1,2);
			ps.setInt(2,-1);
			expectedTypes = new int[]{Types.INTEGER,Types.INTEGER};
			JDBC.assertParameterTypes(ps,expectedTypes);
			expectedRows = new Object[][]{{new Integer(-1),new Integer(-1),new Double(-1.0),new String("def")}};
			JDBC.assertFullResultSet(ps.executeQuery(), expectedRows, false);

			ps = prepareStatement("select * from t1 where c11 + ? = -?");
			ps.setInt(1,-1);
			ps.setInt(2,2);
			JDBC.assertParameterTypes(ps,expectedTypes);
			JDBC.assertFullResultSet(ps.executeQuery(), expectedRows, false);

			ps = prepareStatement("select * from t1 where c11 + c12 = -?");
			ps.setInt(1,2);
			expectedTypes= new int[]{Types.INTEGER};
			JDBC.assertParameterTypes(ps,expectedTypes);
			JDBC.assertFullResultSet(ps.executeQuery(), expectedRows, false);
			ps.close();
		}
		/**
		 * Tests Casting with unary plus and unary minus
		 * @throws Exception
		 */
		public void testCastWithUnaryMinusAndPlus()throws Exception{
			PreparedStatement ps = prepareStatement("select cast(-? as smallint), cast(+? as int) from t1");
			ps.setInt(1,2);
			ps.setInt(2,2);
			int[] expectedTypes= new int[]{Types.SMALLINT,Types.INTEGER};
			JDBC.assertParameterTypes(ps,expectedTypes);
			String [][] expectedRows = new String[][]{{"-2","2"},{"-2","2"}};
			JDBC.assertFullResultSet(ps.executeQuery(), expectedRows, true);
			ps.close();
		}
		/**
		 * Tests NullIf with unary minus and unary plus
		 * @throws Exception
		 */
		public void testNullIfWithUnaryMinusAndPlus() throws Exception{
			PreparedStatement ps = prepareStatement("select nullif(-?,c11) from t1");
			ps.setInt(1,22);
			int[] expectedTypes= new int[]{Types.INTEGER};
			JDBC.assertParameterTypes(ps,expectedTypes);
			Object[][] expectedRows = new Object[][]{{new String("-22")},{new String("-22")}};
			JDBC.assertFullResultSet(ps.executeQuery(), expectedRows, true);
			ps.close();
		}
		/**
		 * Tests SQRT with unary minus and unary plus
		 * @throws Exception
		 */
		public void testSQRTWithUnaryMinusAndPlus() throws Exception{
			PreparedStatement ps = prepareStatement("select sqrt(-?) from t1");
			ps.setInt(1,-64);
			int[] expectedTypes= new int[]{Types.DOUBLE};
			JDBC.assertParameterTypes(ps,expectedTypes);
			Object[][] expectedRows = new Object[][]{{new String("8.0")},{new String("8.0")}};
			JDBC.assertFullResultSet(ps.executeQuery(), expectedRows, true);
			ps.close();
		}
		/**
		 * Tests "select case when -?=c11 then -? else c12 end from t1"
		 * @throws Exception
		 */
		public void testSelectWithUnaryMinusAndPlus() throws Exception{
			PreparedStatement ps =prepareStatement("select case when -?=c11 then -? else c12 end from t1");
			ps.setInt(1,1);
			ps.setInt(2,22);
			int[] expectedTypes = new int[]{Types.INTEGER,Types.SMALLINT};
			JDBC.assertParameterTypes(ps, expectedTypes);
			Object[][] expectedRows = new Object[][]{{new Integer("1")},{new Integer("-22")}};
			JDBC.assertFullResultSet(ps.executeQuery(), expectedRows, false);
			ps.close();
		}
		/**
		 * Negative tests for unary minus and unary plus
		 * @throws Exception
		 */
		public void testExpectedErrors() throws Exception{
			assertCompileError("42X34","select * from t1 where c11 = any (select -? from t2)");
			
			// -?/+? at the beginning and/ at the end of where clause
			assertCompileError("42X19","select * from t1 where -? and c11=c11 or +?");
			
			// -?/+? in like escape function
			assertCompileError("42X37","select * from sys.systables where tablename like -? escape +?");
			
			// -?/+? in binary timestamp function
			assertCompileError("42X37","select timestamp(-?,+?) from t1");
			
			// -? in unary timestamp function
			assertCompileError("42X36","select timestamp(-?) from t1");
			
			// -? in views
			assertCompileError("42X98","create view v1 as select * from t1 where c11 = -?");
			
			// -? in inner join
			assertCompileError("42X37","select * from t1 inner join t1 as t333 on -?");
			
			// -? by itself in where clause
			assertCompileError("42X19","select * from t1 where -?");
			
			// -? is null not allowed because is null allowed on char types only
			assertCompileError("42X37","select * from t1 where -? is null");
			
			// unary plus parameters on both sides of / operator
			assertCompileError("42X35","select * from t1 where c11 = ?/-?");
			
			// unary plus in || operation
			assertCompileError("42X37","select c11 || +? from t1");
			
			// unary minus for char column
			assertCompileError("42X37","select * from t1 where c14 = -?");
			
			// unary plus for char column
			assertCompileError("42X37","select * from t1 where c14 like +?");
			
					
		}
	}

