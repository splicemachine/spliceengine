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

import java.io.UnsupportedEncodingException;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Types;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.JDBC;

/**
 * Set of tests for SQL routines.
 * This tests mainly the SQL definition of routines
 * and the server-side behaviour of routines.
 * Calling of procedures is tested in ProcedureTest.
 *
 */
public class RoutineTest extends BaseJDBCTestCase {

    private static final String CANNOT_STUFF_NULL_INTO_PRIMITIVE = "39004";
    
    public RoutineTest(String name)
    {
        super(name);
    }
    
    public static Test suite() {
        TestSuite suite = new TestSuite(RoutineTest.class, "RoutineTest");
        
        return new CleanDatabaseTestSetup(suite);
    }
    
    
    /**
     * Test that function result data types are resolved correctly for numeric
     * types that Derby supports that are simply mappable or object mappable.
     */
    public void testFunctionResultDataTypeValidation() throws SQLException
    {
        Statement s = createStatement();

        // SMALLINT -> short
        s.executeUpdate(
        "CREATE FUNCTION SMALLINT_P_SHORT(VARCHAR(10)) RETURNS SMALLINT " +
           "EXTERNAL NAME 'java.lang.Short.parseShort' " +
           "LANGUAGE JAVA PARAMETER STYLE JAVA");

        PreparedStatement ps = prepareStatement("VALUES SMALLINT_P_SHORT(?)");
        ps.setString(1, "123");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "123");  

        // SMALLINT -> Integer
        s.executeUpdate(
        "CREATE FUNCTION SMALLINT_O_INTEGER(VARCHAR(10)) RETURNS SMALLINT " +
           "EXTERNAL NAME 'java.lang.Integer.valueOf' " +
           "LANGUAGE JAVA PARAMETER STYLE JAVA");

        ps = prepareStatement("VALUES SMALLINT_O_INTEGER(?)");
        ps.setString(1, "123");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "123");  

        // INTEGER -> int
        s.executeUpdate(
        "CREATE FUNCTION INTEGER_P_INT(VARCHAR(10)) RETURNS INTEGER " +
           "EXTERNAL NAME 'java.lang.Integer.parseInt' " +
           "LANGUAGE JAVA PARAMETER STYLE JAVA");

        ps = prepareStatement("VALUES INTEGER_P_INT(?)");
        ps.setString(1, "123");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "123");  

        // INTEGER -> Integer
        s.executeUpdate(
        "CREATE FUNCTION INTEGER_O_INTEGER(VARCHAR(10)) RETURNS INTEGER " +
           "EXTERNAL NAME 'java.lang.Integer.valueOf' " +
           "LANGUAGE JAVA PARAMETER STYLE JAVA");

        ps = prepareStatement("VALUES INTEGER_O_INTEGER(?)");
        ps.setString(1, "123");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "123");  

        // BIGINT -> long
        s.executeUpdate(
        "CREATE FUNCTION BIGINT_P_LONG(VARCHAR(10)) RETURNS BIGINT " +
           "EXTERNAL NAME 'java.lang.Long.parseLong' " +
           "LANGUAGE JAVA PARAMETER STYLE JAVA");

        ps = prepareStatement("VALUES BIGINT_P_LONG(?)");
        ps.setString(1, "123");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "123");  

        // BIGINT -> Long
        s.executeUpdate(
        "CREATE FUNCTION BIGINT_O_LONG(VARCHAR(10)) RETURNS BIGINT " +
           "EXTERNAL NAME 'java.lang.Long.valueOf' " +
           "LANGUAGE JAVA PARAMETER STYLE JAVA");

        ps = prepareStatement("VALUES BIGINT_O_LONG(?)");
        ps.setString(1, "123");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "123");  

        // REAL -> float
        s.executeUpdate(
        "CREATE FUNCTION REAL_P_FLOAT(VARCHAR(10)) RETURNS REAL " +
           "EXTERNAL NAME 'java.lang.Float.parseFloat' " +
           "LANGUAGE JAVA PARAMETER STYLE JAVA");

        ps = prepareStatement("VALUES REAL_P_FLOAT(?)");
        ps.setString(1, "123.0");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "123.0");  

        // REAL -> Float
        s.executeUpdate(
        "CREATE FUNCTION REAL_O_FLOAT(VARCHAR(10)) RETURNS REAL " +
           "EXTERNAL NAME 'java.lang.Float.valueOf' " +
           "LANGUAGE JAVA PARAMETER STYLE JAVA");

        ps = prepareStatement("VALUES REAL_O_FLOAT(?)");
        ps.setString(1, "123.0");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "123.0");  

        // DOUBLE -> double
        s.executeUpdate(
        "CREATE FUNCTION DOUBLE_P_DOUBLE(VARCHAR(10)) RETURNS DOUBLE " +
           "EXTERNAL NAME 'java.lang.Double.parseDouble' " +
           "LANGUAGE JAVA PARAMETER STYLE JAVA");

        ps = prepareStatement("VALUES DOUBLE_P_DOUBLE(?)");
        ps.setString(1, "123.0");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "123.0");  

        // DOBULE -> Double
        s.executeUpdate(
        "CREATE FUNCTION DOUBLE_O_DOUBLE(VARCHAR(10)) RETURNS DOUBLE " +
           "EXTERNAL NAME 'java.lang.Double.valueOf' " +
           "LANGUAGE JAVA PARAMETER STYLE JAVA");

        ps = prepareStatement("VALUES DOUBLE_O_DOUBLE(?)");
        ps.setString(1, "123.0");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "123.0");
        
        ps.close();
        s.close();
    }
    
    /**
     * Test that RETURNS NULL ON NULL INPUT works properly with 
     * numeric datatypes for null and non-null values.
     */
    public void testFunctionReturnsNullOnNullInput() throws SQLException
    {
        Statement s = createStatement();

        // SMALLINT -> short
        s.executeUpdate(
        "CREATE FUNCTION SMALLINT_P_SHORT_RN(VARCHAR(10)) RETURNS SMALLINT " +
           "EXTERNAL NAME 'java.lang.Short.parseShort' " +
           "LANGUAGE JAVA PARAMETER STYLE JAVA " +
           "RETURNS NULL ON NULL INPUT");

        PreparedStatement ps = prepareStatement("VALUES SMALLINT_P_SHORT_RN(?)");
        ps.setString(1, "123");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "123");  

        ps.setString(1,null);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
        // SMALLINT -> Integer
        s.executeUpdate(
        "CREATE FUNCTION SMALLINT_O_INTEGER_RN(VARCHAR(10)) RETURNS SMALLINT " +
           "EXTERNAL NAME 'java.lang.Integer.valueOf' " +
           "LANGUAGE JAVA PARAMETER STYLE JAVA " +
           "RETURNS NULL ON NULL INPUT");

        ps = prepareStatement("VALUES SMALLINT_O_INTEGER_RN(?)");
        ps.setString(1, "123");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "123");  

        ps.setString(1, null);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
        // INTEGER -> int
        s.executeUpdate(
        "CREATE FUNCTION INTEGER_P_INT_RN(VARCHAR(10)) RETURNS INTEGER " +
           "EXTERNAL NAME 'java.lang.Integer.parseInt' " +
           "LANGUAGE JAVA PARAMETER STYLE JAVA " +
           "RETURNS NULL ON NULL INPUT");

        ps = prepareStatement("VALUES INTEGER_P_INT_RN(?)");
        ps.setString(1, "123");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "123");  

        // INTEGER -> Integer
        s.executeUpdate(
        "CREATE FUNCTION INTEGER_O_INTEGER_RN(VARCHAR(10)) RETURNS INTEGER " +
           "EXTERNAL NAME 'java.lang.Integer.valueOf' " +
           "LANGUAGE JAVA PARAMETER STYLE JAVA " +
           "RETURNS NULL ON NULL INPUT");

        ps = prepareStatement("VALUES INTEGER_O_INTEGER_RN(?)");
        ps.setString(1, "123");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "123");  
        ps.setString(1, null);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
        // BIGINT -> long
        s.executeUpdate(
        "CREATE FUNCTION BIGINT_P_LONG_RN(VARCHAR(10)) RETURNS BIGINT " +
           "EXTERNAL NAME 'java.lang.Long.parseLong' " +
           "LANGUAGE JAVA PARAMETER STYLE JAVA " +
           "RETURNS NULL ON NULL INPUT");

        ps = prepareStatement("VALUES BIGINT_P_LONG_RN(?)");
        ps.setString(1, "123");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "123");  
        ps.setString(1, null);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
        // BIGINT -> Long
        s.executeUpdate(
        "CREATE FUNCTION BIGINT_O_LONG_NR(VARCHAR(10)) RETURNS BIGINT " +
           "EXTERNAL NAME 'java.lang.Long.valueOf' " +
           "LANGUAGE JAVA PARAMETER STYLE JAVA " +
           "RETURNS NULL ON NULL INPUT");

        ps = prepareStatement("VALUES BIGINT_O_LONG_NR(?)");
        ps.setString(1, "123");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "123");  
        ps.setString(1, null);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
        // REAL -> float
        s.executeUpdate(
        "CREATE FUNCTION REAL_P_FLOAT_NR(VARCHAR(10)) RETURNS REAL " +
           "EXTERNAL NAME 'java.lang.Float.parseFloat' " +
           "LANGUAGE JAVA PARAMETER STYLE JAVA " +
           "RETURNS NULL ON NULL INPUT");

        ps = prepareStatement("VALUES REAL_P_FLOAT_NR(?)");
        ps.setString(1, "123.0");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "123.0");  
        ps.setString(1, null);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
        
        // REAL -> Float
        s.executeUpdate(
        "CREATE FUNCTION REAL_O_FLOAT_NR(VARCHAR(10)) RETURNS REAL " +
           "EXTERNAL NAME 'java.lang.Float.valueOf' " +
           "LANGUAGE JAVA PARAMETER STYLE JAVA " +
           "RETURNS NULL ON NULL INPUT");

        ps = prepareStatement("VALUES REAL_O_FLOAT_NR(?)");
        ps.setString(1, "123.0");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "123.0");  
        ps.setString(1, null);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
        
        // DOUBLE -> double
        s.executeUpdate(
        "CREATE FUNCTION DOUBLE_P_DOUBLE_NR(VARCHAR(10)) RETURNS DOUBLE " +
           "EXTERNAL NAME 'java.lang.Double.parseDouble' " +
           "LANGUAGE JAVA PARAMETER STYLE JAVA " +
           "RETURNS NULL ON NULL INPUT");

        ps = prepareStatement("VALUES DOUBLE_P_DOUBLE_NR(?)");
        ps.setString(1, "123.0");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "123.0");  
        ps.setString(1, null);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
        
        // DOBULE -> Double
        s.executeUpdate(
        "CREATE FUNCTION DOUBLE_O_DOUBLE_NR(VARCHAR(10)) RETURNS DOUBLE " +
           "EXTERNAL NAME 'java.lang.Double.valueOf' " +
           "LANGUAGE JAVA PARAMETER STYLE JAVA " +
           "RETURNS NULL ON NULL INPUT");

        ps = prepareStatement("VALUES DOUBLE_O_DOUBLE_NR(?)");
        ps.setString(1, "123.0");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "123.0");
        ps.setString(1, null);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
        
        ps.close();
        s.close();
    }
    
    /**
     * Test that functions handle being called or not called
     * when it is passed a NULL argument correctly.
     * A function can be declared:
     * RETURNS NULL ON NULL INPUT - any argument being NULL means the
     * function is returns NULL without being called.
     * CALLED ON NULL INPUT (default) - function is always called regardless
     * of any arguement being NULL.
     */
    public void testFunctionNullHandling() throws SQLException, UnsupportedEncodingException
    {
        Statement s = createStatement();
        
        // Create three simple functions that take an integer and
        // return its value as a VARCHAR().
        s.executeUpdate(
        "CREATE FUNCTION SV_NOCALL(INTEGER) RETURNS VARCHAR(10) " +
           "RETURNS NULL ON NULL INPUT " +
           "EXTERNAL NAME 'java.lang.String.valueOf'  " +
           "LANGUAGE JAVA PARAMETER STYLE JAVA");
           
        s.executeUpdate("CREATE FUNCTION SV_CALL(INTEGER) RETURNS VARCHAR(10) " +
          "CALLED ON NULL INPUT " +
          "EXTERNAL NAME 'java.lang.String.valueOf' " +
          "LANGUAGE JAVA PARAMETER STYLE JAVA");
          
        s.executeUpdate("CREATE FUNCTION SV_DEFAULT(INTEGER) RETURNS VARCHAR(10) " +
          "EXTERNAL NAME 'java.lang.String.valueOf' " +
          "LANGUAGE JAVA PARAMETER STYLE JAVA");
        
        // Simple cases of calling each function individually
        // Test each function with non-NULL and NULL values.
        PreparedStatement ps = prepareStatement("VALUES SV_NOCALL(?)");
        ps.setInt(1, 42);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "42");  
        ps.setNull(1, Types.INTEGER);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
        ps.close();
        
        
        ps = prepareStatement("VALUES SV_CALL(?)");
        ps.setInt(1, 52);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "52"); 
        
        // NULL will attempt to call the function but be blocked
        // because the Java parameter is a primitive. Since
        // the call attempt it made it is enough to show the
        // correct behaviour.
        ps.setNull(1, Types.INTEGER);
        assertStatementError("39004", ps);
        
        ps.close();
        
        // Default behaviour maps to CALLED ON NULL INPUT
        ps = prepareStatement("VALUES SV_DEFAULT(?)");
        ps.setInt(1, 62);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "62");
        ps.setNull(1, Types.INTEGER);
        assertStatementError("39004", ps);
        ps.close();
        
        // Test that any single argument being null causes NULL to be returned.
        s.executeUpdate(
                "CREATE FUNCTION CONCAT_NOCALL(VARCHAR(10), VARCHAR(10)) " +
                   "RETURNS VARCHAR(20) " +
                   "RETURNS NULL ON NULL INPUT " +
                   "EXTERNAL NAME '" +
                   RoutineTest.class.getName() + ".concat'  " +
                   "LANGUAGE JAVA PARAMETER STYLE JAVA");
         s.executeUpdate(
                "CREATE FUNCTION CONCAT_CALL(VARCHAR(10), VARCHAR(10)) " +
                   "RETURNS VARCHAR(20) " +
                   "CALLED ON NULL INPUT " +
                   "EXTERNAL NAME '" +
                   RoutineTest.class.getName() + ".concat'  " +
                   "LANGUAGE JAVA PARAMETER STYLE JAVA");
        
        ps = prepareStatement("VALUES CONCAT_NOCALL(?, ?)");
        ps.setString(1, "good");
        ps.setString(2, "bye");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "goodbye");
        
        ps.setString(1, null);
        ps.setString(2, "bye");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);

        ps.setString(1, "good");
        ps.setString(2, null);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
        
        ps.setString(1, null);
        ps.setString(2, null);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
        
        ps.close();

        ps = prepareStatement("VALUES CONCAT_CALL(?, ?)");
        ps.setString(1, "good");
        ps.setString(2, "bye");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "goodbye");
        
        ps.setString(1, null);
        ps.setString(2, "bye");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "s1NULLbye");

        ps.setString(1, "good");
        ps.setString(2, null);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "goods2NULL");
        
        ps.setString(1, null);
        ps.setString(2, null);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "s1NULLs2NULL");
        
        ps.close();
        
        // Now nested calls
        ps = prepareStatement(
          "VALUES CONCAT_NOCALL(CONCAT_NOCALL(?, 'RNNI'), CONCAT_CALL(?, 'CONI'))");
        
        ps.setString(1, "p1");
        ps.setString(2, "p2");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "p1RNNIp2CONI");
        
        ps.setString(1, null);
        ps.setString(2, "p2");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
        
        ps.setString(1, "p1");
        ps.setString(2, null);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "p1RNNIs1NULLCONI");
        
        ps.setString(1, null);
        ps.setString(2, null);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
        ps.close();

        ps = prepareStatement(
          "VALUES CONCAT_CALL(CONCAT_NOCALL(?, 'RNNI'), CONCAT_CALL(?, 'CONI'))");
      
        ps.setString(1, "p1");
        ps.setString(2, "p2");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "p1RNNIp2CONI");
      
        ps.setString(1, null);
        ps.setString(2, "p2");
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "s1NULLp2CONI");
      
        ps.setString(1, "p1");
        ps.setString(2, null);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "p1RNNIs1NULLCONI");
      
        ps.setString(1, null);
        ps.setString(2, null);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "s1NULLs1NULLCONI");
        ps.close();

        
        // Nested calls with SQL types that do not need casts
        // and map to primitive types. This had issues see DERBY-479
        s.executeUpdate(
                "CREATE FUNCTION SAME_NOCALL(INTEGER) " +
                   "RETURNS INTEGER " +
                   "RETURNS NULL ON NULL INPUT " +
                   "EXTERNAL NAME '" +
                   RoutineTest.class.getName() + ".same'  " +
                   "LANGUAGE JAVA PARAMETER STYLE JAVA");
        
        s.executeUpdate(
                "CREATE FUNCTION SAME_CALL(INTEGER) " +
                   "RETURNS INTEGER " +
                   "CALLED ON NULL INPUT " +
                   "EXTERNAL NAME '" +
                   RoutineTest.class.getName() + ".same'  " +
                   "LANGUAGE JAVA PARAMETER STYLE JAVA");
        
        ps = prepareStatement("VALUES SAME_NOCALL(SAME_NOCALL(?))");
        ps.setInt(1, 41);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "41");
        ps.setNull(1, Types.INTEGER);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
        ps.close();
        
        ps = prepareStatement("VALUES SAME_NOCALL(SAME_CALL(?))");
        ps.setInt(1, 47);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "47");
        ps.setNull(1, Types.INTEGER);
        assertStatementError("39004", ps); // Can't pass NULL into primitive type
        ps.close();

        ps = prepareStatement("VALUES SAME_CALL(SAME_NOCALL(?))");
        ps.setInt(1, 41);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "41");
        ps.setNull(1, Types.INTEGER);
        assertStatementError("39004", ps); // Can't pass NULL into primitive type
        ps.close();

        ps = prepareStatement("VALUES SAME_CALL(SAME_CALL(?))");
        ps.setInt(1, 53);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "53");
        ps.setNull(1, Types.INTEGER);
        assertStatementError("39004", ps); // Can't pass NULL into primitive type
        ps.close();

        s.executeUpdate(
                "CREATE FUNCTION NOON_NOCALL(TIME) " +
                   "RETURNS TIME " +
                   "RETURNS NULL ON NULL INPUT " +
                   "EXTERNAL NAME '" +
                   RoutineTest.class.getName() + ".nullAtNoon'  " +
                   "LANGUAGE JAVA PARAMETER STYLE JAVA");
        
        s.executeUpdate(
                "CREATE FUNCTION NOON_CALL(TIME) " +
                   "RETURNS TIME " +
                   "CALLED ON NULL INPUT " +
                   "EXTERNAL NAME '" +
                   RoutineTest.class.getName() + ".nullAtNoon'  " +
                   "LANGUAGE JAVA PARAMETER STYLE JAVA");
        
        // Function maps:
        // NULL to 11:00:00 (if null can be passed)
        // 11:00:00 to 11:30:00
        // 12:00:00 to NULL
        // any other time to itself
        
        Time noon = Time.valueOf("12:00:00"); // mapped to null by the function
        Time tea = Time.valueOf("15:30:00");
        
        ps = prepareStatement("VALUES NOON_NOCALL(?)");
        ps.setTime(1, tea);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), tea.toString());
        ps.setTime(1, noon);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
        ps.setTime(1, null);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
        ps.close();

        ps = prepareStatement("VALUES NOON_CALL(?)");
        ps.setTime(1, tea);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), tea.toString());
        ps.setTime(1, noon);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
        ps.setTime(1, null);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "11:00:00");
        ps.close();
        
        // All the nested calls in these cases take take the
        // value 'tea' will return the same value.
        
        ps = prepareStatement("VALUES NOON_NOCALL(NOON_NOCALL(?))");
        ps.setTime(1, tea);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), tea.toString());
        ps.setTime(1, noon); // noon->NULL->NULL
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
        ps.setTime(1, null); // NULL->NULL->NULL
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);
        ps.close();
        
        ps = prepareStatement("VALUES NOON_NOCALL(NOON_CALL(?))");
        ps.setTime(1, tea);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), tea.toString());
               
        // DERBY-1030 RESULT SHOULD BE NULL
        // noon->NULL by inner function
        // NULL->NULL by outer due to RETURN NULL ON NULL INPUT
        ps.setTime(1, noon); // noon->NULL->NULL
        JDBC.assertSingleValueResultSet(ps.executeQuery(), null);        
        ps.setTime(1, null); // NULL->11:00:00->11:30:00
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "11:30:00");

        ps.close();
        
        ps = prepareStatement("VALUES NOON_CALL(NOON_NOCALL(?))");
        ps.setTime(1, tea);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), tea.toString());
        ps.setTime(1, noon); // noon->NULL->11:00:00
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "11:00:00");
        ps.setTime(1, null); // NULL->NULL->11:00:00
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "11:00:00");
        ps.close();
        
        ps = prepareStatement("VALUES NOON_CALL(NOON_CALL(?))");
        ps.setTime(1, tea);
        JDBC.assertSingleValueResultSet(ps.executeQuery(), tea.toString());
        ps.setTime(1, noon); // noon->NULL->11:00:00
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "11:00:00");
        ps.setTime(1, null); // NULL->11:00:00->11:30:00
        JDBC.assertSingleValueResultSet(ps.executeQuery(), "11:30:00");
        ps.close();
        
        s.close();
    }
    
    /**
     * Test function with an aggregate argument. DERBY-3649
     * @throws SQLException
     */
    public void testAggregateArgument() throws SQLException
    {
    	Statement s = createStatement();
    	s.executeUpdate("CREATE TABLE TEST (I INT)");
    	s.executeUpdate("INSERT INTO TEST VALUES(1)");
    	s.executeUpdate("INSERT INTO TEST VALUES(2)");
    	s.executeUpdate("CREATE FUNCTION CheckCount(count integer) RETURNS INTEGER PARAMETER STYLE JAVA NO SQL LANGUAGE JAVA EXTERNAL NAME 'com.splicemachine.dbTesting.functionTests.tests.lang.RoutineTest.checkCount'");
    	ResultSet rs = s.executeQuery("select checkCount(count(*)) from test");
    	JDBC.assertSingleValueResultSet(rs, "2");
    	
    	
    }
    
    /**
     * Test that we don't get verification errors trying to cram nulls
     * into primitive args. See DERBY-4459.
     */
    public void test_4459() throws Exception
    {
    	Statement s = createStatement();

    	s.executeUpdate
            (
             "create function getNullInt() returns int language java parameter style java\n" +
             "external name '" + RoutineTest.class.getName() + ".getNullInt'"
             );
    	s.executeUpdate
            (
             "create function negateInt( a int ) returns int language java parameter style java\n" +
             "external name '" + RoutineTest.class.getName() + ".negateInt'"
             );

        assertStatementError( CANNOT_STUFF_NULL_INTO_PRIMITIVE, s, "values( negateInt( cast( null as int) ) )" );
        assertStatementError( CANNOT_STUFF_NULL_INTO_PRIMITIVE, s, "values( negateInt( getNullInt() ) )" );
    }

    /**
     * DERBY-5749: Too long (non-blank) argument for VARCHAR parameter does not
     * throw as expected.
     */
    public void test_5749() throws SQLException
    {
        Statement s = createStatement();
        s.executeUpdate("create table t5749(v varchar(5))");
        s.executeUpdate(
            "create procedure p5749 (a varchar(5)) modifies sql data " +
            "external name '" + RoutineTest.class.getName() + ".p5749' " +
            "language java parameter style java");
        CallableStatement cs = prepareCall("call p5749(?)");
        cs.setString(1, "123456");

        // This silently truncates before fix of DERBY-5749
        try {
            cs.execute();
            fail();
        } catch (SQLException e) {
            assertSQLState("22001", e);
        }

        // This silently truncates also
        try {
            s.executeUpdate("call p5749('123456')");
            fail();
        } catch (SQLException e) {
            assertSQLState("22001", e);
        }


        PreparedStatement ps = prepareStatement("insert into t5749 values(?)");
        ps.setString(1, "123456");
        // This does not truncate
        try {
            ps.execute();
            fail();
        } catch (SQLException e) {
            assertSQLState("22001", e);
        }
    }




    /*
    ** Routine implementations called from the tests but do
    *  not use DriverManager so that this test can be used on
    *  J2ME/CDC/Foundation with JSR169.
    */
    
    public static String concat(String s1, String s2)
    {
        if (s1 == null)
            s1 = "s1NULL";
        if (s2 == null)
            s2 = "s2NULL";
        return s1.concat(s2);
    }
    
    public static int same(int i)
    {
        return i;
    }
    
    public static Time nullAtNoon(Time t) {
        if (t == null)
            return Time.valueOf("11:00:00");
        String s = t.toString();
        if ("11:00:00".equals(s))
            return Time.valueOf("11:30:00");
        if ("12:00:00".equals(s))
           return null;
        
        return t;
    }
    

    public static int checkCount(int count)
               throws SQLException {
           //   throws ZeroException {

           if (count == 0) {
               //throw new ZeroException();
               throw new SQLException("No results found", "38777");
           }

           return count;
       }

    public static int negateInt( int arg ) { return -arg; }
    public static Integer getNullInt() { return null; }

    public static void p5749 (String s) {
    }
}

