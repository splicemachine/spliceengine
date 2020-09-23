package com.splicemachine.derby.transactions;

import java.sql.*;

import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test.Transactions;

/**
 * This class is the IT test for Python user-defined function.
 * it tests functions that take in different data types as a function's arguments and
 * return these data types as the functions' results. These includes BIGINT, CHAR, DATE,
 * DECIMAL, DOUBLE, DOUBLE PRECISION, FLOAT, INTEGER, NUMERIC, REAL, SMALLINT, TIMESTAMP,
 * TIME, VARCHAR
 * Since LONG VARCHAR, CLOB, TEXT should not be used as user-defined funciton's parameters,
 * These types are tested only as return value
 * @author Ao Zeng
 *		 Created on: 8/1/2018
 */
@Category({Transactions.class,SerialTest.class})
public class PyFunctionIT extends SpliceUnitTest {

    public static final String CLASS_NAME = PyFunctionIT.class.getSimpleName().toUpperCase();

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    // Names of files and SQL objects.
    private static final String SCHEMA_NAME = CLASS_NAME;

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    // Scripts to create and drop testing Python user-defined function
    private static final String CREATE_TEST_BIGINT = String.format("CREATE FUNCTION %s.TEST_BIGINT(var BIGINT) RETURNS BIGINT PARAMETER STYLE JAVA NO SQL LANGUAGE PYTHON AS 'def run(var):\n" +
            "    return var - 1'",SCHEMA_NAME);

    private static final String DROP_TEST_BIGINT  = String.format("DROP FUNCTION %s.TEST_BIGINT",SCHEMA_NAME);

    private static final String CREATE_TEST_BOOLEAN = String.format("CREATE FUNCTION %s.TEST_BOOLEAN(var BOOLEAN) RETURNS BOOLEAN PARAMETER STYLE JAVA NO SQL LANGUAGE PYTHON AS 'def run(var):\n" +
            "    return not var'", SCHEMA_NAME);

    private static final String DROP_TEST_BOOLEAN  = String.format("DROP FUNCTION %s.TEST_BOOLEAN",SCHEMA_NAME);

    private static final String CREATE_TEST_CHAR = String.format("CREATE FUNCTION %s.TEST_CHAR(var CHAR) RETURNS CHAR PARAMETER STYLE JAVA NO SQL LANGUAGE PYTHON AS 'def run(var):\n" +
            "    if var == \"a\":\n" +
            "        return \"s\"\n" +
            "    return \"n\"'", SCHEMA_NAME);

    private static final String DROP_TEST_CHAR  = String.format("DROP FUNCTION %s.TEST_CHAR",SCHEMA_NAME);

    private static final String CREATE_TEST_DATE = String.format("CREATE FUNCTION %s.TEST_DATE(var DATE) RETURNS DATE PARAMETER STYLE JAVA NO SQL LANGUAGE PYTHON AS 'import datetime\n" +
            "def run(var):\n" +
            "    return datetime.date.today()'",SCHEMA_NAME);

    private static final String DROP_TEST_DATE  = String.format("DROP FUNCTION %s.TEST_DATE",SCHEMA_NAME);

    private static final String CREATE_TEST_DECIMAL = String.format("CREATE FUNCTION %s.TEST_DECIMAL(var DECIMAL) RETURNS DECIMAL PARAMETER STYLE JAVA NO SQL LANGUAGE PYTHON AS 'from java.math import BigDecimal\n" +
            "def run(var):\n" +
            "    return var.add(BigDecimal(0.1))'",SCHEMA_NAME);

    private static final String DROP_TEST_DECIMAL  = String.format("DROP FUNCTION %s.TEST_DECIMAL",SCHEMA_NAME);

    private static final String CREATE_TEST_NUMERIC = String.format("CREATE FUNCTION %s.TEST_NUMERIC(var NUMERIC) RETURNS NUMERIC PARAMETER STYLE JAVA NO SQL LANGUAGE PYTHON AS 'from java.math import BigDecimal\n" +
            "def run(var):\n" +
            "    return var.add(BigDecimal(0.1))'", SCHEMA_NAME);

    private static final String DROP_TEST_NUMERIC  = String.format("DROP FUNCTION %s.TEST_NUMERIC",SCHEMA_NAME);

    private static final String CREATE_TEST_DOUBLE = String.format("CREATE FUNCTION %s.TEST_DOUBLE(var DOUBLE) RETURNS DOUBLE PARAMETER STYLE JAVA NO SQL LANGUAGE PYTHON AS 'from java.math import BigDecimal\n" +
            "def run(var):\n" +
            "    return 1.79769E+308'", SCHEMA_NAME);

    private static final String DROP_TEST_DOUBLE  = String.format("DROP FUNCTION %s.TEST_DOUBLE",SCHEMA_NAME);

    private static final String CREATE_TEST_DOUBLE_PRECISION = String.format("CREATE FUNCTION %s.TEST_DOUBLE_PRECISION(var DOUBLE PRECISION) RETURNS DOUBLE PRECISION PARAMETER STYLE JAVA NO SQL LANGUAGE PYTHON AS 'def run(var):\n" +
            "    return 1.79769E+308'", SCHEMA_NAME);

    private static final String DROP_TEST_DOUBLE_PRECISION  = String.format("DROP FUNCTION %s.TEST_DOUBLE_PRECISION",SCHEMA_NAME);

    private static final String CREATE_TEST_FLOAT = String.format("CREATE FUNCTION %s.TEST_FLOAT(var FLOAT) RETURNS FLOAT PARAMETER STYLE JAVA NO SQL LANGUAGE PYTHON AS 'def run(var):\n" +
            "    return 2.225E-307'", SCHEMA_NAME);

    private static final String DROP_TEST_FLOAT  = String.format("DROP FUNCTION %s.TEST_FLOAT",SCHEMA_NAME);

    private static final String CREATE_TEST_INTEGER = String.format("CREATE FUNCTION %s.TEST_INTEGER(var INTEGER) RETURNS INTEGER PARAMETER STYLE JAVA NO SQL LANGUAGE PYTHON AS 'def run(var):\n" +
            "    return var - 2'", SCHEMA_NAME);

    private static final String DROP_TEST_INTEGER  = String.format("DROP FUNCTION %s.TEST_INTEGER",SCHEMA_NAME);

    private static final String CREATE_TEST_REAL = String.format("CREATE FUNCTION %s.TEST_REAL(var REAL) RETURNS REAL PARAMETER STYLE JAVA NO SQL LANGUAGE PYTHON AS 'def run(var):\n" +
            "    return 3.402E+38'", SCHEMA_NAME);

    private static final String DROP_TEST_REAL  = String.format("DROP FUNCTION %s.TEST_REAL",SCHEMA_NAME);

    private static final String CREATE_TEST_SMALLINT = String.format("CREATE FUNCTION %s.TEST_SMALLINT (var SMALLINT) RETURNS SMALLINT PARAMETER STYLE JAVA NO SQL LANGUAGE PYTHON AS 'def run(var):\n" +
            "    return 3267'", SCHEMA_NAME);

    private static final String DROP_TEST_SMALLINT  = String.format("DROP FUNCTION %s.TEST_SMALLINT",SCHEMA_NAME);

    private static final String CREATE_TEST_TIME = String.format("CREATE FUNCTION %s.TEST_TIME(var TIME) RETURNS TIME PARAMETER STYLE JAVA NO SQL LANGUAGE PYTHON AS 'import datetime\n" +
            "def run(var):\n" +
            "    return datetime.time(13,0,0)'", SCHEMA_NAME);

    private static final String DROP_TEST_TIME  = String.format("DROP FUNCTION %s.TEST_TIME",SCHEMA_NAME);

    private static final String CREATE_TEST_TIMESTAMP = String.format("CREATE FUNCTION %s.TEST_TIMESTAMP(var TIMESTAMP) RETURNS TIMESTAMP PARAMETER STYLE JAVA NO SQL LANGUAGE PYTHON AS 'import datetime\n" +
            "def run(var):\n" +
            "    return datetime.datetime.now()'", SCHEMA_NAME);

    private static final String DROP_TEST_TIMESTAMP  = String.format("DROP FUNCTION %s.TEST_TIMESTAMP",SCHEMA_NAME);

    private static final String CREATE_TEST_VARCHAR = String.format("CREATE FUNCTION %s.TEST_VARCHAR(var VARCHAR(50)) RETURNS VARCHAR(50) PARAMETER STYLE JAVA NO SQL LANGUAGE PYTHON AS 'def run(var):\n" +
            "    return var + \"Testing VARCHAR datatype\"'", SCHEMA_NAME);

    private static final String DROP_TEST_VARCHAR  = String.format("DROP FUNCTION %s.TEST_VARCHAR",SCHEMA_NAME);

    private static final String CREATE_TEST_CLOB = String.format("CREATE FUNCTION %s.TEST_CLOB() RETURNS CLOB PARAMETER STYLE JAVA NO SQL LANGUAGE PYTHON AS 'def run():\n" +
            "    return \"Testing CLOB datatype\"'", SCHEMA_NAME);

    private static final String DROP_TEST_CLOB  = String.format("DROP FUNCTION %s.TEST_CLOB",SCHEMA_NAME);

    private static final String CREATE_TEST_TEXT = String.format("CREATE FUNCTION %s.TEST_TEXT() RETURNS TEXT PARAMETER STYLE JAVA NO SQL LANGUAGE PYTHON AS 'def run():\n" +
            "    return \"Testing TEXT datatype\"'", SCHEMA_NAME);

    private static final String DROP_TEST_TEXT  = String.format("DROP FUNCTION %s.TEST_TEXT",SCHEMA_NAME);

    private static final String CREATE_TEST_LONGVARCHAR = String.format("CREATE FUNCTION %s.TEST_LONGVARCHAR() RETURNS LONG VARCHAR PARAMETER STYLE JAVA NO SQL LANGUAGE PYTHON AS 'def run():\n" +
            "    return \"Testing LONG VARCHAR datatype\"'", SCHEMA_NAME);

    private static final String DROP_TEST_LONGVARCHAR  = String.format("DROP FUNCTION %s.TEST_LONGVARCHAR",SCHEMA_NAME);

    // SQL statement to evaluate the function
    private static final String CALL_TEST_BIGINT = String.format("VALUES %s.TEST_BIGINT(9223372036854775807)", SCHEMA_NAME);
    private static final String CALL_TEST_BOOLEAN = String.format("VALUES %s.TEST_BOOLEAN(true)", SCHEMA_NAME);
    private static final String CALL_TEST_CHAR = String.format("VALUES %s.TEST_CHAR('a')", SCHEMA_NAME);
    private static final String CALL_TEST_DATE = String.format("VALUES %s.TEST_DATE('1993-09-23')", SCHEMA_NAME);
    private static final String CALL_TEST_DECIMAL = String.format("VALUES %s.TEST_DECIMAL(2.798765)", SCHEMA_NAME);
    private static final String CALL_TEST_NUMERIC = String.format("VALUES %s.TEST_NUMERIC(2.798765)", SCHEMA_NAME);
    private static final String CALL_TEST_DOUBLE = String.format("VALUES %s.TEST_DOUBLE(2.798765)", SCHEMA_NAME);
    private static final String CALL_TEST_DOUBLE_PRECISION = String.format("VALUES %s.TEST_DOUBLE_PRECISION(2.798765)", SCHEMA_NAME);
    private static final String CALL_TEST_INTEGER = String.format("VALUES %s.TEST_INTEGER(2)", SCHEMA_NAME);
    private static final String CALL_TEST_FLOAT = String.format("VALUES %s.TEST_FLOAT(2.798765)", SCHEMA_NAME);
    private static final String CALL_TEST_REAL = String.format("VALUES %s.TEST_REAL(2.798765)", SCHEMA_NAME);
    private static final String CALL_TEST_SMALLINT = String.format("VALUES %s.TEST_SMALLINT(1)", SCHEMA_NAME);
    private static final String CALL_TEST_TIME = String.format("VALUES %s.TEST_TIME('15:09:02')", SCHEMA_NAME);
    private static final String CALL_TEST_TIMESTAMP = String.format("VALUES %s.TEST_TIMESTAMP('2013-03-23 19:45:00')", SCHEMA_NAME);
    private static final String CALL_TEST_VARCHAR = String.format("VALUES %s.TEST_VARCHAR('THIS IS A TEST ')", SCHEMA_NAME);
    private static final String CALL_TEST_CLOB = String.format("VALUES %s.TEST_CLOB()", SCHEMA_NAME);
    private static final String CALL_TEST_TEXT = String.format("VALUES %s.TEST_TEXT()", SCHEMA_NAME);
    private static final String CALL_TEST_LONGVARCHAR = String.format("VALUES %s.TEST_LONGVARCHAR()", SCHEMA_NAME);

    // Error message
    private static final String INCORRECT_CODE_RESULT_COUNT = "Incorrect return code or result count returned!";

    @Test
    public void testBigInt() throws Exception {
        int rc;
        ResultSet rs;
        rc = methodWatcher.executeUpdate(CREATE_TEST_BIGINT);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
        rs = methodWatcher.executeQuery(CALL_TEST_BIGINT);
        rs.next();
        long retrievedVal = rs.getLong(1);
        Assert.assertEquals(9223372036854775806L, retrievedVal);
        rc = methodWatcher.executeUpdate(DROP_TEST_BIGINT);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
    }

    @Test
    public void testBoolean() throws Exception {
        int rc;
        ResultSet rs;
        rc = methodWatcher.executeUpdate(CREATE_TEST_BOOLEAN);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
        rs = methodWatcher.executeQuery(CALL_TEST_BOOLEAN);
        rs.next();
        boolean retrievedVal = rs.getBoolean(1);
        Assert.assertEquals(false, retrievedVal);
        rc = methodWatcher.executeUpdate(DROP_TEST_BOOLEAN);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
    }

    @Test
    public void testChar() throws Exception {
        int rc;
        ResultSet rs;
        rc = methodWatcher.executeUpdate(CREATE_TEST_CHAR);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
        rs = methodWatcher.executeQuery(CALL_TEST_CHAR);
        rs.next();
        String retrievedVal = rs.getString(1);
        Assert.assertEquals("s", retrievedVal);
        rc = methodWatcher.executeUpdate(DROP_TEST_CHAR);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
    }

    @Test
    public void testDate() throws Exception {
        int rc;
        ResultSet rs;
        rc = methodWatcher.executeUpdate(CREATE_TEST_DATE);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
        rs = methodWatcher.executeQuery(CALL_TEST_DATE);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 1, resultSetSize(rs));
        rc = methodWatcher.executeUpdate(DROP_TEST_DATE);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
    }

    /**
     * The following test is currently ignored due to existing bug in type conversion for
     * BigDecimal -- DB-7321
     */
    @Ignore
    @Test
    public void testDecimal() throws Exception {
        int rc;
        ResultSet rs;
        rc = methodWatcher.executeUpdate(CREATE_TEST_DECIMAL);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
        rs = methodWatcher.executeQuery(CALL_TEST_DECIMAL);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 1, resultSetSize(rs));
        rc = methodWatcher.executeUpdate(DROP_TEST_DECIMAL);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
    }

    /**
     * The following test is currently ignored due to existing bug in type conversion for
     * BigDecimal -- DB-7321
     */
    @Ignore
    @Test
    public void testNumeric() throws Exception {
        int rc;
        ResultSet rs;
        rc = methodWatcher.executeUpdate(CREATE_TEST_NUMERIC);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
        rs = methodWatcher.executeQuery(CALL_TEST_NUMERIC);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 1, resultSetSize(rs));
        rc = methodWatcher.executeUpdate(DROP_TEST_NUMERIC);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
    }

    @Test
    public void testDouble() throws Exception {
        int rc;
        ResultSet rs;
        rc = methodWatcher.executeUpdate(CREATE_TEST_DOUBLE);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
        rs = methodWatcher.executeQuery(CALL_TEST_DOUBLE);
        rs.next();
        Double retrievedVal = rs.getDouble(1);
        Assert.assertEquals((Double )1.79769E+308,retrievedVal);
        rc = methodWatcher.executeUpdate(DROP_TEST_DOUBLE);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
    }

    @Test
    public void testDoublePrecision() throws Exception {
        int rc;
        ResultSet rs;
        rc = methodWatcher.executeUpdate(CREATE_TEST_DOUBLE_PRECISION);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
        rs = methodWatcher.executeQuery(CALL_TEST_DOUBLE_PRECISION);
        rs.next();
        Double retrievedVal = rs.getDouble(1);
        Assert.assertEquals((Double )1.79769E+308,retrievedVal);
        rc = methodWatcher.executeUpdate(DROP_TEST_DOUBLE_PRECISION);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
    }

    @Test
    public void testInteger() throws Exception {
        int rc;
        ResultSet rs;
        rc = methodWatcher.executeUpdate(CREATE_TEST_INTEGER);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
        rs = methodWatcher.executeQuery(CALL_TEST_INTEGER);
        rs.next();
        int retrievedVal = rs.getInt(1);
        Assert.assertEquals(0,retrievedVal);
        rc = methodWatcher.executeUpdate(DROP_TEST_INTEGER);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
    }

    @Test
    public void testFloat() throws Exception {
        int rc;
        ResultSet rs;
        rc = methodWatcher.executeUpdate(CREATE_TEST_FLOAT);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
        rs = methodWatcher.executeQuery(CALL_TEST_FLOAT);
        rs.next();
        Double retrievedVal = rs.getDouble(1);
        Assert.assertEquals((Double)2.225E-307, retrievedVal);
        rc = methodWatcher.executeUpdate(DROP_TEST_FLOAT);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
    }

    @Test
    public void testReal() throws Exception {
        int rc;
        ResultSet rs;
        rc = methodWatcher.executeUpdate(CREATE_TEST_REAL);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
        rs = methodWatcher.executeQuery(CALL_TEST_REAL);
        rs.next();
        Double retrievedVal = rs.getDouble(1);
        Assert.assertEquals((Double)3.4020000005553803E38, retrievedVal);
        rc = methodWatcher.executeUpdate(DROP_TEST_REAL);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
    }

    @Test
    public void testSmallint() throws Exception {
        int rc;
        ResultSet rs;
        rc = methodWatcher.executeUpdate(CREATE_TEST_SMALLINT);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
        rs = methodWatcher.executeQuery(CALL_TEST_SMALLINT);
        rs.next();
        Short retrievedVal = rs.getShort(1);
        short expectedShort = (short) 3267;
        Assert.assertEquals((Short) expectedShort, retrievedVal);
        rc = methodWatcher.executeUpdate(DROP_TEST_SMALLINT);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
    }

    @Test
    public void testTime() throws Exception {
        int rc;
        ResultSet rs;
        rc = methodWatcher.executeUpdate(CREATE_TEST_TIME);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
        rs = methodWatcher.executeQuery(CALL_TEST_TIME);
        rs.next();
        Time retrievedVal = rs.getTime(1);
        Assert.assertEquals(retrievedVal.toString(),"13:00:00");
        rc = methodWatcher.executeUpdate(DROP_TEST_TIME);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
    }

    @Test
    public void testTimestamp() throws Exception {
        int rc;
        ResultSet rs;
        rc = methodWatcher.executeUpdate(CREATE_TEST_TIMESTAMP);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
        rs = methodWatcher.executeQuery(CALL_TEST_TIMESTAMP);
        rs.next();
        Time retrievedVal = rs.getTime(1);
        Assert.assertTrue(retrievedVal!= null);
        rc = methodWatcher.executeUpdate(DROP_TEST_TIMESTAMP);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
    }

    @Test
    public void testVarchar() throws Exception {
        int rc;
        ResultSet rs;
        rc = methodWatcher.executeUpdate(CREATE_TEST_VARCHAR);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
        rs = methodWatcher.executeQuery(CALL_TEST_VARCHAR);
        rs.next();
        String retrievedVal = rs.getString(1);
        Assert.assertEquals("THIS IS A TEST Testing VARCHAR datatype",retrievedVal);
        rc = methodWatcher.executeUpdate(DROP_TEST_VARCHAR);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
    }

    @Test
    public void testClob() throws Exception {
        int rc;
        ResultSet rs;
        rc = methodWatcher.executeUpdate(CREATE_TEST_CLOB);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
        rs = methodWatcher.executeQuery(CALL_TEST_CLOB);
        rs.next();
        Clob retrievedVal = rs.getClob(1);
        Assert.assertTrue(retrievedVal!= null);
        rs.close();
        rc = methodWatcher.executeUpdate(DROP_TEST_CLOB);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
    }

    @Test
    public void testText() throws Exception {
        int rc;
        ResultSet rs;
        rc = methodWatcher.executeUpdate(CREATE_TEST_TEXT);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
        rs = methodWatcher.executeQuery(CALL_TEST_TEXT);
        rs.next();
        Clob retrievedVal = rs.getClob(1);
        Assert.assertTrue(retrievedVal!= null);
        rs.close();
        rc = methodWatcher.executeUpdate(DROP_TEST_TEXT);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
    }

    @Test
    public void testLongvarchar() throws Exception {
        int rc;
        ResultSet rs;
        rc = methodWatcher.executeUpdate(CREATE_TEST_LONGVARCHAR);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
        rs = methodWatcher.executeQuery(CALL_TEST_LONGVARCHAR);
        rs.next();
        String retrievedVal = rs.getString(1);
        Assert.assertEquals("Testing LONG VARCHAR datatype", retrievedVal);
        rc = methodWatcher.executeUpdate(DROP_TEST_LONGVARCHAR);
        Assert.assertEquals(INCORRECT_CODE_RESULT_COUNT, 0, rc);
    }
}