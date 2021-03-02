package com.splicemachine.derby.impl.sql.pyprocedure;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test.SerialTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.InputStream;
import java.io.StringWriter;
import java.sql.*;
import java.util.LinkedList;
import java.util.Queue;

@Category(SerialTest.class) //serial because it loads a jar
public class PyStoredProcedureResultSetFactoryIT extends SpliceUnitTest {
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String CLASS_NAME = PyStoredProcedureResultSetFactoryIT.class.getSimpleName().toUpperCase();
    private static final SpliceSchemaWatcher schema = new SpliceSchemaWatcher(CLASS_NAME);
    private static final String SCHEMA_NAME = CLASS_NAME;

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(schema);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    static final int COL_NUM = 16;
    // Constants for DB connnection
    static final String JDBC_DRIVER = "com.splicemachine.db.jdbc.ClientDriver";
    static final String DB_URL = "jdbc:splice://localhost:1527/splicedb";
    static final String USER = "splice";
    static final String PASSWORD = "admin";
    // Sql Command
    static final String DROP_TEST_TABLE_IF_EXIST ="DROP TABLE TEST_TABLE IF EXISTS";
    static final String CREATE_TEST_TABLE = "CREATE TABLE TEST_TABLE(\n" +
            "BIGINT_COL BIGINT,\n" +
            "BOOLEAN_COL BOOLEAN,\n" +
            "CHAR_COL CHAR,\n" +
            "DATE_COL DATE,\n" +
            "DECIMAL_COL DECIMAL(5,2),\n" +
            "DOUBLE_COL DOUBLE,\n" +
            "FLOAT_COL FLOAT(52),\n" +
            "INTEGER_COL INTEGER,\n" +
            "NUMERIC_COL NUMERIC(5,2),\n" +
            "REAL_COL REAL,\n" +
            "SMALLINT_COL SMALLINT,\n" +
            "TIME_COL TIME,\n" +
            "TIMESTAMP_COL TIMESTAMP,\n" +
            "VARCHAR_COL VARCHAR(30),\n" +
            "CLOB_COL CLOB(30),\n" +
            "TEXT_COL TEXT(30))";

    static final String INSERT_TEST_TABLE_WITH_VALUE = "INSERT INTO TEST_TABLE VALUES(\n" +
            "    9223372036854775807,\n" +              // BIGINT
            "    true,\n" +                             // BOOLEAN
            "    'a',\n" +                              // CHAR
            "    '07/10/1991',\n" +                     // DATE
            "    2.798765,\n" +                         // DECIMAL
            "    3421E+09,\n" +                         // DOUBLE
            "    3421E+09,\n" +                         // FLOAT
            "    11,\n" +                               // INTEGER
            "    1.798765,\n" +                         // NUMERIC
            "    3421E+09,\n" +                         // REAL
            "    32767,\n" +                            // SMALLINT
            "    '15:09:02',\n" +                       // TIME
            "    '2013-03-23 09:45:00',\n" +            // TIMESTAMP
            "    'this is a test for test_table',\n" +  // VARCHAR
            "    'This is clob col',\n" +               // CLOB
            "    'this is text col')";                  // TEXT

    static final String INSERT_TEST_TABLE_WITH_NULL = "INSERT INTO TEST_TABLE VALUES(\n" +
            "    NULL,\n" +
            "    NULL,\n" +
            "    NULL,\n" +
            "    NULL,\n" +
            "    NULL,\n" +
            "    NULL,\n" +
            "    NULL,\n" +
            "    NULL,\n" +
            "    NULL,\n" +
            "    NULL,\n" +
            "    NULL,\n" +
            "    NULL,\n" +
            "    NULL,\n" +
            "    NULL,\n" +
            "    NULL,\n" +
            "    NULL)";

    static final String CREATE_PYPROC = String.format("CREATE PROCEDURE %s.PYPROC_TYPE_UNIT_TEST() PARAMETER STYLE JAVA READS SQL DATA LANGUAGE PYTHON DYNAMIC RESULT SETS 1 AS 'def run(rs):\n" +
            "    c = conn.cursor()\n" +
            "    stmt = \"select * from TEST_TABLE\"\n" +
            "    c.execute(stmt)\n" +
            "    d = c.description\n" +
            "    result = c.fetchall()\n" +
            "    rs[0] = factory.create([d,result])\n" +
            "    conn.commit()\n" +
            "    c.close()\n" +
            "    conn.close()'", SCHEMA_NAME);
    static final String CALL_PYPROC = String.format("CALL %s.PYPROC_TYPE_UNIT_TEST()",SCHEMA_NAME);
    static final String DROP_PYPROC = String.format("DROP PROCEDURE %s.PYPROC_TYPE_UNIT_TEST",SCHEMA_NAME);

    private static String STORED_PROCS_JAR_FILE;
    private static final String FILE_NAME = "SQLJ_IT_PROCS_JAR";
    private static final String JAR_FILE_SQL_NAME = SCHEMA_NAME + "." + FILE_NAME;

    static final String CREATE_JPROC = String.format("CREATE PROCEDURE %s.JPROC_TYPE_UNIT_TEST() PARAMETER STYLE JAVA READS SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.sqlj.SqlJTestProcs.JPROC_TYPE_UNIT_TEST'",SCHEMA_NAME);
    static final String DROP_JPROC = String.format("DROP PROCEDURE %s.JPROC_TYPE_UNIT_TEST",SCHEMA_NAME);
    static final String CALL_JPROC = String.format("CALL %s.JPROC_TYPE_UNIT_TEST()",SCHEMA_NAME);

    public static Connection getConnection() throws SQLException
    {
        Connection conn = DriverManager.getConnection(DB_URL, USER, PASSWORD);
        return conn;
    }

    @BeforeClass
    public static void setUpClass() throws Exception
    {
        // Create the TEST_TABLE and populate it
        spliceClassWatcher.executeUpdate(DROP_TEST_TABLE_IF_EXIST);
        spliceClassWatcher.executeUpdate(CREATE_TEST_TABLE);
        spliceClassWatcher.executeUpdate(INSERT_TEST_TABLE_WITH_NULL);
        spliceClassWatcher.executeUpdate(INSERT_TEST_TABLE_WITH_VALUE);
        // Create the Python Stored Procedure
        spliceClassWatcher.executeUpdate(CREATE_PYPROC);

        // install jar file and set classpath
        STORED_PROCS_JAR_FILE = getSqlItJarFile();
        spliceClassWatcher.executeUpdate(String.format("CALL SQLJ.INSTALL_JAR('%s', '%s', 0)",STORED_PROCS_JAR_FILE,JAR_FILE_SQL_NAME));
        spliceClassWatcher.execute(String.format("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.classpath', '%s')", JAR_FILE_SQL_NAME));
        spliceClassWatcher.executeUpdate(CREATE_JPROC);

    }

    @AfterClass
    public static void tearDownClass() throws Exception
    {
        spliceClassWatcher.executeUpdate(DROP_TEST_TABLE_IF_EXIST);
        spliceClassWatcher.executeUpdate(DROP_JPROC);
        spliceClassWatcher.executeUpdate(DROP_PYPROC);
        spliceClassWatcher.execute("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.classpath', NULL)");
        spliceClassWatcher.executeUpdate(String.format("CALL SQLJ.REMOVE_JAR('%s', 0)", JAR_FILE_SQL_NAME));
    }

    @Test
    public void testStoredProcedure() throws Exception
    {

        ResultSet pyResultSet = methodWatcher.executeQuery(CALL_PYPROC);
        Queue<Object[]> pyRows = new LinkedList<>();

        while(pyResultSet.next()){
            Object[] pyRow = new Object[COL_NUM];
            pyRow[0] = pyResultSet.getLong(1);          // BIGINT
            pyRow[1] = pyResultSet.getBoolean(2);       // BOOLEAN
            pyRow[2] = pyResultSet.getString(3);        // CHAR
            pyRow[3] = pyResultSet.getDate(4);          // DATE
            pyRow[4] = pyResultSet.getBigDecimal(5);    // DECIMAL
            pyRow[5] = pyResultSet.getDouble(6);        // DOUBLE
            pyRow[6] = pyResultSet.getDouble(7);        // FLOAT
            pyRow[7] = pyResultSet.getInt(8);           // INTEGER
            pyRow[8] = pyResultSet.getBigDecimal(9);    // NUMERIC
            pyRow[9] = pyResultSet.getFloat(10);        // REAL
            pyRow[10] = pyResultSet.getShort(11);       // SMALLINT
            pyRow[11] = pyResultSet.getTime(12);        // TIME
            pyRow[12] = pyResultSet.getTimestamp(13);   // TIME_STAMP
            pyRow[13] = pyResultSet.getString(14);      // VARCHAR

            pyRow[14] = getStr(pyResultSet.getClob(15));// CLOB

            pyRow[15] = getStr(pyResultSet.getClob(16));// TEXT
            pyRows.add(pyRow);
        }
        pyResultSet.close();

        ResultSet javaResultSet = methodWatcher.executeQuery(CALL_JPROC);
        while(javaResultSet.next()){
            Object[] pyRow = pyRows.poll();
            Assert.assertEquals(pyRow[0],javaResultSet.getLong(1));         // BIGINT
            Assert.assertEquals(pyRow[1],javaResultSet.getBoolean(2));      // BOOLEAN
            Assert.assertEquals(pyRow[2],javaResultSet.getString(3));       // CHAR
            Assert.assertEquals(pyRow[3],javaResultSet.getDate(4));         // DATE
            Assert.assertEquals(pyRow[4],javaResultSet.getBigDecimal(5));   // DECIMAL
            Assert.assertEquals(pyRow[5],javaResultSet.getDouble(6));       // DOUBLE
            Assert.assertEquals(pyRow[6],javaResultSet.getDouble(7));       // FLOAT
            Assert.assertEquals(pyRow[7],javaResultSet.getInt(8));          // INTEGER
            Assert.assertEquals(pyRow[8],javaResultSet.getBigDecimal(9));   // NUMERIC
            Assert.assertEquals(pyRow[9],javaResultSet.getFloat(10));       // REAL
            Assert.assertEquals(pyRow[10],javaResultSet.getShort(11));      // SMALLINT
            Assert.assertEquals(pyRow[11],javaResultSet.getTime(12));       // TIME
            Assert.assertEquals(pyRow[12],javaResultSet.getTimestamp(13));  // TIME_STAMP
            Assert.assertEquals(pyRow[13],javaResultSet.getString(14));     // VARCHAR

            String javaClobStr = getStr(javaResultSet.getClob(15));         // CLOB
            Assert.assertEquals(pyRow[14], javaClobStr);

            String javaTextStr = getStr(javaResultSet.getClob(16));         // TEXT
            Assert.assertEquals(pyRow[15], javaTextStr);
        }
        javaResultSet.close();
    }

    private String getStr(Clob clob) throws Exception{
        StringWriter w;
        InputStream in;
        if(clob == null) return null;
        in = clob.getAsciiStream();
        w = new StringWriter();
        org.apache.commons.io.IOUtils.copy(in, w);
        String clobStr = w.toString();                                          // CLOB
        w.close();
        return clobStr;
    }
}
