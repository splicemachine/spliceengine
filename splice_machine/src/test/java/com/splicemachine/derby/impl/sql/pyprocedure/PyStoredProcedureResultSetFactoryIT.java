package com.splicemachine.derby.impl.sql.pyprocedure;

import com.splicemachine.derby.impl.sql.execute.operations.DropTableIT;
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

import static org.junit.Assert.assertTrue;

@Category(SerialTest.class) //serial because it loads a jar
public class PyStoredProcedureResultSetFactoryIT extends SpliceUnitTest {
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String CLASS_NAME = DropTableIT.class.getSimpleName().toUpperCase();
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
    static final String INSTALL_JAR = "CALL SQLJ.INSTALL_JAR('%s', 'SPLICE.TXN_IT_PROCS_JAR', 0)";
    static final String REMOVE_JAR = "CALL SQLJ.REMOVE_JAR('SPLICE.TXN_IT_PROCS_JAR', 0)";
    static final String CALL_SET_CLASSPATH_FOR_DEMO = "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.classpath', 'SPLICE.TXN_IT_PROCS_JAR')";
    static final String CALL_SET_CLASSPATH_FOR_DEFAULT = "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.classpath', NULL)";
    static final String CREATE_DEMO_PROC = String.format("CREATE PROCEDURE %s.DEMO() PARAMETER STYLE JAVA READS SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.txn.TxnTestProcs.DEMO'", SCHEMA_NAME);
    static final String DROP_DEMO_PROC = String.format("DROP PROCEDURE %s.DEMO",SCHEMA_NAME);
    static final String DROP_TEST_TABLE_IF_EXIST ="DROP TABLE TEST_TABLE IF EXISTS";
    static final String CREATE_TEST_TABLE = "CREATE TABLE TEST_TABLE(\n" +
            "BIGINT_COL BIGINT NOT NULL,\n" +
            "BOOLEAN_COL BOOLEAN NOT NULL,\n" +
            "CHAR_COL CHAR NOT NULL,\n" +
            "DATE_COL DATE NOT NULL,\n" +
            "DECIMAL_COL DECIMAL(5,2) NOT NULL,\n" +
            "DOUBLE_COL DOUBLE NOT NULL,\n" +
            "FLOAT_COL FLOAT(52) NOT NULL,\n" +
            "INTEGER_COL INTEGER NOT NULL,\n" +
            "NUMERIC_COL NUMERIC(5,2) NOT NULL,\n" +
            "REAL_COL REAL NOT NULL,\n" +
            "SMALLINT_COL SMALLINT NOT NULL,\n" +
            "TIME_COL TIME NOT NULL,\n" +
            "TIMESTAMP_COL TIMESTAMP NOT NULL,\n" +
            "VARCHAR_COL VARCHAR(30) NOT NULL,\n" +
            "CLOB_COL CLOB(30) NOT NULL,\n" +
            "TEXT_COL TEXT(30) NOT NULL)";

    static final String INSERT_TEST_TABLE = "INSERT INTO TEST_TABLE VALUES(\n" +
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

    static final String CREATE_PYPROC = String.format("CREATE PROCEDURE %s.PYPROC_TYPE_UNIT_TEST() PARAMETER STYLE JAVA READS SQL DATA LANGUAGE PYTHON DYNAMIC RESULT SETS 1 EXTERNAL NAME 'def run(rs):\n" +
            "    c = conn.cursor()\n" +
            "    stmt = \"select * from TEST_TABLE {limit 1}\"\n" +
            "    c.execute(stmt)\n" +
            "    d = c.description\n" +
            "    result = c.fetchall()\n" +
            "    rs[0] = factory.create([d,result])\n" +
            "    conn.commit()\n" +
            "    c.close()\n" +
            "    conn.close()'", SCHEMA_NAME);
    static final String CALL_PYPROC = String.format("CALL %s.PYPROC_TYPE_UNIT_TEST()",SCHEMA_NAME);
    static final String DROP_PYPROC = String.format("DROP PROCEDURE %s.PYPROC_TYPE_UNIT_TEST",SCHEMA_NAME);

    static final String CREATE_JPROC = String.format("CREATE PROCEDURE %s.JPROC_TYPE_UNIT_TEST() PARAMETER STYLE JAVA READS SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.txn.TxnTestProcs.JPROC_TYPE_UNIT_TEST'",SCHEMA_NAME);
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

        String STORED_PROCS_JAR_FILE = System.getProperty("user.dir").replaceFirst("splice_machine","hbase_sql")+"/target/txn-it/txn-it.jar";//getJarFileForClass(TxnTestProcs.class);
        assertTrue("Cannot find procedures jar file: "+STORED_PROCS_JAR_FILE, STORED_PROCS_JAR_FILE != null &&
                STORED_PROCS_JAR_FILE.endsWith("jar"));


        // Install and create the Wrapper Proc (DEMO) for Python Stored Procedure
        spliceClassWatcher.executeUpdate(String.format(INSTALL_JAR, STORED_PROCS_JAR_FILE));
        spliceClassWatcher.executeUpdate(CALL_SET_CLASSPATH_FOR_DEMO);
        spliceClassWatcher.executeUpdate(CREATE_DEMO_PROC);
        // Create the TEST_TABLE and populate it
        spliceClassWatcher.executeUpdate(DROP_TEST_TABLE_IF_EXIST);
        spliceClassWatcher.executeUpdate(CREATE_TEST_TABLE);
        spliceClassWatcher.executeUpdate(INSERT_TEST_TABLE);
        // Create the Python Stored Procedure
        spliceClassWatcher.executeUpdate(CREATE_PYPROC);
        spliceClassWatcher.executeUpdate(CREATE_JPROC);
    }

    @AfterClass
    public static void tearDownClass() throws Exception
    {
        spliceClassWatcher.executeUpdate(DROP_DEMO_PROC);
        spliceClassWatcher.executeUpdate(DROP_TEST_TABLE_IF_EXIST);
        spliceClassWatcher.executeUpdate(CALL_SET_CLASSPATH_FOR_DEFAULT);
        spliceClassWatcher.executeUpdate(REMOVE_JAR);
        spliceClassWatcher.executeUpdate(DROP_JPROC);
        spliceClassWatcher.executeUpdate(DROP_PYPROC);
    }

    @Test
    public void testStoredProcedure() throws Exception
    {
        StringWriter w;
        InputStream in;
        ResultSet pyResultSet = methodWatcher.executeQuery(CALL_PYPROC);
        Object[] pyRow = new Object[COL_NUM];

        while(pyResultSet.next()){
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

            in = pyResultSet.getClob(15).getAsciiStream();
            w = new StringWriter();
            org.apache.commons.io.IOUtils.copy(in, w);
            pyRow[14] = w.toString();                               // CLOB
            w.close();

            in = pyResultSet.getClob(16).getAsciiStream();
            w = new StringWriter();
            org.apache.commons.io.IOUtils.copy(in, w);
            pyRow[15] = w.toString();                               // TEXT
            w.close();
        }
        pyResultSet.close();

        ResultSet javaResultSet = methodWatcher.executeQuery(CALL_JPROC);
        while(javaResultSet.next()){
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

            in = javaResultSet.getClob(15).getAsciiStream();
            w = new StringWriter();
            org.apache.commons.io.IOUtils.copy(in, w);
            String javaClobStr = w.toString();                                          // CLOB
            w.close();
            Assert.assertEquals(pyRow[14], javaClobStr);

            in = javaResultSet.getClob(16).getAsciiStream();
            w = new StringWriter();
            org.apache.commons.io.IOUtils.copy(in, w);
            String javaTextStr = w.toString();                                          // TEXT
            w.close();
            Assert.assertEquals(pyRow[15], javaTextStr);
        }
        javaResultSet.close();
    }
}
