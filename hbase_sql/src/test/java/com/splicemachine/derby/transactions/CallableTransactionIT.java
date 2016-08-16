/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.transactions;

import static org.junit.Assert.assertTrue;

import java.sql.*;

import com.splicemachine.derby.test.framework.*;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test.Transactions;

/**
 * This class tests the transactional correctness of the Splice Machine stored procedure execution framework.
 * The stored procedures are contained in an external JAR file that are dynamically loaded into the Splice Machine
 * database with the SQLJ jar file loading system procedures.  If your tests require a custom stored procedure,
 * you can add it to that jar file and load it into your 'IT' tests like you see below in the tests.
 *
 *
 * @author David Winters
 *		 Created on: 2/27/15
 */
@Category({Transactions.class,SerialTest.class}) //made serial because it loads a jar
public class CallableTransactionIT extends SpliceUnitTest {

	public static final String CLASS_NAME = CallableTransactionIT.class.getSimpleName().toUpperCase();

	// Names of files and SQL objects.
	private static final String SCHEMA_NAME = CLASS_NAME;
	private static final String JAR_FILE_SQL_NAME = SCHEMA_NAME + ".TXN_IT_PROCS_JAR";
	private static final String EMPLOYEE_TABLE_NAME_BASE = SCHEMA_NAME + ".EMPLOYEE";

	// SQL SQL statements to load the custom jar files and add it to the CLASSPATH.
	private static final String CALL_INSTALL_JAR_FORMAT_STRING = "CALL SQLJ.INSTALL_JAR('%s', '%s', 0)";
	private static final String CALL_REMOVE_JAR_FORMAT_STRING = "CALL SQLJ.REMOVE_JAR('%s', 0)";
	private static final String CALL_SET_CLASSPATH_FORMAT_STRING = "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.classpath', '%s')";

	// SQL statements to create and drop stored procedures.
	private static final String CREATE_PROC_CREATE_EMPLOYEE_TABLE = String.format("CREATE PROCEDURE %s.CREATE_EMPLOYEE_TABLE(IN tableName VARCHAR(40)) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 0 EXTERNAL NAME 'org.splicetest.txn.TxnTestProcs.CREATE_EMPLOYEE_TABLE'", SCHEMA_NAME);
	private static final String DROP_PROC_CREATE_EMPLOYEE_TABLE = String.format("DROP PROCEDURE %s.CREATE_EMPLOYEE_TABLE", SCHEMA_NAME);
	private static final String CREATE_PROC_DROP_EMPLOYEE_TABLE = String.format("CREATE PROCEDURE %s.DROP_EMPLOYEE_TABLE(IN tableName VARCHAR(40)) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 0 EXTERNAL NAME 'org.splicetest.txn.TxnTestProcs.DROP_EMPLOYEE_TABLE'", SCHEMA_NAME);
	private static final String DROP_PROC_DROP_EMPLOYEE_TABLE = String.format("DROP PROCEDURE %s.DROP_EMPLOYEE_TABLE", SCHEMA_NAME);
	private static final String CREATE_PROC_INSERT_EMPLOYEE = String.format("CREATE PROCEDURE %s.INSERT_EMPLOYEE(IN tableName VARCHAR(40), IN id INT, IN fname VARCHAR(20), IN lname VARCHAR(30)) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 0 EXTERNAL NAME 'org.splicetest.txn.TxnTestProcs.INSERT_EMPLOYEE'", SCHEMA_NAME);
	private static final String DROP_PROC_INSERT_EMPLOYEE = String.format("DROP PROCEDURE %s.INSERT_EMPLOYEE", SCHEMA_NAME);
	private static final String CREATE_PROC_GET_EMPLOYEE = String.format("CREATE PROCEDURE %s.GET_EMPLOYEE(IN tableName VARCHAR(40), IN id INT) PARAMETER STYLE JAVA READS SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.txn.TxnTestProcs.GET_EMPLOYEE'", SCHEMA_NAME);
	private static final String DROP_PROC_GET_EMPLOYEE = String.format("DROP PROCEDURE %s.GET_EMPLOYEE", SCHEMA_NAME);
	private static final String CREATE_PROC_INSERT_AND_GET_EMPLOYEE = String.format("CREATE PROCEDURE %s.INSERT_AND_GET_EMPLOYEE(IN tableName VARCHAR(40), IN id INT, IN fname VARCHAR(20), IN lname VARCHAR(30)) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.txn.TxnTestProcs.INSERT_AND_GET_EMPLOYEE'", SCHEMA_NAME);
	private static final String DROP_PROC_INSERT_AND_GET_EMPLOYEE = String.format("DROP PROCEDURE %s.INSERT_AND_GET_EMPLOYEE", SCHEMA_NAME);
	private static final String CREATE_PROC_CREATE_INSERT_AND_GET_EMPLOYEE = String.format("CREATE PROCEDURE %s.CREATE_INSERT_AND_GET_EMPLOYEE(IN tableName VARCHAR(40), IN id INT, IN fname VARCHAR(20), IN lname VARCHAR(30)) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.txn.TxnTestProcs.CREATE_INSERT_AND_GET_EMPLOYEE'", SCHEMA_NAME);
	private static final String DROP_PROC_CREATE_INSERT_AND_GET_EMPLOYEE = String.format("DROP PROCEDURE %s.CREATE_INSERT_AND_GET_EMPLOYEE", SCHEMA_NAME);
	private static final String CREATE_PROC_INSERT_AND_GET_EMPLOYEE_COMMIT_TXN = String.format("CREATE PROCEDURE %s.INSERT_AND_GET_EMPLOYEE_COMMIT_TXN(IN tableName VARCHAR(40), IN id INT, IN fname VARCHAR(20), IN lname VARCHAR(30)) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.txn.TxnTestProcs.INSERT_AND_GET_EMPLOYEE_COMMIT_TXN'", SCHEMA_NAME);
	private static final String DROP_PROC_INSERT_AND_GET_EMPLOYEE_COMMIT_TXN = String.format("DROP PROCEDURE %s.INSERT_AND_GET_EMPLOYEE_COMMIT_TXN", SCHEMA_NAME);
	private static final String CREATE_PROC_INSERT_AND_GET_EMPLOYEE_NO_COMMIT_TXN = String.format("CREATE PROCEDURE %s.INSERT_AND_GET_EMPLOYEE_NO_COMMIT_TXN(IN tableName VARCHAR(40), IN id INT, IN fname VARCHAR(20), IN lname VARCHAR(30)) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.txn.TxnTestProcs.INSERT_AND_GET_EMPLOYEE_NO_COMMIT_TXN'", SCHEMA_NAME);
	private static final String DROP_PROC_INSERT_AND_GET_EMPLOYEE_NO_COMMIT_TXN = String.format("DROP PROCEDURE %s.INSERT_AND_GET_EMPLOYEE_NO_COMMIT_TXN", SCHEMA_NAME);
	private static final String CREATE_PROC_INSERT_AND_GET_EMPLOYEE_ROLLBACK_TXN = String.format("CREATE PROCEDURE %s.INSERT_AND_GET_EMPLOYEE_ROLLBACK_TXN(IN tableName VARCHAR(40), IN id INT, IN fname VARCHAR(20), IN lname VARCHAR(30)) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.txn.TxnTestProcs.INSERT_AND_GET_EMPLOYEE_ROLLBACK_TXN'", SCHEMA_NAME);
	private static final String DROP_PROC_INSERT_AND_GET_EMPLOYEE_ROLLBACK_TXN = String.format("DROP PROCEDURE %s.INSERT_AND_GET_EMPLOYEE_ROLLBACK_TXN", SCHEMA_NAME);
	private static final String CREATE_PROC_INSERT_AND_GET_EMPLOYEE_RELEASE_SVPT = String.format("CREATE PROCEDURE %s.INSERT_AND_GET_EMPLOYEE_RELEASE_SVPT(IN tableName VARCHAR(40), IN id INT, IN fname VARCHAR(20), IN lname VARCHAR(30)) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.txn.TxnTestProcs.INSERT_AND_GET_EMPLOYEE_RELEASE_SVPT'", SCHEMA_NAME);
	private static final String DROP_PROC_INSERT_AND_GET_EMPLOYEE_RELEASE_SVPT = String.format("DROP PROCEDURE %s.INSERT_AND_GET_EMPLOYEE_RELEASE_SVPT", SCHEMA_NAME);
	private static final String CREATE_PROC_INSERT_AND_GET_EMPLOYEE_NO_RELEASE_SVPT = String.format("CREATE PROCEDURE %s.INSERT_AND_GET_EMPLOYEE_NO_RELEASE_SVPT(IN tableName VARCHAR(40), IN id INT, IN fname VARCHAR(20), IN lname VARCHAR(30)) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.txn.TxnTestProcs.INSERT_AND_GET_EMPLOYEE_NO_RELEASE_SVPT'", SCHEMA_NAME);
	private static final String DROP_PROC_INSERT_AND_GET_EMPLOYEE_NO_RELEASE_SVPT = String.format("DROP PROCEDURE %s.INSERT_AND_GET_EMPLOYEE_NO_RELEASE_SVPT", SCHEMA_NAME);
	private static final String CREATE_PROC_INSERT_AND_GET_EMPLOYEE_ROLLBACK_SVPT = String.format("CREATE PROCEDURE %s.INSERT_AND_GET_EMPLOYEE_ROLLBACK_SVPT(IN tableName VARCHAR(40), IN id INT, IN fname VARCHAR(20), IN lname VARCHAR(30)) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.txn.TxnTestProcs.INSERT_AND_GET_EMPLOYEE_ROLLBACK_SVPT'", SCHEMA_NAME);
	private static final String DROP_PROC_INSERT_AND_GET_EMPLOYEE_ROLLBACK_SVPT = String.format("DROP PROCEDURE %s.INSERT_AND_GET_EMPLOYEE_ROLLBACK_SVPT", SCHEMA_NAME);
    private static final String CREATE_PROC_GET_EMPLOYEE_MULTIPLE_OUTPUT_PARAMS = String.format("CREATE PROCEDURE %s.GET_EMPLOYEE_MULTIPLE_OUTPUT_PARAMS(IN tableName VARCHAR(40), IN id INT, OUT errorCode VARCHAR(100), OUT errorMessage VARCHAR(100)) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.txn.TxnTestProcs.GET_EMPLOYEE_MULTIPLE_OUTPUT_PARAMS'", SCHEMA_NAME);
    private static final String DROP_PROC_GET_EMPLOYEE_MULTIPLE_OUTPUT_PARAMS = String.format("DROP PROCEDURE %s.GET_EMPLOYEE_MULTIPLE_OUTPUT_PARAMS", SCHEMA_NAME);

	// SQL statements to call stored procedures.
	private static final String CALL_CREATE_EMPLOYEE_TABLE_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".CREATE_EMPLOYEE_TABLE('%s')";
	private static final String CALL_DROP_EMPLOYEE_TABLE_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".DROP_EMPLOYEE_TABLE('%s')";
	private static final String CALL_INSERT_EMPLOYEE_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".INSERT_EMPLOYEE('%s', 2, 'Barney', 'Rubble')";
	private static final String CALL_GET_EMPLOYEE_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".GET_EMPLOYEE('%s', 2)";
	private static final String CALL_INSERT_AND_GET_EMPLOYEE_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".INSERT_AND_GET_EMPLOYEE('%s', 2, 'Barney', 'Rubble')";
	private static final String CALL_CREATE_INSERT_AND_GET_EMPLOYEE_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".CREATE_INSERT_AND_GET_EMPLOYEE('%s', 1, 'Fred', 'Flintstone')";
	private static final String CALL_INSERT_AND_GET_EMPLOYEE_COMMIT_TXN_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".INSERT_AND_GET_EMPLOYEE_COMMIT_TXN('%s', 2, 'Barney', 'Rubble')";
	private static final String CALL_INSERT_AND_GET_EMPLOYEE_NO_COMMIT_TXN_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".INSERT_AND_GET_EMPLOYEE_NO_COMMIT_TXN('%s', 2, 'Barney', 'Rubble')";
	private static final String CALL_INSERT_AND_GET_EMPLOYEE_ROLLBACK_TXN_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".INSERT_AND_GET_EMPLOYEE_ROLLBACK_TXN('%s', 2, 'Barney', 'Rubble')";
	private static final String CALL_INSERT_AND_GET_EMPLOYEE_RELEASE_SVPT_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".INSERT_AND_GET_EMPLOYEE_RELEASE_SVPT('%s', 2, 'Barney', 'Rubble')";
	private static final String CALL_INSERT_AND_GET_EMPLOYEE_NO_RELEASE_SVPT_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".INSERT_AND_GET_EMPLOYEE_NO_RELEASE_SVPT('%s', 2, 'Barney', 'Rubble')";
	private static final String CALL_INSERT_AND_GET_EMPLOYEE_ROLLBACK_SVPT_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".INSERT_AND_GET_EMPLOYEE_ROLLBACK_SVPT('%s', 2, 'Barney', 'Rubble')";
    private static final String CALL_GET_EMPLOYEE_MULTIPLE_OUTPUT_PARAMS = "CALL " + SCHEMA_NAME + ".GET_EMPLOYEE_MULTIPLE_OUTPUT_PARAMS(?, ?, ?, ?)";

	// SQL queries.

    private static final TestConnectionPool connPool = new TestConnectionPool();

    @Rule public final RuledConnection conn = new RuledConnection(connPool,true);

    @BeforeClass
    public static void setUpClass() throws Exception {
        String STORED_PROCS_JAR_FILE = SpliceUnitTest.getArchitectureSqlDirectory()+"/target/txn-it/txn-it.jar";//getJarFileForClass(TxnTestProcs.class);
        assertTrue("Cannot find procedures jar file: "+STORED_PROCS_JAR_FILE,STORED_PROCS_JAR_FILE.endsWith("jar"));

        try(Connection conn = connPool.getConnection()){
            new SchemaRule(conn,CLASS_NAME).setupSchema();
            try(Statement s = conn.createStatement()){
               // Install the jar file of user-defined stored procedures.
               s.executeUpdate(String.format(CALL_INSTALL_JAR_FORMAT_STRING, STORED_PROCS_JAR_FILE, JAR_FILE_SQL_NAME));

               // Add the jar file into the DB class path.
               s.executeUpdate(String.format(CALL_SET_CLASSPATH_FORMAT_STRING, JAR_FILE_SQL_NAME));

               // Recompile the stored statements since the SQLJ and CLASSPATH stored procedures invalidate all of the stored statements.
               // Recompiling will avoid write-write conflicts between the concurrent IT execution threads.

               // Create the user-defined stored procedures to create and drop the EMPLOYEE table.
               s.executeUpdate(CREATE_PROC_CREATE_EMPLOYEE_TABLE);
               s.executeUpdate(CREATE_PROC_DROP_EMPLOYEE_TABLE);
           }
        }
    }

    @AfterClass
    public static void tearDownClass() throws Exception {

		// Drop the user-defined stored procedures to create and drop the EMPLOYEE table.
        try(Connection conn = connPool.getConnection()){
           try(Statement s = conn.createStatement()){
               s.executeUpdate(DROP_PROC_CREATE_EMPLOYEE_TABLE);
               s.executeUpdate(DROP_PROC_DROP_EMPLOYEE_TABLE);

               // Remove the jar file from the DB class path.
//		spliceClassWatcher.executeUpdate(CALL_SET_CLASSPATH_TO_DEFAULT);

               // Remove the jar file from the DB.
               s.executeUpdate(String.format(CALL_REMOVE_JAR_FORMAT_STRING, JAR_FILE_SQL_NAME));
           }
        }
    }

	/**
	 * Test creating a table and dropping a table in two different stored procedures.
	 * @throws Exception
	 */
	@Test
	public void testCreateTableDropTable2Procedures() throws Exception {
		String employeeTableName = EMPLOYEE_TABLE_NAME_BASE + "1";

		// Create the table.
        try(Statement s = conn.createStatement()){
            int rc=s.executeUpdate(String.format(CALL_CREATE_EMPLOYEE_TABLE_FORMAT_STRING,employeeTableName));
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);

            // Drop the table.
            rc=s.executeUpdate(String.format(CALL_DROP_EMPLOYEE_TABLE_FORMAT_STRING,employeeTableName));
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);
        }
	}

	/**
	 * Test creating a table, inserting a row, returning a scanned row, and dropping the table in four different stored procedures.
	 * @throws Exception
	 */
	@Test
	public void testCreateTableInsertRowSelectRowDropTable4Procedures() throws Exception {
		String employeeTableName = EMPLOYEE_TABLE_NAME_BASE + "2";

        try(Statement s = conn.createStatement()){
            // Create the user-defined stored procedures.
            int rc=s.executeUpdate(CREATE_PROC_INSERT_EMPLOYEE);
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);
            rc=s.executeUpdate(CREATE_PROC_GET_EMPLOYEE);
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);

            // Create the table.
            rc=s.executeUpdate(String.format(CALL_CREATE_EMPLOYEE_TABLE_FORMAT_STRING,employeeTableName));
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);

            // Insert the row.
            rc=s.executeUpdate(String.format(CALL_INSERT_EMPLOYEE_FORMAT_STRING,employeeTableName));
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);

            // Get the row.
            try(ResultSet rs=s.executeQuery(String.format(CALL_GET_EMPLOYEE_FORMAT_STRING,employeeTableName))){
                Assert.assertEquals("Incorrect # of rows returned!",1,resultSetSize(rs));
            }

            // Drop the user-defined stored procedures.
            rc=s.executeUpdate(DROP_PROC_INSERT_EMPLOYEE);
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);
            rc=s.executeUpdate(DROP_PROC_GET_EMPLOYEE);
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);

            // Drop the table.
            rc=s.executeUpdate(String.format(CALL_DROP_EMPLOYEE_TABLE_FORMAT_STRING,employeeTableName));
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);
        }
	}

	/**
	 * Test inserting a row and returning a scanned row in one stored procedure.
	 * @throws Exception
	 */
	@Test
	public void testCreateTableInsertAndSelectRowDropTable3Procedures() throws Exception {
		String employeeTableName = EMPLOYEE_TABLE_NAME_BASE + "3";

        try(Statement s = conn.createStatement()){
            // Create the user-defined stored procedures.
            int rc=s.executeUpdate(CREATE_PROC_INSERT_AND_GET_EMPLOYEE);
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);

            // Create the table.
            rc=s.executeUpdate(String.format(CALL_CREATE_EMPLOYEE_TABLE_FORMAT_STRING,employeeTableName));
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);

            // Insert and get the row in one stored procedure.
            try(ResultSet rs=s.executeQuery(String.format(CALL_INSERT_AND_GET_EMPLOYEE_FORMAT_STRING,employeeTableName))){
                Assert.assertEquals("Incorrect # of rows returned!",1,resultSetSize(rs));
            }

            // Drop the user-defined stored procedures.
            rc=s.executeUpdate(DROP_PROC_INSERT_AND_GET_EMPLOYEE);
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);

            // Drop the table.
            rc=s.executeUpdate(String.format(CALL_DROP_EMPLOYEE_TABLE_FORMAT_STRING,employeeTableName));
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);
        }
	}

	/**
	 * Test creating a table, inserting a row, and returning a scanned row all in one stored procedure.
	 * @throws Exception
	 */
	@Test
	public void testCreateTableInsertAndSelectRowDropTable2Procedures() throws Exception {
		String employeeTableName = EMPLOYEE_TABLE_NAME_BASE + "4";

        try(Statement s = conn.createStatement()){
            // Create the user-defined stored procedures.
            int rc=s.executeUpdate(CREATE_PROC_CREATE_INSERT_AND_GET_EMPLOYEE);
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);

            // Create the table, insert a row, and get the row in one stored procedure.
            try(ResultSet rs=s.executeQuery(String.format(CALL_CREATE_INSERT_AND_GET_EMPLOYEE_FORMAT_STRING,employeeTableName))){
                Assert.assertEquals("Incorrect # of rows returned!",1,resultSetSize(rs));
            }

            // Drop the user-defined stored procedures.
            rc=s.executeUpdate(DROP_PROC_CREATE_INSERT_AND_GET_EMPLOYEE);
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);

            // Drop the table.
            rc=s.executeUpdate(String.format(CALL_DROP_EMPLOYEE_TABLE_FORMAT_STRING,employeeTableName));
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);
        }
	}

	/**
	 * Test inserting a row and returning a scanned row in one stored procedure with an explicit transaction commit.
	 * @throws Exception
	 */
	@Test
	public void testInsertAndSelectRowProcedureWithExplicitTransactionCommit() throws Exception {
		String employeeTableName = EMPLOYEE_TABLE_NAME_BASE + "5";

        try(Statement s = conn.createStatement()){
            // Create the user-defined stored procedures.
            int rc=s.executeUpdate(CREATE_PROC_INSERT_AND_GET_EMPLOYEE_COMMIT_TXN);
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);

            // Create the table.
            rc=s.executeUpdate(String.format(CALL_CREATE_EMPLOYEE_TABLE_FORMAT_STRING,employeeTableName));
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);

            // Insert and get the row in one stored procedure.
            try(ResultSet rs=s.executeQuery(String.format(CALL_INSERT_AND_GET_EMPLOYEE_COMMIT_TXN_FORMAT_STRING,employeeTableName))){
                Assert.assertEquals("Incorrect # of rows returned!",1,resultSetSize(rs));
            }

            // Drop the user-defined stored procedures.
            rc=s.executeUpdate(DROP_PROC_INSERT_AND_GET_EMPLOYEE_COMMIT_TXN);
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);

            // Drop the table.
            rc=s.executeUpdate(String.format(CALL_DROP_EMPLOYEE_TABLE_FORMAT_STRING,employeeTableName));
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);
        }
	}

    @Test
    @Ignore("DB-3177 - this test simulates the issue but it is tbd whether we will change product")
    public void testInsertAndSelectRowProcedureWithExplicitTransactionCommit2() throws Exception {

        // Same as testInsertAndSelectRowProcedureWithExplicitTransactionCommit,
        // except that one cheats by asserting the returned result set from
        // the stored procedure. What we need to test is that a fresh select query
        // will find the records that the stored procedure inserted.

        String employeeTableName = EMPLOYEE_TABLE_NAME_BASE + "5b";

        // TestConnection conn = new TestConnection(SpliceNetConnection.getConnectionAs(SpliceNetConnection.DEFAULT_USER, SpliceNetConnection.DEFAULT_USER_PASSWORD));
        conn.setAutoCommit(false);
        try(Statement s = conn.createStatement()){

            // Create the user-defined stored procedures.
            int rc=s.executeUpdate(CREATE_PROC_INSERT_AND_GET_EMPLOYEE_COMMIT_TXN);
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);

            // Create the table.
            rc=s.executeUpdate(String.format(CALL_CREATE_EMPLOYEE_TABLE_FORMAT_STRING,employeeTableName));
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);

            // Insert and get the row in one stored procedure.
            try(ResultSet rs=s.executeQuery(String.format(CALL_INSERT_AND_GET_EMPLOYEE_COMMIT_TXN_FORMAT_STRING,employeeTableName))){
                Assert.assertEquals("Incorrect # of rows returned!",1,resultSetSize(rs));
            }

            // Select to look for what the stored proc inserted
            try(ResultSet rs=s.executeQuery(String.format("SELECT * FROM %s",employeeTableName))){
                Assert.assertEquals("Incorrect # of rows returned!",1,resultSetSize(rs));
            }

            // Drop the user-defined stored procedures.
            rc=s.executeUpdate(DROP_PROC_INSERT_AND_GET_EMPLOYEE_COMMIT_TXN);
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);

            // Drop the table.
            rc=s.executeUpdate(String.format(CALL_DROP_EMPLOYEE_TABLE_FORMAT_STRING,employeeTableName));
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);
        }finally{
            //noinspection ThrowFromFinallyBlock
            conn.rollback();
            //noinspection ThrowFromFinallyBlock
            conn.reset();
        }
    }

    /**
	 * Test inserting a row and returning a scanned row in one stored procedure without an explicit transaction commit.
	 * @throws Exception
	 */
	@Test
	public void testInsertAndSelectRowProcedureWithoutExplicitTransactionCommit() throws Exception{
        String employeeTableName=EMPLOYEE_TABLE_NAME_BASE+"6";

        try(Statement s = conn.createStatement()){
            // Create the user-defined stored procedures.
            int rc=s.executeUpdate(CREATE_PROC_INSERT_AND_GET_EMPLOYEE_NO_COMMIT_TXN);
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);

            // Create the table.
            rc=s.executeUpdate(String.format(CALL_CREATE_EMPLOYEE_TABLE_FORMAT_STRING,employeeTableName));
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);

            // Insert and get the row in one stored procedure.
            try(ResultSet rs=s.executeQuery(String.format(CALL_INSERT_AND_GET_EMPLOYEE_NO_COMMIT_TXN_FORMAT_STRING,employeeTableName))){
                Assert.assertEquals("Incorrect # of rows returned!",1,resultSetSize(rs));
            }

            // Drop the user-defined stored procedures.
            rc=s.executeUpdate(DROP_PROC_INSERT_AND_GET_EMPLOYEE_NO_COMMIT_TXN);
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);

            // Drop the table.
            rc=s.executeUpdate(String.format(CALL_DROP_EMPLOYEE_TABLE_FORMAT_STRING,employeeTableName));
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);
        }
	}

	/**
	 * Test inserting a row and returning a scanned row in one stored procedure with a transaction rollback.
	 * @throws Exception
	 */
	@Test
	public void testInsertAndSelectRowProcedureWithTransactionRollback() throws Exception {
		String employeeTableName = EMPLOYEE_TABLE_NAME_BASE + "7";

        try(Statement s = conn.createStatement()){
            // Create the user-defined stored procedures.
            int rc=s.executeUpdate(CREATE_PROC_INSERT_AND_GET_EMPLOYEE_ROLLBACK_TXN);
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);

            // Create the table.
            rc=s.executeUpdate(String.format(CALL_CREATE_EMPLOYEE_TABLE_FORMAT_STRING,employeeTableName));
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);

            // Insert and get the row in one stored procedure.
            try(ResultSet rs=s.executeQuery(String.format(CALL_INSERT_AND_GET_EMPLOYEE_ROLLBACK_TXN_FORMAT_STRING,employeeTableName))){
                Assert.assertEquals("Incorrect # of rows returned!",0,resultSetSize(rs));
            }

            // Drop the user-defined stored procedures.
            rc=s.executeUpdate(DROP_PROC_INSERT_AND_GET_EMPLOYEE_ROLLBACK_TXN);
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);

            // Drop the table.
            rc=s.executeUpdate(String.format(CALL_DROP_EMPLOYEE_TABLE_FORMAT_STRING,employeeTableName));
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);
        }
	}

	/**
	 * Test inserting a row and returning a scanned row in one stored procedure with an explicit savepoint release.
	 * @throws Exception
	 */
	@Test
	public void testInsertAndSelectRowProcedureWithExplicitSavepointRelease() throws Exception {
		String employeeTableName = EMPLOYEE_TABLE_NAME_BASE + "8";

        try(Statement s = conn.createStatement()){
            // Create the user-defined stored procedures.
            int rc=s.executeUpdate(CREATE_PROC_INSERT_AND_GET_EMPLOYEE_RELEASE_SVPT);
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);

            // Create the table.
            rc=s.executeUpdate(String.format(CALL_CREATE_EMPLOYEE_TABLE_FORMAT_STRING,employeeTableName));
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);

            // Insert and get the row in one stored procedure.
            try(ResultSet rs=s.executeQuery(String.format(CALL_INSERT_AND_GET_EMPLOYEE_RELEASE_SVPT_FORMAT_STRING,employeeTableName))){
                Assert.assertEquals("Incorrect # of rows returned!",1,resultSetSize(rs));
            }

            // Drop the user-defined stored procedures.
            rc=s.executeUpdate(DROP_PROC_INSERT_AND_GET_EMPLOYEE_RELEASE_SVPT);
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);

            // Drop the table.
            rc=s.executeUpdate(String.format(CALL_DROP_EMPLOYEE_TABLE_FORMAT_STRING,employeeTableName));
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);
        }
	}

	/**
	 * Test inserting a row and returning a scanned row in one stored procedure without an explicit savepoint release.
	 * @throws Exception
	 */
	@Test
	public void testInsertAndSelectRowProcedureWithoutExplicitSavepointRelease() throws Exception {
		String employeeTableName = EMPLOYEE_TABLE_NAME_BASE + "9";

        try(Statement s = conn.createStatement()){
            // Create the user-defined stored procedures.
            int rc=s.executeUpdate(CREATE_PROC_INSERT_AND_GET_EMPLOYEE_NO_RELEASE_SVPT);
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);

            // Create the table.
            rc=s.executeUpdate(String.format(CALL_CREATE_EMPLOYEE_TABLE_FORMAT_STRING,employeeTableName));
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);

            // Insert and get the row in one stored procedure.
            try(ResultSet rs=s.executeQuery(String.format(CALL_INSERT_AND_GET_EMPLOYEE_NO_RELEASE_SVPT_FORMAT_STRING,employeeTableName))){
                Assert.assertEquals("Incorrect # of rows returned!",1,resultSetSize(rs));
            }

            // Drop the user-defined stored procedures.
            rc=s.executeUpdate(DROP_PROC_INSERT_AND_GET_EMPLOYEE_NO_RELEASE_SVPT);
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);

            // Drop the table.
            rc=s.executeUpdate(String.format(CALL_DROP_EMPLOYEE_TABLE_FORMAT_STRING,employeeTableName));
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);
        }
	}

	/**
	 * Test inserting a row and returning a scanned row in one stored procedure with a savepoint rollback.
	 * @throws Exception
	 */
	@Test
	public void testInsertAndSelectRowProcedureWithSavepointRollback() throws Exception {
		String employeeTableName = EMPLOYEE_TABLE_NAME_BASE + "10";

        try(Statement s = conn.createStatement()){
            // Create the user-defined stored procedures.
            int rc=s.executeUpdate(CREATE_PROC_INSERT_AND_GET_EMPLOYEE_ROLLBACK_SVPT);
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);

            // Create the table.
            rc=s.executeUpdate(String.format(CALL_CREATE_EMPLOYEE_TABLE_FORMAT_STRING,employeeTableName));
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);

            // Insert and get the row in one stored procedure.
            try(ResultSet rs=s.executeQuery(String.format(CALL_INSERT_AND_GET_EMPLOYEE_ROLLBACK_SVPT_FORMAT_STRING,employeeTableName))){
                Assert.assertEquals("Incorrect # of rows returned!",0,resultSetSize(rs));
            }

            // Drop the user-defined stored procedures.
            rc=s.executeUpdate(DROP_PROC_INSERT_AND_GET_EMPLOYEE_ROLLBACK_SVPT);
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);

            // Drop the table.
            rc=s.executeUpdate(String.format(CALL_DROP_EMPLOYEE_TABLE_FORMAT_STRING,employeeTableName));
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);
        }
	}

    /**
     * Tests ability for stored procedure to return not just a result set,
     * but multiple OUTPUT parameters.
     * @throws Exception
     */
    @Test
    public void testProcedureWithMultipleOutputParameters() throws Exception {
        String employeeTableName = EMPLOYEE_TABLE_NAME_BASE + "11";

        try(Statement s = conn.createStatement()){
            // Create the user-defined stored procedures.
            int rc=s.executeUpdate(CREATE_PROC_GET_EMPLOYEE_MULTIPLE_OUTPUT_PARAMS);
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);

            // Create the table.
            rc=s.executeUpdate(String.format(CALL_CREATE_EMPLOYEE_TABLE_FORMAT_STRING,employeeTableName));
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);

            try(CallableStatement stmt=conn.prepareCall(CALL_GET_EMPLOYEE_MULTIPLE_OUTPUT_PARAMS)){
                stmt.setString(1,employeeTableName);
                stmt.setLong(2,12345L); // won't be found
                stmt.registerOutParameter(3,Types.VARCHAR);
                stmt.registerOutParameter(4,Types.VARCHAR);

                try(ResultSet rs=stmt.executeQuery()){
                    Assert.assertEquals("Incorrect # of rows returned!",0,resultSetSize(rs));
                    Assert.assertEquals("Incorrect error code","0",stmt.getString(3));
                    Assert.assertEquals("Incorrect error message","Success",stmt.getString(4));
                }
            }

            // Drop the user-defined stored procedures.
            rc=s.executeUpdate(DROP_PROC_GET_EMPLOYEE_MULTIPLE_OUTPUT_PARAMS);
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);

            // Drop the table.
            rc=s.executeUpdate(String.format(CALL_DROP_EMPLOYEE_TABLE_FORMAT_STRING,employeeTableName));
            Assert.assertEquals("Incorrect return code or result count returned!",0,rc);
        }
    }
}
