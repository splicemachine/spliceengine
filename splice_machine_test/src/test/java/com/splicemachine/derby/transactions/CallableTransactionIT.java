package com.splicemachine.derby.transactions;

import java.sql.ResultSet;

import com.splicemachine.test.Transactions;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

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
@Category({Transactions.class})
public class CallableTransactionIT extends SpliceUnitTest {

	public static final String CLASS_NAME = CallableTransactionIT.class.getSimpleName().toUpperCase();

	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

	// Names of files and SQL objects.
	private static final String SCHEMA_NAME = CLASS_NAME;
	private static final String STORED_PROCS_JAR_FILE = getResourceDirectory() + "/txn-it-procs/txn-it-procs-1.0.1.jar";
	private static final String JAR_FILE_SQL_NAME = SCHEMA_NAME + ".TXN_IT_PROCS_JAR";
	private static final String EMPLOYEE_TABLE_NAME_BASE = SCHEMA_NAME + ".EMPLOYEE";

	// SQL SQL statements to load the custom jar files and add it to the CLASSPATH.
	private static final String CALL_INSTALL_JAR_FORMAT_STRING = "CALL SQLJ.INSTALL_JAR('%s', '%s', 0)";
	private static final String CALL_REMOVE_JAR_FORMAT_STRING = "CALL SQLJ.REMOVE_JAR('%s', 0)";
	private static final String CALL_SET_CLASSPATH_FORMAT_STRING = "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.classpath', '%s')";
	private static final String CALL_SET_CLASSPATH_TO_DEFAULT = "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.classpath', NULL)";

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
	private static final String CREATE_PROC_INSERT_UPDATE_AND_GET_EMPLOYEE = String.format("CREATE PROCEDURE %s.INSERT_UPDATE_AND_GET_EMPLOYEE(IN tableName VARCHAR(40), IN id INT, IN fname VARCHAR(20), IN lname VARCHAR(30), IN fname2 VARCHAR(20), IN lname2 VARCHAR(30)) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.txn.TxnTestProcs.INSERT_UPDATE_AND_GET_EMPLOYEE'", SCHEMA_NAME);
	private static final String DROP_PROC_INSERT_UPDATE_AND_GET_EMPLOYEE = String.format("DROP PROCEDURE %s.INSERT_UPDATE_AND_GET_EMPLOYEE", SCHEMA_NAME);
	private static final String CREATE_PROC_INSERT_UPDATE_GETx2_DELETE_AND_GET_EMPLOYEE = String.format("CREATE PROCEDURE %s.INSERT_UPDATE_GETx2_DELETE_AND_GET_EMPLOYEE(IN tableName VARCHAR(40), IN id INT, IN fname VARCHAR(20), IN lname VARCHAR(30), IN fname2 VARCHAR(20), IN lname2 VARCHAR(30), IN id2 INT) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.txn.TxnTestProcs.INSERT_UPDATE_GETx2_DELETE_AND_GET_EMPLOYEE'", SCHEMA_NAME);
	private static final String DROP_PROC_INSERT_UPDATE_GETx2_DELETE_AND_GET_EMPLOYEE = String.format("DROP PROCEDURE %s.INSERT_UPDATE_GETx2_DELETE_AND_GET_EMPLOYEE", SCHEMA_NAME);
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

	// SQL statements to call stored procedures.
	private static final String CALL_CREATE_EMPLOYEE_TABLE_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".CREATE_EMPLOYEE_TABLE('%s')";
	private static final String CALL_DROP_EMPLOYEE_TABLE_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".DROP_EMPLOYEE_TABLE('%s')";
	private static final String CALL_INSERT_EMPLOYEE_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".INSERT_EMPLOYEE('%s', 2, 'Barney', 'Rubble')";
	private static final String CALL_GET_EMPLOYEE_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".GET_EMPLOYEE('%s', 2)";
	private static final String CALL_INSERT_AND_GET_EMPLOYEE_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".INSERT_AND_GET_EMPLOYEE('%s', 2, 'Barney', 'Rubble')";
	private static final String CALL_CREATE_INSERT_AND_GET_EMPLOYEE_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".CREATE_INSERT_AND_GET_EMPLOYEE('%s', 1, 'Fred', 'Flintstone')";
	private static final String CALL_INSERT_UPDATE_AND_GET_EMPLOYEE_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".INSERT_UPDATE_AND_GET_EMPLOYEE('%s', 2, 'Barney', 'Rubble', 'Wilma', 'Flintsone')";
	private static final String CALL_INSERT_UPDATE_GETx2_DELETE_AND_GET_EMPLOYEE_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".INSERT_UPDATE_GETx2_DELETE_AND_GET_EMPLOYEE('%s', 2, 'Barney', 'Rubble', 'Wilma', 'Flintsone', 3)";
	private static final String CALL_INSERT_AND_GET_EMPLOYEE_COMMIT_TXN_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".INSERT_AND_GET_EMPLOYEE_COMMIT_TXN('%s', 2, 'Barney', 'Rubble')";
	private static final String CALL_INSERT_AND_GET_EMPLOYEE_NO_COMMIT_TXN_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".INSERT_AND_GET_EMPLOYEE_NO_COMMIT_TXN('%s', 2, 'Barney', 'Rubble')";
	private static final String CALL_INSERT_AND_GET_EMPLOYEE_ROLLBACK_TXN_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".INSERT_AND_GET_EMPLOYEE_ROLLBACK_TXN('%s', 2, 'Barney', 'Rubble')";
	private static final String CALL_INSERT_AND_GET_EMPLOYEE_RELEASE_SVPT_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".INSERT_AND_GET_EMPLOYEE_RELEASE_SVPT('%s', 2, 'Barney', 'Rubble')";
	private static final String CALL_INSERT_AND_GET_EMPLOYEE_NO_RELEASE_SVPT_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".INSERT_AND_GET_EMPLOYEE_NO_RELEASE_SVPT('%s', 2, 'Barney', 'Rubble')";
	private static final String CALL_INSERT_AND_GET_EMPLOYEE_ROLLBACK_SVPT_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".INSERT_AND_GET_EMPLOYEE_ROLLBACK_SVPT('%s', 2, 'Barney', 'Rubble')";

	// SQL queries.
	private static final String SELECT_FROM_SYSTABLES_BY_TABLENAME = "SELECT * FROM SYS.SYSTABLES WHERE TABLENAME = ?";

	@ClassRule
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
			.around(spliceSchemaWatcher);

	@Rule
	public SpliceWatcher methodWatcher = new SpliceWatcher();

    @BeforeClass
    public static void setUpClass() throws Exception {

		// Install the jar file of user-defined stored procedures.
		spliceClassWatcher.executeUpdate(String.format(CALL_INSTALL_JAR_FORMAT_STRING, STORED_PROCS_JAR_FILE, JAR_FILE_SQL_NAME));

		// Add the jar file into the DB class path.
		spliceClassWatcher.executeUpdate(String.format(CALL_SET_CLASSPATH_FORMAT_STRING, JAR_FILE_SQL_NAME));

		// Recompile the stored statements since the SQLJ and CLASSPATH stored procedures invalidate all of the stored statements.
		// Recompiling will avoid write-write conflicts between the concurrent IT execution threads.
//		spliceClassWatcher.executeUpdate("call SYSCS_UTIL.SYSCS_RECOMPILE_INVALID_STORED_STATEMENTS()");

		// Create the user-defined stored procedures to create and drop the EMPLOYEE table.
		spliceClassWatcher.executeUpdate(CREATE_PROC_CREATE_EMPLOYEE_TABLE);
		spliceClassWatcher.executeUpdate(CREATE_PROC_DROP_EMPLOYEE_TABLE);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {

		// Drop the user-defined stored procedures to create and drop the EMPLOYEE table.
		spliceClassWatcher.executeUpdate(DROP_PROC_CREATE_EMPLOYEE_TABLE);
		spliceClassWatcher.executeUpdate(DROP_PROC_DROP_EMPLOYEE_TABLE);

		// Remove the jar file from the DB class path.
		spliceClassWatcher.executeUpdate(CALL_SET_CLASSPATH_TO_DEFAULT);

		// Remove the jar file from the DB.
		spliceClassWatcher.executeUpdate(String.format(CALL_REMOVE_JAR_FORMAT_STRING, JAR_FILE_SQL_NAME));
    }

	/**
	 * Test creating a table and dropping a table in two different stored procedures.
	 * @throws Exception
	 */
	@Test
	public void testCreateTableDropTable2Procedures() throws Exception {
		String employeeTableName = EMPLOYEE_TABLE_NAME_BASE + "1";
		int rc = 0;

		// Create the table.
		rc = methodWatcher.executeUpdate(String.format(CALL_CREATE_EMPLOYEE_TABLE_FORMAT_STRING, employeeTableName));
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Drop the table.
		rc = methodWatcher.executeUpdate(String.format(CALL_DROP_EMPLOYEE_TABLE_FORMAT_STRING, employeeTableName));
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);
	}

	/**
	 * Test creating a table, inserting a row, returning a scanned row, and dropping the table in four different stored procedures.
	 * @throws Exception
	 */
	@Test
	public void testCreateTableInsertRowSelectRowDropTable4Procedures() throws Exception {
		String employeeTableName = EMPLOYEE_TABLE_NAME_BASE + "2";
		int rc = 0;
		ResultSet rs = null;

		// Create the user-defined stored procedures.
		rc = methodWatcher.executeUpdate(CREATE_PROC_INSERT_EMPLOYEE);
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);
		rc = methodWatcher.executeUpdate(CREATE_PROC_GET_EMPLOYEE);
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Create the table.
		rc = methodWatcher.executeUpdate(String.format(CALL_CREATE_EMPLOYEE_TABLE_FORMAT_STRING, employeeTableName));
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Insert the row.
		rc = methodWatcher.executeUpdate(String.format(CALL_INSERT_EMPLOYEE_FORMAT_STRING, employeeTableName));
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Get the row.
		rs = methodWatcher.executeQuery(String.format(CALL_GET_EMPLOYEE_FORMAT_STRING, employeeTableName));
		Assert.assertEquals("Incorrect # of rows returned!", 1, resultSetSize(rs));
		rs.close();

		// Drop the user-defined stored procedures.
		rc = methodWatcher.executeUpdate(DROP_PROC_INSERT_EMPLOYEE);
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);
		rc = methodWatcher.executeUpdate(DROP_PROC_GET_EMPLOYEE);
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Drop the table.
		rc = methodWatcher.executeUpdate(String.format(CALL_DROP_EMPLOYEE_TABLE_FORMAT_STRING, employeeTableName));
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);
	}

	/**
	 * Test inserting a row and returning a scanned row in one stored procedure.
	 * @throws Exception
	 */
	@Test
	public void testCreateTableInsertAndSelectRowDropTable3Procedures() throws Exception {
		String employeeTableName = EMPLOYEE_TABLE_NAME_BASE + "3";
		int rc = 0;
		ResultSet rs = null;

		// Create the user-defined stored procedures.
		rc = methodWatcher.executeUpdate(CREATE_PROC_INSERT_AND_GET_EMPLOYEE);
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Create the table.
		rc = methodWatcher.executeUpdate(String.format(CALL_CREATE_EMPLOYEE_TABLE_FORMAT_STRING, employeeTableName));
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Insert and get the row in one stored procedure.
		rs = methodWatcher.executeQuery(String.format(CALL_INSERT_AND_GET_EMPLOYEE_FORMAT_STRING, employeeTableName));
		Assert.assertEquals("Incorrect # of rows returned!", 1, resultSetSize(rs));
		rs.close();

		// Drop the user-defined stored procedures.
		rc = methodWatcher.executeUpdate(DROP_PROC_INSERT_AND_GET_EMPLOYEE);
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Drop the table.
		rc = methodWatcher.executeUpdate(String.format(CALL_DROP_EMPLOYEE_TABLE_FORMAT_STRING, employeeTableName));
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);
	}

	/**
	 * Test creating a table, inserting a row, and returning a scanned row all in one stored procedure.
	 * @throws Exception
	 */
	@Test
	public void testCreateTableInsertAndSelectRowDropTable2Procedures() throws Exception {
		String employeeTableName = EMPLOYEE_TABLE_NAME_BASE + "4";
		int rc = 0;
		ResultSet rs = null;

		// Create the user-defined stored procedures.
		rc = methodWatcher.executeUpdate(CREATE_PROC_CREATE_INSERT_AND_GET_EMPLOYEE);
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Create the table, insert a row, and get the row in one stored procedure.
		rs = methodWatcher.executeQuery(String.format(CALL_CREATE_INSERT_AND_GET_EMPLOYEE_FORMAT_STRING, employeeTableName));
		Assert.assertEquals("Incorrect # of rows returned!", 1, resultSetSize(rs));
		rs.close();

		// Drop the user-defined stored procedures.
		rc = methodWatcher.executeUpdate(DROP_PROC_CREATE_INSERT_AND_GET_EMPLOYEE);
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Drop the table.
		rc = methodWatcher.executeUpdate(String.format(CALL_DROP_EMPLOYEE_TABLE_FORMAT_STRING, employeeTableName));
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);
	}

	/**
	 * Test inserting a row and returning a scanned row in one stored procedure with an explicit transaction commit.
	 * @throws Exception
	 */
	@Test
	public void testInsertAndSelectRowProcedureWithExplicitTransactionCommit() throws Exception {
		String employeeTableName = EMPLOYEE_TABLE_NAME_BASE + "5";
		int rc = 0;
		ResultSet rs = null;

		// Create the user-defined stored procedures.
		rc = methodWatcher.executeUpdate(CREATE_PROC_INSERT_AND_GET_EMPLOYEE_COMMIT_TXN);
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Create the table.
		rc = methodWatcher.executeUpdate(String.format(CALL_CREATE_EMPLOYEE_TABLE_FORMAT_STRING, employeeTableName));
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Insert and get the row in one stored procedure.
		rs = methodWatcher.executeQuery(String.format(CALL_INSERT_AND_GET_EMPLOYEE_COMMIT_TXN_FORMAT_STRING, employeeTableName));
		Assert.assertEquals("Incorrect # of rows returned!", 1, resultSetSize(rs));
		rs.close();

		// Drop the user-defined stored procedures.
		rc = methodWatcher.executeUpdate(DROP_PROC_INSERT_AND_GET_EMPLOYEE_COMMIT_TXN);
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Drop the table.
		rc = methodWatcher.executeUpdate(String.format(CALL_DROP_EMPLOYEE_TABLE_FORMAT_STRING, employeeTableName));
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);
	}

	/**
	 * Test inserting a row and returning a scanned row in one stored procedure without an explicit transaction commit.
	 * @throws Exception
	 */
	@Test
	public void testInsertAndSelectRowProcedureWithoutExplicitTransactionCommit() throws Exception {
		String employeeTableName = EMPLOYEE_TABLE_NAME_BASE + "6";
		int rc = 0;
		ResultSet rs = null;

		// Create the user-defined stored procedures.
		rc = methodWatcher.executeUpdate(CREATE_PROC_INSERT_AND_GET_EMPLOYEE_NO_COMMIT_TXN);
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Create the table.
		rc = methodWatcher.executeUpdate(String.format(CALL_CREATE_EMPLOYEE_TABLE_FORMAT_STRING, employeeTableName));
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Insert and get the row in one stored procedure.
		rs = methodWatcher.executeQuery(String.format(CALL_INSERT_AND_GET_EMPLOYEE_NO_COMMIT_TXN_FORMAT_STRING, employeeTableName));
		Assert.assertEquals("Incorrect # of rows returned!", 1, resultSetSize(rs));
		rs.close();

		// Drop the user-defined stored procedures.
		rc = methodWatcher.executeUpdate(DROP_PROC_INSERT_AND_GET_EMPLOYEE_NO_COMMIT_TXN);
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Drop the table.
		rc = methodWatcher.executeUpdate(String.format(CALL_DROP_EMPLOYEE_TABLE_FORMAT_STRING, employeeTableName));
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);
	}

	/**
	 * Test inserting a row and returning a scanned row in one stored procedure with a transaction rollback.
	 * @throws Exception
	 */
	@Test
	public void testInsertAndSelectRowProcedureWithTransactionRollback() throws Exception {
		String employeeTableName = EMPLOYEE_TABLE_NAME_BASE + "7";
		int rc = 0;
		ResultSet rs = null;

		// Create the user-defined stored procedures.
		rc = methodWatcher.executeUpdate(CREATE_PROC_INSERT_AND_GET_EMPLOYEE_ROLLBACK_TXN);
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Create the table.
		rc = methodWatcher.executeUpdate(String.format(CALL_CREATE_EMPLOYEE_TABLE_FORMAT_STRING, employeeTableName));
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Insert and get the row in one stored procedure.
		rs = methodWatcher.executeQuery(String.format(CALL_INSERT_AND_GET_EMPLOYEE_ROLLBACK_TXN_FORMAT_STRING, employeeTableName));
		Assert.assertEquals("Incorrect # of rows returned!", 0, resultSetSize(rs));
		rs.close();

		// Drop the user-defined stored procedures.
		rc = methodWatcher.executeUpdate(DROP_PROC_INSERT_AND_GET_EMPLOYEE_ROLLBACK_TXN);
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Drop the table.
		rc = methodWatcher.executeUpdate(String.format(CALL_DROP_EMPLOYEE_TABLE_FORMAT_STRING, employeeTableName));
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);
	}

	/**
	 * Test inserting a row and returning a scanned row in one stored procedure with an explicit savepoint release.
	 * @throws Exception
	 */
	@Test
	public void testInsertAndSelectRowProcedureWithExplicitSavepointRelease() throws Exception {
		String employeeTableName = EMPLOYEE_TABLE_NAME_BASE + "8";
		int rc = 0;
		ResultSet rs = null;

		// Create the user-defined stored procedures.
		rc = methodWatcher.executeUpdate(CREATE_PROC_INSERT_AND_GET_EMPLOYEE_RELEASE_SVPT);
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Create the table.
		rc = methodWatcher.executeUpdate(String.format(CALL_CREATE_EMPLOYEE_TABLE_FORMAT_STRING, employeeTableName));
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Insert and get the row in one stored procedure.
		rs = methodWatcher.executeQuery(String.format(CALL_INSERT_AND_GET_EMPLOYEE_RELEASE_SVPT_FORMAT_STRING, employeeTableName));
		Assert.assertEquals("Incorrect # of rows returned!", 1, resultSetSize(rs));
		rs.close();

		// Drop the user-defined stored procedures.
		rc = methodWatcher.executeUpdate(DROP_PROC_INSERT_AND_GET_EMPLOYEE_RELEASE_SVPT);
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Drop the table.
		rc = methodWatcher.executeUpdate(String.format(CALL_DROP_EMPLOYEE_TABLE_FORMAT_STRING, employeeTableName));
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);
	}

	/**
	 * Test inserting a row and returning a scanned row in one stored procedure without an explicit savepoint release.
	 * @throws Exception
	 */
	@Test
	public void testInsertAndSelectRowProcedureWithoutExplicitSavepointRelease() throws Exception {
		String employeeTableName = EMPLOYEE_TABLE_NAME_BASE + "9";
		int rc = 0;
		ResultSet rs = null;

		// Create the user-defined stored procedures.
		rc = methodWatcher.executeUpdate(CREATE_PROC_INSERT_AND_GET_EMPLOYEE_NO_RELEASE_SVPT);
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Create the table.
		rc = methodWatcher.executeUpdate(String.format(CALL_CREATE_EMPLOYEE_TABLE_FORMAT_STRING, employeeTableName));
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Insert and get the row in one stored procedure.
		rs = methodWatcher.executeQuery(String.format(CALL_INSERT_AND_GET_EMPLOYEE_NO_RELEASE_SVPT_FORMAT_STRING, employeeTableName));
		Assert.assertEquals("Incorrect # of rows returned!", 1, resultSetSize(rs));
		rs.close();

		// Drop the user-defined stored procedures.
		rc = methodWatcher.executeUpdate(DROP_PROC_INSERT_AND_GET_EMPLOYEE_NO_RELEASE_SVPT);
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Drop the table.
		rc = methodWatcher.executeUpdate(String.format(CALL_DROP_EMPLOYEE_TABLE_FORMAT_STRING, employeeTableName));
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);
	}

	/**
	 * Test inserting a row and returning a scanned row in one stored procedure with a savepoint rollback.
	 * @throws Exception
	 */
	@Test
	public void testInsertAndSelectRowProcedureWithSavepointRollback() throws Exception {
		String employeeTableName = EMPLOYEE_TABLE_NAME_BASE + "10";
		int rc = 0;
		ResultSet rs = null;

		// Create the user-defined stored procedures.
		rc = methodWatcher.executeUpdate(CREATE_PROC_INSERT_AND_GET_EMPLOYEE_ROLLBACK_SVPT);
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Create the table.
		rc = methodWatcher.executeUpdate(String.format(CALL_CREATE_EMPLOYEE_TABLE_FORMAT_STRING, employeeTableName));
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Insert and get the row in one stored procedure.
		rs = methodWatcher.executeQuery(String.format(CALL_INSERT_AND_GET_EMPLOYEE_ROLLBACK_SVPT_FORMAT_STRING, employeeTableName));
		Assert.assertEquals("Incorrect # of rows returned!", 0, resultSetSize(rs));
		rs.close();

		// Drop the user-defined stored procedures.
		rc = methodWatcher.executeUpdate(DROP_PROC_INSERT_AND_GET_EMPLOYEE_ROLLBACK_SVPT);
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Drop the table.
		rc = methodWatcher.executeUpdate(String.format(CALL_DROP_EMPLOYEE_TABLE_FORMAT_STRING, employeeTableName));
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);
	}
}
