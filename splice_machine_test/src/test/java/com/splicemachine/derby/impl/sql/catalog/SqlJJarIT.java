package com.splicemachine.derby.impl.sql.catalog;

import java.sql.ResultSet;

import com.splicemachine.test.SerialTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

/**
 * This class tests the SQLJ JAR file loading system procedures (INSTALL_JAR, REPLACE_JAR, and REMOVE_JAR).
 * The stored procedures are contained in an external JAR file that are dynamically loaded into the Splice Machine
 * database with the SQLJ JAR file loading system procedures.  If your tests require a custom stored procedure,
 * you can add it to that JAR file and load into your 'IT' tests like you see below in the tests.
 *
 * @see org.splicetest.sqlj.SqlJTestProcs
 *
 * @author David Winters
 *		 Created on: 9/25/14
 */
@Category(SerialTest.class)
@Ignore("DB-4272")
public class SqlJJarIT extends SpliceUnitTest {

	public static final String CLASS_NAME = SqlJJarIT.class.getSimpleName().toUpperCase();

	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

	// Names of files and SQL objects.
	private static final String SCHEMA_NAME = CLASS_NAME;
	private static final String STORED_PROCS_JAR_FILE = getResourceDirectory() + "/sqlj-it-procs/sqlj-it-procs-1.0.1.jar";
	private static final String JAR_FILE_SQL_NAME = SCHEMA_NAME + ".SQLJ_IT_PROCS_JAR";

	// SQL statements to create and drop stored procedures.
	private static final String CREATE_PROC_SIMPLE_ONE_ARG = String.format("CREATE PROCEDURE %s.SIMPLE_ONE_ARG_PROC(IN name VARCHAR(30)) PARAMETER STYLE JAVA READS SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.sqlj.SqlJTestProcs.SIMPLE_ONE_ARG_PROC'", SCHEMA_NAME);
	private static final String DROP_PROC_SIMPLE_ONE_ARG = String.format("DROP PROCEDURE %s.SIMPLE_ONE_ARG_PROC", SCHEMA_NAME);

	// SQL statements to call stored procedures.
	private static final String CALL_INSTALL_JAR_FORMAT_STRING = "CALL SQLJ.INSTALL_JAR('%s', '%s', 0)";
	private static final String CALL_REPLACE_JAR_FORMAT_STRING = "CALL SQLJ.REPLACE_JAR('%s', '%s')";
	private static final String CALL_REMOVE_JAR_FORMAT_STRING = "CALL SQLJ.REMOVE_JAR('%s', 0)";
	private static final String CALL_SET_CLASSPATH_FORMAT_STRING = "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.classpath', '%s')";
	private static final String CALL_SET_CLASSPATH_TO_DEFAULT = "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.classpath', NULL)";
	private static final String CALL_SIMPLE_ONE_ARG_PROC_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".SIMPLE_ONE_ARG_PROC('%s')";
	private static final String CALL_SET_GLOBAL_CLASSPATH_FORMAT_STRING = "CALL SYSCS_UTIL.SYSCS_SET_GLOBAL_DATABASE_PROPERTY('derby.database.classpath', '%s')";
	private static final String CALL_SET_GLOBAL_CLASSPATH_TO_DEFAULT = "CALL SYSCS_UTIL.SYSCS_SET_GLOBAL_DATABASE_PROPERTY('derby.database.classpath', NULL)";
	private static final String CALL_GET_GLOBAL_CLASSPATH = "CALL SYSCS_UTIL.SYSCS_GET_GLOBAL_DATABASE_PROPERTY('derby.database.classpath')";

	// SQL queries.
	private static final String SELECT_FROM_SYSFILES = "SELECT * FROM SYS.SYSFILES";

	@ClassRule
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
			.around(spliceSchemaWatcher);

	@Rule
	public SpliceWatcher methodWatcher = new SpliceWatcher();

	/*
	 * PLEASE NOTE:
	 * I would personally rather see smaller tests without as many assertions as this one.
	 * Unfortunately, it would be difficult to split this test into multiple tests unless
	 * each test had its own custom jar file similar to what we do for unique table names
	 * for tests.  Without unique custom jars, the parallel execution of these tests would
	 * cause failures as the tests would clobber each other.  And creating and maintaining
	 * a dozen or so custom jar files would be painful.
	 */
	@Test
	public void testJarWithSimpleOneArgProc() throws Exception {
		ResultSet rs = null;
		int rc = 0;

		/*
		 * ========================================================================================
		 * Test the local database CLASSPATH for the region server.
		 * ========================================================================================
		 */

		// Count the number of SYSFILES.
		rs = methodWatcher.executeQuery(SELECT_FROM_SYSFILES);
		int numSysFiles = resultSetSize(rs);

		// Install the jar file of user-defined stored procedures.
		rc = methodWatcher.executeUpdate(String.format(CALL_INSTALL_JAR_FORMAT_STRING, STORED_PROCS_JAR_FILE, JAR_FILE_SQL_NAME));
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Add the jar file into the local DB class path.
		rc = methodWatcher.executeUpdate(String.format(CALL_SET_CLASSPATH_FORMAT_STRING, JAR_FILE_SQL_NAME));
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Create the user-defined stored procedure.
		rc = methodWatcher.executeUpdate(CREATE_PROC_SIMPLE_ONE_ARG);
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Call the user-defined stored procedure.
		rs = methodWatcher.executeQuery(String.format(CALL_SIMPLE_ONE_ARG_PROC_FORMAT_STRING, "foobar"));
		Assert.assertTrue("Incorrect rows returned!", resultSetSize(rs) > 10);

		// Replace the jar file.
		rc = methodWatcher.executeUpdate(String.format(CALL_REPLACE_JAR_FORMAT_STRING, STORED_PROCS_JAR_FILE, JAR_FILE_SQL_NAME));
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Call the user-defined stored procedure again.
		rs = methodWatcher.executeQuery(String.format(CALL_SIMPLE_ONE_ARG_PROC_FORMAT_STRING, "foobar"));
		Assert.assertTrue("Incorrect rows returned!", resultSetSize(rs) > 10);

		// Drop the user-defined stored procedure.
		rc = methodWatcher.executeUpdate(DROP_PROC_SIMPLE_ONE_ARG);
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Remove the jar file from the DB class path.
		rc = methodWatcher.executeUpdate(CALL_SET_CLASSPATH_TO_DEFAULT);
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Remove the jar file from the DB.
		rc = methodWatcher.executeUpdate(String.format(CALL_REMOVE_JAR_FORMAT_STRING, JAR_FILE_SQL_NAME));
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Compare that the number of SYSFILES matches the original count.
		rs = methodWatcher.executeQuery(SELECT_FROM_SYSFILES);
		Assert.assertEquals("Incorrect rows returned!", numSysFiles, resultSetSize(rs));

		/*
		 * ========================================================================================
		 * Test the global database CLASSPATH for all region servers in the cluster.
		 * Note: This test should also pass for a stand-alone server.
		 * ========================================================================================
		 */

		// Count the number of SYSFILES.
		rs = methodWatcher.executeQuery(SELECT_FROM_SYSFILES);
		numSysFiles = resultSetSize(rs);

		// Install the jar file of user-defined stored procedures.
		rc = methodWatcher.executeUpdate(String.format(CALL_INSTALL_JAR_FORMAT_STRING, STORED_PROCS_JAR_FILE, JAR_FILE_SQL_NAME));
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Add the jar file into the global DB class path.
		rc = methodWatcher.executeUpdate(String.format(CALL_SET_GLOBAL_CLASSPATH_FORMAT_STRING, JAR_FILE_SQL_NAME));
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Create the user-defined stored procedure.
		rc = methodWatcher.executeUpdate(CREATE_PROC_SIMPLE_ONE_ARG);
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Call the user-defined stored procedure.
		rs = methodWatcher.executeQuery(String.format(CALL_SIMPLE_ONE_ARG_PROC_FORMAT_STRING, "foobar"));
		Assert.assertTrue("Incorrect rows returned!", resultSetSize(rs) > 10);

		// Replace the jar file.
		rc = methodWatcher.executeUpdate(String.format(CALL_REPLACE_JAR_FORMAT_STRING, STORED_PROCS_JAR_FILE, JAR_FILE_SQL_NAME));
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Call the user-defined stored procedure again.
		rs = methodWatcher.executeQuery(String.format(CALL_SIMPLE_ONE_ARG_PROC_FORMAT_STRING, "foobar"));
		Assert.assertTrue("Incorrect rows returned!", resultSetSize(rs) > 10);

		// Drop the user-defined stored procedure.
		rc = methodWatcher.executeUpdate(DROP_PROC_SIMPLE_ONE_ARG);
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Check that the global DB class path is correct.
		rs = methodWatcher.executeQuery(CALL_GET_GLOBAL_CLASSPATH);
		String dbClassPath = null;
		if (rs.next()) {
			dbClassPath = rs.getString(2);
		}
		Assert.assertEquals("Global database CLASSPATH is incorrect!", JAR_FILE_SQL_NAME, dbClassPath);

		// Remove the jar file from the global DB class path.
		rc = methodWatcher.executeUpdate(CALL_SET_GLOBAL_CLASSPATH_TO_DEFAULT);
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Remove the jar file from the DB.
		rc = methodWatcher.executeUpdate(String.format(CALL_REMOVE_JAR_FORMAT_STRING, JAR_FILE_SQL_NAME));
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Compare that the number of SYSFILES matches the original count.
		rs = methodWatcher.executeQuery(SELECT_FROM_SYSFILES);
		Assert.assertEquals("Incorrect rows returned!", numSysFiles, resultSetSize(rs));
	}
}
