package com.splicemachine.derby.impl.sql.catalog;

import java.sql.ResultSet;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

/**
 * Tests for the SQLJ JAR file management stored procedures.
 *
 * @author David Winters
 *		 Created on: 9/25/14
 */
public class SqlJJarIT extends SpliceUnitTest {

	public static final String CLASS_NAME = SqlJJarIT.class.getSimpleName().toUpperCase();

	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

	// Names of files and SQL objects.
	private static final String SCHEMA_NAME = CLASS_NAME;
	private static final String STORED_PROCS_JAR_FILE = getResourceDirectory() + "/sqlj-it-procs/sqlj-it-procs-1.0-SNAPSHOT.jar";
	private static final String JAR_FILE_SQL_NAME = SCHEMA_NAME + ".SQLJ_IT_PROCS_JAR";

	// SQL statements to create and drop stored procedures.
	private static final String CREATE_SIMPLE_ONE_ARG_PROC =
		"CREATE PROCEDURE " + SCHEMA_NAME + ".SIMPLE_ONE_ARG_PROC(IN name VARCHAR(30)) " +
		"PARAMETER STYLE JAVA READS SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 " +
		"EXTERNAL NAME 'org.splicetest.sqlj.SqlJTestProcs.SIMPLE_ONE_ARG_PROC'";
	private static final String DROP_SIMPLE_ONE_ARG_PROC =
		"DROP PROCEDURE " + SCHEMA_NAME + ".SIMPLE_ONE_ARG_PROC";

	// SQL statements to call stored procedures.
	private static final String CALL_INSTALL_JAR_FORMAT_STRING = "CALL SQLJ.INSTALL_JAR('%s', '%s', 0)";
	private static final String CALL_REPLACE_JAR_FORMAT_STRING = "CALL SQLJ.REPLACE_JAR('%s', '%s')";
	private static final String CALL_REMOVE_JAR_FORMAT_STRING = "CALL SQLJ.REMOVE_JAR('%s', 0)";
	private static final String CALL_SET_CLASSPATH_FORMAT_STRING = "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.classpath', '%s')";
	private static final String CALL_SET_CLASSPATH_TO_DEFAULT = "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.classpath', NULL)";
	private static final String CALL_SIMPLE_ONE_ARG_PROC_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".SIMPLE_ONE_ARG_PROC('%s')";

	// SQL queries.
	private static final String SELECT_FROM_SYSFILES = "SELECT * FROM SYS.SYSFILES";

	@ClassRule
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
			.around(spliceSchemaWatcher);

	@Rule
	public SpliceWatcher methodWatcher = new SpliceWatcher();

	@Test
	public void testJarWithSimpleOneArgProc() throws Exception {
		ResultSet rs = null;
		int rc = 0;

		// Count the number of SYSFILES.
		rs = methodWatcher.executeQuery(SELECT_FROM_SYSFILES);
		int numSysFiles = resultSetSize(rs);

		// Install the jar file of user-defined stored procedures.
		rc = methodWatcher.executeUpdate(String.format(CALL_INSTALL_JAR_FORMAT_STRING, STORED_PROCS_JAR_FILE, JAR_FILE_SQL_NAME));
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Add the jar file into the DB class path.
		rc = methodWatcher.executeUpdate(String.format(CALL_SET_CLASSPATH_FORMAT_STRING, JAR_FILE_SQL_NAME));
		Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

		// Create the user-defined stored procedure.
		rc = methodWatcher.executeUpdate(CREATE_SIMPLE_ONE_ARG_PROC);
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
		rc = methodWatcher.executeUpdate(DROP_SIMPLE_ONE_ARG_PROC);
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
	}
}
