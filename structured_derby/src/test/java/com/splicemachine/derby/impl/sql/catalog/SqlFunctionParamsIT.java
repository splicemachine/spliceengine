package com.splicemachine.derby.impl.sql.catalog;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;

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
 * Tests for the SYSIBM.SQLFUNCTIONPARAMS stored procedure.
 * This stored procedure is used by the DatabaseMetaData.getFunctionColumns() API in JDBC.
 *
 * @author David Winters
 *		 Created on: 9/26/14
 */
public class SqlFunctionParamsIT extends SpliceUnitTest {
	public static final String CLASS_NAME = SqlFunctionParamsIT.class.getSimpleName().toUpperCase();

	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

	@ClassRule
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
			.around(spliceSchemaWatcher);

	@Rule
	public SpliceWatcher methodWatcher = new SpliceWatcher();

	@Test
	public void testLikeComparison() throws Exception {
		ResultSet rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS(null, null, 'TO_DATE', '%MAT', null)");
		Assert.assertEquals("Incorrect rows returned!", 1, resultSetSize(rs));
	}

	@Test
	public void testResultSetColumnTypes() throws Exception {
		ResultSet rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS(null, null, 'TO_DATE', 'FORMAT', null)");
		int count = 0;
		while (rs.next()) {
			ResultSetMetaData rsmd = rs.getMetaData();
			Assert.assertEquals("Incorrect SQL type for column!", Types.VARCHAR, rsmd.getColumnType(1));
			Assert.assertEquals("Incorrect SQL type for column!", Types.VARCHAR, rsmd.getColumnType(2));
			Assert.assertEquals("Incorrect SQL type for column!", Types.VARCHAR, rsmd.getColumnType(3));
			Assert.assertEquals("Incorrect SQL type for column!", Types.VARCHAR, rsmd.getColumnType(4));
			Assert.assertEquals("Incorrect SQL type for column!", Types.SMALLINT, rsmd.getColumnType(5));
			Assert.assertEquals("Incorrect SQL type for column!", Types.INTEGER, rsmd.getColumnType(6));
			Assert.assertEquals("Incorrect SQL type for column!", Types.VARCHAR, rsmd.getColumnType(7));
			Assert.assertEquals("Incorrect SQL type for column!", Types.INTEGER, rsmd.getColumnType(8));
			Assert.assertEquals("Incorrect SQL type for column!", Types.INTEGER, rsmd.getColumnType(9));
			Assert.assertEquals("Incorrect SQL type for column!", Types.SMALLINT, rsmd.getColumnType(10));
			Assert.assertEquals("Incorrect SQL type for column!", Types.SMALLINT, rsmd.getColumnType(11));
			Assert.assertEquals("Incorrect SQL type for column!", Types.SMALLINT, rsmd.getColumnType(12));
			Assert.assertEquals("Incorrect SQL type for column!", Types.VARCHAR, rsmd.getColumnType(13));
			Assert.assertEquals("Incorrect SQL type for column!", Types.INTEGER, rsmd.getColumnType(14));
			Assert.assertEquals("Incorrect SQL type for column!", Types.INTEGER, rsmd.getColumnType(15));
			Assert.assertEquals("Incorrect SQL type for column!", Types.VARCHAR, rsmd.getColumnType(16));
			Assert.assertEquals("Incorrect SQL type for column!", Types.VARCHAR, rsmd.getColumnType(17));
			count++;
		}
		Assert.assertEquals("Incorrect rows returned!", 1, count);
	}
}
