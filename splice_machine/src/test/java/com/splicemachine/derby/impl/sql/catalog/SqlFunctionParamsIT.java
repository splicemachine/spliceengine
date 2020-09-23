/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

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
	public void testCatalogNameLikeComparison() throws Exception {
		ResultSet rs = null;
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS(null, null, 'TO_DATE', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 3, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS('%', null, 'TO_DATE', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 3, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS('%FUN', null, 'TO_DATE', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 3, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS('SYS%', null, 'TO_DATE', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 3, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS('SYSFUN', null, 'TO_DATE', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 3, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS('SY%UN', null, 'TO_DATE', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 3, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS('S%S%UN', null, 'TO_DATE', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 3, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS('S%S%U%', null, 'TO_DATE', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 3, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS('%Y%%U%', null, 'TO_DATE', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 3, resultSetSize(rs));
	}

	@Test
	public void testSchemaNameLikeComparison() throws Exception {
		ResultSet rs = null;
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS(null, null, 'TO_DATE', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 3, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS(null, '%', 'TO_DATE', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 3, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS(null, '%FUN', 'TO_DATE', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 3, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS(null, 'SYS%', 'TO_DATE', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 3, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS(null, 'SYSFUN', 'TO_DATE', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 3, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS(null, 'SY%UN', 'TO_DATE', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 3, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS(null, 'S%S%UN', 'TO_DATE', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 3, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS(null, 'S%S%U%', 'TO_DATE', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 3, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS(null, '%Y%%U%', 'TO_DATE', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 3, resultSetSize(rs));
	}

	@Test
	public void testFuncNameLikeComparison() throws Exception {
		ResultSet rs = null;
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS(null, null, null, null, null)");
		Assert.assertTrue("Incorrect rows returned!", resultSetSize(rs) > 50);
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS(null, null, '%', null, null)");
		Assert.assertTrue("Incorrect rows returned!", resultSetSize(rs) > 50);
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS(null, null, '%_DATE', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 6, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS(null, null, 'TO_DA%', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 3, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS(null, null, 'TO_DATE', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 3, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS(null, null, 'TO%DATE', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 3, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS(null, null, 'T%_D%TE', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 6, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS(null, null, 'T%_D%T%', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 6, resultSetSize(rs));
	}

	@Test
	public void testParamNameLikeComparison() throws Exception {
		ResultSet rs = null;
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS(null, null, 'TO_DATE', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 3, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS(null, null, 'TO_DATE', '%', null)");
		Assert.assertEquals("Incorrect rows returned!", 3, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS(null, null, 'TO_DATE', '%MAT', null)");
		Assert.assertEquals("Incorrect rows returned!", 1, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS(null, null, 'TO_DATE', 'FOR%', null)");
		Assert.assertEquals("Incorrect rows returned!", 1, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS(null, null, 'TO_DATE', 'FORMAT', null)");
		Assert.assertEquals("Incorrect rows returned!", 1, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS(null, null, 'TO_DATE', 'FOR%MAT', null)");
		Assert.assertEquals("Incorrect rows returned!", 1, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS(null, null, 'TO_DATE', 'F%RM%T', null)");
		Assert.assertEquals("Incorrect rows returned!", 1, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS(null, null, 'TO_DATE', 'FOR%M%T%', null)");
		Assert.assertEquals("Incorrect rows returned!", 1, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS(null, null, 'TO_DATE', '%O%MA%', null)");
		Assert.assertEquals("Incorrect rows returned!", 1, resultSetSize(rs));
	}

	@Test
	public void testResultSetColumnTypesForJDBC() throws Exception {
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
