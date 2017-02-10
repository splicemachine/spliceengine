/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

import java.sql.CallableStatement;
import java.sql.ResultSet;

import com.splicemachine.test.SerialTest;
import org.apache.commons.dbutils.DbUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import com.splicemachine.derby.test.framework.SpliceIndexWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

/**
 * Tests for the SYSIBM.SQLPROCEDURECOLS stored procedure.
 * This stored procedure is used by the 'show indexes' command in ij.
 *
 * @author David Winters
 *         Created on: 3/5/14
 */
public class SqlStatisticsIT extends SpliceUnitTest {
    public static final String CLASS_NAME = SqlStatisticsIT.class.getSimpleName().toUpperCase();
	private static Logger LOG = Logger.getLogger(SqlStatisticsIT.class);

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
	private static final String TABLE_NAME_1 = CLASS_NAME + "_T1";
	private static final String TABLE_NAME_2 = CLASS_NAME + "_T2";
	private static final String INDEX_NAME_1 = "IDX_" + TABLE_NAME_1 + "_C1";
	private static final String INDEX_NAME_2 = "IDX_" + TABLE_NAME_2 + "_C1C2C3";
    private static String tableDef = "(C1 INT, C2 INT, C3 INT)";
    protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_NAME_1, CLASS_NAME, tableDef);
    protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE_NAME_2, CLASS_NAME, tableDef);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher1)
            .around(spliceTableWatcher2);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testShowAllIndexes() throws Exception{
            Assert.assertTrue("Incorrect rows returned!", getResultSetCountFromShowIndexes(null, null) > 50);  // There should be roughly 73 indexes on the SYS tables by default.  No hard coding since the # of indexes in SYS is subject to change.
    }

    @Test
    public void testShowIndexesInSchema() throws Exception{
            Assert.assertTrue("Incorrect rows returned!", getResultSetCountFromShowIndexes("SYS", null) > 50);  // There should be roughly 73 indexes on the SYS tables by default.  No hard coding since the # of indexes in SYS is subject to change.
    }

    @Test
    public void testShowIndexesFromSchemaDotTable() throws Exception{
            Assert.assertEquals("Incorrect rows returned!", 3, getResultSetCountFromShowIndexes("SYS", "SYSTABLES"));  // There should be 3 indexes on the SYSTABLES table.
    }

    @Test
    public void testShowIndexesFromTable() throws Exception{
            Assert.assertEquals("Incorrect rows returned!", 3, getResultSetCountFromShowIndexes(null, "SYSTABLES"));  // There should be 3 indexes on the SYSTABLES table.
    }

    @Test
    public void testCreateSingleColumnIndex() throws Exception {
    	SpliceIndexWatcher indexWatcher = new SpliceIndexWatcher(TABLE_NAME_1, CLASS_NAME, INDEX_NAME_1, CLASS_NAME, "(C1)", false);
    	indexWatcher.starting(null);
        Assert.assertTrue("Incorrect rows returned!", getResultSetCountFromShowIndexes(null, null) > 50);   // There should be roughly 74 indexes.  No hard coding since the # of indexes in SYS is subject to change.
        Assert.assertEquals("Incorrect rows returned!", 1, getResultSetCountFromShowIndexes(CLASS_NAME, null));  // There should be 1 index row for the APP schema.
        Assert.assertEquals("Incorrect rows returned!", 1, getResultSetCountFromShowIndexes(CLASS_NAME, TABLE_NAME_1));  // There should be 1 index row for the T1 table.
        Assert.assertEquals("Incorrect rows returned!", 1, getResultSetCountFromShowIndexes(null, TABLE_NAME_1));   // There should be 1 index row for the T1 table.
        indexWatcher.finished(null);
    }

    @Test
    public void testCreateMultiColumnIndex() throws Exception {
    	SpliceIndexWatcher indexWatcher1 = new SpliceIndexWatcher(TABLE_NAME_1, CLASS_NAME, INDEX_NAME_1, CLASS_NAME, "(C1)", false);
    	SpliceIndexWatcher indexWatcher2 = new SpliceIndexWatcher(TABLE_NAME_2, CLASS_NAME, INDEX_NAME_2, CLASS_NAME, "(C1, C2, C3)", false);
    	indexWatcher1.starting(null);
    	indexWatcher2.starting(null);
        Assert.assertTrue("Incorrect rows returned!", getResultSetCountFromShowIndexes(null, null) > 50);   // There should be roughly 77 indexes.  No hard coding since the # of indexes in SYS is subject to change.
        Assert.assertEquals("Incorrect rows returned!", 4, getResultSetCountFromShowIndexes(CLASS_NAME, null));  // There should be 4 index rows for the APP schema.
        Assert.assertEquals("Incorrect rows returned!", 3, getResultSetCountFromShowIndexes(CLASS_NAME, TABLE_NAME_2));  // There should be 3 index rows for the T2 table.
        Assert.assertEquals("Incorrect rows returned!", 3, getResultSetCountFromShowIndexes(null, TABLE_NAME_2));   // There should be 3 index rows for the T2 table.
        indexWatcher1.finished(null);
        indexWatcher2.finished(null);
    }

    private int getResultSetCountFromShowIndexes(String schemaName, String tableName) throws Exception {
    	if (schemaName == null) {
    		schemaName = "null";
    	} else {
    		schemaName = "'" + schemaName + "'";
    	}
    	if (tableName == null) {
    		tableName = "null";
    	} else {
    		tableName = "'" + tableName + "'";
    	}
        CallableStatement cs = methodWatcher.prepareCall(format("call SYSIBM.SQLSTATISTICS(null, %s, %s, 1, 1, null)", schemaName, tableName), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = cs.executeQuery();
        int count = 0;
        LOG.trace(format("Show Indexes Args: schema = %s, table = %s", schemaName, tableName));
        while (rs.next()) {
            String schema = rs.getString("TABLE_SCHEM");
            String table = rs.getString("TABLE_NAME");
            String index = rs.getString("INDEX_NAME");
            String column = rs.getString("COLUMN_NAME");
            int position = rs.getInt("ORDINAL_POSITION");
            LOG.trace(format("Show Indexes Results: schema = %s, table = %s, index = %s, column = %s, position = %s", schema, table, index, column, position));
            count++;
        }
        LOG.trace(format("Show Indexes Results: count = %s", count));
        DbUtils.closeQuietly(rs);
        return count;
    }
}
