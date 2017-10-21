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
package com.splicemachine.hbase;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.ResultSet;

/**
 * Created by yxia on 10/20/17.
 */
public class CollectStatsIT extends SpliceUnitTest {
    private static final String SCHEMA=CollectStatsIT.class.getSimpleName().toUpperCase();
    public static final String CLASS_NAME = CollectStatsIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @Test
    public void testStatsCollectionWithEmptyPartitions()throws Exception {
        /* create table with 6 partitions, among them 5 are empty and 1 partition has exactly 512 rows
         */
        methodWatcher.executeUpdate("drop table if exists TY");
        methodWatcher.executeUpdate("create table TY (a1 int, b1 int, primary key(a1))");
        String sql = format("call SYSCS_UTIL.SYSCS_SPLIT_TABLE_OR_INDEX" +
                        "('%s', 'TY',null, 'a1', '%s', '|', null, null, null, null, -1, '%s', true, null)",
                SCHEMA,
                SpliceUnitTest.getResourceDirectory()+"table_split_key1.csv",
                SpliceUnitTest.getResourceDirectory() + "baddir");

        methodWatcher.executeUpdate(sql);

        sql = format("call SYSCS_UTIL.IMPORT_DATA" +
                        "('%s', 'TY', null, '%s', null, null, 'MM/dd/yyyy HH:mm:ss', null, null, -1, '%s', true, null)",
                SCHEMA,
                SpliceUnitTest.getResourceDirectory()+"ty.csv",
                SpliceUnitTest.getResourceDirectory() + "baddir");

        methodWatcher.executeQuery(sql);

        /* collect stats */
        sql = format("analyze table %s.TY", SCHEMA);
        methodWatcher.executeQuery(sql);

        /* check table statistics */
        ResultSet rs = methodWatcher.executeQuery(String.format("select total_row_count from " +
                "sys.systablestatistics where schemaname='%s' and tablename='TY'", SCHEMA));
        Assert.assertTrue("Unable to find table statistics for table!", rs.next());
        Assert.assertEquals("Incorrect rowcount value", 512, rs.getLong(1));
        rs.close();

        /* check column statistics on A1  */
        rs = methodWatcher.executeQuery(String.format("select min_value, max_value from " +
                "sys.syscolumnstatistics where schemaname='%s' and tablename='TY' and columnname='A1'", SCHEMA));
        Assert.assertTrue("Unable to find column statistics for table!", rs.next());
        Assert.assertEquals("Incorrect min value", 3001, rs.getLong(1));
        Assert.assertEquals("Incorrect max value", 3512, rs.getLong(2));

        rs.close();

        /* test stats collection on empty table */
        methodWatcher.executeUpdate("delete from TY");

        sql = format("analyze table %s.TY", SCHEMA);
        methodWatcher.executeQuery(sql);

        /* check table statistics again */
        rs = methodWatcher.executeQuery(String.format("select total_row_count from " +
                "sys.systablestatistics where schemaname='%s' and tablename='TY'", SCHEMA));
        Assert.assertTrue("Unable to find table statistics for table!", rs.next());
        Assert.assertEquals("Incorrect rowcount value", 0, rs.getLong(1));
        rs.close();

        /* check column statistics on A1 again  */
        rs = methodWatcher.executeQuery(String.format("select min_value, max_value from " +
                "sys.syscolumnstatistics where schemaname='%s' and tablename='TY' and columnname='A1'", SCHEMA));
        Assert.assertTrue("Unable to find column statistics for table!", rs.next());
        int minValue = rs.getInt(1);
        Assert.assertTrue("Incorrect min value", rs.wasNull());
        int maxValue = rs.getInt(2);
        Assert.assertTrue("Incorrect max value", rs.wasNull());

        rs.close();
    }
}
