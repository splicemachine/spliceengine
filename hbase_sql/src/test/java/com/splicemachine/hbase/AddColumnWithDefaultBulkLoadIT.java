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
package com.splicemachine.hbase;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 * Created by yxia on 11/1/17.
 */
public class AddColumnWithDefaultBulkLoadIT extends SpliceUnitTest {
    private static final String SCHEMA_NAME=AddColumnWithDefaultBulkLoadIT.class.getSimpleName().toUpperCase();
    public static final String CLASS_NAME = AddColumnWithDefaultBulkLoadIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    private static String BADDIR;
    private static String BULKLOADDIR;

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table T1 (a1 int, b1 int, primary key(a1))")
                .create();

        new TableCreator(conn)
                .withCreate("create table T2 (a2 int, b2 int, c2 varchar(10), primary key (a2))")
                .withInsert("insert into t2 values(?,?,?)")
                .withRows(rows(
                        row(2,2,"A2"),
                        row(3,3,"A3"),
                        row(3004,4,"A3004"),
                        row(3005,5,"A3005"),
                        row(5006,6,"A5006"),
                        row(5007,7,"A5007")))
                .create();

        /* split the table into multiple partitions */
        spliceClassWatcher.executeUpdate(format("call SYSCS_UTIL.SYSCS_SPLIT_TABLE_OR_INDEX" +
                        "('%s', 'T1',null, 'a1', '%s', '|', null, null, null, null, -1, '%s', true, null)",
                        SCHEMA_NAME,
                        SpliceUnitTest.getResourceDirectory()+"table_split_key1.csv",
                        BADDIR));

        /* bulk import the rows */
        spliceClassWatcher.executeQuery(format("call SYSCS_UTIL.IMPORT_DATA" +
                        "('%s', 'T1', null, '%s', null, null, 'MM/dd/yyyy HH:mm:ss', null, null, -1, '%s', true, null)",
                        SCHEMA_NAME,
                        SpliceUnitTest.getResourceDirectory()+"t1_part1.csv",
                        BADDIR));

        /* add a new not-null column with default value */
        spliceClassWatcher.executeUpdate("alter table T1 add column c1 varchar(10) not null default 'ZZZZZ'");

        /* bulk import more rows */
        spliceClassWatcher.executeQuery(format("call SYSCS_UTIL.IMPORT_DATA" +
                        "('%s', 'T1', null, '%s', null, null, 'MM/dd/yyyy HH:mm:ss', null, null, -1, '%s', true, null)",
                SCHEMA_NAME,
                SpliceUnitTest.getResourceDirectory()+"t1_part2.csv",
                BADDIR));

        /* collect stats */
        spliceClassWatcher.getOrCreateConnection().prepareStatement("analyze schema " + CLASS_NAME).execute();

        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        BADDIR = SpliceUnitTest.createBadLogDirectory(SCHEMA_NAME).getCanonicalPath();
        BULKLOADDIR = SpliceUnitTest.createBulkLoadDirectory(SCHEMA_NAME).getCanonicalPath();
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }

    @AfterClass
    public static void cleanup() throws Exception {
        String sql = String.format("delete from T1 --splice-properties bulkDeleteDirectory='%s'", BULKLOADDIR);
        spliceClassWatcher.execute(sql);
        ResultSet rs = spliceClassWatcher.executeQuery("select count(*) from T1");
        rs.next();
        int count = rs.getInt(1);
        Assert.assertTrue(count==0);
        FileUtils.deleteDirectory(new File(BULKLOADDIR));
    }

    @Test
    public void testBulkIndexCreation() throws Exception {
        String sql = format("create index idx_t1 on t1 (c1, b1) auto splitkeys hfile location '%s'", BULKLOADDIR);
        methodWatcher.executeUpdate(sql);

        /* verify the correctness of the index */
        /* Q1 -- check count */
        sql = "select count(*) from t1 --splice-properties index=idx_t1";

        ResultSet rs = methodWatcher.executeQuery(sql);
        String expected = "1  |\n" +
                "------\n" +
                "1536 |";
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* Q2 */
        sql = "select * from t1 --splice-properties index=idx_t1\n , t2 where c1=c2";
        expected = "A1  | B1  | C1   | A2  |B2 | C2   |\n" +
                "------------------------------------\n" +
                "  2  |  2  | A2   |  2  | 2 | A2   |\n" +
                "  3  |  3  | A3   |  3  | 3 | A3   |\n" +
                "5006 |5006 |A5006 |5006 | 6 |A5006 |\n" +
                "5007 |5007 |A5007 |5007 | 7 |A5007 |";
        rs = methodWatcher.executeQuery(sql);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        sql = format("delete from T1 --splice-properties bulkDeleteDirectory='%s'\n where c1='ZZZZZ'", BULKLOADDIR);
        methodWatcher.executeUpdate(sql);

        /* verify the correctness of the delete*/
        /* Q3 -- check count */
        sql = "select count(*) from t1";
        expected = "1  |\n" +
                "------\n" +
                "1024 |";
        rs = methodWatcher.executeQuery(sql);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* Q4 -- check count through index */
        sql = "select count(*) from t1 --splice-properties index=idx_t1";
        rs = methodWatcher.executeQuery(sql);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }
}
