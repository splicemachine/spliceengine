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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.junit.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
/**
 * Created by jyuan on 7/30/15.
 */
public class AnalyzeTableIT extends SpliceUnitTest {
    private static final String SCHEMA = AnalyzeTableIT.class.getSimpleName();
    private static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void setup() throws Exception {
        classWatcher.execute("grant access on schema " + SCHEMA + " to public");
        classWatcher.executeUpdate("create table T1 (I INT)");
        classWatcher.executeUpdate("create table T2 (I INT)");
        classWatcher.executeUpdate("create table T3 (c char(4), i int, d double)");

        classWatcher.executeUpdate("create index T3_IDX1 on t3 (d, i)");
        classWatcher.executeUpdate("create index T3_IDX2 on t3 (upper(c), mod(i,2))");

        classWatcher.executeUpdate("insert into T1 values 1, 2, 3");
        classWatcher.executeUpdate("insert into T2 values 1, 2, 3, 4, 5, 6, 7, 8, 9");
        classWatcher.executeUpdate("insert into T3 values ('abc', 11, 1.1), ('def', 20, 2.2), ('jkl', 21, 2.2), ('ghi', 30, 3.3), ('xyz', 40, 3.3)");

        classWatcher.execute("call syscs_util.syscs_create_user('analyzeuser', 'passwd')");
        classWatcher.execute("grant execute on procedure syscs_util.collect_table_statistics to analyzeuser");
        classWatcher.execute("grant execute on procedure syscs_util.collect_schema_statistics to analyzeuser");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        classWatcher.execute("call syscs_util.syscs_drop_user('analyzeuser')");
    }

    @Test
    public void testAnalyzeTable() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("analyze table AnalyzeTableIT.T1");
        int count = 0;
        while(rs.next()) {
            count++;
        }
        Assert.assertEquals(1, count);
    }

    @Test
    public void testAnalyseAliasTable() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("analyse table AnalyzeTableIT.T1");
        int count = 0;
        while(rs.next()) {
            count++;
        }
        Assert.assertEquals(1, count);
    }

    @Test
    public void testAnalyzeSchema() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("analyze schema AnalyzeTableIT");
        int count = 0;
        while(rs.next()) {
            count++;
        }
        Assert.assertEquals(4, count);
    }

    @Test
    public void testAnalyseAliasSchema() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("analyse schema AnalyzeTableIT");
        int count = 0;
        while(rs.next()) {
            count++;
        }
        // 3 tables + 1 expression-based index
        Assert.assertEquals(4, count);
    }

    @Test
    public void testDefaultSchema() throws Exception {
        Connection connection = methodWatcher.getOrCreateConnection();
        Statement statement = connection.createStatement();
        statement.execute("set schema " + SCHEMA);
        ResultSet rs = statement.executeQuery("Analyze table t1");
        int count = 0;
        while (rs.next()) {
            ++count;
            String schema = rs.getString(1);
            Assert.assertTrue(schema.compareToIgnoreCase(SCHEMA) == 0);
        }
        // 3 tables + 1 expression-based index
        Assert.assertEquals(1, count);
    }

    @Test
    public void testDefaultSchemaAlias() throws Exception {
        Connection connection = methodWatcher.getOrCreateConnection();
        Statement statement = connection.createStatement();
        statement.execute("set schema " + SCHEMA);
        ResultSet rs = statement.executeQuery("Analyse table t1");
        int count = 0;
        while (rs.next()) {
            ++count;
            String schema = rs.getString(1);
            Assert.assertTrue(schema.compareToIgnoreCase(SCHEMA) == 0);
        }
        Assert.assertEquals(1, count);
    }

    @Test
    public void testAnalyzeTablePrivilege() throws Exception {
        String message = null;
        String expected = null;
        try {
            Connection connection = methodWatcher.connectionBuilder().user("analyzeuser").password("passwd").build();
            Statement statement = connection.createStatement();
            statement.execute("analyze table AnalyzeTableIT.T2");
        }
        catch (SQLSyntaxErrorException e) {
            expected = "4251M";
            message = e.getSQLState();
        }
        Assert.assertNotNull(message);
        Assert.assertNotNull(expected);
        Assert.assertTrue(message.compareTo(expected) == 0);
    }

    @Test
    public void testAnalyzeSchemaPrivilege() throws Exception {

        String message = null;
        String expected = null;
        try {
            Connection connection = methodWatcher.connectionBuilder().user("analyzeuser").password("passwd").build();
            Statement statement = connection.createStatement();
            statement.execute("analyze schema AnalyzeTableIT");
        }
        catch (SQLSyntaxErrorException e) {
            expected = "4251M";
            message = e.getSQLState();
        }

        Assert.assertNotNull(message);
        Assert.assertNotNull(expected);
        Assert.assertTrue(message.compareTo(expected) == 0);
    }

    @Test
    public void testAnalyzeTableWithIndexes() throws Exception {
        try (ResultSet rs = methodWatcher.executeQuery("analyze table AnalyzeTableIT.T3")) {
            int count = 0;
            while(rs.next()) {
                count++;
                if (count == 1) {
                    Assert.assertTrue(rs.getString(2).contains("T3"));
                } else if (count == 2) {
                    Assert.assertTrue(rs.getString(2).contains("T3_IDX2"));
                }
            }
            // 2 rows for table and expression-based index, no statistics collected on T3_IDX1
            Assert.assertEquals(2, count);
        }

        // check sys.syscolumnstats for index columns in T3_IDX2, should have 2 columns
        String query = "select count(*) from sys.sysconglomerates c, sys.syscolumnstats sc " +
                "where c.conglomeratenumber = sc.conglom_id and c.isindex = true and c.conglomeratename='T3_IDX2'";

        String expected = "1 |\n" +
                "----\n" +
                " 2 |";

        try (ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    @Test
    public void testDropExprIndexStatistics() throws Exception {
        String tableName = "TEST_DROP_EXPR_INDEX_STATS";
        methodWatcher.executeUpdate(format("create schema if not exists %s_TEST", SCHEMA));
        methodWatcher.executeUpdate(format("set schema %s_TEST", SCHEMA));
        methodWatcher.executeUpdate(format("create table if not exists %s (c char(3))", tableName));
        methodWatcher.executeUpdate(format("create index %s_idx on %s (upper(c))", tableName, tableName));
        methodWatcher.executeQuery(format("analyze table %s", tableName));

        String checkQuery = format("select count(*) from sys.sysconglomerates c, sys.systablestats ts " +
                "where c.conglomeratename='%s_IDX' and c.conglomeratenumber = ts.conglomerateid", tableName);
        String expected = "1 |\n" +
                "----\n" +
                " 1 |";

        try (ResultSet rs = methodWatcher.executeQuery(checkQuery)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        // dropping an expression-based index should drop its statistics, too
        methodWatcher.executeUpdate(format("drop index %s_idx", tableName));
        try (ResultSet rs = methodWatcher.executeQuery(checkQuery)) {
            expected = "1 |\n" +
                    "----\n" +
                    " 0 |";
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }
}
