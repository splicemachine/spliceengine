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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.spark_project.guava.collect.Lists;

import java.sql.ResultSet;
import java.util.Collection;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 *
 *
 */
@RunWith(Parameterized.class)
public class JoinOperationIT extends SpliceUnitTest {
    private static final String SCHEMA = JoinOperationIT.class.getSimpleName().toUpperCase();
    private static SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(6);
        params.add(new Object[]{"NESTEDLOOP","true"});
        params.add(new Object[]{"SORTMERGE","true"});
        params.add(new Object[]{"BROADCAST","true"});
        params.add(new Object[]{"NESTEDLOOP","false"});
        params.add(new Object[]{"SORTMERGE","false"});
        params.add(new Object[]{"BROADCAST","false"});
        return params;
    }

    private String joinStrategy;
    private String useSparkString;

    public JoinOperationIT(String joinStrategy, String useSparkString) {
        this.joinStrategy = joinStrategy;
        this.useSparkString = useSparkString;
    }

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createSharedTables() throws Exception {
        TestConnection connection=spliceClassWatcher.getOrCreateConnection();
        new TableCreator(connection)
                .withCreate("create table FOO (col1 int primary key, col2 int)")
                .withInsert("insert into FOO values(?,?)")
                .withRows(rows(row(1, 1), row(2, 1), row(3, 1), row(4, 1), row(5, 1))).create();

        new TableCreator(connection)
                .withCreate("create table FOO2 (col1 int primary key, col2 int)")
                .withInsert("insert into FOO2 values(?,?)")
                .withRows(rows(row(1, 5), row(3, 7), row(5, 9))).create();

        new TableCreator(connection)
                .withCreate("create table a (v varchar(12))")
                .withInsert("insert into a values(?)")
                .withRows(rows(row("1"), row("2"), row("3 "), row("4 "))).create();

        new TableCreator(connection)
                .withCreate("create table b (v varchar(12))")
                .withInsert("insert into b values(?)")
                .withRows(rows(row("1"), row("2 "), row("3"), row("4 "))).create();

        new TableCreator(connection)
                .withCreate("create table a1 (i int, primary key(i))")
                .withInsert("insert into a1 values(?)")
                .withRows(rows(row(1), row(2), row(3), row(4))).create();

        new TableCreator(connection)
                .withCreate("create table b1 (i int)")
                .withInsert("insert into b1 values(?)")
                .withRows(rows(row(3), row(4), row(1), row(2))).create();
    }

    @Test
    public void testInnerJoinNoRestriction() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format(
                "select count(*), max(foo.col1) from --Splice-properties joinOrder=FIXED\n " +
                        "foo, foo2 --Splice-properties joinStrategy=%s, useSpark=%s\n" +
                        "where foo.col1 = foo2.col1", this.joinStrategy,this.useSparkString
        ));
        rs.next();
        Assert.assertEquals(String.format("Missing Records for %s", joinStrategy), 3, rs.getInt(1));
        Assert.assertEquals(String.format("Wrong max for %s", joinStrategy), 5, rs.getInt(2));
    }

    @Test
    public void testInnerJoinRestriction() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format(
                "select count(*), min(foo.col1) from --Splice-properties joinOrder=FIXED\n" +
                        " foo inner join foo2 --Splice-properties joinStrategy=%s, useSpark=%s\n" +
                        "on foo.col1 = foo2.col1 and foo.col1+foo2.col2>6", this.joinStrategy,this.useSparkString
        ));
        rs.next();
        Assert.assertEquals(String.format("Missing Records for %s", joinStrategy), 2, rs.getInt(1));
        Assert.assertEquals(String.format("Wrong min for %s", joinStrategy), 3, rs.getInt(2));
    }

    @Test
    @Ignore("DB-5173 no valid plan execution for sortmerge, bcast and merge")
    public void testInnerAntiJoinNoRestriction() throws Exception {
        System.out.println(joinStrategy);
        ResultSet rs = methodWatcher.executeQuery(String.format(
                "select count(*) from --SPLICE-PROPERTIES joinOrder=FIXED\n" +
                        " foo where not exists (select * from foo2 --SPLICE-PROPERTIES joinStrategy=%s, useSpark=%s\n" +
                        "where foo.col1 = foo2.col1)", this.joinStrategy, this.useSparkString
        ));
        rs.next();
        Assert.assertEquals(String.format("Missing Records for %s", joinStrategy), 2, rs.getInt(1));
    }

    @Test
    public void testInnerAntiJoinRestriction() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format(
                "select count(*) from --Splice-properties joinOrder=FIXED\n" +
                        " foo where not exists (select * from foo2 --Splice-properties joinStrategy=%s, useSpark=%s\n" +
                        "where foo.col1 = foo2.col1 and foo.col1+foo2.col2>6)", this.joinStrategy,this.useSparkString
        ));
        rs.next();
        Assert.assertEquals(String.format("Missing Records for %s", joinStrategy), 3, rs.getInt(1));
    }

    @Test
    public void testOuterJoinNoRestriction() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format(
                "select count(*), sum(CASE WHEN foo2.col1 is null THEN 0 ELSE 1 END) from --Splice-properties joinOrder=FIXED\n" +
                        " foo left outer join foo2 --Splice-properties joinStrategy=%s, useSpark=%s\n" +
                        "on foo.col1 = foo2.col1", this.joinStrategy, this.useSparkString
        ));
        rs.next();
        Assert.assertEquals(String.format("Missing Records for %s", joinStrategy), 5, rs.getInt(1));
        Assert.assertEquals(String.format("Missing Records for %s", joinStrategy), 3, rs.getInt(2));
    }

    @Test
    public void testOuterJoinRestriction() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format(
                "select count(*), sum(CASE WHEN foo2.col2 is null THEN 0 ELSE 1 END) from --Splice-properties joinOrder=FIXED\n" +
                        " foo left outer join foo2 --Splice-properties joinStrategy=%s, useSpark=%s\n" +
                        "on foo.col1 = foo2.col1 and foo.col1+foo2.col2>6", this.joinStrategy, this.useSparkString
        ));
        rs.next();
        Assert.assertEquals(String.format("Missing Records for %s", joinStrategy), 5, rs.getInt(1));
        Assert.assertEquals(String.format("Missing Records for %s", joinStrategy), 2, rs.getInt(2));
    }

    @Test
    public void testTrimJoinFunctionCall() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format(
                "select * from --Splice-properties joinOrder=FIXED\n" +
                        " a, b --Splice-properties joinStrategy=%s, useSpark=%s\n" +
                        "where rtrim(a.v) = b.v order by a.v", this.joinStrategy, this.useSparkString
        ));
        Assert.assertTrue("First Row Not Returned", rs.next());
        Assert.assertEquals("1", rs.getString(1));
        Assert.assertEquals("1", rs.getString(2));
        Assert.assertTrue("Second Row Not Returned", rs.next());
        Assert.assertEquals("3 ", rs.getString(1));
        Assert.assertEquals("3", rs.getString(2));
        Assert.assertFalse("Third Row Returned", rs.next());
    }

    @Test
    public void testJoinSortAvoidance() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format(
                "select * from --SPLICE-PROPERTIES joinOrder=FIXED\n" +
                        " b1" +
                        ", a1 --SPLICE-PROPERTIES joinStrategy=%s\n" +
                        "where a1.i=b1.i " +
                        "order by a1.i", this.joinStrategy
        ));
        int i = 0;
        while (rs.next()) {
            i++;
            Assert.assertEquals("not sorted correctly",i,rs.getInt(1));
        }
    }
}