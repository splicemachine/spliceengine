/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
import com.splicemachine.homeless.TestUtils;
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
 * Created by yxia on 3/26/19.
 */
@RunWith(Parameterized.class)
public class RecursiveWithStatementIT extends SpliceUnitTest {
    private static final String SCHEMA = RecursiveWithStatementIT.class.getSimpleName().toUpperCase();
    private static SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{"false"});
        params.add(new Object[]{"true"});
        return params;
    }

    private String useSparkString;

    public RecursiveWithStatementIT(String useSparkString) {
        this.useSparkString = useSparkString;
    }

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createSharedTables() throws Exception {
        TestConnection connection = spliceClassWatcher.getOrCreateConnection();

        /* t1 represent a tree of the following :
                              1
                              |
                         |---------|
                        2          3
                        |           |
                   |----------|  |------------|
                   4          5  6            7
                              |               |
                              8               9
         */
        new TableCreator(connection)
                .withCreate("create table t1 (a1 int, b1 int)")
                .withInsert("insert into t1 values(?,?)")
                .withRows(rows(row(1, 2),
                        row(1, 3),
                        row(2, 4),
                        row(2, 5),
                        row(3, 6),
                        row(3, 7),
                        row(5, 8),
                        row(7, 9))).create();

        new TableCreator(connection)
                .withCreate("create table t2 (a2 int, name varchar(10))")
                .withInsert("insert into t2 values(?,?)")
                .withRows(rows(row(1, "A"),
                               row(2, "B"),
                               row(3, "C"),
                               row(4, "D"),
                               row(5, "E"),
                               row(6, "F"),
                               row(7, "G"),
                               row(8, "H"),
                               row(9,"I"))).create();

        new TableCreator(connection)
                .withCreate("create table t3 (a3 int, b3 int)")
                .withInsert("insert into t3 values(?,?)")
                .withRows(rows(row(1, 2),
                        row(1, 3),
                        row(2, 4),
                        row(2, 5),
                        row(3, 6),
                        row(3, 7),
                        row(5, 8),
                        row(7, 9))).create();
    }

    @Test
    public void testRecursiveWithWithJoin() throws Exception {
        String sqlText = format("with recursive dt as (" +
                "select distinct a1, name, 1 as level from t1 --splice-properties useSpark=%s\n" +
                ", t2 where a1=a2 and a1=1 " +
                "UNION ALL \n " +
                "select t1.b1 as a1, t2.name, level+1 as level from dt, t1, t2 where dt.a1=t1.a1 and t1.b1 = t2.a2 and dt.level < 10)\n" +
                "select * from dt order by a1", this.useSparkString);

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected = "A1 |NAME | LEVEL |\n" +
                "------------------\n" +
                " 1 |  A  |   1   |\n" +
                " 2 |  B  |   2   |\n" +
                " 3 |  C  |   2   |\n" +
                " 4 |  D  |   3   |\n" +
                " 5 |  E  |   3   |\n" +
                " 6 |  F  |   3   |\n" +
                " 7 |  G  |   3   |\n" +
                " 8 |  H  |   4   |\n" +
                " 9 |  I  |   4   |";

        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRecursiveWithWithoutJoin() throws Exception {

    }

    @Test
    public void testRecursiveWithWhereSeedHasASetOperation() throws Exception {

    }

    @Test
    public void testRecursiveWithWithAlias() throws Exception {

    }

    @Test
    public void testRecursiveWithWithDifferentJoinStrategy() throws Exception {

    }

    @Test
    public void testMultipleRecursiveWith() throws Exception {

    }

    @Test
    public void testNestedRecursiveWith() throws Exception {

    }

    @Test
    public void testMultipleSelfReferencesInRecursionBody() throws Exception {

    }

    @Test
    public void negativeTestWithNoRecursion() throws Exception {

    }
}
