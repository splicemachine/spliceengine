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

import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

/**
 * Created by yxia on 10/30/17.
 */
@Category(value = {SerialTest.class})
@RunWith(Parameterized.class)
public class AddColumnWithDefaultIT extends SpliceUnitTest {
    private static final String CLASS_NAME = AddColumnWithDefaultIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{"true"});
        params.add(new Object[]{"false"});
        return params;
    }

    private String useSparkString;

    public AddColumnWithDefaultIT(String useSparkString) {
        this.useSparkString = useSparkString;
    }

    @ClassRule
    public  static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(schemaWatcher.schemaName);

    @BeforeClass
    public static void createSharedTables() throws Exception {
        TestConnection connection=spliceClassWatcher.getOrCreateConnection();
        new TableCreator(connection)
                .withCreate("create table t1 (a1 int, b1 int, c1 int, primary key(a1))")
                .withIndex("create index idx_t1 on t1 (b1,c1)")
                .withInsert("insert into t1 values(?,?,?)")
                .withRows(rows(row(1, 1, 1),
                        row(2, 2, 2),
                        row(3, 3, 3),
                        row(4, 4, 4))).create();

        // add not null column to t1 with default value
        connection.createStatement().executeUpdate("alter table t1 add column d1 int not null default 999");

        new TableCreator(connection)
                .withCreate("create table t2 (a2 int, b2 int, c2 int)")
                .withInsert("insert into t2 values(?,?,?)")
                .withRows(rows(row(1, 1, 1),
                        row(2, 2, 2),
                        row(2, 2, 2),
                        row(4, 4, 4),
                        row(5, 5, 5),
                        row(5, 5, 5),
                        row(5, 5, 5))).create();

        // add not null column to t2 with default value
        connection.createStatement().executeUpdate("alter table t2 add column d2 int not null default 999");

        new TableCreator(connection)
                .withCreate("create table t3 (a3 int, b3 int, c3 int)")
                .withInsert("insert into t3 values(?,?,?)")
                .withRows(rows(row(1, 1, 1),
                        row(2, 2, 2),
                        row(4, 4, 4),
                        row(5, 5, 5))).create();

        new TableCreator(connection)
                .withCreate("create table t4 (a4 int)")
                .withInsert("insert into t4 values(?)")
                .withRows(rows(row(1),
                        row(2),
                        row(3))).create();
    }

    private Connection conn;

    @Before
    public void setUpTest() throws Exception{
        conn=methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
    }

    @After
    public void tearDownTest() throws Exception{
        try {
            conn.rollback();
        } catch (Exception e) {} // Swallow for HFile Bit Running in Control
    }

    @Test
    public void testSelectOnTableWithNewColumn() throws Exception {
        /* insert more rows to t1 */
        methodWatcher.executeUpdate(format("insert into t1 select a1+10, b1, c1, d1 from t1 --splice-properties useSpark=%s", useSparkString));

        /* Q1: select all columns */
        String sql = format("select * from t1 --splice-properties useSpark=%s", useSparkString);

        String expected = "A1 |B1 |C1 |D1  |\n" +
                "-----------------\n" +
                " 1 | 1 | 1 |999 |\n" +
                "11 | 1 | 1 |999 |\n" +
                "12 | 2 | 2 |999 |\n" +
                "13 | 3 | 3 |999 |\n" +
                "14 | 4 | 4 |999 |\n" +
                " 2 | 2 | 2 |999 |\n" +
                " 3 | 3 | 3 |999 |\n" +
                " 4 | 4 | 4 |999 |";
        ResultSet rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* Q2: select columns in different order and with predicate */
        sql = format("select d1, a1, b1 from t1 --splice-properties useSpark=%s\n where b1>3", useSparkString);
        expected = "D1  |A1 |B1 |\n" +
                "-------------\n" +
                "999 |14 | 4 |\n" +
                "999 | 4 | 4 |";
        rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* Update some rows with non-default values */
        methodWatcher.executeUpdate(format("update t1 --splice-properties useSpark=%s\n set d1=c1 where a1 between 3 and 12", useSparkString));

        /* Q3: check the result after update  */
        sql = format("select * from t1 --splice-properties useSpark=%s\n", useSparkString);
        expected = "A1 |B1 |C1 |D1  |\n" +
                "-----------------\n" +
                " 1 | 1 | 1 |999 |\n" +
                "11 | 1 | 1 | 1  |\n" +
                "12 | 2 | 2 | 2  |\n" +
                "13 | 3 | 3 |999 |\n" +
                "14 | 4 | 4 |999 |\n" +
                " 2 | 2 | 2 |999 |\n" +
                " 3 | 3 | 3 | 3  |\n" +
                " 4 | 4 | 4 | 4  |";
        rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* Q4-1: select with predicate on the column with default value */
        sql = format("select d1, a1, b1 from t1 --splice-properties useSpark=%s\n where d1=999", useSparkString);
        expected = "D1  |A1 |B1 |\n" +
                "-------------\n" +
                "999 | 1 | 1 |\n" +
                "999 |13 | 3 |\n" +
                "999 |14 | 4 |\n" +
                "999 | 2 | 2 |";
        rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* Q4-2: select with predicate on the column with default value */
        sql = format("select d1, a1, b1 from t1 --splice-properties useSpark=%s\n where d1 between 3 and 12", useSparkString);
        expected = "D1 |A1 |B1 |\n" +
                "------------\n" +
                " 3 | 3 | 3 |\n" +
                " 4 | 4 | 4 |";
        rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* Q5: test distinct scan */
        sql = format("select distinct a2, b2, c2, d2 from t2 --splice-properties useSpark=%s\n where a2<>1", useSparkString);
        expected = "A2 |B2 |C2 |D2  |\n" +
                "-----------------\n" +
                " 2 | 2 | 2 |999 |\n" +
                " 4 | 4 | 4 |999 |\n" +
                " 5 | 5 | 5 |999 |";
        rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* Q6-1: test query with index lookup */
        sql = format("select a1, b1, d1 from t1 --splice-properties index=idx_t1, useSpark=%s\n where b1=3", useSparkString);
        expected = "A1 |B1 |D1  |\n" +
                "-------------\n" +
                "13 | 3 |999 |\n" +
                " 3 | 3 | 3  |";
        rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* Q6-2: test query with index lookup */
        sql = format("select a1, b1, d1 from t1 --splice-properties index=idx_t1, useSpark=%s\n where d1=999", useSparkString);
        expected = "A1 |B1 |D1  |\n" +
                "-------------\n" +
                " 1 | 1 |999 |\n" +
                "13 | 3 |999 |\n" +
                "14 | 4 |999 |\n" +
                " 2 | 2 |999 |";
        rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* join */
        sql = format("select a1, b1, d1 from t1, t2 --splice-properties useSpark=%s\n where a1=a2 and d1 in (1,2,3,4)", useSparkString);
        expected = "A1 |B1 |D1 |\n" +
                "------------\n" +
                " 4 | 4 | 4 |";
        rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* aggregation */
        sql = format("select d1, count(*) from t1 --splice-properties useSpark=%s\n where b1<=3 group by 1", useSparkString);
        expected = "D1  | 2 |\n" +
                "---------\n" +
                " 1  | 1 |\n" +
                " 2  | 1 |\n" +
                " 3  | 1 |\n" +
                "999 | 3 |";
        rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* order by */
        sql = format("select d1, a1, b1 from t1 --splice-properties useSpark=%s\n where b1<=3 order by 1 desc, 2, 3", useSparkString);
        expected = "D1  |A1 |B1 |\n" +
                "-------------\n" +
                "999 | 1 | 1 |\n" +
                "999 | 2 | 2 |\n" +
                "999 |13 | 3 |\n" +
                " 3  | 3 | 3 |\n" +
                " 2  |12 | 2 |\n" +
                " 1  |11 | 1 |";
        rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }


    @Test
    public void testUpdate() throws Exception {
        //test update with condition on column with default
        methodWatcher.executeUpdate(format("update t1 --splice-properties useSpark=%s\n set d1=a1 where a1>=3", useSparkString));
        methodWatcher.executeUpdate(format("update t1 --splice-properties useSpark=%s\n set c1=c1+d1 where d1=999", useSparkString));

        /* Q1: select all columns */
        String sql = format("select * from t1 --splice-properties useSpark=%s", useSparkString);

        String expected = "A1 |B1 | C1  |D1  |\n" +
                "-------------------\n" +
                " 1 | 1 |1000 |999 |\n" +
                " 2 | 2 |1001 |999 |\n" +
                " 3 | 3 |  3  | 3  |\n" +
                " 4 | 4 |  4  | 4  |";
        ResultSet rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testDelete() throws Exception {
        //test update with condition on column with default
        methodWatcher.executeUpdate(format("update t1 --splice-properties useSpark=%s\n set d1=a1 where a1>=3", useSparkString));
        methodWatcher.executeUpdate(format("delete from t1 --splice-properties useSpark=%s\n where d1=999", useSparkString));

        /* Q1: select all columns */
        String sql = format("select * from t1 --splice-properties useSpark=%s", useSparkString);

        String expected = "A1 |B1 |C1 |D1 |\n" +
                "----------------\n" +
                " 3 | 3 | 3 | 3 |\n" +
                " 4 | 4 | 4 | 4 |";
        ResultSet rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testCreateIndex() throws Exception {
        /* insert more rows to t1 */
        methodWatcher.executeUpdate(format("insert into t1 select a1+10, b1, c1, c1 from t1 --splice-properties useSpark=%s", useSparkString));

        //create an index on d1
        methodWatcher.executeUpdate("create index idx2_t1 on t1(d1, b1)");

        /* Q1: select all columns using non-covering index */
        String sql = format("select d1, c1, b1, a1 from t1 --splice-properties index=idx2_t1, useSpark=%s", useSparkString);

        String expected = "D1  |C1 |B1 |A1 |\n" +
                "-----------------\n" +
                " 1  | 1 | 1 |11 |\n" +
                " 2  | 2 | 2 |12 |\n" +
                " 3  | 3 | 3 |13 |\n" +
                " 4  | 4 | 4 |14 |\n" +
                "999 | 1 | 1 | 1 |\n" +
                "999 | 2 | 2 | 2 |\n" +
                "999 | 3 | 3 | 3 |\n" +
                "999 | 4 | 4 | 4 |";
        ResultSet rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* Q2: select through covering index */
        sql = format("select d1, b1 from t1 --splice-properties index=idx2_t1, useSpark=%s\n where d1 < 555", useSparkString);

        expected = "D1 |B1 |\n" +
                "--------\n" +
                " 1 | 1 |\n" +
                " 2 | 2 |\n" +
                " 3 | 3 |\n" +
                " 4 | 4 |";
        rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* Q2-1: select through covering index */
        sql = format("select d1, b1 from t1 --splice-properties index=idx2_t1, useSpark=%s\n where d1 >= 555", useSparkString);

        expected = "D1  |B1 |\n" +
                "---------\n" +
                "999 | 1 |\n" +
                "999 | 2 |\n" +
                "999 | 3 |\n" +
                "999 | 4 |";
        rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        //create an index on (d1, c1) excluding default
        methodWatcher.executeUpdate("create index idx3_t1 on t1(d1, c1) exclude default keys");

        /* Q3: index excluding default case */
        sql = format("select d1, a1, b1 from t1 --splice-properties index=idx3_t1, useSpark=%s\n where d1 < 555", useSparkString);

        expected = "D1 |A1 |B1 |\n" +
                "------------\n" +
                " 1 |11 | 1 |\n" +
                " 2 |12 | 2 |\n" +
                " 3 |13 | 3 |\n" +
                " 4 |14 | 4 |";
        rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testSystemVariableAsDefaultValue() throws Exception {
        // add not null column with default to current user
        methodWatcher.executeUpdate("alter table t1 add column e1 varchar(30) not null default USER");

         /* insert more rows to t1 */
        methodWatcher.executeUpdate(format("insert into t1 select a1+10, b1, c1, d1, 'Alice' from t1 --splice-properties useSpark=%s", useSparkString));

        /* Q1 -- select */
        String sql = format("select * from t1 --splice-properties useSpark=%s\n where e1='Alice'", useSparkString);

        String expected = "A1 |B1 |C1 |D1  | E1   |\n" +
                "------------------------\n" +
                "11 | 1 | 1 |999 |Alice |\n" +
                "12 | 2 | 2 |999 |Alice |\n" +
                "13 | 3 | 3 |999 |Alice |\n" +
                "14 | 4 | 4 |999 |Alice |";
        ResultSet rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* Q2 */
        sql = format("select * from t1 --splice-properties useSpark=%s\n where e1=USER", useSparkString);

        expected = "A1 |B1 |C1 |D1  |  E1   |\n" +
                "-------------------------\n" +
                " 1 | 1 | 1 |999 |SPLICE |\n" +
                " 2 | 2 | 2 |999 |SPLICE |\n" +
                " 3 | 3 | 3 |999 |SPLICE |\n" +
                " 4 | 4 | 4 |999 |SPLICE |";
        rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testConstantExpressionAsDefaultValue() throws Exception {
        // add not null column with default to current user
        methodWatcher.executeUpdate("alter table t1 add column e1 date not null default DATE('2017-11-06')");

         /* insert more rows to t1 */
        methodWatcher.executeUpdate(format("insert into t1 select a1+10, b1, c1, d1, e1-1 from t1 --splice-properties useSpark=%s", useSparkString));

        /* Q1 -- select */
        String sql = format("select * from t1 --splice-properties useSpark=%s\n where e1=DATE ('2017-11-05')", useSparkString);

        String expected = "A1 |B1 |C1 |D1  |    E1     |\n" +
                "-----------------------------\n" +
                "11 | 1 | 1 |999 |2017-11-05 |\n" +
                "12 | 2 | 2 |999 |2017-11-05 |\n" +
                "13 | 3 | 3 |999 |2017-11-05 |\n" +
                "14 | 4 | 4 |999 |2017-11-05 |";
        ResultSet rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* Q2 */
        sql = format("select * from t1 --splice-properties useSpark=%s\n where e1<>DATE ('2017-11-05')", useSparkString);

        expected = "A1 |B1 |C1 |D1  |    E1     |\n" +
                "-----------------------------\n" +
                " 1 | 1 | 1 |999 |2017-11-06 |\n" +
                " 2 | 2 | 2 |999 |2017-11-06 |\n" +
                " 3 | 3 | 3 |999 |2017-11-06 |\n" +
                " 4 | 4 | 4 |999 |2017-11-06 |";
        rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testAlterColumnDefaultInteraction() throws Exception {
        // alter column change default value
        methodWatcher.executeUpdate("alter table t1 alter column d1 default 888");

        // check the content
        String sql = format("select * from t1 --splice-properties useSpark=%s", useSparkString);

        String expected = "A1 |B1 |C1 |D1  |\n" +
                "-----------------\n" +
                " 1 | 1 | 1 |999 |\n" +
                " 2 | 2 | 2 |999 |\n" +
                " 3 | 3 | 3 |999 |\n" +
                " 4 | 4 | 4 |999 |";
        ResultSet rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

         /* insert more rows to t1, d1 now should take the default value 888 */
        methodWatcher.executeUpdate(format("insert into t1(a1,b1,c1) select a1+10, b1, c1 from t1 --splice-properties useSpark=%s", useSparkString));

        /* check the content  */
        sql = format("select * from t1 --splice-properties useSpark=%s", useSparkString);

        expected = "A1 |B1 |C1 |D1  |\n" +
                "-----------------\n" +
                " 1 | 1 | 1 |999 |\n" +
                "11 | 1 | 1 |888 |\n" +
                "12 | 2 | 2 |888 |\n" +
                "13 | 3 | 3 |888 |\n" +
                "14 | 4 | 4 |888 |\n" +
                " 2 | 2 | 2 |999 |\n" +
                " 3 | 3 | 3 |999 |\n" +
                " 4 | 4 | 4 |999 |";
        rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* check content with predicate */
        sql = format("select * from t1 --splice-properties useSpark=%s\n where d1<>888", useSparkString);

        expected = "A1 |B1 |C1 |D1  |\n" +
                "-----------------\n" +
                " 1 | 1 | 1 |999 |\n" +
                " 2 | 2 | 2 |999 |\n" +
                " 3 | 3 | 3 |999 |\n" +
                " 4 | 4 | 4 |999 |";
        rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testDropColumnDefaultInteraction() throws Exception {
        // alter column change default value
        methodWatcher.executeUpdate("alter table t1 alter column d1 drop default");

        // check the content
        String sql = format("select * from t1 --splice-properties useSpark=%s", useSparkString);

        String expected = "A1 |B1 |C1 |D1  |\n" +
                "-----------------\n" +
                " 1 | 1 | 1 |999 |\n" +
                " 2 | 2 | 2 |999 |\n" +
                " 3 | 3 | 3 |999 |\n" +
                " 4 | 4 | 4 |999 |";
        ResultSet rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

         /* insert more rows to t1, we need to explicitly provide value for d1 as no default is specified now */
        methodWatcher.executeUpdate(format("insert into t1(a1,b1,c1,d1) select a1+10, b1, c1, c1 from t1 --splice-properties useSpark=%s", useSparkString));

        /* check the content  */
        sql = format("select * from t1 --splice-properties useSpark=%s", useSparkString);

        expected = "A1 |B1 |C1 |D1  |\n" +
                "-----------------\n" +
                " 1 | 1 | 1 |999 |\n" +
                "11 | 1 | 1 | 1  |\n" +
                "12 | 2 | 2 | 2  |\n" +
                "13 | 3 | 3 | 3  |\n" +
                "14 | 4 | 4 | 4  |\n" +
                " 2 | 2 | 2 |999 |\n" +
                " 3 | 3 | 3 |999 |\n" +
                " 4 | 4 | 4 |999 |";
        rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* check content with predicate */
        sql = format("select * from t1 --splice-properties useSpark=%s\n where d1>500", useSparkString);

        expected = "A1 |B1 |C1 |D1  |\n" +
                "-----------------\n" +
                " 1 | 1 | 1 |999 |\n" +
                " 2 | 2 | 2 |999 |\n" +
                " 3 | 3 | 3 |999 |\n" +
                " 4 | 4 | 4 |999 |";
        rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testModifyColumnAsNullableInteraction() throws Exception {
        // alter column change default value
        methodWatcher.executeUpdate("alter table t1 alter column d1 null");

        // check the content
        String sql = format("select * from t1 --splice-properties useSpark=%s", useSparkString);

        String expected = "A1 |B1 |C1 |D1  |\n" +
                "-----------------\n" +
                " 1 | 1 | 1 |999 |\n" +
                " 2 | 2 | 2 |999 |\n" +
                " 3 | 3 | 3 |999 |\n" +
                " 4 | 4 | 4 |999 |";
        ResultSet rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

         /* insert more rows to t1 */
        methodWatcher.executeUpdate(format("insert into t1(a1,b1,c1) select a1+10, b1, c1 from t1 --splice-properties useSpark=%s", useSparkString));

        /* check the content  */
        sql = format("select * from t1 --splice-properties useSpark=%s", useSparkString);

        expected = "A1 |B1 |C1 |D1  |\n" +
                "-----------------\n" +
                " 1 | 1 | 1 |999 |\n" +
                "11 | 1 | 1 |999 |\n" +
                "12 | 2 | 2 |999 |\n" +
                "13 | 3 | 3 |999 |\n" +
                "14 | 4 | 4 |999 |\n" +
                " 2 | 2 | 2 |999 |\n" +
                " 3 | 3 | 3 |999 |\n" +
                " 4 | 4 | 4 |999 |";
        rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* check content with predicate */
        sql = format("select * from t1 --splice-properties useSpark=%s\n where d1>500", useSparkString);

        expected = "A1 |B1 |C1 |D1  |\n" +
                "-----------------\n" +
                " 1 | 1 | 1 |999 |\n" +
                "11 | 1 | 1 |999 |\n" +
                "12 | 2 | 2 |999 |\n" +
                "13 | 3 | 3 |999 |\n" +
                "14 | 4 | 4 |999 |\n" +
                " 2 | 2 | 2 |999 |\n" +
                " 3 | 3 | 3 |999 |\n" +
                " 4 | 4 | 4 |999 |";
        rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testColumnsOfDateTime() throws Exception {
        // add not null column to t3 with default value
        methodWatcher.executeUpdate("alter table t3 add column d3 DATE not null default '2017-01-01'");
        methodWatcher.executeUpdate("alter table t3 add column e3 TIME not null default '00:00:00'");
        methodWatcher.executeUpdate("alter table t3 add column f3 TIMESTAMP not null default '2017-01-01 00:00:00'");

        methodWatcher.executeUpdate("create index idx1_t3 on t3 (b3, d3)");
        methodWatcher.executeUpdate("insert into t3 values (10, 10, 10, '2017-12-05', '17:11:24', '2017-12-05 17:11:24')");

        /* case 1: check partial covering index */
        String sql = format("select a3, d3, YEAR(d3), e3, f3, f3-1 from t3 --splice-properties index=idx1_t3, useSpark=%s\n where b3=10", useSparkString);
        String expected = "A3 |    D3     |  3  |   E3    |         F3           |          6           |\n" +
                "------------------------------------------------------------------------------\n" +
                "10 |2017-12-05 |2017 |17:11:24 |2017-12-05 17:11:24.0 |2017-12-04 17:11:24.0 |";
        ResultSet rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        sql = format("select a3, d3, YEAR(d3), e3, f3, f3-1 from t3 --splice-properties index=idx1_t3, useSpark=%s\n where b3=5", useSparkString);
        expected = "A3 |    D3     |  3  |   E3    |         F3           |          6           |\n" +
                "------------------------------------------------------------------------------\n" +
                " 5 |2017-01-01 |2017 |00:00:00 |2017-01-01 00:00:00.0 |2016-12-31 00:00:00.0 |";
        rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* case 2: check covering index */
        sql = format("select b3, d3, YEAR(d3) from t3 --splice-properties index=idx1_t3, useSpark=%s", useSparkString);
        expected = "B3 |    D3     |  3  |\n" +
                "----------------------\n" +
                " 1 |2017-01-01 |2017 |\n" +
                "10 |2017-12-05 |2017 |\n" +
                " 2 |2017-01-01 |2017 |\n" +
                " 4 |2017-01-01 |2017 |\n" +
                " 5 |2017-01-01 |2017 |";
        rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* case 3: check direct access of the base table */
        sql = format("select a3, d3, YEAR(d3), e3, f3, f3-1 from t3 --splice-properties index=null, useSpark=%s", useSparkString);
        expected = "A3 |    D3     |  3  |   E3    |         F3           |          6           |\n" +
                "------------------------------------------------------------------------------\n" +
                " 1 |2017-01-01 |2017 |00:00:00 |2017-01-01 00:00:00.0 |2016-12-31 00:00:00.0 |\n" +
                "10 |2017-12-05 |2017 |17:11:24 |2017-12-05 17:11:24.0 |2017-12-04 17:11:24.0 |\n" +
                " 2 |2017-01-01 |2017 |00:00:00 |2017-01-01 00:00:00.0 |2016-12-31 00:00:00.0 |\n" +
                " 4 |2017-01-01 |2017 |00:00:00 |2017-01-01 00:00:00.0 |2016-12-31 00:00:00.0 |\n" +
                " 5 |2017-01-01 |2017 |00:00:00 |2017-01-01 00:00:00.0 |2016-12-31 00:00:00.0 |";
        rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testColumnsOfCharType() throws Exception {
        // add not null column to t3 with default value
        methodWatcher.executeUpdate("alter table t4 add column b4 VARCHAR(4) not null default 'Z'");
        methodWatcher.executeUpdate("alter table t4 add column c4 CHAR(4) not null default 'ZZZZ'");
        methodWatcher.executeUpdate("alter table t4 add column d4 LONG VARCHAR not null default 'LZZZZ'");

        methodWatcher.executeUpdate("create index idx1_t4 on t4 (b4, c4)");
        methodWatcher.executeUpdate("insert into t4 values (10, 'AA', 'AAAA', 'LAAAA')");

        /* case 1: check partial covering index */
        String sql = format("select a4, b4 || '-' || c4 || '-' || D4 from t4 --splice-properties index=idx1_t4, useSpark=%s\n where a4=10", useSparkString);
        String expected = "A4 |      2       |\n" +
                "-------------------\n" +
                "10 |AA-AAAA-LAAAA |";
        ResultSet rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        sql = format("select a4, b4 || '-' || c4 || '-' || D4 from t4 --splice-properties index=idx1_t4, useSpark=%s\n where a4=3", useSparkString);
        expected = "A4 |      2      |\n" +
                "------------------\n" +
                " 3 |Z-ZZZZ-LZZZZ |";
        rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* case 2: check direct access of the base table */
        sql = format("select a4, b4 || '-' || c4 || '-' || D4 from t4 --splice-properties index=null, useSpark=%s", useSparkString);
        expected = "A4 |      2       |\n" +
                "-------------------\n" +
                " 1 |Z-ZZZZ-LZZZZ  |\n" +
                "10 |AA-AAAA-LAAAA |\n" +
                " 2 |Z-ZZZZ-LZZZZ  |\n" +
                " 3 |Z-ZZZZ-LZZZZ  |";
        rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testColumnsOfCharTypeWithDefaultOfSmallerSize() throws Exception {
        // add not null column to t4 with default value
        methodWatcher.executeUpdate("alter table t4 add column b4 CHAR(6) not null default 'ZZZZ'");

        methodWatcher.executeUpdate("create index idx1_t4 on t4 (b4)");
        methodWatcher.executeUpdate("insert into t4 values (10, 'AAAA')");

        /* case 1: check partial covering index */
        String sql = format("select a4, '-' || b4 || '-' from t4 --splice-properties index=idx1_t4, useSpark=%s\n where a4=10", useSparkString);
        String expected = "A4 |    2    |\n" +
                "--------------\n" +
                "10 |-AAAA  - |";
        ResultSet rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        sql = format("select a4, '-' || b4 || '-'  from t4 --splice-properties index=idx1_t4, useSpark=%s\n where a4=3", useSparkString);
        expected = "A4 |    2    |\n" +
                "--------------\n" +
                " 3 |-ZZZZ  - |";
        rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* case 2: check direct access of the base table */
        sql = format("select a4, '-' || b4 || '-' from t4 --splice-properties index=null, useSpark=%s", useSparkString);
        expected = "A4 |    2    |\n" +
                "--------------\n" +
                " 1 |-ZZZZ  - |\n" +
                "10 |-AAAA  - |\n" +
                " 2 |-ZZZZ  - |\n" +
                " 3 |-ZZZZ  - |";
        rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testColumnsOfCharTypeWithDefaultOfLargerSize() throws Exception {
        // case 1: add not null column to t4 with default value of bigger size, exepct to error out
        try {
            methodWatcher.executeUpdate("alter table t4 add column b4 CHAR(4) not null default 'ZZZZZ'");
            Assert.fail("the update statement should fail");
        } catch (SQLException e) {
            Assert.assertEquals("Upexpected failure: "+ e.getMessage(), e.getSQLState(), SQLState.LANG_STRING_TRUNCATION);
        }

        // case 2: add not null column to t4 with default value of bigger size, but after trimming the trailing space,
        // the value can fit in the column, for this case, we don't want to error out
        methodWatcher.executeUpdate("alter table t4 add column b4 CHAR(4) not null default 'ZZZZ    '");

        methodWatcher.executeUpdate("insert into t4 values (10, 'AAAA')");
        String sql = format("select a4, '-' || b4 || '-' from t4 --splice-properties index=null, useSpark=%s", useSparkString);
        String expected = "A4 |   2   |\n" +
                "------------\n" +
                " 1 |-ZZZZ- |\n" +
                "10 |-AAAA- |\n" +
                " 2 |-ZZZZ- |\n" +
                " 3 |-ZZZZ- |";
        ResultSet rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }


    @Test
    public void testAlterCharColumnWithDefaultOfNonmatchingSize() throws Exception {
        methodWatcher.executeUpdate("alter table t4 add column b4 CHAR(6) not null default 'ZZZZ'");
        methodWatcher.executeUpdate("insert into t4 values (10, 'AAAA')");
        // change to a default value which is of smaller size than the definition, it should go through padding spaces
        methodWatcher.executeUpdate("alter table t4 alter column b4 default 'YYYY' ");
        methodWatcher.executeUpdate("insert into t4(a4) values (11)");

        // change to a default value which is of larger size but after trimming trailing spaces, it can fit, it should
        //go through
        methodWatcher.executeUpdate("alter table t4 alter column b4 default 'XXXX   ' ");
        methodWatcher.executeUpdate("insert into t4(a4) values (12)");

        String sql = format("select a4, '-' || b4 || '-' from t4 --splice-properties index=null, useSpark=%s", useSparkString);
        String expected = "A4 |    2    |\n" +
                "--------------\n" +
                " 1 |-ZZZZ  - |\n" +
                "10 |-AAAA  - |\n" +
                "11 |-YYYY  - |\n" +
                "12 |-XXXX  - |\n" +
                " 2 |-ZZZZ  - |\n" +
                " 3 |-ZZZZ  - |";
        ResultSet rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // change to a default value which is of larger size with no trailing spaces, it should error out
        try {
            methodWatcher.executeUpdate("alter table t4 alter column b4 default 'XXXXXXX'");
            Assert.fail("the update statement should fail");
        } catch (SQLException e) {
            Assert.assertEquals("Upexpected failure: "+ e.getMessage(), e.getSQLState(), SQLState.LANG_STRING_TRUNCATION);
        }

    }

    @Test
    public void testColumnsOfNumericType() throws Exception {
        // add not null column to t3 with default value
        methodWatcher.executeUpdate("alter table t4 add column b4 smallint not null default -32768");
        methodWatcher.executeUpdate("alter table t4 add column c4 bigint not null default -9223372036854775808");
        methodWatcher.executeUpdate("alter table t4 add column d4 real not null default -1.00");
        methodWatcher.executeUpdate("alter table t4 add column e4 double not null default -2.00");
        methodWatcher.executeUpdate("alter table t4 add column f4 decimal(5,2)  not null default -3.33");

        methodWatcher.executeUpdate("create index idx1_t4 on t4 (b4, c4)");
        methodWatcher.executeUpdate("insert into t4 values (10, 10, 1000000, 10.11, 10.22, 10.33)");

        /* case 1: check partial covering index */
        String sql = format("select a4, b4+1, c4+1, d4+1, e4+1, f4+1 from t4 --splice-properties index=idx1_t4, useSpark=%s\n where a4=10", useSparkString);
        String expected = "A4 | 2 |   3    |  4   |  5   |  6   |\n" +
                "--------------------------------------\n" +
                "10 |11 |1000001 |11.11 |11.22 |11.33 |";
        ResultSet rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        sql = format("select a4, b4+1, c4+1, d4+1, e4+1, f4+1 from t4 --splice-properties index=idx1_t4, useSpark=%s\n where a4=3", useSparkString);
        expected = "A4 |   2   |          3          | 4  |  5  |  6   |\n" +
                "----------------------------------------------------\n" +
                " 3 |-32767 |-9223372036854775807 |0.0 |-1.0 |-2.33 |";
        rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* case 2: check direct access of the base table */
        sql = format("select a4, b4+1, c4+1, d4+1, e4+1, f4+1 from t4 --splice-properties index=null, useSpark=%s", useSparkString);
        expected = "A4 |   2   |          3          |  4   |  5   |  6   |\n" +
                "-------------------------------------------------------\n" +
                " 1 |-32767 |-9223372036854775807 | 0.0  |-1.0  |-2.33 |\n" +
                "10 |  11   |       1000001       |11.11 |11.22 |11.33 |\n" +
                " 2 |-32767 |-9223372036854775807 | 0.0  |-1.0  |-2.33 |\n" +
                " 3 |-32767 |-9223372036854775807 | 0.0  |-1.0  |-2.33 |";
        rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testColumnsOfBoolean() throws Exception {
        // add not null column to t3 with default value
        methodWatcher.executeUpdate("alter table t4 add column b4 boolean not null default false");

        methodWatcher.executeUpdate("create index idx1_t4 on t4 (b4)");
        methodWatcher.executeUpdate("insert into t4 values (10, true)");

        /* case 1: check partial covering index */
        String sql = format("select a4, b4 from t4 --splice-properties index=idx1_t4, useSpark=%s\n where a4=10", useSparkString);
        String expected = "A4 | B4  |\n" +
                "----------\n" +
                "10 |true |";
        ResultSet rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        sql = format("select a4, b4 from t4 --splice-properties index=idx1_t4, useSpark=%s\n where a4=3", useSparkString);
        expected = "A4 | B4   |\n" +
                "-----------\n" +
                " 3 |false |";
        rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* case 2: check direct access of the base table */
        sql = format("select a4, b4 from t4 --splice-properties index=null, useSpark=%s", useSparkString);
        expected = "A4 | B4   |\n" +
                "-----------\n" +
                " 1 |false |\n" +
                "10 |true  |\n" +
                " 2 |false |\n" +
                " 3 |false |";
        rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testColumnsOfBlob() throws Exception {
        // add not null column to t4 with default value
        methodWatcher.executeUpdate("alter table t4 add column b4 blob(10) not null default cast(X'AB' as blob(10))");
        String sql = format("select b4 from t4 --splice-properties useSpark=%s", useSparkString);
        ResultSet rs = methodWatcher.executeQuery(sql);
        while (rs.next()) {
            Blob blob = rs.getBlob(1);
            byte[] expected = new byte[] {(byte)0xAB};
            Assert.assertArrayEquals(expected, blob.getBytes(1, 10));
        }
        assertEquals("", TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        try {
            methodWatcher.executeUpdate("alter table t4 add column c4 blob(1) not null default cast(X'ABCD' as blob(2))");
            Assert.fail("the update statement should fail");
        } catch (SQLException e) {
            Assert.assertEquals("Upexpected failure: "+ e.getMessage(), e.getSQLState(), SQLState.LANG_STRING_TRUNCATION);
        }
    }
}
