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

package com.splicemachine.derby.impl.sql.execute.operations.joins;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import com.splicemachine.test_tools.TableCreator;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

/**
 * Integ test for Bug 887 and Bug 976.
 *
 * @author Jeff Cunningham
 *         Date: 11/19/13
 */
public class JoinIT extends SpliceUnitTest {
    public static final String CLASS_NAME = JoinIT.class.getSimpleName();

    protected static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);

    protected static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    protected static final SpliceTableWatcher A_TABLE = new SpliceTableWatcher("A",schemaWatcher.schemaName,
            "(a1 int not null primary key, a2 int, a3 int, a4 int, a5 int, a6 int)");
    protected static final SpliceTableWatcher B_TABLE = new SpliceTableWatcher("B",schemaWatcher.schemaName,
            "(b1 int not null primary key, b2 int, b3 int, b4 int, b5 int, b6 int)");
    protected static final SpliceTableWatcher C_TABLE = new SpliceTableWatcher("C",schemaWatcher.schemaName,
            "(c1 int not null, c2 int, c3 int not null, c4 int, c5 int, c6 int)");
    protected static final SpliceTableWatcher D_TABLE = new SpliceTableWatcher("D",schemaWatcher.schemaName,
            "(d1 int not null, d2 int, d3 int not null, d4 int, d5 int, d6 int)");
    protected static final SpliceTableWatcher B_PRIME_TABLE = new SpliceTableWatcher("B_PRIME",schemaWatcher.schemaName,
            "(b1 int, b2 int, b3 int, b4 int, b5 int, b6 int, c1 int not null, c2 int, c3 int not null, c4 int, c5 int, c6 int)");
    protected static final SpliceTableWatcher T1_TABLE = new SpliceTableWatcher("T1",schemaWatcher.schemaName,
            "(c1 char(10), c2 char(19), c3 char(14), c4 char(1), c5 char(4), c6 char(6), c7 char(11), primary key (c1, c2, c3))");
    protected static final SpliceTableWatcher T2_TABLE = new SpliceTableWatcher("T2",schemaWatcher.schemaName,
            "(c1 char(1) primary key)");
    protected static final SpliceTableWatcher T3_TABLE = new SpliceTableWatcher("T3",schemaWatcher.schemaName,
            "(c1 char(6) primary key)");
    protected static final SpliceTableWatcher T4_TABLE = new SpliceTableWatcher("T4",schemaWatcher.schemaName,
            "(c1 numeric(10,0) primary key)");
    protected static final SpliceTableWatcher T5_TABLE = new SpliceTableWatcher("T5",schemaWatcher.schemaName,
            "(c1 char(3) primary key)");
    protected static final SpliceTableWatcher T6_TABLE = new SpliceTableWatcher("T6",schemaWatcher.schemaName,
            "(c1 char(19), c2 char(3), c3 char(3), c4 varchar(40))");
    protected static final SpliceTableWatcher T7_TABLE = new SpliceTableWatcher("T7",schemaWatcher.schemaName,
            "(c1 numeric(10, 3))");
    protected static final SpliceTableWatcher T8_TABLE = new SpliceTableWatcher("T8",schemaWatcher.schemaName,
            "(c1 char(3))");

    private static final String A_VALS =
            "INSERT INTO A VALUES (1,1,3,6,NULL,2),(2,3,2,4,2,2),(3,4,2,NULL,NULL,NULL),(4,NULL,4,2,5,2),(5,2,3,5,7,4),(7,1,4,2,3,4),(8,8,8,8,8,8),(6,7,3,2,3,4)";
    private static final String B_VALS =
            "INSERT INTO B VALUES (6,7,2,3,NULL,1),(4,5,9,6,3,2),(1,4,2,NULL,NULL,NULL),(5,NULL,2,2,5,2),(3,2,3,3,1,4),(7,3,3,3,3,3),(9,3,3,3,3,3)";
    private static final String C_VALS =
            "INSERT INTO C VALUES (3,7,7,3,NULL,1),(8,3,9,1,3,2),(1,4,1,NULL,NULL,NULL),(3,NULL,1,2,4,2),(2,2,5,3,2,4),(1,7,2,3,1,1),(3,8,4,2,4,6)";
    private static final String D_VALS =
            "INSERT INTO D VALUES (1,7,2,3,NULL,3),(2,3,9,1,1,2),(2,2,2,NULL,3,2),(1,NULL,3,2,2,1),(2,2,5,3,2,3),(2,5,6,3,7,2)";
    private static final String B_PRIME_VALS =
            "INSERT INTO B_PRIME VALUES (6,7,2,3,NULL,1,3,7,7,3,NULL,1),(4,5,9,6,3,2,8,3,9,1,3,2),(1,4,2,NULL,NULL,NULL,1,4,1,NULL,NULL,NULL),"+
                    "(5,NULL,2,2,5,2,3,NULL,1,2,4,2),(3,2,3,3,1,4,2,2,5,3,2,4),(7,3,3,3,3,3,1,7,2,3,1,1),(9,3,3,3,3,3,3,8,4,2,4,6)";
    private static final String T1_VALS =
            "INSERT INTO T1 VALUES ('2016-03-17','4271782685317057','C11C0C23352586','0','1234','123456','           ')";
    private static final String T2_VALS = "INSERT INTO T2 VALUES ('0')";
    private static final String T3_VALS = "INSERT INTO T3 VALUES ('123456')";
    private static final String T4_VALS = "INSERT INTO T4 VALUES (123456)";
    private static final String T5_VALS = "INSERT INTO T5 VALUES ('   ')";
    private static final String T6_VALS = "INSERT INTO T6 (c1, c2, c3) VALUES ('123456789', 'Y', 'Y')";

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(schemaWatcher)
            .around(A_TABLE)
            .around(B_TABLE)
            .around(C_TABLE)
            .around(D_TABLE)
            .around(B_PRIME_TABLE)
            .around(T1_TABLE)
            .around(T2_TABLE)
            .around(T3_TABLE)
            .around(T4_TABLE)
            .around(T5_TABLE)
            .around(T6_TABLE)
            .around(T7_TABLE)
            .around(T8_TABLE)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        spliceClassWatcher.getStatement().executeUpdate(A_VALS);
                        spliceClassWatcher.getStatement().executeUpdate(B_VALS);
                        spliceClassWatcher.getStatement().executeUpdate(C_VALS);
                        spliceClassWatcher.getStatement().executeUpdate(D_VALS);
                        spliceClassWatcher.getStatement().executeUpdate(B_PRIME_VALS);
                        spliceClassWatcher.getStatement().executeUpdate(T1_VALS);
                        spliceClassWatcher.getStatement().executeUpdate(T2_VALS);
                        spliceClassWatcher.getStatement().executeUpdate(T3_VALS);
                        spliceClassWatcher.getStatement().executeUpdate(T4_VALS);
                        spliceClassWatcher.getStatement().executeUpdate(T5_VALS);
                        spliceClassWatcher.getStatement().executeUpdate(T6_VALS);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        spliceClassWatcher.closeAll();
                    }
                }
            }).around(new SpliceDataWatcher(){
                @Override
                protected void starting(Description description){
                   try(CallableStatement cs = spliceClassWatcher.prepareCall("call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS(?,false)")){
                       cs.setString(1,schemaWatcher.schemaName);
                       cs.execute();
                   }catch(Exception e){
                       throw new RuntimeException(e);
                   }
                }
            });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    /**
     * Tests bug 887 - NPE and ConcurrentModificationException
     *
     * @throws Exception fail
     */
    @Test
    public void testBug887() throws Exception {
        String query = format("select a1,b1,c1,c3,d1,d3 from %s.D join (%s.A left outer join (%s.B join %s.C on b2=c2) on a1=b1) on d3=b3 and d1=a2",
                CLASS_NAME, CLASS_NAME, CLASS_NAME, CLASS_NAME);

        ResultSet rs = methodWatcher.executeQuery(query);
        int nRows = resultSetSize(rs);
        Assert.assertEquals("Expecting 2 rows from join.", 2, nRows);
    }

    /**
     * Tests bug 887 - NPE and ConcurrentModificationException
     *
     * @throws Exception fail
     */
    @Test
    public void testJoinBug887() throws Exception {
        String query = format("select * from %s.B join %s.C on b2=c2",
                CLASS_NAME, CLASS_NAME, CLASS_NAME, CLASS_NAME);

        ResultSet rs = methodWatcher.executeQuery(query);
//        int nRows = TestUtils.printResult(query, rs, System.out);
        int nRows = resultSetSize(rs);
        Assert.assertEquals("Expecting 6 rows from join.", 6, nRows);
    }

    /**
     * Bug 976 (the original bug)
     * Getting one less row from splice.
     *
     * @throws Exception fail
     */
    @Test
    public void testAllJoinBug976Compare() throws Exception {

        String query = format("select a1,b1,c1,c3,d1,d3 from %s.D join (%s.A left outer join (%s.B join %s.C on b2=c2) on a1=b1) on d3=b3 and d1=a2",
                CLASS_NAME, CLASS_NAME, CLASS_NAME, CLASS_NAME);

        String expectedColumns = "A1 B1 C1 C3 D1 D3";
        List<String> expectedRows = Arrays.asList(
                "7 7 8 9 1 3",
                "1 1 1 1 1 2");
        ResultSet rs = methodWatcher.executeQuery(query);
        TestUtils.FormattedResult expected = TestUtils.FormattedResult.ResultFactory.convert(query, expectedColumns, expectedRows, "\\s+");
        TestUtils.FormattedResult actual = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
        Assert.assertEquals("Actual results didn't match expected.", expected.toString(), actual.toString());
    }

    /**
     * Bug 976
     * Getting different results from splice for this Left Outer nested join.
     *
     * @throws Exception fail
     */
    @Test
    public void testLeftOuterJoinBug976Compare() throws Exception {
        String query = "select * from JoinIT.A left outer join (JoinIT.B join JoinIT.C on b2=c2) on a1=b1";

        String expectedColumns = "A1 A2 A3 A4 A5 A6 B1 B2 B3 B4 B5 B6 C1 C2 C3 C4 C5 C6";
        List<String> expectedRows = Arrays.asList(
                "3 4 2 (null) (null) (null) 3 2 3 3 1 4 2 2 5 3 2 4",
                "7 1 4 2 3 4 7 3 3 3 3 3 8 3 9 1 3 2",
                "1 1 3 6 (null) 2 1 4 2 (null) (null) (null) 1 4 1 (null) (null) (null)",
                "6 7 3 2 3 4 6 7 2 3 (null) 1 3 7 7 3 (null) 1",
                "6 7 3 2 3 4 6 7 2 3 (null) 1 1 7 2 3 1 1",
                "2 3 2 4 2 2 (null) (null) (null) (null) (null) (null) (null) (null) (null) (null) (null) (null)",
                "5 2 3 5 7 4 (null) (null) (null) (null) (null) (null) (null) (null) (null) (null) (null) (null)",
                "8 8 8 8 8 8 (null) (null) (null) (null) (null) (null) (null) (null) (null) (null) (null) (null)",
                "4 (null) 4 2 5 2 (null) (null) (null) (null) (null) (null) (null) (null) (null) (null) (null) (null)");
        ResultSet rs = methodWatcher.executeQuery(query);
        TestUtils.FormattedResult expected = TestUtils.FormattedResult.ResultFactory.convert(query, expectedColumns, expectedRows, "\\s+");
        TestUtils.FormattedResult actual = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
        Assert.assertEquals("Actual results didn't match expected.", expected.toString(), actual.toString());
    }

    /**
     * Bug 976
     * We get expected results from this (non-outer) join.
     *
     * @throws Exception fail
     */
    @Test
    public void testJoinBug976Compare() throws Exception {

        String query = format("select * from %s.A join (%s.B join %s.C on b2=c2) on a1=b1",
                CLASS_NAME, CLASS_NAME, CLASS_NAME, CLASS_NAME);

        String expectedColumns = "A1 A2 A3 A4 A5 A6 B1 B2 B3 B4 B5 B6 C1 C2 C3 C4 C5 C6";
        List<String> expectedRows = Arrays.asList(
                "3 4 2 (null) (null) (null) 3 2 3 3 1 4 2 2 5 3 2 4",
                "7 1 4 2 3 4 7 3 3 3 3 3 8 3 9 1 3 2",
                "1 1 3 6 (null) 2 1 4 2 (null) (null) (null) 1 4 1 (null) (null) (null)",
                "6 7 3 2 3 4 6 7 2 3 (null) 1 3 7 7 3 (null) 1",
                "6 7 3 2 3 4 6 7 2 3 (null) 1 1 7 2 3 1 1");
        ResultSet rs = methodWatcher.executeQuery(query);
        TestUtils.FormattedResult expected = TestUtils.FormattedResult.ResultFactory.convert(query, expectedColumns, expectedRows, "\\s+");
        TestUtils.FormattedResult actual = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
        Assert.assertEquals("Actual results didn't match expected.", expected.toString(), actual.toString());
    }

    /**
     * Bug 976
     * Nested join returns expected rows.
     *
     * @throws Exception fail
     */
    @Test
    public void testSmallJoinBug976Compare() throws Exception {

        String query = format("select * from %s.B join %s.C on b2=c2",
                CLASS_NAME, CLASS_NAME, CLASS_NAME, CLASS_NAME);

        String expectedColumns = "B1 B2 B3 B4 B5 B6 C1 C2 C3 C4 C5 C6";
        List<String> expectedRows = Arrays.asList(
                "3 2 3 3 1 4 2 2 5 3 2 4",
                "9 3 3 3 3 3 8 3 9 1 3 2",
                "7 3 3 3 3 3 8 3 9 1 3 2",
                "1 4 2 (null) (null) (null) 1 4 1 (null) (null) (null)",
                "6 7 2 3 (null) 1 3 7 7 3 (null) 1",
                "6 7 2 3 (null) 1 1 7 2 3 1 1");
        ResultSet rs = methodWatcher.executeQuery(query);
        TestUtils.FormattedResult expected = TestUtils.FormattedResult.ResultFactory.convert(query, expectedColumns, expectedRows, "\\s+");
        TestUtils.FormattedResult actual = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
        Assert.assertEquals("Actual results didn't match expected.", expected.toString(), actual.toString());
    }

    /**
     * Bug 976
     * Check to see that the outer join knows what it's doing by setting up the right side
     * with a table instead of a join.
     *
     * @throws Exception fail
     */
    @Test
    public void testLeftOuterJoinWithTable() throws Exception {
        String query = format("select * from %s.A left outer join %s.B_PRIME on a1=b1",
                CLASS_NAME, CLASS_NAME);

        String expectedColumns = "A1 A2 A3 A4 A5 A6 B1 B2 B3 B4 B5 B6 C1 C2 C3 C4 C5 C6";
        List<String> expectedRows = Arrays.asList(
                "6 7 3 2 3 4 6 7 2 3 (null) 1 3 7 7 3 (null) 1",
                "4 (null) 4 2 5 2 4 5 9 6 3 2 8 3 9 1 3 2",
                "1 1 3 6 (null) 2 1 4 2 (null) (null) (null) 1 4 1 (null) (null) (null)",
                "5 2 3 5 7 4 5 (null) 2 2 5 2 3 (null) 1 2 4 2",
                "3 4 2 (null) (null) (null) 3 2 3 3 1 4 2 2 5 3 2 4",
                "7 1 4 2 3 4 7 3 3 3 3 3 1 7 2 3 1 1",
                "8 8 8 8 8 8 (null) (null) (null) (null) (null) (null) (null) (null) (null) (null) (null) (null)",
                "2 3 2 4 2 2 (null) (null) (null) (null) (null) (null) (null) (null) (null) (null) (null) (null)");
        ResultSet rs = methodWatcher.executeQuery(query);
        TestUtils.FormattedResult expected = TestUtils.FormattedResult.ResultFactory.convert(query, expectedColumns, expectedRows, "\\s+");
        TestUtils.FormattedResult actual = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
        Assert.assertEquals("Actual results didn't match expected.", expected.toString(), actual.toString());
    }

    /* DB-1672: Prior to fix BroadcastJoinStrategy would return not feasible if table was created in the last 1/2 sec or so. */
    @Test
    public void testBroadcastJoinJustAfterTableCreation() throws Exception {

        for (int i = 0; i < 6; i = i + 2) {

            String tableName1 = String.valueOf(RandomStringUtils.randomAlphabetic(9));
            String tableName2 = String.valueOf(RandomStringUtils.randomAlphabetic(9));

            TestConnection connection=methodWatcher.getOrCreateConnection();
            new TableCreator(connection).withCreate("create table %s (c1 int, c2 int, c3 int)")
                    .withTableName(schemaWatcher.schemaName+"."+tableName1).create();

            new TableCreator(connection).withCreate("create table %s (c1 int, c2 int, c3 int)")
                    .withTableName(schemaWatcher.schemaName+"."+tableName2).create();
            connection.collectStats(schemaWatcher.schemaName,tableName1);
            connection.collectStats(schemaWatcher.schemaName,tableName2);

            String sql = "select * from %1$s.%2$s a --SPLICE-PROPERTIES joinStrategy=BROADCAST \n join %1$s.%2$s b on a.c1=b.c2 ";

            ResultSet rs = methodWatcher.prepareStatement(String.format(sql,schemaWatcher.schemaName, tableName1, tableName2)).executeQuery();

            Assert.assertFalse(rs.next());
        }

    }

    //SPLICE-1072
    @Test
    public void testSubqueryAndHashableJoin() throws Exception {

        String sqlText = "select t3.c1 ||  '; ' \n" +
                "from    t1 \n" +
                "        left outer join (Select c1 FROM t2)   a13\n" +
                "          on    (t1.c4 = a13.c1)\n" +
                "        left outer join joinit.t3\n" +
                "          on    (t1.c5 = t3.c1)\n" +
                "        left outer join joinit.t4\n" +
                "          on    (t1.c6 = CAST(t4.c1 as CHAR(6)))\n" +
                "        left outer join joinit.t5\n" +
                "          on    (SUBSTR(t1.c7,1, 3) = t5.c1)" ;

        ResultSet rs = methodWatcher.prepareStatement(sqlText).executeQuery();
        Assert.assertTrue(rs.next());
        Assert.assertNull(rs.getObject(1));

        sqlText = "select  \n" +
                "SUBSTR(CHAR(t7.c1),1,3)\n" +
                "from  t6\n" +
                "left outer join (select SUBSTR(CHAR(c1),1,3) as c2 from t7) a119\n" +
                "on  (t6.c2 = a119.c2)\n" +
                "left outer join t7\n" +
                "on  (t6.c3 = SUBSTR(CHAR(t7.c1),1,3) )\n" +
                "left outer join t8\n" +
                "on  (SUBSTR(t6.c4,1,3) = t8.c1)\n" +
                "where t6.c1 = '123456789'";
        rs = methodWatcher.prepareStatement(sqlText).executeQuery();
        Assert.assertTrue(rs.next());
        Assert.assertNull(rs.getObject(1));
    }
}
