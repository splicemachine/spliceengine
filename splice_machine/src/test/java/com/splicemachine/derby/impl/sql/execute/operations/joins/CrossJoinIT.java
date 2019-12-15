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

package com.splicemachine.derby.impl.sql.execute.operations.joins;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.spark_project.guava.collect.Lists;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Collection;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

/**
 * @author Scott Fines
 *         Date: 5/20/15
 */


@RunWith(Parameterized.class)
public class CrossJoinIT extends SpliceUnitTest {


    private Boolean useSpark;
    private String idx;
    private static final String NO_IDX = "NO_IDX";
    private static final String NOT_COVER_IDX = "NOT_COVER_IDX";
    private static final String COVER_IDX = "COVER_IDX";
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{true, NO_IDX});
        params.add(new Object[]{false, NO_IDX});
        params.add(new Object[]{true, NOT_COVER_IDX});
        params.add(new Object[]{false, NOT_COVER_IDX});
        params.add(new Object[]{true, COVER_IDX});
        params.add(new Object[]{false, COVER_IDX});
        return params;
    }

    public CrossJoinIT(Boolean useSpark, String idx) {
        this.useSpark = useSpark;
        this.idx = idx;
    }

    public static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CrossJoinIT.class.getSimpleName().toUpperCase());

    public static final SpliceTableWatcher a= new SpliceTableWatcher("A",schemaWatcher.schemaName,"(c1 int, c2 int)");
    public static final SpliceTableWatcher b= new SpliceTableWatcher("B",schemaWatcher.schemaName,"(c2 int,c3 int)");
    public static final SpliceTableWatcher date_dim= new SpliceTableWatcher("date_dim",schemaWatcher.schemaName,"(d_year int, d_qoy int)");
    public static final SpliceTableWatcher t1= new SpliceTableWatcher("t1",schemaWatcher.schemaName,"(a1 int, b1 int, c1 int)");
    public static final SpliceTableWatcher t2= new SpliceTableWatcher("t2",schemaWatcher.schemaName,"(a2 int, b2 int)");
    public static final SpliceTableWatcher s1= new SpliceTableWatcher("s1",schemaWatcher.schemaName,"(a1 int, b1 char(2), c1 char(10), d1 char(10), e1 char(20))");
    public static final SpliceTableWatcher s2= new SpliceTableWatcher("s2",schemaWatcher.schemaName,"(a2 int, b2 boolean, c2 date, d2 time, e2 timestamp)");
    public static final SpliceTableWatcher s3 = new SpliceTableWatcher("s3", schemaWatcher.schemaName, "(num1 dec(31,1), num2 double, num3 float, num4 real, num5 int)");
    public static final SpliceTableWatcher bigTable = new SpliceTableWatcher("bigTable", schemaWatcher.schemaName, "(c1 int, c2 int)");


    public static final SpliceWatcher classWatcher = new SpliceWatcher(CrossJoinIT.class.getSimpleName().toUpperCase());
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher)
            .around(a)
            .around(b)
            .around(date_dim)
            .around(t1)
            .around(t2)
            .around(s1)
            .around(s2)
            .around(s3)
            .around(bigTable)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try (PreparedStatement ps = classWatcher.prepareStatement("insert into " + s3 + "(num1,num2,num3,num4,num5) values (?,?,?,?,?)")) {
                        ps.setBigDecimal(1, BigDecimal.ONE);
                        ps.setDouble(2, Double.valueOf(1));
                        ps.setFloat(3, Float.valueOf(1));
                        ps.setFloat(4, Float.valueOf(1));
                        ps.setInt(5, 1);
                        ps.execute();
                        ps.setBigDecimal(1, BigDecimal.valueOf(-1.1));
                        ps.setDouble(2, Double.valueOf("-1.1"));
                        ps.setDouble(3, Double.valueOf("-1.1"));
                        ps.setFloat(4, Float.valueOf("-1.1"));
                        ps.setInt(5, -1);
                        ps.execute();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            })
            .around(new SpliceDataWatcher(){
                @Override
                protected void starting(Description description){
                    try(PreparedStatement ps = classWatcher.prepareStatement("insert into "+a+"(c1,c2) values (?,?)")){
                        ps.setInt(1,1);ps.setInt(2,1);ps.execute();
                        ps.setInt(1,2);ps.setInt(2,2);ps.execute();
                        ps.setInt(1,3);ps.setInt(2,3);ps.execute();
                    }catch(Exception e){
                        throw new RuntimeException(e);
                    }
                }
            }).around(new SpliceDataWatcher(){
                @Override
                protected void starting(Description description){
                    try(PreparedStatement ps = classWatcher.prepareStatement("insert into "+b+"(c2,c3) values (?,?)")){
                        ps.setInt(1,1);ps.setInt(2,1);ps.execute();
                        ps.setInt(1,2);ps.setInt(2,2);ps.execute();
                        ps.setInt(1,3);ps.setInt(2,3);ps.execute();
                    }catch(Exception e){
                        throw new RuntimeException(e);
                    }
                }
            }).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try (PreparedStatement ps = classWatcher.prepareStatement("insert into " + date_dim + "(d_year, d_qoy) values (?,?)")) {
                        ps.setInt(1,1999);ps.setInt(2,1);ps.execute();
                        ps.setInt(1,1999);ps.setInt(2,2);ps.execute();
                        ps.setInt(1,1999);ps.setInt(2,3);ps.execute();
                        ps.setInt(1,1999);ps.setInt(2,4);ps.execute();
                        ps.setInt(1,2000);ps.setInt(2,1);ps.execute();
                        ps.setInt(1,2000);ps.setInt(2,2);ps.execute();
                        ps.setInt(1,2000);ps.setInt(2,3);ps.execute();
                        ps.setInt(1,2000);ps.setInt(2,4);ps.execute();
                        ps.setInt(1,2001);ps.setInt(2,1);ps.execute();
                        ps.setInt(1,2001);ps.setInt(2,2);ps.execute();
                        ps.setInt(1,2001);ps.setInt(2,3);ps.execute();
                        ps.setInt(1,2001);ps.setInt(2,4);ps.execute();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try (PreparedStatement ps = classWatcher.prepareStatement("insert into " + t1 + "(a1, b1, c1) values (?,?,?)")) {
                        ps.setInt(1, 1);ps.setInt(2, 2);ps.setInt(3, 3);ps.execute();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try(PreparedStatement ps = classWatcher.prepareStatement("insert into "+t2 +"(a2,b2) values (?,?)")){
                        ps.setInt(1,1);ps.setInt(2,22);ps.execute();
                        ps.setInt(1,4);ps.setInt(2,44);ps.execute();
                    }catch(Exception e){
                        throw new RuntimeException(e);
                    }
                }
            }).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try (PreparedStatement ps = classWatcher.prepareStatement("insert into " + s1 + "(a1, b1, c1, d1, e1) values (?,?,?,?,?)")) {
                        ps.setInt(1, 1);ps.setString(2, "1");ps.setString(3, "2018-11-11");
                        ps.setString(4, "13:00:00");ps.setString(5, "2018-12-24 11:11:11");ps.execute();
                    } catch (Exception e) {
                throw new RuntimeException(e);
            }
            }}).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try (PreparedStatement ps = classWatcher.prepareStatement("insert into " + s2 + "(a2, b2, c2, d2, e2) values (?,?,?,?,?)")) {
                        ps.setInt(1, 1);ps.setBoolean(2,true);ps.setDate(3, Date.valueOf("2018-11-11"));
                        ps.setTime(4, Time.valueOf("13:00:00"));ps.setTimestamp(5, Timestamp.valueOf("2018-12-24 11:11:11"));ps.execute();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }})
            ;

    private static TestConnection conn;
    private static final int numIterations = 30;

    @BeforeClass
    public static void setUpClass() throws Exception{
        conn = classWatcher.getOrCreateConnection();
    }

    public static void createData(Connection conn, String schemaName) throws Exception {
        new TableCreator(conn)
                .withCreate("create table t3 (a3 int, b3 int, c3 int, d3 int)")
                .withInsert("insert into t3 values(?,?,?,?)")
                .withRows(rows(
                        row(1095236,0,37770,0)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t4 (a4 int, b4 int, c4 numeric(31,0) not null, d4 numeric(31,0) not null, primary key(c4,d4))")
                .withInsert("insert into t4 values(?,?,?,?)")
                .withRows(rows(
                        row(1,1,1,1),
                        row(2,2,2,2),
                        row(3,3,3,3),
                        row(4,4,4,4),
                        row(5,5,5,5),
                        row(6,6,6,6),
                        row(7,7,7,7),
                        row(8,8,8,8),
                        row(9,9,9,9),
                        row(10,10,10,10)))
                .create();

        int factor = 10;
        for (int i = 1; i <= 12; i++) {
            classWatcher.executeUpdate(SpliceUnitTest.format("insert into t4 select a4, b4,c4+%d, d4 from t4", factor));
            factor = factor * 2;
        }

        new TableCreator(conn)
        .withCreate("create table tab1 (a int, b int, primary key(a))")
        .withInsert("insert into tab1 values(?,?)")
        .withRows(rows(
        row(1,1),
        row(2,2),
        row(3,3),
        row(4,4),
        row(5,5),
        row(6,6),
        row(7,7),
        row(8,8),
        row(9,9),
        row(10,10)))
        .create();

        new TableCreator(conn)
        .withCreate("create table tab2 (a int, b int, primary key(a))")
        .withInsert("insert into tab2 values(?,?)")
        .withRows(rows(
        row(1,1),
        row(2,2),
        row(3,3),
        row(4,4),
        row(5,5),
        row(6,6),
        row(7,7),
        row(8,8),
        row(9,9),
        row(10,10)))
        .create();

        new TableCreator(conn)
        .withCreate("create table tab3 (a int, b int)")
        .withIndex("create index ix on tab3(a)")
        .withInsert("insert into tab3 values(?,?)")
        .withRows(rows(
        row(5,5),
        row(6,6),
        row(7,7),
        row(8,8),
        row(9,9),
        row(10,10)))
        .create();

        factor = 10;
        for (int i = 1; i <= 8; i++) {
            classWatcher.executeUpdate(SpliceUnitTest.format("insert into tab1 select a+%d,b from tab1", factor));
            factor = factor * 2;
        }
        conn.commit();


        PreparedStatement stat = classWatcher.prepareStatement("insert into " + bigTable + " values (?,?)");
        for (int i = 1; i <= 50005; i++) {
            stat.setInt(1, i);
            stat.setInt(2, i);
            stat.execute();
        }

        classWatcher.execute(format("create index big_idx on %s(c1)", bigTable));
        classWatcher.execute("CALL SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS( 'CROSSJOINIT', false)");
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(classWatcher.getOrCreateConnection(), schemaWatcher.toString());
    }

    @Test
    public void testPickCrossJoinNonEquality() throws Exception {
        String sqlText =
                format("explain select count(*) from %s as s1 inner join %s as s2 " +
                        "--SPLICE-PROPERTIES useSpark=true \n" +
                        " on s1.c1 > s2.c2" , bigTable, bigTable);
        rowContainsQuery(6, sqlText,"CrossJoin", classWatcher);
    }

    @Test
    public void testPickMergeJoinEquality() throws Exception {
        String sqlText =
                format("explain select count(*) from %s as s1 inner join %s as s2 " +
                        "--SPLICE-PROPERTIES useSpark=%s \n" +
                        " on s1.c1 = s2.c2" , bigTable, bigTable, useSpark);
        rowContainsQuery(6, sqlText,"MergeSort", classWatcher);
    }

    @Test
    public void testPickBroadcastEquality() throws Exception {
        String sqlText =
                format("explain select count(*) from %s as a inner join %s as b " +
                        "--SPLICE-PROPERTIES useSpark=%s \n" +
                        " on a.a1 = b.a1" , s1, s1, useSpark);
        rowContainsQuery(6, sqlText,"Broadcast", classWatcher);
    }


    @Test
    public void testPickBroadcastWithIndex() throws Exception {
        String sqlText =
                format("explain select count(*) from %s as s1 inner join %s as s2 " +
                        "--SPLICE-PROPERTIES useSpark=%s \n" +
                        " on s1.c1 = s2.c2 and s2.c2 < 10" , bigTable, bigTable, useSpark);
        rowContainsQuery(6, sqlText,"Broadcast", classWatcher);
    }

    @Ignore("Ignore this test because of DB-8204")
    @Test
    public void testNumericColumnsCrossJoin() throws Exception {

        String expected = "1 |\n" +
            "----\n" +
            " 1 |\n" +
            " 1 |";
    
        String expected2 = "1 |\n" +
            "----\n" +
            " 1 |";
        
        String sqlText = format("select 1 from  --splice-properties joinOrder=fixed\n" +
            s3 + " tab1, " + s3 + " tab2 " +
            " --SPLICE-PROPERTIES joinStrategy=CROSS,useSpark=%s \n" +
            " where tab1.num1=tab2.num2", useSpark
        );

        ResultSet rs = classWatcher.executeQuery(sqlText);
        String resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();
    
        sqlText = format("select 1 from  --splice-properties joinOrder=fixed\n" +
            s3 + " tab1, " + s3 + " tab2 " +
            " --SPLICE-PROPERTIES joinStrategy=CROSS,useSpark=%s \n" +
            " where tab1.num1=tab2.num3", useSpark
        );
    
        rs = classWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();
    
        sqlText = format("select 1 from  --splice-properties joinOrder=fixed\n" +
            s3 + " tab1, " + s3 + " tab2 " +
            " --SPLICE-PROPERTIES joinStrategy=CROSS,useSpark=%s \n" +
            " where tab1.num1=tab2.num4", useSpark
        );
    
        rs = classWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();
    
        sqlText = format("select 1 from  --splice-properties joinOrder=fixed\n" +
            s3 + " tab1, " + s3 + " tab2 " +
            " --SPLICE-PROPERTIES joinStrategy=CROSS,useSpark=%s \n" +
            " where tab1.num1=tab2.num5", useSpark
        );
    
        rs = classWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected2 + "\n,actual result: " + resultString, expected2, resultString);
        rs.close();
    
        sqlText = format("select 1 from  --splice-properties joinOrder=fixed\n" +
            s3 + " tab1, " + s3 + " tab2 " +
            " --SPLICE-PROPERTIES joinStrategy=CROSS,useSpark=%s \n" +
            " where tab1.num3=tab2.num1", useSpark
        );
    
        rs = classWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();
    
        sqlText = format("select 1 from  --splice-properties joinOrder=fixed\n" +
            s3 + " tab1, " + s3 + " tab2 " +
            " --SPLICE-PROPERTIES joinStrategy=CROSS,useSpark=%s \n" +
            " where tab1.num3=tab2.num2", useSpark
        );
    
        rs = classWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();
    
        sqlText = format("select 1 from " + s3 + " tab1, " + s3 + " tab2 " +
            " --SPLICE-PROPERTIES joinStrategy=CROSS,useSpark=%s \n" +
            " where tab1.num3=tab2.num4", useSpark
        );
    
        rs = classWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();
    
        sqlText = format("select 1 from  --splice-properties joinOrder=fixed\n" +
            s3 + " tab1, " + s3 + " tab2 " +
            " --SPLICE-PROPERTIES joinStrategy=CROSS,useSpark=%s \n" +
            " where tab1.num3=tab2.num5", useSpark
        );
    
        rs = classWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected2 + "\n,actual result: " + resultString, expected2, resultString);
        rs.close();
    }


    @Test
    public void testCrossJoinWithIntToNumericCast() throws Exception {
        String sqlText = format("select c4 from --splice-properties joinOrder=fixed\n" +
                "t4 t --splice-properties useSpark=%s\n" +
                ",t3 c --splice-properties joinStrategy=CROSS\n" +
                "where c.c3=t.c4 and t.c4 >=37770 and t.c4 <37771", useSpark);
        String expected = "C4   |\n" +
                "-------\n" +
                "37770 |";

        ResultSet rs = classWatcher.executeQuery(sqlText);
        String resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();
    }

    @Test
    public void testSingleTableWithCrossJoinHint() throws Exception {
        String sqlText = format("select * from \n" +
                "a --splice-properties joinStrategy=CROSS, useSpark=%s\n" +
                "where c2=1", useSpark);
        try {
            classWatcher.executeQuery(sqlText);
            Assert.fail("Query should fail with no valid exeuction plan!");
        }catch (SQLException e) {
            Assert.assertTrue("Invalid exception thrown: " + e, e.getMessage().startsWith("No valid execution plan"));
        }
    }

    @Test
    public void testInequalityCrossJoin() throws Exception {
        try {
            if (idx.equals(NOT_COVER_IDX)) {
                classWatcher.execute("create index tab1_a_idx on tab1(a)");
            } else if (idx.equals(COVER_IDX)) {
                classWatcher.execute("create index tab1_all_idx on tab1(a, b)");
            }
        } catch (SQLException ignored) {
        }
        String sqlText = format("select count(*) from --splice-properties joinOrder=fixed\n" +
        "tab1 tt1 --splice-properties useSpark=%s\n" +
        "inner join tab1 tt2 --splice-properties useSpark=%s,joinStrategy=CROSS\n" +
        "on tt1.a between tt2.a - 5 and tt2.a + 5 and tt1.a in (10, 20, 30, 40)", useSpark, useSpark);


        ResultSet rs = classWatcher.executeQuery(sqlText);
        Assert.assertTrue("count(*) did not return the correct number of records!",rs.next());
        Assert.assertEquals("Wrong Count", 44, rs.getInt(1));

        sqlText = format("select count(*) from --splice-properties joinOrder=fixed\n" +
        "tab1 tt1 --splice-properties useSpark=%s\n" +
        "inner join tab1 tt2 --splice-properties useSpark=%s,joinStrategy=CROSS\n" +
        "on tt1.a between tt2.a - 5 and tt2.a + 5 and tt1.a in (10, 20, 30, 40)", useSpark, useSpark);

        rs = classWatcher.executeQuery(sqlText);
        Assert.assertTrue("count(*) did not return the correct number of records!",rs.next());
        Assert.assertEquals("Wrong Count", 44, rs.getInt(1));


        try {
            sqlText = format("select count(*) from  --splice-properties joinOrder=fixed\n" +
                    "tab1 --splice-properties useSpark=%s\n" +
                    "where tab1.a in (1, 3, 5) and\n" +
                    "        exists (select * from tab3 --splice-properties useSpark=%s,joinStrategy=cross\n" +
                    "                where tab1.a between tab3.a - 1 and tab3.a)", useSpark, useSpark);

            classWatcher.executeQuery(sqlText);
            Assert.fail("Cross join should be invalidate on exists join");
        } catch (SQLSyntaxErrorException ignored) {
        }

        try {
            sqlText = format("select count(*) from  --splice-properties joinOrder=fixed\n" +
                    "tab1 --splice-properties useSpark=%s\n" +
                    "where tab1.a in (1, 3, 5) and\n" +
                    "    not exists (select * from tab3 --splice-properties useSpark=%s,joinStrategy=cross\n" +
                    "                where tab1.a between tab3.a - 1 and tab3.a)", useSpark, useSpark);

            rs = classWatcher.executeQuery(sqlText);
            Assert.fail("Cross join should be invalidate on non exists join");
        } catch (SQLSyntaxErrorException ignored) {
        }

        try {
            sqlText = format("select count(*) from  --splice-properties joinOrder=fixed\n" +
                    "        tab1 --splice-properties useSpark=%s\n" +
                    "        where tab1.b in\n" +
                    "         (select b from tab3 --splice-properties useSpark=%s,joinStrategy=cross\n" +
                    "                   where tab1.a between tab3.a - 1 and tab3.a)", useSpark, useSpark);

            rs = classWatcher.executeQuery(sqlText);
            Assert.fail("Cross join should be invalidate on in join");
        } catch (SQLSyntaxErrorException ignored) {
        }

        rs.close();
    }

    @Test
    public void testCharBooleanColumnsCrossJoin() throws Exception {
        try {
            if (idx.equals(NOT_COVER_IDX)) {
                classWatcher.execute("create index s2_b2_idx on s2(b2)");
            } else if (idx.equals(COVER_IDX)) {
                classWatcher.execute("create index s2_all_idx on s2(a2, b2, c2, d2, e2)");
            }
        } catch (SQLException ignored) {
        }
        String sqlText =
                format("select count(s1.a1) from  --splice-properties joinOrder=fixed\n" + s1 +
                " inner join " + s2 + " --SPLICE-PROPERTIES joinStrategy=CROSS,useSpark=%s \n" +
                " on s1.b1 = s2.b2" , useSpark
        ) ;
        ResultSet rs = classWatcher.executeQuery(sqlText);

        Assert.assertTrue("rs.next() failed", rs.next());
        int c = rs.getInt(1);
        Assert.assertTrue("count(s1.a1) returned incorrect number of rows:", (c == 1));

        sqlText = format("select count(s1.a1) from  --splice-properties joinOrder=fixed\n" + s1 +
                " inner join " + s2 + " --SPLICE-PROPERTIES joinStrategy=CROSS,useSpark=%s \n" +
                " on s2.b2 = s1.b1" , useSpark
        ) ;
        rs = classWatcher.executeQuery(sqlText);

        Assert.assertTrue("rs.next() failed", rs.next());
        c = rs.getInt(1);
        Assert.assertTrue("count(s1.a1) returned incorrect number of rows:", (c == 1));

        rs.close();
    }

    @Test
    public void testCharDateColumnsCrossJoin() throws Exception {
        String sqlText =
                format("select count(s1.a1) from  --splice-properties joinOrder=fixed\n" + s1 +
                " inner join " + s2 + " --SPLICE-PROPERTIES joinStrategy=CROSS,useSpark=%s \n" +
                " on s1.c1 = s2.c2" , useSpark
        ) ;
        ResultSet rs = classWatcher.executeQuery(sqlText);

        Assert.assertTrue("rs.next() failed", rs.next());
        int c = rs.getInt(1);
        Assert.assertTrue("count(s1.a1) returned incorrect number of rows:", (c == 1));

        sqlText = format("select count(s1.a1) from  --splice-properties joinOrder=fixed\n" + s1 +
                " inner join " + s2 + " --SPLICE-PROPERTIES joinStrategy=CROSS,useSpark=%s \n" +
                " on s2.c2 = s1.c1" , useSpark
        ) ;
        rs = classWatcher.executeQuery(sqlText);

        Assert.assertTrue("rs.next() failed", rs.next());
        c = rs.getInt(1);
        Assert.assertTrue("count(s1.a1) returned incorrect number of rows:", (c == 1));

        rs.close();
    }

    @Test
    public void testIllegalFormatCharDateColumnsCrossJoin() throws Exception {
        String sqlText =
                format("select count(s1.a1) from  --splice-properties joinOrder=fixed\n" + s1 +
                " inner join " + s2 + " --SPLICE-PROPERTIES joinStrategy=CROSS,useSpark=%s \n" +
                " on s1.b1 = s2.c2" , useSpark
        ) ;
        try (ResultSet rs = classWatcher.executeQuery(sqlText)) {
            Assert.fail("Exception not thrown");
        } catch (SQLDataException e) {
            assertEquals("22007", e.getSQLState());
        }
    }

    @Test
    public void testComparableCharColumnsCrossJoin() throws Exception {
        String sqlText =
                format("select count(s1.a1) from  --splice-properties joinOrder=fixed\n" + s1 +
                " inner join " + s2 + " --SPLICE-PROPERTIES joinStrategy=CROSS,useSpark=%s \n" +
                " on s1.b1 = s2.a2" , useSpark
        ) ;

        // The following query with CHAR/INT comparison should not throw an error.
        try (ResultSet rs = classWatcher.executeQuery(sqlText)) {

        }
    }

    @Test
    public void testCharTimeColumnsCrossJoin() throws Exception {
        String sqlText =
                format("select count(s1.a1) from  --splice-properties joinOrder=fixed\n" + s1 +
                " inner join " + s2 + " --SPLICE-PROPERTIES joinStrategy=CROSS,useSpark=%s \n" +
                " on s1.d1 = s2.d2" , useSpark
        ) ;
        ResultSet rs = classWatcher.executeQuery(sqlText);

        Assert.assertTrue("rs.next() failed", rs.next());
        int c = rs.getInt(1);
        Assert.assertTrue("count(s1.a1) returned incorrect number of rows:", (c == 1));

        sqlText = format("select count(s1.a1) from  --splice-properties joinOrder=fixed\n" + s1 +
                " inner join " + s2 + " --SPLICE-PROPERTIES joinStrategy=CROSS,useSpark=%s \n" +
                " on s2.d2 = s1.d1" , useSpark
        ) ;
        rs = classWatcher.executeQuery(sqlText);

        Assert.assertTrue("rs.next() failed", rs.next());
        c = rs.getInt(1);
        Assert.assertTrue("count(s1.a1) returned incorrect number of rows:", (c == 1));

        rs.close();
    }

    @Test
    public void testCharTimestampColumnsCrossJoin() throws Exception {
        String sqlText =
                format("select count(s1.a1) from  --splice-properties joinOrder=fixed\n" + s1 +
                " inner join " + s2 + " --SPLICE-PROPERTIES joinStrategy=CROSS,useSpark=%s \n" +
                " on s1.e1 = s2.e2" , useSpark
        ) ;
        ResultSet rs = classWatcher.executeQuery(sqlText);

        Assert.assertTrue("rs.next() failed", rs.next());
        int c = rs.getInt(1);
        Assert.assertTrue("count(s1.a1) returned incorrect number of rows:", (c == 1));

        sqlText = format("select count(s1.a1) from  --splice-properties joinOrder=fixed\n" + s1 +
                " inner join " + s2 + " --SPLICE-PROPERTIES joinStrategy=CROSS,useSpark=%s \n" +
                " on s2.e2 = s1.e1" , useSpark
        ) ;
        rs = classWatcher.executeQuery(sqlText);

        Assert.assertTrue("rs.next() failed", rs.next());
        c = rs.getInt(1);
        Assert.assertTrue("count(s1.a1) returned incorrect number of rows:", (c == 1));

        rs.close();
    }


}
