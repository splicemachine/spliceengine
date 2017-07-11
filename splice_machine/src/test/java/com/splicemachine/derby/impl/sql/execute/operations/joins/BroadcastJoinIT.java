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

package com.splicemachine.derby.impl.sql.execute.operations.joins;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

/**
 * @author Scott Fines
 *         Date: 5/20/15
 */
public class BroadcastJoinIT{
    public static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(BroadcastJoinIT.class.getSimpleName().toUpperCase());

    public static final SpliceTableWatcher a= new SpliceTableWatcher("A",schemaWatcher.schemaName,"(c1 int, c2 int)");
    public static final SpliceTableWatcher b= new SpliceTableWatcher("B",schemaWatcher.schemaName,"(c2 int,c3 int)");
    public static final SpliceTableWatcher date_dim= new SpliceTableWatcher("date_dim",schemaWatcher.schemaName,"(d_year int, d_qoy int)");
    public static final SpliceTableWatcher t1= new SpliceTableWatcher("t1",schemaWatcher.schemaName,"(a1 int, b1 int, c1 int)");
    public static final SpliceTableWatcher t2= new SpliceTableWatcher("t2",schemaWatcher.schemaName,"(a2 int, b2 int)");

    public static final SpliceWatcher classWatcher = new SpliceWatcher(BroadcastJoinIT.class.getSimpleName().toUpperCase());
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher)
            .around(a)
            .around(b)
            .around(date_dim)
            .around(t1)
            .around(t2)
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
            });

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

        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(classWatcher.getOrCreateConnection(), schemaWatcher.toString());
    }

    @Test
    @Ignore("Takes a super long time to work, and then knocks over the region server with an OOM")
    public void testBroadcastJoinDoesNotCauseRegionServerToCollapse() throws Exception{
        String querySQL = "select count(*) from --SPLICE-PROPERTIES joinOrder=FIXED\n" +
                " "+a+" l,"+ b+" r --SPLICE-PROPERTIES joinStrategy=BROADCAST\n" +
                " where l.c2 = r.c2";
        String insertSQL = "insert into "+ b+"(c2,c3) select * from "+b;
        try(PreparedStatement queryStatement = conn.prepareStatement(querySQL)){
            try(PreparedStatement insertStatement = conn.prepareStatement(insertSQL)){
                for(int i=0;i<numIterations;i++){
                    insertStatement.execute();
                    try(ResultSet rs = queryStatement.executeQuery()){
                        Assert.assertTrue("Weird: count(*) did not return the correct number of records!",rs.next());
                    }
                }
            }
        }
    }

    @Test
    public void testInClauseBroadCastJoin() throws Exception {
        String sqlText = "select count(*) from " + date_dim + " d --SPLICE-PROPERTIES useSpark = true \n" +
                "where d.d_qoy in (select d_qoy from " + date_dim + " )" ;
        ResultSet rs = classWatcher.executeQuery(sqlText);

        Assert.assertTrue("rs.next() failed", rs.next());
        int c = rs.getInt(1);
        Assert.assertTrue("count(*) returned incorrect number of rows:", (c == 12));
        rs.close();

        sqlText = "with ws_wh as (select * from " + date_dim + " dim " +
        " where dim.d_qoy > 2 ) " +
        " select count(*) " +
        "   from  "+ date_dim + " d --SPLICE-PROPERTIES useSpark = true \n" +
        "   where d.d_qoy in (select d_qoy from ws_wh)";

        rs = classWatcher.executeQuery(sqlText);
        Assert.assertTrue("rs.next() failed", rs.next());
        c = rs.getInt(1);
        Assert.assertTrue("count(*) returned incorrect number of rows:", (c == 6));
        rs.close();
    }

    @Test
    public void testRightOuterJoinViaBroadCastJoin() throws Exception {
        String sqlText = "select a1,a2,b1,b2,c1 from " + t1 + " right join " + t2 +" --SPLICE-PROPERTIES useSpark = true \n" +
                "on a1 = a2 order by 2 desc" ;
        ResultSet rs = classWatcher.executeQuery(sqlText);

        Assert.assertTrue("rs.next() failed", rs.next());
        int a1 = rs.getInt(1);
        Assert.assertTrue("incorrect result:", rs.wasNull());
        int a2 = rs.getInt(2);
        Assert.assertTrue("incorrect result:", (a2==4));
        rs.close();
    }

    @Test
    public void testBroadCastJoinWithIntToNumericCast() throws Exception {
        String sqlText = "select c4 from --splice-properties joinOrder=fixed\n" +
                "t4 t --splice-properties useSpark=true\n" +
                ",t3 c --splice-properties joinStrategy=broadcast\n" +
                "where c.c3=t.c4 and t.c4 >=37770 and t.c4 <37771";
        String expected = "C4   |\n" +
                "-------\n" +
                "37770 |";

        ResultSet rs = classWatcher.executeQuery(sqlText);
        String resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();
    }
}
