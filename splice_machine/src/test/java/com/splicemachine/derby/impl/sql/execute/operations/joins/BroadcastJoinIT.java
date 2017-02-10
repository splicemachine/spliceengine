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
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @author Scott Fines
 *         Date: 5/20/15
 */
public class BroadcastJoinIT{
    public static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(BroadcastJoinIT.class.getSimpleName().toUpperCase());

    public static final SpliceTableWatcher a= new SpliceTableWatcher("A",schemaWatcher.schemaName,"(c1 int, c2 int)");
    public static final SpliceTableWatcher b= new SpliceTableWatcher("B",schemaWatcher.schemaName,"(c2 int,c3 int)");
    public static final SpliceTableWatcher date_dim= new SpliceTableWatcher("date_dim",schemaWatcher.schemaName,"(d_year int, d_qoy int)");

    public static final SpliceWatcher classWatcher = new SpliceWatcher();
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher)
            .around(a)
            .around(b)
            .around(date_dim)
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
            });

    private static TestConnection conn;
    private static final int numIterations = 30;

    @BeforeClass
    public static void setUpClass() throws Exception{
        conn = classWatcher.getOrCreateConnection();
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
}
