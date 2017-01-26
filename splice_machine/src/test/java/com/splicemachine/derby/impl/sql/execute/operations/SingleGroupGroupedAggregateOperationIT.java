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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.spark_project.guava.collect.Lists;
import com.splicemachine.test.SlowTest;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test.suites.Stats;

/**
 * Tests aggregations around single-group entries.
 * @author Scott Fines
 *
 */
public class SingleGroupGroupedAggregateOperationIT extends SpliceUnitTest {
    private static Logger LOG = Logger.getLogger(SingleGroupGroupedAggregateOperationIT.class);
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String CLASS_NAME = SingleGroupGroupedAggregateOperationIT.class.getSimpleName().toUpperCase();
    public static final String TABLE_NAME_1 = "T";
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher(TABLE_NAME_1,CLASS_NAME,"(username varchar(40),i int)");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher)
            .around(new SpliceDataWatcher(){
                @Override
                protected void starting(Description description) {
                    try {
                        spliceClassWatcher.setAutoCommit(false);
                        PreparedStatement ps = spliceClassWatcher.prepareStatement(format("insert into %s.%s (username, i) values (?,?)",CLASS_NAME,TABLE_NAME_1));
                        List<String> users = Arrays.asList("jzhang","sfines",
                                "jleach","steve","george","tompson",
                                "alabama","tweak","schnoz","poster",
                                "melissa","van","emiko","caution",
                                "coco","mack","simba","quinn",
                                "zoey","danni");
                        for(int i=0;i< size;i++){
                            for(String user:users){
                                int value = i*10;

                                if(!unameStats.containsKey(user))
                                    unameStats.put(user,new Stats());
                                unameStats.get(user).add(value);

                                ps.setString(1, user);
                                ps.setInt(2, value);
                                ps.executeUpdate();
                            }
                        }
                        spliceClassWatcher.commit();
//			        spliceClassWatcher.splitTable(TABLE_NAME_1,CLASS_NAME,size/3);
                    } catch (Exception e) {
                        LOG.error("Error importing data", e);
                        throw new RuntimeException(e);
                    }
                    finally {
                        spliceClassWatcher.closeAll();
                    }
                }

            });

    @Rule public SpliceWatcher methodWatcher = new SpliceWatcher();
    private static Map<String,Stats> unameStats = new HashMap<String,Stats>();
    private static final int size = 10;


    @Test(timeout=5000)
    public void testGroupedAggregateReturnsNoRowsForEmpty() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("select username, count(i) from %s where username is null group by username",spliceTableWatcher));
        Assert.assertFalse("Results were returned!",rs.next());
    }

    @Test(timeout=5000)
    public void testGroupedWithInOperator() throws Exception{
        ResultSet rs = methodWatcher.executeQuery(format("select username, count(i) from %s where username in ('sfines','jzhang') group by username",this.getTableReference(TABLE_NAME_1)));
        int row =0;
        while(rs.next()){
            String uname = rs.getString(1);
            int count = rs.getInt(2);
            int correctCount = unameStats.get(uname).getCount();
            Assert.assertEquals("Incorrect count for uname "+ uname,correctCount,count);
            row++;
        }
        Assert.assertEquals("Not all groups found!", 2,row);
    }

    @Test(timeout=5000)
    public void testGroupedCountOperation() throws Exception{
        ResultSet rs = methodWatcher.executeQuery(format("select username,count(i) from %s group by username",this.getTableReference(TABLE_NAME_1)));
        int row =0;
        List<String> results = Lists.newArrayList();
        while(rs.next()){
            String uname = rs.getString(1);
            int count = rs.getInt(2);
            results.add(String.format("uname=%s,count=%d",uname,count));
            Stats stats = unameStats.get(uname);
            if(stats==null){
                for(String result:results){
                    System.out.println(result);
                }
            }
            row++;
        }
        Assert.assertEquals("Not all groups found!", unameStats.size(),row);
    }

    @Test
    @Category(SlowTest.class)
    public void testRepeatedGroupedCount() throws Exception {
        /* Regression test for Bug 306 */
        for(int i=0;i<1000;i++){
            testGroupedCountOperation();
        }
    }

    @Test(timeout=5000)
    public void testGroupedMinOperation() throws Exception{
        ResultSet rs = methodWatcher.executeQuery(format("select username,min(i) from %s group by username",this.getTableReference(TABLE_NAME_1)));
        int row =0;
        while(rs.next()){
            String uname = rs.getString(1);
            int min = rs.getInt(2);
            int correctMin = unameStats.get(uname).getMin();
            Assert.assertEquals("Incorrect min for uname "+ uname,correctMin,min);
            row++;
        }
        Assert.assertEquals("Not all groups found!", unameStats.size(),row);
    }

    @Test(timeout=5000)
    public void testGroupedMaxOperation() throws Exception{
        ResultSet rs = methodWatcher.executeQuery(format("select username,max(i) from %s group by username",this.getTableReference(TABLE_NAME_1)));
        int row =0;
        while(rs.next()){
            String uname = rs.getString(1);
            Assert.assertTrue("Uname "+ uname+" is unexpected",unameStats.containsKey(uname));
            int max = rs.getInt(2);
            int correctMax = unameStats.get(uname).getMax();
            Assert.assertEquals("Incorrect max for uname "+ uname,correctMax,max);
            row++;
        }
        Assert.assertEquals("Not all groups found!", unameStats.size(),row);
    }

    @Test(timeout=5000)
    public void testGroupedAvgOperation() throws Exception{
        ResultSet rs = methodWatcher.executeQuery(format("select username,avg(i) from %s group by username",this.getTableReference(TABLE_NAME_1)));
        int row =0;
        while(rs.next()){
            String uname = rs.getString(1);
            int avg = rs.getInt(2);
            int correctAvg = unameStats.get(uname).getAvg();
            Assert.assertEquals("Incorrect count for uname "+ uname,correctAvg,avg);
            row++;
        }
        Assert.assertEquals("Not all groups found!", unameStats.size(),row);
    }

    @Test(timeout=5000)
    public void testGroupedSumOperation() throws Exception{
        ResultSet rs = methodWatcher.executeQuery(format("select username,sum(i) from %s group by username",this.getTableReference(TABLE_NAME_1)));
        int row =0;
        while(rs.next()){
            String uname = rs.getString(1);
            int sum = rs.getInt(2);
            long correctSum = unameStats.get(uname).getSum();
            Assert.assertEquals("Incorrect count for uname "+ uname,correctSum,sum);
            row++;
        }
        Assert.assertEquals("Not all groups found!", unameStats.size(),row);
    }

    @Test(timeout=20000)
    public void testGroupBySubselects() throws Exception {
        /*Regression test for DB-2014*/
        String query = String.format(
                        "select\n" +
                        "        t.username\n" +
                        "        ,sum(ts.\"Sum\")\n" +
                        "        ,avg(ta.\"Avg\")\n" +
                        "from\n" +
                        "        %1$s t\n" +
                        "        inner join (select username, sum(i) as \"Sum\" from %1$s group by username) as ts on ts.username = t.username\n" +
                        "        inner join (select username, avg(i) as \"Avg\" from %1$s group by username) as ta on ta.username = t.username\n" +
                        "group by t.username"
        ,spliceTableWatcher);
        ResultSet rs = methodWatcher.executeQuery(query);
        int rowCount =0;
        //TODO -sf- make this more flexible
        while(rs.next()){
            String uname = rs.getString(1);
            Assert.assertEquals("Incorrect sum!",4500,rs.getInt(2));
            Assert.assertEquals("Incorrect avg!",45,rs.getInt(3));
            rowCount++;
        }
        Assert.assertEquals("Not all groups fetched!",unameStats.size(),rowCount);

    }
}
