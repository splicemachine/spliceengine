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

import org.spark_project.guava.collect.Sets;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.concurrent.ConcurrentTicker;
import com.splicemachine.concurrent.IncrementingClock;
import com.splicemachine.concurrent.TickingClock;
import com.splicemachine.derby.test.framework.*;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Set;

/**
 * This tests basic table scans with and without projection/restriction
 */

public class DistinctScanOperationIT extends SpliceUnitTest{
    protected static SpliceWatcher spliceClassWatcher=new SpliceWatcher();
    protected static SpliceSchemaWatcher schema=new SpliceSchemaWatcher(DistinctScanOperationIT.class.getSimpleName());
    protected static SpliceTableWatcher foo=new SpliceTableWatcher("FOO",DistinctScanOperationIT.class.getSimpleName(),"(si int, sa varchar(40))");
    protected static SpliceTableWatcher ts=new SpliceTableWatcher("TS",DistinctScanOperationIT.class.getSimpleName(),"(si int, t timestamp)");
    protected static SpliceTableWatcher tab=new SpliceTableWatcher("TAB",DistinctScanOperationIT.class.getSimpleName(),"(I INT, D DOUBLE)");

    private static int size=10;
    private static long startTime;

    @ClassRule
    public static TestRule chain=RuleChain.outerRule(spliceClassWatcher)
            .around(schema)
            .around(foo)
            .around(ts)
            .around(tab)
            .around(new SpliceDataWatcher(){
                @Override
                protected void starting(Description description){
                    try{
                        spliceClassWatcher.setAutoCommit(true);
                        PreparedStatement ps=spliceClassWatcher.prepareStatement("insert into "+foo+" values (?,?)");
                        for(int i=0;i<size;i++){
                            ps.setInt(1,i);
                            ps.setString(2,Integer.toString(i+1));
                            ps.executeUpdate();

                            if(i%2==0){
                                //add a duplicate row
                                ps.setInt(1,i);
                                ps.setString(2,Integer.toString(i+1));
                                ps.executeUpdate();
                            }
                        }

                        ps=spliceClassWatcher.prepareStatement("insert into "+ts+" values (?,?)");
                        TickingClock clock = new ConcurrentTicker(0l);
                        startTime=clock.currentTimeMillis();
                        for(int i=0;i<size;i++){
                            ps.setInt(1,i);
                            ps.setTimestamp(2,new Timestamp(clock.currentTimeMillis()));
                            ps.execute();
                            clock.tickMillis(1l);
                        }
                        ps=spliceClassWatcher.prepareStatement("insert into "+tab+" values (?,?)");
                        for(int i=0;i<10;i++){
                            ps.setInt(1,i);
                            ps.setDouble(2,i);
                            for(int j=0;j<100;j++){
                                ps.addBatch();
                            }
                            ps.executeBatch();
                        }

                    }catch(Exception e){
                        throw new RuntimeException(e);
                    }finally{
                        spliceClassWatcher.closeAll();
                    }
                }

            });

    @Rule
    public SpliceWatcher methodWatcher=new SpliceWatcher();


    @Test
    public void testDistinctScanOperation() throws Exception{
        ResultSet rs=methodWatcher.executeQuery("select distinct si from "+foo);
        Set<Integer> priorResults=Sets.newHashSet();
        while(rs.next()){
            int next=rs.getInt(1);
            Assert.assertTrue("Duplicate value "+next+" returned!",!priorResults.contains(next));
            priorResults.add(next);
        }
        Assert.assertEquals(size,priorResults.size());
    }

    @Test
    public void testDistinctScanGetNextRowCore() throws Exception{
        ResultSet rs=methodWatcher.executeQuery(String.format("select count(*) from (select distinct si from %s) a",foo));
        Assert.assertTrue("No rows returned, 1 row expected",rs.next());
        Assert.assertEquals("10 distinct results expected",10,rs.getInt(1));
        Assert.assertFalse("More than one row was returned",rs.next());
    }

    @Test
    public void testDistinctString() throws Exception{
        ResultSet rs=methodWatcher.executeQuery("select distinct sa from "+foo);
        Set<String> priorResults=Sets.newHashSet();
        while(rs.next()){
            String next=rs.getString(1);
            Assert.assertTrue("Duplicate value "+next+" returned!",!priorResults.contains(next));
            priorResults.add(next);
        }
        Assert.assertEquals(size,priorResults.size());
    }

    @Test
    public void testDistinctTimestamp() throws Exception{
        ResultSet rs=methodWatcher.executeQuery("select distinct t from "+ts);
        Set<Timestamp> timestampSet=Sets.newHashSet();
        while(rs.next()){
            Timestamp ts=rs.getTimestamp(1);
            Assert.assertTrue("Duplicate value "+ts+" returned!",!timestampSet.contains(ts));
            Assert.assertTrue("Timestamp value returned too low!",startTime<=ts.getTime());
            timestampSet.add(ts);
        }

        Assert.assertEquals("Incorrect number of returned records!",size,timestampSet.size());
    }

//    @Test
    public void testRepeatedDistinctTimestamp() throws Exception{
        for(int i=0;i<100;i++){
            testDistinctTimestamp();
        }
    }

    @Test
    public void testSelectDistinctWorks() throws Exception{
        ResultSet rs=methodWatcher.executeQuery("select distinct i from "+tab+" order by i");
        int i=0;
        while(rs.next()){
            int val=rs.getInt(1);
            Assert.assertFalse("Value ["+i+"] was unexpectedly null!",rs.wasNull());
            Assert.assertEquals("Incorrect column value!",i,val);
            i++;
        }
        Assert.assertEquals("Incorrect row count!",i,10);
    }

    private static final double ERROR=Math.pow(1,-12);

    @Test
    public void testSelectDistinctWorksForAllFields() throws Exception{
        ResultSet rs=methodWatcher.executeQuery("select distinct i,d from "+tab+" order by i,d");

        int i=0;
        while(rs.next()){
            int val1=rs.getInt(1);
            Assert.assertFalse("Value i=["+i+"] was unexpectedly null",rs.wasNull());
            Assert.assertEquals(i,val1);

            double val2=rs.getDouble(2);
            Assert.assertFalse("Value d=["+i+"] was unexpectedly null",rs.wasNull());
            Assert.assertEquals((double)i,val2,ERROR);
            i++;
        }
        Assert.assertEquals("Incorrect row count!",i,10);
    }

    @Test
    public void testSelectDistinctOutOrOrder() throws Exception{
        ResultSet rs=methodWatcher.executeQuery("select distinct d,i from "+tab);

        int i=0;
        while(rs.next()){
            int val1=rs.getInt(2);
            Assert.assertFalse("Value i=["+i+"] was unexpectedly null",rs.wasNull());

            double val2=rs.getDouble(1);
            Assert.assertFalse("Value d=["+i+"] was unexpectedly null",rs.wasNull());
            Assert.assertEquals((double)val1,val2,ERROR);
            i++;
        }
        Assert.assertEquals("Incorrect row count!",i,10);
    }

    /*
     * These are actually tests of the SortOperation, not of DistinctScan, but since
     * they refer to a table schema here, I left them alone (-SF-)
     */
    @Test
    public void testDistinctWithSortAsc() throws Exception{
        ResultSet rs=methodWatcher.executeQuery("select distinct d,i from "+tab+" order by i asc");

        int[] correct=new int[size];
        for(int i=0;i<size;i++){
            correct[i]=i;
        }

        int[] actual=new int[size];
        int i=0;
        while(rs.next()){
            int val1=rs.getInt(2);
            Assert.assertFalse("Value i=["+i+"] was unexpectedly null",rs.wasNull());
            Assert.assertEquals(i,val1);
            actual[i]=val1;

            double val2=rs.getDouble(1);
            Assert.assertFalse("Value d=["+i+"] was unexpectedly null",rs.wasNull());
            Assert.assertEquals((double)i,val2,ERROR);
            i++;
        }
        Assert.assertEquals("Incorrect row count!",i,size);

        Assert.assertArrayEquals("Incorrect sort order!",correct,actual);
    }

    @Test
    public void testDistinctWithSortDesc() throws Exception{
        ResultSet rs=methodWatcher.executeQuery("select distinct d,i from "+tab+" order by i desc");

        int[] correct=new int[size];
        for(int i=size-1, p=0;i>=0;i--,p++){
            correct[p]=i;
        }

        int[] actual=new int[size];
        int i=0;
        while(rs.next()){
            int val1=rs.getInt(2);
            Assert.assertFalse("Value i=["+i+"] was unexpectedly null",rs.wasNull());
            Assert.assertEquals(correct[i],val1);
            actual[i]=val1;

            double val2=rs.getDouble(1);
            Assert.assertFalse("Value d=["+i+"] was unexpectedly null",rs.wasNull());
            Assert.assertEquals((double)correct[i],val2,ERROR);
            i++;
        }
        Assert.assertEquals("Incorrect row count!",i,size);

        Assert.assertArrayEquals("Incorrect sort order!",correct,actual);
    }
}
