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

import org.spark_project.guava.base.Joiner;
import org.spark_project.guava.collect.Lists;
import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;

/**
 * This tests basic table scans with and without projection/restriction
 */
public class ProjectRestrictOperationIT extends SpliceUnitTest{
    private static final int MIN_DECIMAL_DIVIDE_SCALE=4;
    private static Logger LOG=Logger.getLogger(ProjectRestrictOperationIT.class);
    public static final String CLASS_NAME=ProjectRestrictOperationIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher=new SpliceWatcher(CLASS_NAME);
    public static final String TABLE_NAME="A";
    protected static SpliceSchemaWatcher spliceSchemaWatcher=new SpliceSchemaWatcher(CLASS_NAME);
    protected static SpliceTableWatcher spliceTableWatcher=new SpliceTableWatcher(TABLE_NAME,CLASS_NAME,"(si varchar(40),sa varchar(40),sc int)");
    protected static SpliceTableWatcher spliceTableWatcher2=new SpliceTableWatcher("B",CLASS_NAME,"(sc int,sd int,se decimal(7,2))");

    @ClassRule
    public static TestRule chain=RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher)
            .around(spliceTableWatcher2)
            .around(TestUtils.createFileDataWatcher(spliceClassWatcher,"test_data/employee.sql",CLASS_NAME))
            .around(new SpliceDataWatcher(){
                @Override
                protected void starting(Description description){
                    try{
                        PreparedStatement ps=spliceClassWatcher.prepareStatement(String.format("insert into %s.%s values (?,?,?)",CLASS_NAME,TABLE_NAME));
                        for(int i=0;i<10;i++){
                            ps.setString(1,Integer.toString(i));
                            ps.setString(2,Integer.toString(i)+" Times ten");
                            ps.setInt(3,i);
                            ps.executeUpdate();
                        }

                        ps=spliceClassWatcher.prepareStatement(String.format("insert into %s values (?,?,?)",spliceTableWatcher2.toString()));
                        for(int i=0;i<10;i++){
                            ps.setInt(1,i);
                            ps.setInt(2,i*2+1);
                            ps.setBigDecimal(3,new BigDecimal(i*3));
                            ps.executeUpdate();
                        }
                    }catch(Exception e){
                        throw new RuntimeException(e);
                    }finally{
                        spliceClassWatcher.closeAll();
                    }
                }

            });

    @Rule
    public SpliceWatcher methodWatcher=new SpliceWatcher(CLASS_NAME);

    @Test
    public void testCanDivideSafely() throws Exception{
        ResultSet rs=methodWatcher.executeQuery("select sc,sd,se,(sc*sd)/se from "+spliceTableWatcher2.toString()+" where se !=0");
        List<String> results=Lists.newArrayList();
        while(rs.next()){
            int sc=rs.getInt(1);
            int sd=rs.getInt(2);
            BigDecimal se=rs.getBigDecimal(3);
            BigDecimal div=rs.getBigDecimal(4);

            BigDecimal correctDiv=new BigDecimal(sc*sd).divide(se,MIN_DECIMAL_DIVIDE_SCALE,BigDecimal.ROUND_DOWN);
//  SPLICE-898            Assert.assertEquals("Incorrect division!",div.compareTo(correctDiv),0);

            results.add(String.format("sc=%d,sd=%d,se=%f,div=%f",sc,sd,se,div));
        }
        for(String result : results){
            LOG.debug(result);
        }
        Assert.assertEquals("Incorrect row count returned",9,results.size());

    }

    @Test
    public void testResrictWrapperOnTableScanPreparedStatement() throws Exception{
        PreparedStatement s=methodWatcher.prepareStatement(format("select * from %s where si like ?",this.getTableReference(TABLE_NAME)));
        s.setString(1,"%5%");
        ResultSet rs=s.executeQuery();
        int i=0;
        while(rs.next()){
            i++;
            Assert.assertNotNull(rs.getString(1));
            Assert.assertNotNull(rs.getString(2));
        }
        Assert.assertEquals(1,i);
    }

    @Test
    public void testRestrictOnAnythingTableScan() throws Exception{
        ResultSet rs=methodWatcher.executeQuery(format("select * from %s %s",this.getTableReference(TABLE_NAME),"where si like '%'"));
        int count=0;
        while(rs.next()){
            Assert.assertNotNull("a.si is null!",rs.getString(1));
            Assert.assertNotNull("b.si is null!",rs.getString(2));
            count++;
        }
        Assert.assertEquals("Incorrect number of rows returned!",10,count);
    }

    @Test
    public void testResrictWrapperOnTableScan() throws Exception{
        ResultSet rs=methodWatcher.executeQuery(format("select * from %s %s",this.getTableReference(TABLE_NAME),"where si like '%5%'"));
        int i=0;
        while(rs.next()){
            i++;
            Assert.assertNotNull(rs.getString(1));
            Assert.assertNotNull(rs.getString(2));
        }
        Assert.assertEquals(1,i);
    }

    @Test
    public void testProjectRestrictWrapperOnTableScan() throws Exception{
        ResultSet rs=methodWatcher.executeQuery(format("select si || 'Chicken Dumplings' from %s %s",this.getTableReference(TABLE_NAME),"where si like '%5%'"));
        int i=0;
        while(rs.next()){
            i++;
            Assert.assertNotNull(rs.getString(1));
            Assert.assertEquals("String Concatenation Should Match","5"+"Chicken Dumplings",rs.getString(1));
        }
        Assert.assertEquals(1,i);
    }

    @Test
    public void testInPredicate() throws Exception{
        List<String> corrects=Arrays.asList("1","2","4");
        String correctPredicate=Joiner.on("','").join(corrects);
        ResultSet rs=methodWatcher.executeQuery(format("select * from %s where si in ('"+correctPredicate+"')",this.getTableReference(TABLE_NAME)));
        List<String> results=Lists.newArrayList();
        while(rs.next()){
            String si=rs.getString(1);
            String sa=rs.getString(2);
            Assert.assertTrue("incorrect si returned!",corrects.contains(si));
            Assert.assertNotNull("incorrect sa!",sa);
            results.add(String.format("si=%s,sa=%s",si,sa));
        }
        Assert.assertEquals("Incorrect num rows returned!",corrects.size(),results.size());
    }

    @Test
    public void testSumOperator() throws Exception{
        int increment=2;
        ResultSet rs=methodWatcher.executeQuery(format("select sc, sc+"+increment+" from %s",this.getTableReference(TABLE_NAME)));
        List<String> results=Lists.newArrayList();
        while(rs.next()){
            int sc=rs.getInt(1);
            int scIncr=rs.getInt(2);
            Assert.assertEquals("Sum doesn't work!",sc+increment,scIncr);
            results.add(String.format("sc:%d,scIncr:%d",sc,scIncr));
        }
        Assert.assertEquals("Incorrect num rows returned!",10,results.size());
    }

    @Test
    public void testTimesOperator() throws Exception{
        int multiplier=2;
        ResultSet rs=methodWatcher.executeQuery(format("select sc, sc*"+multiplier+" from %s",this.getTableReference(TABLE_NAME)));
        List<String> results=Lists.newArrayList();
        while(rs.next()){
            int sc=rs.getInt(1);
            int scIncr=rs.getInt(2);
            Assert.assertEquals("Sum doesn't work!",sc*multiplier,scIncr);
            results.add(String.format("sc:%d,scIncr:%d",sc,scIncr));
        }
        Assert.assertEquals("Incorrect num rows returned!",10,results.size());
    }

    @Test
    public void testModulusOperator() throws Exception{
        int modulus=2;
        ResultSet rs=methodWatcher.executeQuery(format("select sc, mod(sc,"+modulus+") from %s",this.getTableReference(TABLE_NAME)));
        List<String> results=Lists.newArrayList();
        while(rs.next()){
            int sc=rs.getInt(1);
            int scIncr=rs.getInt(2);
            Assert.assertEquals("Modulus doesn't work!",sc%modulus,scIncr);
            results.add(String.format("sc:%d,scIncr:%d",sc,scIncr));
        }
        Assert.assertEquals("Incorrect num rows returned!",10,results.size());
    }

    @Test
    public void testSubStringOperator() throws Exception{
        ResultSet rs=methodWatcher.executeQuery(format("select sa, substr(sa,1,1) from %s",this.getTableReference(TABLE_NAME)));
        List<String> results=Lists.newArrayList();
        while(rs.next()){
            String sa=rs.getString(1);
            String sub=rs.getString(2);
            String subSa=sa.substring(0,1);
            Assert.assertEquals("Incorrect sub!",subSa,sub);
            results.add(String.format("sa=%s,sub=%s",sa,sub));
        }
        Assert.assertEquals("Incorrect num rows returned!",10,results.size());
    }

    @Test
    public void testConcatOperator() throws Exception{
        String concat="b";
        ResultSet rs=methodWatcher.executeQuery(format("select si,si||'"+concat+"' from %s",this.getTableReference(TABLE_NAME)));
        List<String> results=Lists.newArrayList();
        while(rs.next()){
            String sa=rs.getString(1);
            String concatAttempt=rs.getString(2);
            String correct=sa+concat;
            Assert.assertEquals("Incorrect concat!",correct,concatAttempt);
            results.add(String.format("sa=%s,concatAttempt=%s",sa,concatAttempt));
        }
        Assert.assertEquals("Incorrect num rows returned!",10,results.size());
    }

    @Test
    public void testUpperLowerOperator() throws Exception{
        ResultSet rs=methodWatcher.executeQuery(format("select sa,UPPER(sa),LOWER(sa) from %s",this.getTableReference(TABLE_NAME)));
        List<String> results=Lists.newArrayList();
        while(rs.next()){
            String sa=rs.getString(1);
            String upper=rs.getString(2);
            String lower=rs.getString(3);
            String correctUpper=sa.toUpperCase();
            String correctLower=sa.toLowerCase();
            Assert.assertEquals("upper incorrect",correctUpper,upper);
            Assert.assertEquals("lower incorrect",correctLower,lower);
            results.add(String.format("sa:%s,upper:%s,lower:%s",sa,upper,lower));
        }
        Assert.assertEquals("Incorrect num rows returned!",10,results.size());
    }

    @Test
    public void testConstantIntCompareFalseFilter() throws Exception{
        ResultSet rs=methodWatcher.executeQuery("select * from "+this.getPaddedTableReference("A")+"where 2 < 1");
        Assert.assertFalse("1 or more results were found when none were expected",rs.next());
    }

    @Test
    public void testConstantBooleanFalseFilter() throws Exception{
        ResultSet rs=methodWatcher.executeQuery("select * from "+this.getPaddedTableReference("A")+"where false");
        Assert.assertFalse("1 or more results were found when none were expected",rs.next());
    }

    @Test
    public void testArithmeticOperators() throws Exception{
        ResultSet rs=methodWatcher.executeQuery("select sc,sd,se, sd-sc,sd+sc,sd*sc,se/sd from "+spliceTableWatcher2+" where true");
        List<String> results=Lists.newArrayList();
        while(rs.next()){
            int sc=rs.getInt(1);
            int sd=rs.getInt(2);
            BigDecimal se=rs.getBigDecimal(3);
            int diff=rs.getInt(4);
            int sum=rs.getInt(5);
            int mult=rs.getInt(6);
            BigDecimal div=rs.getBigDecimal(7);

            int correctDiff=sd-sc;
            int correctSum=sd+sc;
            int correctMult=sd*sc;

            BigDecimal correctDiv=se.divide(new BigDecimal(sd),MIN_DECIMAL_DIVIDE_SCALE,BigDecimal.ROUND_DOWN);

            Assert.assertEquals("Incorrect diff!",correctDiff,diff);
            Assert.assertEquals("Incorrect sum!",correctSum,sum);
            Assert.assertEquals("Incorrect mult!",correctMult,mult);
//  SPLICE-898           Assert.assertEquals("Incorrect Div!",correctDiv.compareTo(div),0);


            results.add(String.format("sc=%d,sd=%d,se=%f,diff=%d,sum=%d,mult=%d,div=%f%n",sc,sd,se,diff,sum,mult,div));
        }

        for(String result : results){
            LOG.info(result);
        }
        Assert.assertEquals("incorrect rows returned",10,results.size());
    }

    @Test
    public void testInWithUncorrelatedSubquery() throws Exception{
        Object[][] expected=new Object[][]{{"Alice"}};

        ResultSet rs=methodWatcher.executeQuery("select t1.empname from staff t1 where t1.empnum in "+
                "(select t3.empnum from works t3 where t3.pnum in "+
                "   (select t2.pnum from proj t2 where t2.city='Tampa'))");
        List results=TestUtils.resultSetToArrays(rs);

        Assert.assertArrayEquals(expected,results.toArray());
    }
}
