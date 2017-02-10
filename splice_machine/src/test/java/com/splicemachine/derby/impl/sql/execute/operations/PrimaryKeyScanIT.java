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

import org.spark_project.guava.collect.Lists;
import org.spark_project.guava.collect.Maps;
import com.splicemachine.derby.test.framework.*;
import com.splicemachine.utils.Pair;
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
import java.util.List;
import java.util.Map;

/**
 * @author Scott Fines
 *         Created on: 3/6/13
 */
public class PrimaryKeyScanIT extends SpliceUnitTest{
    protected static SpliceWatcher spliceClassWatcher=new SpliceWatcher();
    public static final String CLASS_NAME=PrimaryKeyScanIT.class.getSimpleName().toUpperCase();
    public static final String TABLE_NAME="A";
    protected static SpliceSchemaWatcher spliceSchemaWatcher=new SpliceSchemaWatcher(CLASS_NAME);
    protected static SpliceTableWatcher spliceTableWatcher=new SpliceTableWatcher(TABLE_NAME,CLASS_NAME,"(pk_1 varchar(50), pk_2 varchar(50),val int, PRIMARY KEY(pk_1,pk_2))");
    protected static SpliceTableWatcher spliceTableWatcher2=new SpliceTableWatcher("B",CLASS_NAME,
            "(pk_1 varchar(50),pk_2 varchar(50), val int, PRIMARY KEY(pk_2,pk_1))");

    protected static String INSERT_VALUES=String.format("insert into %s.%s (pk_1, pk_2,val) values (?,?,?)",CLASS_NAME,TABLE_NAME);
    protected static String INSERT_VALUES2=String.format("insert into %s.%s (pk_1, pk_2,val) values (?,?,?)",CLASS_NAME,"B");

    @ClassRule
    public static TestRule chain=RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher)
            .around(spliceTableWatcher2)
            .around(new SpliceDataWatcher(){
                @Override
                protected void starting(Description description){
                    try{
                        PreparedStatement ps=spliceClassWatcher.prepareStatement(INSERT_VALUES);
                        for(int i=0;i<pk1Size;i++){
                            String pk1="pk_1_"+i;
                            for(int j=0;j<pk2Size;j++){
                                String pk2="pk_2_"+j;
                                int val=i;
                                ps.setString(1,pk1);
                                ps.setString(2,pk2);
                                ps.setInt(3,val);
                                ps.executeUpdate();
                                correctData.put(Pair.newPair(pk1,pk2),val);
                            }
                        }

                        ps=spliceClassWatcher.prepareStatement(INSERT_VALUES2);
                        for(int i=0;i<pk1Size;i++){
                            String pk1="pk_1_"+i;
                            for(int j=0;j<pk2Size;j++){
                                String pk2="pk_2_"+j;
                                int val=i;
                                ps.setString(1,pk1);
                                ps.setString(2,pk2);
                                ps.setInt(3,val);
                                ps.executeUpdate();
                            }
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

    private static final Map<Pair<String, String>, Integer> correctData=Maps.newHashMap();
    private static final int pk1Size=11;
    private static final int pk2Size=11;

    /*
    @Test
    public void testLookupSpeed() throws Exception{
    	PreparedStatement ps = methodWatcher.prepareStatement(format("select * from %s where c_w_id = ? and c_d_id = ? and c_id = ?",this.getTableReference("CUSTOMER")));
    	for (int i = 0; i<5000; i++) {
    		ps.setInt(1, i);
    		ps.setInt(2, i);
    		ps.setInt(3, i);
    		ResultSet rs = ps.executeQuery();
    		while (rs.next()) {
    			
    		}
    	}
    }
	*/

    @Test
    public void testCountAllData() throws Exception{
        ResultSet rs=methodWatcher.executeQuery(format("select count(*) from %s",this.getTableReference(TABLE_NAME)));
        Assert.assertTrue("No Results returned!",rs.next());
        Assert.assertEquals("Incorrect count returned",pk1Size*pk2Size,rs.getInt(1));
    }

    @Test
    public void testScanAllData() throws Exception{
        ResultSet rs=methodWatcher.executeQuery(format("select * from %s",this.getTableReference(TABLE_NAME)));
        List<String> results=Lists.newArrayListWithExpectedSize(correctData.size());
        while(rs.next()){
            String pk1=rs.getString(1);
            String pk2=rs.getString(2);
            int val=rs.getInt(3);
            Pair<String, String> pair=Pair.newPair(pk1,pk2);
            Assert.assertTrue("could not find pair!",correctData.containsKey(pair));
            Assert.assertEquals("Incorrect value for pair!",correctData.get(pair).intValue(),val);
            results.add(String.format("pk_1: %s,pk_2: %s, val:%d",pk1,pk2,val));
        }
        Assert.assertEquals("Incorrect number of rows returned!",correctData.size(),results.size());
    }

    @Test
    public void testScanForNullEntries() throws Exception{
        ResultSet rs=methodWatcher.executeQuery(format("select * from %s where pk_1 is null",this.getTableReference(TABLE_NAME)));
        Assert.assertTrue("Expected 0 rows returned!",!rs.next());
    }

    @Test
    public void testPkOneQualifiedScan() throws Exception{
        ResultSet rs=methodWatcher.executeQuery(format("select * from %s where pk_1 = 'pk_1_1'",this.getTableReference(TABLE_NAME)));
        List<String> results=Lists.newArrayListWithExpectedSize(1);
        while(rs.next()){
            String pk1=rs.getString(1);
            String pk2=rs.getString(2);
            int val=rs.getInt(3);
            Pair<String, String> pair=Pair.newPair(pk1,pk2);
            Assert.assertTrue("could not find pair!",correctData.containsKey(pair));
            Assert.assertEquals("Incorrect value for pair!",correctData.get(pair).intValue(),val);
            results.add(String.format("pk_1: %s,pk_2: %s, val:%d",pk1,pk2,val));
        }
        Assert.assertEquals("Incorrect number of rows returned!",pk2Size,results.size());
    }

    @Test
    public void testPkTwoQualifiedScanWithTwoQualifiers() throws Exception{
        ResultSet rs=methodWatcher.executeQuery(format("select * from %s where pk_1 = 'pk_1_1' and pk_2 = 'pk_2_1'",this.getTableReference(TABLE_NAME)));
        List<String> results=Lists.newArrayListWithExpectedSize(1);
        while(rs.next()){
            String pk1=rs.getString(1);
            String pk2=rs.getString(2);
            int val=rs.getInt(3);
            Pair<String, String> pair=Pair.newPair(pk1,pk2);
            Assert.assertTrue("could not find pair!",correctData.containsKey(pair));
            Assert.assertEquals("Incorrect value for pair!",correctData.get(pair).intValue(),val);
            results.add(String.format("pk_1: %s,pk_2: %s, val:%d",pk1,pk2,val));
        }
        Assert.assertEquals("Incorrect number of rows returned!",1,results.size());
    }

    @Test
    public void testPkTwoQualifiedScan() throws Exception{
        ResultSet rs=methodWatcher.executeQuery(format("select * from %s where pk_2 = 'pk_2_1'",this.getTableReference(TABLE_NAME)));
        List<String> results=Lists.newArrayListWithExpectedSize(1);
        while(rs.next()){
            String pk1=rs.getString(1);
            String pk2=rs.getString(2);
            int val=rs.getInt(3);
            Pair<String, String> pair=Pair.newPair(pk1,pk2);
            Assert.assertTrue("could not find pair!",correctData.containsKey(pair));
            Assert.assertEquals("Incorrect value for pair!",correctData.get(pair).intValue(),val);
            results.add(String.format("pk_1: %s,pk_2: %s, val:%d",pk1,pk2,val));
        }
        Assert.assertEquals("Incorrect number of rows returned!",pk1Size,results.size());
    }

    @Test
    public void testRestrictedScan() throws Exception{
        ResultSet rs=methodWatcher.executeQuery(format("select pk_1,val from %s where pk_1 = 'pk_1_1'",this.getTableReference(TABLE_NAME)));
        List<String> results=Lists.newArrayListWithExpectedSize(1);
        while(rs.next()){
            String pk1=rs.getString(1);
            int val=rs.getInt(2);
            results.add(String.format("pk_1: %s,val:%d",pk1,val));
        }
        Assert.assertEquals("Incorrect number of rows returned!",pk2Size,results.size());
    }

    @Test
    public void testRestrictedScanOnOutOfOrderPks() throws Exception{
        ResultSet rs=methodWatcher.executeQuery(format("select pk_2,val from %s where pk_2 = 'pk_2_1'",spliceTableWatcher2));
        List<String> results=Lists.newArrayListWithExpectedSize(1);
        while(rs.next()){
            String pk2=rs.getString(1);
            int val=rs.getInt(2);
            results.add(String.format("pk_2: %s,val:%d",pk2,val));
        }
        Assert.assertEquals("Incorrect number of rows returned!",pk2Size,results.size());
    }

    @Test
    public void testScanWithGreaterThanOperator() throws Exception{
        ResultSet rs=methodWatcher.executeQuery(format("select * from %s where pk_1 > 'pk_1_0'",this.getTableReference(TABLE_NAME)));
        List<String> results=Lists.newArrayListWithExpectedSize(1);
        while(rs.next()){
            String pk1=rs.getString(1);
            String pk2=rs.getString(2);
            int val=rs.getInt(3);
            Pair<String, String> pair=Pair.newPair(pk1,pk2);
            Assert.assertTrue("could not find pair!",correctData.containsKey(pair));
            Assert.assertEquals("Incorrect value for pair!",correctData.get(pair).intValue(),val);
            results.add(String.format("pk_1: %s,pk_2: %s, val:%d",pk1,pk2,val));
        }
        Assert.assertEquals("Incorrect number of rows returned!",pk2Size*(pk1Size-1),results.size());
    }


    @Test
    public void testScanWithGreaterThanOrEqualsOperator() throws Exception{
        ResultSet rs=methodWatcher.executeQuery(format("select * from %s where pk_1 >= 'pk_1_1'",this.getTableReference(TABLE_NAME)));
        List<String> results=Lists.newArrayListWithExpectedSize(1);
        while(rs.next()){
            String pk1=rs.getString(1);
            String pk2=rs.getString(2);
            int val=rs.getInt(3);
            Pair<String, String> pair=Pair.newPair(pk1,pk2);
            Assert.assertTrue("could not find pair!",correctData.containsKey(pair));
            Assert.assertEquals("Incorrect value for pair!",correctData.get(pair).intValue(),val);
            results.add(String.format("pk_1: %s,pk_2: %s, val:%d",pk1,pk2,val));
        }
        Assert.assertEquals("Incorrect number of rows returned!",pk2Size*(pk1Size-1),results.size());
    }

    @Test
    public void testScanWithLessThanOperator() throws Exception{
        ResultSet rs=methodWatcher.executeQuery(format("select * from %s where pk_1 < 'pk_1_"+(pk1Size-2)+"'",this.getTableReference(TABLE_NAME)));
        List<String> results=Lists.newArrayListWithExpectedSize(1);
        while(rs.next()){
            String pk1=rs.getString(1);
            String pk2=rs.getString(2);
            int val=rs.getInt(3);
            Pair<String, String> pair=Pair.newPair(pk1,pk2);
            Assert.assertTrue("could not find pair!",correctData.containsKey(pair));
            Assert.assertEquals("Incorrect value for pair!",correctData.get(pair).intValue(),val);
            results.add(String.format("pk_1: %s,pk_2: %s, val:%d",pk1,pk2,val));
        }
        Assert.assertEquals("Incorrect number of rows returned!",pk2Size*(pk1Size-1),results.size());
    }

    @Test
    public void testScanWithLessThanOrEqualsOperator() throws Exception{
        ResultSet rs=methodWatcher.executeQuery(format("select * from %s where pk_1<= 'pk_1_"+(pk1Size-2)+"'",this.getTableReference(TABLE_NAME)));
        List<String> results=Lists.newArrayListWithExpectedSize(1);
        while(rs.next()){
            String pk1=rs.getString(1);
            String pk2=rs.getString(2);
            int val=rs.getInt(3);
            Pair<String, String> pair=Pair.newPair(pk1,pk2);
            Assert.assertTrue("could not find pair!",correctData.containsKey(pair));
            Assert.assertEquals("Incorrect value for pair!",correctData.get(pair).intValue(),val);
            results.add(String.format("pk_1: %s,pk_2: %s, val:%d",pk1,pk2,val));
        }
        Assert.assertEquals("Incorrect number of rows returned!",pk2Size*pk1Size,results.size());
    }

    @Test
    public void testScanGreaterThanLessEquals() throws Exception{
        ResultSet rs=methodWatcher.executeQuery(format("select * from %s where pk_1> 'pk_1_0' and pk_1 <='pk_1_"+(pk1Size-2)+"'",this.getTableReference(TABLE_NAME)));
        List<String> results=Lists.newArrayListWithExpectedSize(1);
        while(rs.next()){
            String pk1=rs.getString(1);
            String pk2=rs.getString(2);
            int val=rs.getInt(3);
            Pair<String, String> pair=Pair.newPair(pk1,pk2);
            Assert.assertTrue("could not find pair!",correctData.containsKey(pair));
            Assert.assertEquals("Incorrect value for pair!",correctData.get(pair).intValue(),val);
            results.add(String.format("pk_1: %s,pk_2: %s, val:%d",pk1,pk2,val));
        }
        Assert.assertEquals("Incorrect number of rows returned!",pk2Size*(pk1Size-1),results.size());
    }

    @Test
    public void testScanGreaterEqualLessEquals() throws Exception{
        ResultSet rs=methodWatcher.executeQuery(format("select * from %s where pk_1>= 'pk_1_1' and pk_1 <='pk_1_"+(pk1Size-3)+"'",this.getTableReference(TABLE_NAME)));
        List<String> results=Lists.newArrayListWithExpectedSize(1);
        while(rs.next()){
            String pk1=rs.getString(1);
            String pk2=rs.getString(2);
            int val=rs.getInt(3);
            Pair<String, String> pair=Pair.newPair(pk1,pk2);
            Assert.assertTrue("could not find pair!",correctData.containsKey(pair));
            Assert.assertEquals("Incorrect value for pair!",correctData.get(pair).intValue(),val);
            results.add(String.format("pk_1: %s,pk_2: %s, val:%d",pk1,pk2,val));
        }
        Assert.assertEquals("Incorrect number of rows returned!",pk2Size*(pk1Size-2),results.size());
    }

    @Test
    public void testScanGreaterThanLess() throws Exception{
        ResultSet rs=methodWatcher.executeQuery(format("select * from %s where pk_1> 'pk_1_0' and pk_1 <'pk_1_"+(pk1Size-2)+"'",this.getTableReference(TABLE_NAME)));
        List<String> results=Lists.newArrayListWithExpectedSize(1);
        while(rs.next()){
            String pk1=rs.getString(1);
            String pk2=rs.getString(2);
            int val=rs.getInt(3);
            Pair<String, String> pair=Pair.newPair(pk1,pk2);
            Assert.assertTrue("could not find pair!",correctData.containsKey(pair));
            Assert.assertEquals("Incorrect value for pair!",correctData.get(pair).intValue(),val);
            results.add(String.format("pk_1: %s,pk_2: %s, val:%d",pk1,pk2,val));
        }
        Assert.assertEquals("Incorrect number of rows returned!",pk2Size*(pk1Size-2),results.size());
    }

    @Test
    public void testScanGreaterEqualsLess() throws Exception{
        ResultSet rs=methodWatcher.executeQuery(format("select * from %s where pk_1>= 'pk_1_1' and pk_1 <'pk_1_"+(pk1Size-2)+"'",this.getTableReference(TABLE_NAME)));
        List<String> results=Lists.newArrayListWithExpectedSize(1);
        while(rs.next()){
            String pk1=rs.getString(1);
            String pk2=rs.getString(2);
            int val=rs.getInt(3);
            Pair<String, String> pair=Pair.newPair(pk1,pk2);
            Assert.assertTrue("could not find pair!",correctData.containsKey(pair));
            Assert.assertEquals("Incorrect value for pair!",correctData.get(pair).intValue(),val);
            results.add(String.format("pk_1: %s,pk_2: %s, val:%d",pk1,pk2,val));
        }
        Assert.assertEquals("Incorrect number of rows returned!",pk2Size*(pk1Size-2),results.size());
    }
}
