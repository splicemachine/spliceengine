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
import org.spark_project.guava.collect.Sets;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.Assert;
import org.apache.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.List;
import java.util.Set;

/**
 * @author Scott Fines
 *         Created on: 2/19/13
 */
public class NullableColumnIT extends SpliceUnitTest { 
    private static final Logger LOG = Logger.getLogger(NullableColumnIT.class);
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	public static final String CLASS_NAME = NullableColumnIT.class.getSimpleName().toUpperCase();
	public static final String TABLE_NAME_1 = "A";
	public static final String TABLE_NAME_2 = "B";
    private static final int size = 10;
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);	
	protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_NAME_1,CLASS_NAME,"(name varchar(40),value int)");
	protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE_NAME_2,CLASS_NAME,"(name varchar(40),value int)");
	
	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher1)
		.around(spliceTableWatcher2)
		.around(new SpliceDataWatcher(){
			@Override
			protected void starting(Description description) {
				try {
					 PreparedStatement ps = spliceClassWatcher.prepareStatement(String.format("insert into %s.%s (name, value) values (?,?)",CLASS_NAME,TABLE_NAME_1));
				        PreparedStatement ps2 = spliceClassWatcher.prepareStatement(String.format("insert into %s.%s (name, value) values (?,?)",CLASS_NAME,TABLE_NAME_2));
				        for(int i=0;i<size;i++){
				            ps.setString(1,"sfines");
				            if(i%2==0)
				                ps.setNull(2, Types.INTEGER);
				            else
				                ps.setInt(2,i);
				            ps.executeUpdate();

				            if(i%3==0)
				                ps2.setNull(1,Types.VARCHAR);
				            else
				                ps2.setString(1,"jzhang");
				            ps2.setInt(2,i);
				            ps2.executeUpdate();
				        }
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				finally {
					spliceClassWatcher.closeAll();
				}
			}
			
		});
	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testScanNullableColumns() throws Exception{
        ResultSet rs = methodWatcher.executeQuery(format("select * from %s",this.getTableReference(TABLE_NAME_1)));
        List<String> results = Lists.newArrayList();
        while(rs.next()){
            String name = rs.getString(1);
            Object value = rs.getObject(2);
            Assert.assertNotNull("Name not specified!",name);
            if(value!=null)
                Assert.assertTrue("Wrong type specified!",value instanceof Integer);
            results.add(String.format("%s:%s",name,value!=null? value :  "null"));
        }
        Assert.assertEquals("Incorrect rows returned!",size,results.size());
    }

    @Test
    public void testGroupByNullableColumns() throws Exception{
        ResultSet rs = methodWatcher.executeQuery(format("select name, count(*) from %s group by name",this.getTableReference(TABLE_NAME_2)));
        int rowsReturned =0;
        List<String> results = Lists.newArrayList();
        while(rs.next()){
            String name = rs.getString(1);
            int count = rs.getInt(2);
            Assert.assertTrue("Incorrect count returned",count>0);
            results.add(String.format("%s:%d",name,count));
            rowsReturned++;
        }
        Assert.assertEquals("Incorrect rows returned!",2,rowsReturned);
    }

    @Test
    public void testCountNullableColumns() throws Exception{
        ResultSet rs = methodWatcher.executeQuery(format("select name, count(*) from %s group by name",this.getTableReference(TABLE_NAME_1)));
        int rowsReturned =0;
        while(rs.next()){
            String name = rs.getString(1);
            int count = rs.getInt(2);
            Assert.assertNotNull("Name not specified!",name);
            Assert.assertEquals("Incorrect count returned",size,count);
            rowsReturned++;
        }
        Assert.assertEquals("Incorrect rows returned!",1,rowsReturned);
    }

    @Test
    public void testWildcardWhereNull() throws Exception{
        ResultSet rs = methodWatcher.executeQuery(format("select * from %s where name is null", this.getTableReference(TABLE_NAME_2)));
        Set<Integer> correctResults = Sets.newHashSet(0,3,6,9);
        int rowsReturned =0;
        while(rs.next()){
            String name = rs.getString(1);
            int value = rs.getInt(2);
            Assert.assertNull(name);
            Assert.assertTrue("value not in correct results: "+ value,correctResults.contains(value));
            System.out.printf("name=%s,value=%d%n",name,value);
            rowsReturned++;
        }
        Assert.assertEquals("Incorrect rows returned!", 4, rowsReturned);
    }

    @Test
    public void testSingleColumnWhereNull() throws Exception{
        ResultSet rs = methodWatcher.executeQuery(format("select name from %s where name is null", this.getTableReference(TABLE_NAME_2)));
        int rowsReturned =0;
        while(rs.next()){
            String name = rs.getString(1);
            Assert.assertNull(name);
            rowsReturned++;
        }
        Assert.assertEquals("Incorrect rows returned!", 4, rowsReturned);
    }

}
