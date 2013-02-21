package org.apache.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.derby.test.DerbyTestRule;
import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;

/**
 * @author Scott Fines
 *         Created on: 2/19/13
 */
public class NullableColumnTest {
    private static final Logger LOG = Logger.getLogger(NullableColumnTest.class);

    private static final Map<String,String> tableSchemas = Maps.newHashMap();
    static{
        tableSchemas.put("nulltest","name varchar(40),value int");
        tableSchemas.put("nulltest2","name varchar(40),value int");
    }

    @Rule public static DerbyTestRule rule = new DerbyTestRule(tableSchemas,false,LOG);
    private static final int size = 10;

    @BeforeClass
    public static void startup() throws Exception{
        DerbyTestRule.start();
        rule.createTables() ;
        insertData();
    }

    @AfterClass
    public static void shutdown() throws Exception{
        rule.dropTables();
        DerbyTestRule.shutdown();
    }

    private static void insertData() throws SQLException{
        PreparedStatement ps = rule.prepareStatement("insert into nulltest (name, value) values (?,?)");
        PreparedStatement ps2 = rule.prepareStatement("insert into nulltest2 (name, value) values (?,?)");
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
    }

    @Test
    public void testScanNullableColumns() throws Exception{
        ResultSet rs = rule.executeQuery("select * from nulltest");
        List<String> results = Lists.newArrayList();
        while(rs.next()){
            String name = rs.getString(1);
            Object value = rs.getObject(2);
            Assert.assertNotNull("Name not specified!",name);
            if(value!=null)
                Assert.assertTrue("Wrong type specified!",value instanceof Integer);
            results.add(String.format("%s:%s",name,value!=null? value :  "null"));
        }
        for(String result:results){
            LOG.info(result);
        }

        Assert.assertEquals("Incorrect rows returned!",size,results.size());
    }

    @Test
    public void testGroupByNullableColumns() throws Exception{
        ResultSet rs = rule.executeQuery("select name, count(*) from nulltest2 group by name");
        int rowsReturned =0;
        List<String> results = Lists.newArrayList();
        while(rs.next()){
            String name = rs.getString(1);
            int count = rs.getInt(2);
            Assert.assertTrue("Incorrect count returned",count>0);
            results.add(String.format("%s:%d",name,count));
            rowsReturned++;
        }
        for(String result:results){
            LOG.info(result);
        }

        Assert.assertEquals("Incorrect rows returned!",2,rowsReturned);
    }

    @Test
    public void testCountNullableColumns() throws Exception{
        ResultSet rs = rule.executeQuery("select name, count(*) from nulltest group by name");
        int rowsReturned =0;
        while(rs.next()){
            String name = rs.getString(1);
            int count = rs.getInt(2);
            Assert.assertNotNull("Name not specified!",name);
            Assert.assertEquals("Incorrect count returned",size,count);
            LOG.info(String.format("%s:%d",name,count));
            rowsReturned++;
        }

        Assert.assertEquals("Incorrect rows returned!",1,rowsReturned);
    }

}
