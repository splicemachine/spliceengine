package com.splicemachine.derby.impl.sql.execute.action;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.derby.test.DerbyTestRule;
import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * @author Scott Fines
 *         Created on: 3/7/13
 */
public class UniqueIndexTest {
    private static final Logger LOG = Logger.getLogger(UniqueIndexTest.class);

    private static final Map<String,String> tableMap = Maps.newHashMap();
    static{
        tableMap.put("t","name varchar(40), val int");
    }

    @Rule public DerbyTestRule rule = new DerbyTestRule(tableMap,LOG);

    @BeforeClass
    public static void startup() throws Exception{
        DerbyTestRule.start();
    }

    @AfterClass
    public static void shutdown() throws Exception{
        DerbyTestRule.shutdown();
    }

    @Test
    public void testCanUseUniqueIndex() throws Exception{
        /*
         * Basic test to ensure that a Unique Index can be used
         * to perform lookups.
         *
         * We create the Index BEFORE we add data, to ensure that
         * we don't deal with any kind of situation which might
         * arise from adding the index after data exists
         *
         * Basically, create an index, then add some data to the table,
         * then scan for data through the index and make sure that the
         * correct data returns.
         */
        //create the index
        rule.getStatement().execute("create unique index t_name on t (name)");

        //now add some data
        String name = "sfines";
        int value = 2;
        rule.getStatement().execute("insert into t (name,val) values ('"+name+"',"+value+")");

        //now check that we can get data out for the proper key
        ResultSet resultSet = rule.executeQuery("select * from t where name = '" + name + "'");
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while(resultSet.next()){
            String retName = resultSet.getString(1);
            int val = resultSet.getInt(2);
            Assert.assertEquals("Incorrect name returned!",name,retName);
            Assert.assertEquals("Incorrect value returned!",value,val);
            results.add(String.format("name:%s,value:%d",retName,val));
        }
        for(String result:results){
            LOG.info(result);
        }
        Assert.assertEquals("Incorrect number of rows returned!",1,results.size());
    }

    @Test
    public void testCanCreateIndexFromExistingData() throws Exception{
        /*
         * Tests that adding an index to an existing data set will
         * result in a correct and consistent index
         *
         * Basically, add some data to the table, then create the index,
         * then perform a lookup on that same data via the index to ensure
         * that the index will find those values.
         */
        String name = "sfines";
        int value =2;
        rule.getStatement().execute("insert into t (name,val) values ('"+name+"',"+value+")");

        //create the index
        rule.getStatement().execute("create unique index t_name on t (name)");

        //now check that we can get data out for the proper key
        ResultSet resultSet = rule.executeQuery("select * from t where name = '" + name + "'");
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while(resultSet.next()){
            String retName = resultSet.getString(1);
            int val = resultSet.getInt(2);
            Assert.assertEquals("Incorrect name returned!",name,retName);
            Assert.assertEquals("Incorrect value returned!",value,val);
            results.add(String.format("name:%s,value:%d",retName,val));
        }
        for(String result:results){
            LOG.info(result);
        }
        Assert.assertEquals("Incorrect number of rows returned!",1,results.size());
    }

    @Test
    public void testCanCreateIndexFromExistingDataAndThenAddData() throws Exception{
        /*
         * Tests that adding an index to an existing data set will
         * result in a correct and consistent index, that we can safely add data to
         *
         * Basically, add some data, create an index off of that, and then
         * add some more data, and check to make sure that the new data shows up as well
         */
        testCanCreateIndexFromExistingData();

        //add some more data
        String name = "jzhang";
        int value =2;
        rule.getStatement().execute("insert into t (name,val) values ('"+name+"',"+value+")");


        //now check that we can get data out for the proper key
        ResultSet resultSet = rule.executeQuery("select * from t where name = '" + name + "'");
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while(resultSet.next()){
            String retName = resultSet.getString(1);
            int val = resultSet.getInt(2);
            Assert.assertEquals("Incorrect name returned!",name,retName);
            Assert.assertEquals("Incorrect value returned!",value,val);
            results.add(String.format("name:%s,value:%d",retName,val));
        }
        for(String result:results){
            LOG.info(result);
        }
        Assert.assertEquals("Incorrect number of rows returned!",1,results.size());
    }

    @Test
    public void testViolateUniqueConstraint() throws Exception{
        /*
         * Tests that the uniqueness constraint is correctly managed.
         *
         * Basically, create an index, add some data, then try and
         * add some duplicate data, and validate that the duplicate
         * data cannot succeed.
         */
        //create the index
        rule.getStatement().execute("create unique index t_name on t (name)");

        //insert data
        String name = "sfines";
        int value =2;
        rule.getStatement().execute("insert into t (name,val) values ('"+name+"',"+value+")");

        //insert it again, and expect a violation
        boolean failed=false;
        try{
            rule.getStatement().execute("insert into t (name,val) values ('"+name+"',"+value+")");
        }catch(SQLException se){
            failed=true;
        }
        Assert.assertTrue("Did not report a duplicate key violation!",failed);
    }

    @Test
    public void testCanDropIndex() throws Exception{
        /*
         * Tests that we can safely drop the index, and constraints
         * will also be dropped.
         *
         * Basically, create an index, add some data, validate
         * that the uniqueness constraint holds, then drop the index
         * and validate that A) the uniqueness constraint doesn't hold
         * and B) the index is no longer used in the lookup.
         */
        //ensure that the uniqueness constraint holds
        testViolateUniqueConstraint();

        //drop the index
        rule.getStatement().execute("drop index t_name");

        //validate that we can add duplicates now
        String name = "sfines";
        int value = 2;
        rule.getStatement().execute("insert into t (name,val) values ('"+name+"',"+value+")");

        ResultSet resultSet = rule.executeQuery("select * from t where name = '" + name + "'");
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while(resultSet.next()){
            String retName = resultSet.getString(1);
            int val = resultSet.getInt(2);
            Assert.assertEquals("Incorrect name returned!",name,retName);
            Assert.assertEquals("Incorrect value returned!",value,val);
            results.add(String.format("name:%s,value:%d",retName,val));
        }
        for(String result:results){
            LOG.info(result);
        }
        Assert.assertEquals("Incorrect number of rows returned!",2,results.size());
    }


}
