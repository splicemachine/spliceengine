package com.splicemachine.derby.impl.sql.execute.action;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.derby.test.DerbyTestRule;
import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * @author Scott Fines
 *         Created on: 3/8/13
 */
public class NonUniqueIndexTest {
    private static final Logger LOG = Logger.getLogger(NonUniqueIndexTest.class);

    private static final Map<String,String> tableMap = Maps.newHashMap();
    static{
        tableMap.put("t","name varchar(40), val int");
        tableMap.put("t2","n_1 varchar(40),n_2 varchar(30),val int");
    }

    @Rule
    public DerbyTestRule rule = new DerbyTestRule(tableMap,LOG);

    @BeforeClass
    public static void startup() throws Exception{
        DerbyTestRule.start();
    }

    @AfterClass
    public static void shutdown() throws Exception{
        DerbyTestRule.shutdown();
    }

    @Test
    public void testCanCreateIndexWithMultipleEntries() throws Exception{
        rule.getStatement().execute("create index t_2_name on t2(n_1,n_2)");
        //now add some data
        PreparedStatement ps = rule.prepareStatement("insert into t2 (n_1,n_2,val) values (?,?,?)");

        String n1 = "sfines";
        ps.setString(1,n1);
        String n2 = "mathematician";
        ps.setString(2,n2);
        int value = 2;
        ps.setInt(3,2);

        ps.execute();

        //now check that we can get data out for the proper key
        PreparedStatement query = rule.prepareStatement("select * from t2 where n_1 = ? and n_2 = ?");
        query.setString(1,n1);
        query.setString(2,n2);
        ResultSet resultSet = query.executeQuery();
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while(resultSet.next()){
            String retN1 = resultSet.getString(1);
            String retN2 = resultSet.getString(2);
            int val = resultSet.getInt(3);
            Assert.assertEquals("Incorrect n1 returned!", n1, retN1);
            Assert.assertEquals("Incorrect n2 returned!", n2, retN2);
            Assert.assertEquals("Incorrect value returned!",value,val);
            results.add(String.format("n1:%s,n2:%s,value:%d",retN1,retN2,val));
        }
        for(String result:results){
            LOG.info(result);
        }
        Assert.assertEquals("Incorrect number of rows returned!",1,results.size());
    }

    @Test
    public void testCanUseIndex() throws Exception{
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
        rule.getStatement().execute("create index t_name on t (name)");

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
            Assert.assertEquals("Incorrect name returned!", name, retName);
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
        rule.getStatement().execute("create index t_name on t (name)");

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
    public void testCanAddDuplicate() throws Exception{

        testCanUseIndex();

        String name = "sfines";
        int value = 3;
        rule.getStatement().execute("insert into t (name,val) values ('"+name+"',"+value+")");

        ResultSet resultSet = rule.executeQuery("select * from t where name = '"+name+"'");
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while(resultSet.next()){
            String retName = resultSet.getString(1);
            int val = resultSet.getInt(2);
            Assert.assertEquals("Incorrect name returned!", name, retName);
            results.add(String.format("name:%s,value:%d",retName,val));
        }
        for(String result:results){
            LOG.info(result);
        }
        Assert.assertEquals("Incorrect number of rows returned!",2,results.size());
    }

    @Test
    public void testCanDeleteEntry() throws Exception{
        testCanAddDuplicate();

        String name = "sfines";
        int value = 2;
        rule.getStatement().execute("delete from t where name = '"+name+"' and val = "+value);

        assertSelectCorrect("sfines",1);

    }

    @Test
    public void testCanUpdateEntryIndexChanges() throws Exception{
        testCanUseIndex();

        String oldName = "sfines";
        String newName = "jzhang";
        rule.getStatement().execute("update t set name = '"+newName+"' where name = '"+oldName+"'");

        ResultSet rs = rule.executeQuery("select * from t where name = '"+ oldName +"'");
        Assert.assertTrue("Rows returned incorrectly",!rs.next());

        assertSelectCorrect(newName,1);
    }

    @Test
    @Ignore("ignored until Bug #309 can be resolved and we can properly drop views")
    public void testCanAddIndexToViewedTable() throws Exception{
        /*
         * Regression test for Bug 149. Confirm that we can add a view and then add an index
         * and stuff
         */
        rule.getStatement().execute("create view t_view as select name,val from t where val > 1");
        try{
            testCanUseIndex();
        }finally{
            rule.getStatement().execute("drop view t_view");
        }
    }

    private void assertSelectCorrect(String name, int size) throws Exception{
        ResultSet resultSet = rule.executeQuery("select * from t where name = '"+name+"'");
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while(resultSet.next()){
            String retName = resultSet.getString(1);
            int val = resultSet.getInt(2);
            Assert.assertEquals("Incorrect name returned!", name, retName);
            results.add(String.format("name:%s,value:%d",retName,val));
        }
        for(String result:results){
            LOG.info(result);
        }
        Assert.assertEquals("Incorrect number of rows returned!",size,results.size());
    }
}
