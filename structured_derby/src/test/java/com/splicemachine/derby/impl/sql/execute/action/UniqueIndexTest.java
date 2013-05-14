package com.splicemachine.derby.impl.sql.execute.action;

import com.google.common.collect.Lists;
import com.splicemachine.derby.test.framework.SpliceIndexWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * @author Scott Fines
 *         Created on: 3/7/13
 */
public class UniqueIndexTest extends SpliceUnitTest {
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	public static final String CLASS_NAME = UniqueIndexTest.class.getSimpleName().toUpperCase();
	public static final String TABLE_NAME_1 = "A";
	public static final String TABLE_NAME_2 = "B";
	public static final String TABLE_NAME_3 = "C";
	public static final String TABLE_NAME_4 = "D";
	public static final String TABLE_NAME_5 = "E";
	public static final String TABLE_NAME_6 = "F";
	public static final String TABLE_NAME_7 = "G";
    public static final String TABLE_NAME_8 = "H";
	public static final String INDEX_11 = "IDX_A1";
	public static final String INDEX_21 = "IDX_B1";
	public static final String INDEX_31 = "IDX_C1";
	public static final String INDEX_41 = "IDX_D1";
	public static final String INDEX_51 = "IDX_E1";
	public static final String INDEX_61 = "IDX_F1";
	public static final String INDEX_71 = "IDX_G1";
    public static final String INDEX_81 = "IDX_H1";
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
	protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_NAME_1,CLASS_NAME,"(name varchar(40), val int)");
	protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE_NAME_2,CLASS_NAME,"(name varchar(40), val int)");
	protected static SpliceTableWatcher spliceTableWatcher3 = new SpliceTableWatcher(TABLE_NAME_3,CLASS_NAME,"(name varchar(40), val int)");
	protected static SpliceTableWatcher spliceTableWatcher4 = new SpliceTableWatcher(TABLE_NAME_4,CLASS_NAME,"(name varchar(40), val int)");
	protected static SpliceTableWatcher spliceTableWatcher5 = new SpliceTableWatcher(TABLE_NAME_5,CLASS_NAME,"(name varchar(40), val int)");
	protected static SpliceTableWatcher spliceTableWatcher6 = new SpliceTableWatcher(TABLE_NAME_6,CLASS_NAME,"(name varchar(40), val int)");
	protected static SpliceTableWatcher spliceTableWatcher7 = new SpliceTableWatcher(TABLE_NAME_7,CLASS_NAME,"(name varchar(40), val int)");
    protected static SpliceTableWatcher uniqueTableWatcher = new SpliceTableWatcher(TABLE_NAME_8,CLASS_NAME,"(name varchar(40), val int, unique(val))");

    @Override
    public String getSchemaName() {
        return CLASS_NAME;
    }

    @ClassRule
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher1)
		.around(spliceTableWatcher2)
		.around(spliceTableWatcher3)
		.around(spliceTableWatcher4)
		.around(spliceTableWatcher5)
		.around(spliceTableWatcher6)
		.around(spliceTableWatcher7);
//        .around(uniqueTableWatcher);

	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

    /**
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
    @Test(timeout=10000)
    public void testCanUseUniqueIndex() throws Exception{
    	new SpliceIndexWatcher(TABLE_NAME_1,CLASS_NAME,INDEX_11,CLASS_NAME,"(name)",true).starting(null);
        //now add some data
        String name = "sfines";
        int value = 2;
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",this.getTableReference(TABLE_NAME_1),name,value));

        //now check that we can get data out for the proper key
        ResultSet resultSet = methodWatcher.executeQuery(format("select * from %s where name = '%s'", this.getTableReference(TABLE_NAME_1),name));
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while(resultSet.next()){
            String retName = resultSet.getString(1);
            int val = resultSet.getInt(2);
            Assert.assertEquals("Incorrect name returned!",name,retName);
            Assert.assertEquals("Incorrect value returned!",value,val);
            results.add(String.format("name:%s,value:%d",retName,val));
        }
        Assert.assertEquals("Incorrect number of rows returned!",1,results.size());
    }
    /**
     * Tests that adding an index to an existing data set will
     * result in a correct and consistent index
     *
     * Basically, add some data to the table, then create the index,
     * then perform a lookup on that same data via the index to ensure
     * that the index will find those values.
     */
    @Test(timeout=10000)
    public void testCanCreateIndexFromExistingData() throws Exception{
        String name = "sfines";
        int value =2;
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",this.getTableReference(TABLE_NAME_2),name,value));
        //create the index
        new SpliceIndexWatcher(TABLE_NAME_2,CLASS_NAME,INDEX_21,CLASS_NAME,"(name)",true).starting(null);

        //now check that we can get data out for the proper key
        ResultSet resultSet = methodWatcher.executeQuery(format("select * from %s where name = '%s'",this.getTableReference(TABLE_NAME_2),name));
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while(resultSet.next()){
            String retName = resultSet.getString(1);
            int val = resultSet.getInt(2);
            Assert.assertEquals("Incorrect name returned!",name,retName);
            Assert.assertEquals("Incorrect value returned!",value,val);
            results.add(String.format("name:%s,value:%d",retName,val));
        }
        Assert.assertEquals("Incorrect number of rows returned!",1,results.size());
    }
    /**
     * Tests that adding an index to an existing data set will
     * result in a correct and consistent index, that we can safely add data to
     *
     * Basically, add some data, create an index off of that, and then
     * add some more data, and check to make sure that the new data shows up as well
     */
    @Test(timeout=10000)
    public void testCanCreateIndexFromExistingDataAndThenAddData() throws Exception{
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",this.getTableReference(TABLE_NAME_3),"sfines",2));
        //create the index
        new SpliceIndexWatcher(TABLE_NAME_3,CLASS_NAME,INDEX_31,CLASS_NAME,"(name)",true).starting(null);
        //add some more data
        String name = "jzhang";
        int value =2;
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",this.getTableReference(TABLE_NAME_3),name,value));

        //now check that we can get data out for the proper key
        ResultSet resultSet = methodWatcher.executeQuery(format("select * from %s where name = '%s'",this.getTableReference(TABLE_NAME_3),name));
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while(resultSet.next()){
            String retName = resultSet.getString(1);
            int val = resultSet.getInt(2);
            Assert.assertEquals("Incorrect name returned!",name,retName);
            Assert.assertEquals("Incorrect value returned!",value,val);
            results.add(String.format("name:%s,value:%d",retName,val));
        }
        Assert.assertEquals("Incorrect number of rows returned!",1,results.size());
    }
    /**
     * Tests that the uniqueness constraint is correctly managed.
     *
     * Basically, create an index, add some data, then try and
     * add some duplicate data, and validate that the duplicate
     * data cannot succeed.
     */
    @Test(expected=SQLException.class,timeout=10000)
    public void testViolateUniqueConstraint() throws Exception{
        new SpliceIndexWatcher(TABLE_NAME_4,CLASS_NAME,INDEX_41,CLASS_NAME,"(name)",true).starting(null);
        String name = "sfines";
        int value =2;
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",this.getTableReference(TABLE_NAME_4),name,value));
        try{
            methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",this.getTableReference(TABLE_NAME_4),name,value));
        }catch(SQLException se){
            SpliceLogUtils.error(Logger.getLogger(UniqueIndexTest.class), se);
            if(se.getMessage().contains("unique"))
                throw se;
        }
        Assert.assertTrue("Did not report a duplicate key violation!",false);
    }
    /**
     * Tests that we can safely drop the index, and constraints
     * will also be dropped.
     *
     * Basically, create an index, add some data, validate
     * that the uniqueness constraint holds, then drop the index
     * and validate that A) the uniqueness constraint doesn't hold
     * and B) the index is no longer used in the lookup.
     */
    @Test(timeout=10000)
    public void testCanDropIndex() throws Exception{

        //ensure that the uniqueness constraint holds
    	SpliceIndexWatcher indexWatcher = new SpliceIndexWatcher(TABLE_NAME_5,CLASS_NAME,INDEX_51,CLASS_NAME,"(name)",true);
    	indexWatcher.starting(null);
    	String name = "sfines";
        int value =2;
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",this.getTableReference(TABLE_NAME_5),name,value));
        try{
            methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",this.getTableReference(TABLE_NAME_5),name,value));
            Assert.assertTrue("Unqiueness Constraint Violated!",false);
        }catch(SQLException se){
        }
        indexWatcher.finished(null);

        //validate that we can add duplicates now
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",this.getTableReference(TABLE_NAME_5),name,value));

        ResultSet resultSet = methodWatcher.executeQuery(format("select * from %s where name = '%s'",this.getTableReference(TABLE_NAME_5),name));
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while(resultSet.next()){
            String retName = resultSet.getString(1);
            int val = resultSet.getInt(2);
            Assert.assertEquals("Incorrect name returned!",name,retName);
            Assert.assertEquals("Incorrect value returned!",value,val);
            results.add(String.format("name:%s,value:%d",retName,val));
        }
        Assert.assertEquals("Incorrect number of rows returned!",2,results.size());
    }
    /**
     * Tests that we can safely delete a record, and have it
     * percolate through to the index.
     *
     * Basically, create an index, add some data, check if its
     * present, then delete some data and check that it's not
     * there anymore
     */
    @Test(timeout = 10000)
    public void testCanDeleteEntry() throws Exception{
    	new SpliceIndexWatcher(TABLE_NAME_6,CLASS_NAME,INDEX_61,CLASS_NAME,"(name)",true).starting(null);
        String name = "sfines";
        int value = 2;
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",this.getTableReference(TABLE_NAME_6),name,value));
        methodWatcher.getStatement().execute(format("delete from %s where name = '%s'",this.getTableReference(TABLE_NAME_6),name));
        ResultSet rs = methodWatcher.executeQuery(format("select * from %s where name = '%s'",this.getTableReference(TABLE_NAME_6),name));
        Assert.assertTrue("Rows are returned incorrectly",!rs.next());
    }

    @Test(timeout= 10000)
    public void testCanUpdateEntryIndexChanges() throws Exception{
       	new SpliceIndexWatcher(TABLE_NAME_7,CLASS_NAME,INDEX_71,CLASS_NAME,"(name)",true).starting(null);
        String name = "sfines";
        int value = 2;
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",this.getTableReference(TABLE_NAME_7),name,value));
 
        String newName = "jzhang";
        methodWatcher.getStatement().execute(format("update %s set name = '%s' where name = '%s'", this.getTableReference(TABLE_NAME_7),newName,name));

        ResultSet rs = methodWatcher.executeQuery(format("select * from %s where name = '%s'",this.getTableReference(TABLE_NAME_7),name));
        Assert.assertTrue("Rows are returned incorrectly",!rs.next());

        rs = methodWatcher.executeQuery(format("select * from %s where name = '%s'",this.getTableReference(TABLE_NAME_7),newName));
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while(rs.next()){
            String retName = rs.getString(1);
            int val = rs.getInt(2);
            Assert.assertEquals("Incorrect name returned!",newName,retName);
            results.add(String.format("name:%s,value:%d",retName,val));
        }
        Assert.assertEquals("Incorrect number of rows returned!",1,results.size());

    }

    @Test(timeout = 10000)
    @Ignore("Ignored until Bug 332 is resolved")
    public void testUniqueInTableCreationViolatesPrimaryKey() throws Exception{
        String name = "sfines";
        int value =2;
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",this.getTableReference(TABLE_NAME_8),name,value));
        try{
            methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",this.getTableReference(TABLE_NAME_8),name,value));
        }catch(SQLException se){
            throw se;
        }
        Assert.assertTrue("Did not report a duplicate key violation!",false);
    }


}
