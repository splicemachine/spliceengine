package com.splicemachine.derby.impl.sql.execute.actions;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import com.splicemachine.derby.test.framework.SpliceIndexWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.utils.ErrorState;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * @author Scott Fines
 *         Created on: 3/7/13
 */
public class UniqueIndexIT extends SpliceUnitTest { 
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	public static final String CLASS_NAME = UniqueIndexIT.class.getSimpleName().toUpperCase();
	public static final String TABLE_NAME_1 = "A";
	public static final String TABLE_NAME_2 = "B";
	public static final String TABLE_NAME_3 = "C";
	public static final String TABLE_NAME_4 = "D";
	public static final String TABLE_NAME_5 = "E";
	public static final String TABLE_NAME_6 = "F";
	public static final String TABLE_NAME_7 = "G";
    public static final String TABLE_NAME_8 = "H";
    public static final String TABLE_NAME_9 = "I";
    public static final String TABLE_NAME_10 = "J";
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
    protected static SpliceTableWatcher uniqueTableWatcher8 = new SpliceTableWatcher(TABLE_NAME_8,CLASS_NAME,"(name varchar(40), val int, constraint FOO unique(val))");
    protected static SpliceTableWatcher spliceTableWatcher9 = new SpliceTableWatcher(TABLE_NAME_9,CLASS_NAME,"(name varchar(40), val int)");
    protected static SpliceTableWatcher spliceTableWatcher10 = new SpliceTableWatcher(TABLE_NAME_10,CLASS_NAME,"(name varchar(40), val int)");

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
        .around(spliceTableWatcher7)
        .around(uniqueTableWatcher8)
        .around(spliceTableWatcher9)
        .around(spliceTableWatcher10);

	
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
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
                                                    this.getTableReference(TABLE_NAME_1), name, value));

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
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)", this.getTableReference(TABLE_NAME_2), name, value));
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
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)", this.getTableReference(TABLE_NAME_3), "sfines", 2));
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
            SpliceLogUtils.error(Logger.getLogger(UniqueIndexIT.class),se); 
            if(se.getMessage().contains("unique"))
                throw se;
        }
        Assert.fail("Did not report a duplicate key violation!");
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
            Assert.fail("Uniqueness constraint violated");
        }catch(SQLException se){
            Assert.assertEquals(ErrorState.LANG_DUPLICATE_KEY_CONSTRAINT.getSqlState(),se.getSQLState());
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
        Assert.assertEquals("Incorrect number of rows returned!", 2, results.size());
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

    /**
     * DB-1020
     * Tests that we can safely delete a record, and have it
     * percolate through to the index.
     *
     * NULL values should NOT be deleted.
     */
    @Test
    public void testCanDeleteIndexWithNulls() throws Exception{
        new SpliceIndexWatcher(TABLE_NAME_9,CLASS_NAME,INDEX_61,CLASS_NAME,"(val)",false).starting(null);
				PreparedStatement ps = methodWatcher.prepareStatement(String.format("insert into %s (name,val) values (?,?)",spliceTableWatcher9));
				ps.setString(1,"sfines");
				ps.setInt(2,2);
				ps.addBatch();
				ps.setString(1,"lfines");
				ps.setInt(2,-2);
				ps.addBatch();
				ps.setNull(1, Types.VARCHAR);
				ps.setNull(2,Types.INTEGER);
				ps.addBatch();
				ps.setString(1,"0");
				ps.setInt(2,0);
				ps.addBatch();
				int[] updated = ps.executeBatch();
				Assert.assertEquals("Incorrect update number!",4,updated.length);
				System.out.println(Arrays.toString(updated));
//				methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
//                                                    this.getTableReference(TABLE_NAME_9), "sfines", 2));
//        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
//                                                    this.getTableReference(TABLE_NAME_9), "lfines", -2));
//        methodWatcher.getStatement().execute(format("insert into %s (name,val) values (null,null)",
//                                                    this.getTableReference(TABLE_NAME_9)));
//        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('0',0)",
//                                                    this.getTableReference(TABLE_NAME_9)));

        String query = format("select * from %s",this.getTableReference(TABLE_NAME_9));
        ResultSet rs = methodWatcher.executeQuery(query);
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
        Assert.assertEquals(fr.toString(), 4, fr.size());

        methodWatcher.getStatement().execute(format("delete from %s where val > 0",this.getTableReference(TABLE_NAME_9)));
        rs = methodWatcher.executeQuery(query);

        fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
        Assert.assertEquals(fr.toString(), 3, fr.size());

        methodWatcher.getStatement().execute(format("delete from %s where val < 0",this.getTableReference(TABLE_NAME_9)));
        rs = methodWatcher.executeQuery(query);

        fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
        Assert.assertEquals(fr.toString(), 2, fr.size());
    }

    /**
     * DB-1020
     * Tests that we can safely delete a record, and have it
     * percolate through to the unique index.
     *
     * NULL values should NOT be deleted.
     */
    @Test
    public void testCanDeleteUniqueIndexWithoutNulls() throws Exception{
        new SpliceIndexWatcher(TABLE_NAME_10,CLASS_NAME,INDEX_61,CLASS_NAME,"(val)",true).starting(null);
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
                                                    this.getTableReference(TABLE_NAME_10), "sfines", -2));
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
                                                    this.getTableReference(TABLE_NAME_10), "lfines", 2));
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values (null,null)",
                                                    this.getTableReference(TABLE_NAME_10)));
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('0',0)",
                                                    this.getTableReference(TABLE_NAME_10)));

        String query = format("select * from %s",this.getTableReference(TABLE_NAME_10));
        ResultSet rs = methodWatcher.executeQuery(query);
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
//        System.out.println("1:");
//        System.out.println(fr.toString());
        Assert.assertEquals(fr.toString(), 4, fr.size());

        methodWatcher.getStatement().execute(format("delete from %s where val > 0",this.getTableReference(TABLE_NAME_10)));
        rs = methodWatcher.executeQuery(query);

        fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
//        System.out.println("2:");
//        System.out.println(fr.toString());
        Assert.assertEquals(fr.toString(), 3, fr.size());

        methodWatcher.getStatement().execute(format("delete from %s where val < 0",this.getTableReference(TABLE_NAME_10)));
        rs = methodWatcher.executeQuery(query);

        fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
//        System.out.println("3:");
//        System.out.println(fr.toString());
        Assert.assertEquals(fr.toString(), 2, fr.size());
    }

    /**
     * DB-1092
     * Tests that we can delete a row with a non-null value in unique index column then insert same row.
     *
     */
    @Test
    public void testCanDeleteUniqueIndexWithoutNulls2() throws Exception{
        String indexName = "IDX2";
        String tableName = "T2";
        new MyWatcher(tableName,CLASS_NAME, "(name varchar(40), val int)").create(null);
        new SpliceIndexWatcher(tableName,CLASS_NAME,indexName,CLASS_NAME,"(val)",true).starting(null);
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
                                                    this.getTableReference(tableName), "sfines", -2));
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
                                                    this.getTableReference(tableName), "lfines", 2));
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
                                                    this.getTableReference(tableName), "MyNull",null));
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('0',0)",
                                                    this.getTableReference(tableName)));

        String query = format("select * from %s",this.getTableReference(tableName));
        ResultSet rs = methodWatcher.executeQuery(query);
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
        Assert.assertEquals(fr.toString(), 4, fr.size());
//        System.out.println("1:");
//        System.out.println(fr.toString());
//
//        System.out.println(TestUtils.indexQuery(methodWatcher.getOrCreateConnection(), CLASS_NAME, tableName));

        methodWatcher.getStatement().execute(format("delete from %s", this.getTableReference(tableName)));
        rs = methodWatcher.executeQuery(query);
        fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
        Assert.assertEquals(fr.toString(), 0, fr.size());
//        System.out.println("2:");
//        System.out.println(fr.toString());

        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
                                                    this.getTableReference(tableName), "0",0));
        rs = methodWatcher.executeQuery(query);
        fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
        Assert.assertEquals(fr.toString(), 1, fr.size());
//        System.out.println("3:");
//        System.out.println(fr.toString());

        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
                                                    this.getTableReference(tableName), "sfines",-2));
        rs = methodWatcher.executeQuery(query);
        fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
        Assert.assertEquals(fr.toString(), 2, fr.size());
//        System.out.println("4:");
//        System.out.println(fr.toString());
    }

    /**
     * DB-1092
     * Tests that we can delete a row with null in unique index column then insert same row.
     *
     */
    @Test
    public void testCanDeleteUniqueIndexWithNulls() throws Exception{
        String indexName = "IDX1";
        String tableName = "T1";
        new MyWatcher(tableName,CLASS_NAME, "(name varchar(40), val int)").create(null);
        new SpliceIndexWatcher(tableName,CLASS_NAME,indexName,CLASS_NAME,"(val)",true).starting(null);
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
                                                    this.getTableReference(tableName), "sfines", -2));
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
                                                    this.getTableReference(tableName), "lfines", 2));
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
                                                    this.getTableReference(tableName), "MyNull",null));
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('0',0)",
                                                    this.getTableReference(tableName)));

        String query = format("select * from %s",this.getTableReference(tableName));
        ResultSet rs = methodWatcher.executeQuery(query);
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
        Assert.assertEquals(fr.toString(), 4, fr.size());
//        System.out.println("1:");
//        System.out.println(fr.toString());
//
//        System.out.println(TestUtils.indexQuery(methodWatcher.getOrCreateConnection(), CLASS_NAME, tableName));

        methodWatcher.getStatement().execute(format("delete from %s where val is null", this.getTableReference(tableName)));
        rs = methodWatcher.executeQuery(query);
        fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
        Assert.assertEquals(fr.toString(), 3, fr.size());
//        System.out.println("2:");
//        System.out.println(fr.toString());

        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",
                                                    this.getTableReference(tableName), "MyNull",null));
        rs = methodWatcher.executeQuery(query);
        fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
        Assert.assertEquals(fr.toString(), 4, fr.size());
//        System.out.println("3:");
//        System.out.println(fr.toString());
    }

    @Test(timeout = 10000)
    public void testCanDeleteThenInsertEntryInTransaction() throws Exception {
        new SpliceIndexWatcher(TABLE_NAME_6, CLASS_NAME, INDEX_61, CLASS_NAME, "(name)", true).starting(null);
        String name = "sfines";
        int value = 2;
        methodWatcher.getOrCreateConnection().setAutoCommit(false);
        methodWatcher.getStatement().execute(format("insert into %s (name, val) values ('%s', %s)", this.getTableReference(TABLE_NAME_6), name, value));
        methodWatcher.getStatement().execute(format("delete from %s", this.getTableReference(TABLE_NAME_6), name));
        methodWatcher.getStatement().execute(format("insert into %s (name, val) values ('%s', %s)", this.getTableReference(TABLE_NAME_6), name, value));
        ResultSet rs = methodWatcher.executeQuery(format("select * from %s where name = '%s'", this.getTableReference(TABLE_NAME_6), name));
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while (rs.next()) {
            String retName = rs.getString(1);
            int val = rs.getInt(2);
            results.add(String.format("name:%s,value:%d", retName, val));
        }
        Assert.assertEquals("Incorrect number of rows returned!", 1, results.size());
        methodWatcher.getOrCreateConnection().commit();
    }

    @Test(timeout = 10000)
    public void testCanInsertThenDeleteEntryInTransaction() throws Exception {
        new SpliceIndexWatcher(TABLE_NAME_6, CLASS_NAME, INDEX_61, CLASS_NAME, "(name)", true).starting(null);
        insertThenDelete();
        methodWatcher.getOrCreateConnection().commit();
    }

    private void insertThenDelete() throws Exception {
        methodWatcher.getOrCreateConnection().setAutoCommit(false);
        String name = "sfines";
        int value = 2;
        methodWatcher.getStatement().execute(format("insert into %s (name, val) values ('%s', %s)", this.getTableReference(TABLE_NAME_6), name, value));
        methodWatcher.getStatement().execute(format("delete from %s where name = '%s'", this.getTableReference(TABLE_NAME_6), name));
        ResultSet rs = methodWatcher.executeQuery(format("select * from %s where name = '%s'", this.getTableReference(TABLE_NAME_6), name));
        List<String> results = Lists.newArrayListWithExpectedSize(0);
        while (rs.next()) {
            String retName = rs.getString(1);
            int val = rs.getInt(2);
            results.add(String.format("name:%s,value:%d", retName, val));
        }
        Assert.assertEquals("Incorrect number of rows returned!", 0, results.size());
    }

    @Test(timeout = 10000)
    public void testCanInsertThenDeleteThenInsertAgainInTransaction() throws Exception{
        new SpliceIndexWatcher(TABLE_NAME_6, CLASS_NAME, INDEX_61, CLASS_NAME, "(name)", true).starting(null);
        insertThenDelete();
        insertThenDelete();
        methodWatcher.getOrCreateConnection().commit();
    }

//    @Test(timeout= 10000)
    @Test
    public void testCanUpdateEntryIndexChanges() throws Exception{
       	new SpliceIndexWatcher(TABLE_NAME_7,CLASS_NAME,INDEX_71,CLASS_NAME,"(name)",true).starting(null);
        String name = "sfines";
        int value = 2;
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",spliceTableWatcher7,name,value));
 
        String newName = "jzhang";
        methodWatcher.getStatement().execute(format("update %s set name = '%s' where name = '%s'", spliceTableWatcher7,newName,name));

        ResultSet rs = methodWatcher.executeQuery(format("select * from %s where name = '%s'",spliceTableWatcher7,name));
        Assert.assertTrue("Rows are returned incorrectly",!rs.next());

        rs = methodWatcher.executeQuery(format("select * from %s where name = '%s'",spliceTableWatcher7,newName));
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while(rs.next()){
            String retName = rs.getString(1);
            int val = rs.getInt(2);
            Assert.assertEquals("Incorrect name returned!",newName,retName);
            results.add(String.format("name:%s,value:%d",retName,val));
        }
        Assert.assertEquals("Incorrect number of rows returned!",1,results.size());

    }

    @Test(timeout = 10000,expected=SQLException.class)
    public void testUniqueInTableCreationViolatesPrimaryKey() throws Exception{
        String name = "sfines";
        int value =2;
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",this.getTableReference(TABLE_NAME_8),name,value));
        try{
            methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('%s',%s)",this.getTableReference(TABLE_NAME_8),name,value));
        }catch(SQLException se){
            Assert.assertEquals("Incorrect SQL State returned", SQLState.LANG_DUPLICATE_KEY_CONSTRAINT,se.getSQLState());
            Assert.assertTrue(se.getMessage().contains("identified by 'FOO' defined on 'H'"));
            throw se;
        }
        Assert.fail("Did not report a duplicate key violation!");
    }

}
