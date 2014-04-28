package com.splicemachine.derby.impl.sql.execute.actions;

import com.google.common.collect.Lists;
import com.splicemachine.derby.test.framework.SpliceIndexWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

/**
 * @author Scott Fines
 *         Created on: 3/8/13
 */
public class NonUniqueIndexIT extends SpliceUnitTest { 
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	public static final String CLASS_NAME = NonUniqueIndexIT.class.getSimpleName().toUpperCase();
	public static final String TABLE_NAME_1 = "A";
	public static final String TABLE_NAME_2 = "B";
	public static final String TABLE_NAME_3 = "C";
	public static final String TABLE_NAME_4 = "D";
	public static final String TABLE_NAME_5 = "E";
	public static final String TABLE_NAME_6 = "F";
    public static final String TABLE_NAME_7 = "G";
    public static final String TABLE_NAME_8 = "H";
    public static final String TABLE_NAME_9 = "I";
	public static final String INDEX_11 = "IDX_A1";
	public static final String INDEX_21 = "IDX_B1";
	public static final String INDEX_31 = "IDX_C1";
	public static final String INDEX_41 = "IDX_D1";
	public static final String INDEX_51 = "IDX_E1";
	public static final String INDEX_61 = "IDX_F1";
    public static final String INDEX_62 = "IDX_F2";
    public static final String INDEX_81 = "IDX_H2";
    public static final String INDEX_91 = "IDX_I1";

	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
	protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_NAME_1,spliceSchemaWatcher.schemaName,"(name varchar(40), val int)");
	protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE_NAME_2,spliceSchemaWatcher.schemaName,"(n_1 varchar(40),n_2 varchar(30),val int)");
	protected static SpliceTableWatcher spliceTableWatcher3 = new SpliceTableWatcher(TABLE_NAME_3,spliceSchemaWatcher.schemaName,"(name varchar(40), val int)");
	protected static SpliceTableWatcher spliceTableWatcher4 = new SpliceTableWatcher(TABLE_NAME_4,spliceSchemaWatcher.schemaName,"(name varchar(40), val int)");
	protected static SpliceTableWatcher spliceTableWatcher5 = new SpliceTableWatcher(TABLE_NAME_5,spliceSchemaWatcher.schemaName,"(name varchar(40), val int)");
	protected static SpliceTableWatcher spliceTableWatcher6 = new SpliceTableWatcher(TABLE_NAME_6,spliceSchemaWatcher.schemaName,"(name varchar(40), val int)");
    protected static SpliceTableWatcher spliceTableWatcher7 = new SpliceTableWatcher(TABLE_NAME_7,spliceSchemaWatcher.schemaName,"(name varchar(40), val int)");
    protected static SpliceTableWatcher spliceTableWatcher8 = new SpliceTableWatcher(TABLE_NAME_8,spliceSchemaWatcher.schemaName,"(oid decimal(5),name varchar(40))");
    protected static SpliceTableWatcher spliceTableWatcher9 = new SpliceTableWatcher(TABLE_NAME_9,spliceSchemaWatcher.schemaName,"(c1 int, c2 int, c3 int)");

    @Override
    public String getSchemaName() {
        return spliceSchemaWatcher.schemaName;
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
            .around(spliceTableWatcher8)
            .around(spliceTableWatcher9);

	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test(timeout=10000)
    public void testCanCreateIndexWithMultipleEntries() throws Exception{
        new SpliceIndexWatcher(TABLE_NAME_2,spliceSchemaWatcher.schemaName,INDEX_21,spliceSchemaWatcher.schemaName,"(n_1,n_2)").starting(null);
        PreparedStatement ps = methodWatcher.prepareStatement(format("insert into %s (n_1,n_2,val) values (?,?,?)",this.getTableReference(TABLE_NAME_2)));
        String n1 = "sfines";
        ps.setString(1,n1);
        String n2 = "mathematician";
        ps.setString(2,n2);
        int value = 2;
        ps.setInt(3,2);
        ps.execute();
        //now check that we can get data out for the proper key
        PreparedStatement query = methodWatcher.prepareStatement(format("select * from %s where n_1 = ? and n_2 = ?",this.getTableReference(TABLE_NAME_2)));
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
        Assert.assertEquals("Incorrect number of rows returned!",1,results.size());

    }
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
    public void testCanUseIndex() throws Exception{
        new SpliceIndexWatcher(TABLE_NAME_1,spliceSchemaWatcher.schemaName,INDEX_11,spliceSchemaWatcher.schemaName,"(name)").starting(null);
        String name = "sfines";
        int value = 2;
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('"+name+"',"+value+")",this.getTableReference(TABLE_NAME_1)));
        //now check that we can get data out for the proper key
        ResultSet resultSet = methodWatcher.executeQuery(format("select * from %s where name = '" + name + "'",this.getTableReference(TABLE_NAME_1)));
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while(resultSet.next()){
            String retName = resultSet.getString(1);
            int val = resultSet.getInt(2);
            Assert.assertEquals("Incorrect name returned!", name, retName);
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
    @Test(timeout=15000)
    public void testCanCreateIndexFromExistingData() throws Exception{
        String name = "sfines";
        int value =2;
        PreparedStatement ps = methodWatcher.prepareStatement("insert into "+spliceTableWatcher3+" (name,val) values (?,?)");
        ps.setString(1, name);
        ps.setInt(2, value);
        int numRowsUpdated  = ps.executeUpdate();
        Assert.assertEquals("Incorrect number of rows inserted!",1,numRowsUpdated);

        new SpliceIndexWatcher(TABLE_NAME_3,spliceSchemaWatcher.schemaName,INDEX_31,spliceSchemaWatcher.schemaName,"(name)").starting(null);
        //now check that we can get data out for the proper key
        ps = methodWatcher.prepareStatement("select * from "+ spliceTableWatcher3+ " where name = ?");
        ps.setString(1,name);
        ResultSet resultSet = ps.executeQuery();
        try{
            List<String> results = Lists.newArrayListWithExpectedSize(1);
            while(resultSet.next()){
                String retName = resultSet.getString(1);
                int val = resultSet.getInt(2);
                Assert.assertEquals("Incorrect name returned!",name,retName);
                Assert.assertEquals("Incorrect value returned!",value,val);
                results.add(String.format("name:%s,value:%d",retName,val));
            }
            Assert.assertEquals("Incorrect number of rows returned!",1,results.size());
        }finally{
            resultSet.close();
        }
    }
    /**
     * Tests that adding an index to an existing data set will
     * result in a correct and consistent index
     *
     * Basically, add some data to the table, then create the index,
     * then perform a lookup on that same data via the index to ensure
     * that the index will find those values.
     */
    @Test(timeout=15000)
    public void testCanCreateIndexFromExistingDataWithDecimalType() throws Exception{
        BigDecimal oid = BigDecimal.valueOf(2);
        String name = "sfines";
        PreparedStatement preparedStatement = methodWatcher.prepareStatement("insert into " + spliceTableWatcher8 + " values (?,?)");
        preparedStatement.setBigDecimal(1,oid);
        preparedStatement.setString(2,name);
        preparedStatement.execute();
    	new SpliceIndexWatcher(TABLE_NAME_8,spliceSchemaWatcher.schemaName,INDEX_81,spliceSchemaWatcher.schemaName,"(name)").starting(null);
        //now check that we can get data out for the proper key
        preparedStatement = methodWatcher.prepareStatement("select * from "+ spliceTableWatcher8+" where oid = ?");
        preparedStatement.setBigDecimal(1,oid);

        ResultSet resultSet = preparedStatement.executeQuery();
        try{
            List<String> results = Lists.newArrayListWithExpectedSize(1);
            while(resultSet.next()){
                BigDecimal retOid = resultSet.getBigDecimal(1);
                String retName = resultSet.getString(2);
                Assert.assertEquals("Incorrect name returned!",name,retName);
                Assert.assertEquals("Incorrect oid returned!",oid,retOid);
                results.add(String.format("name:%s,value:%s",retName,retOid));
            }
            Assert.assertEquals("Incorrect number of rows returned!",1,results.size());
        }finally{
            resultSet.close();
        }
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
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('sfines',2)",this.getTableReference(TABLE_NAME_4)));
    	new SpliceIndexWatcher(TABLE_NAME_4,spliceSchemaWatcher.schemaName,INDEX_41,spliceSchemaWatcher.schemaName,"(name)").starting(null);
        //add some more data
        String name = "jzhang";
        int value =2;
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('"+name+"',"+value+")",this.getTableReference(TABLE_NAME_4)));

        //now check that we can get data out for the proper key
        ResultSet resultSet = methodWatcher.executeQuery(format("select * from %s where name = '" + name + "'",this.getTableReference(TABLE_NAME_4)));
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
    
//    @Test(timeout=10000))
    @Test
    public void testCanAddDuplicateAndDelete() throws Exception{
    	new SpliceIndexWatcher(TABLE_NAME_5,spliceSchemaWatcher.schemaName,INDEX_51,spliceSchemaWatcher.schemaName,"(name)").starting(null);
        String name = "sfines";
        addDuplicateAndDeleteTest(name);
    }

    @Test
    public void testRepeatedAddDuplicateAndDelete() throws Exception {
        new SpliceIndexWatcher(TABLE_NAME_5,spliceSchemaWatcher.schemaName,INDEX_51,spliceSchemaWatcher.schemaName,"(name)").starting(null);
        for (int i = 0; i < 50; i++) {
            System.out.println(i);
            String name = Integer.toString(i);
            addDuplicateAndDeleteTest(name);
        }
    }

    private void addDuplicateAndDeleteTest(String name) throws Exception {
        PreparedStatement ps = methodWatcher.prepareStatement(format("insert into %s (name,val) values (?,?)",spliceTableWatcher5));
        ps.setString(1,name);
        ps.setInt(2, 2);
        Assert.assertEquals("Incorrect insertion count!", 1, ps.executeUpdate());
        ps.setString(1, name);
        ps.setInt(2, 3);
        Assert.assertEquals("Incorrect insertion count!", 1, ps.executeUpdate());

        ResultSet resultSet = methodWatcher.executeQuery(format("select * from %s where name = '%s'",spliceTableWatcher5,name));
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while(resultSet.next()){
            String retName = resultSet.getString(1);
            int val = resultSet.getInt(2);
            Assert.assertEquals("Incorrect name returned!", name, retName);
            results.add(String.format("name:%s,value:%d", retName, val));
        }
        Assert.assertEquals("Incorrect number of rows returned!",2,results.size());
        for(String result:results){
            System.out.println(result);
        }

        int deleted = methodWatcher.getStatement().executeUpdate(format("delete from %s where name = '%s' and val = 2", spliceTableWatcher5, name));
        Assert.assertEquals("Incorrect number of rows were deleted!",1,deleted);
        assertSelectCorrect(spliceSchemaWatcher.schemaName,TABLE_NAME_5,name,1);
    }

    @Test(timeout=10000)
    public void testCanDeleteEverything() throws Exception {
        new SpliceIndexWatcher(TABLE_NAME_5,spliceSchemaWatcher.schemaName,INDEX_51,spliceSchemaWatcher.schemaName,"(name)").starting(null);
        methodWatcher.getStatement().execute(format("insert into %s.%s (name,val) values ('sfines',2)",spliceSchemaWatcher.schemaName,TABLE_NAME_5));
        methodWatcher.getStatement().execute(format("delete from %s",spliceTableWatcher5.toString()));

        ResultSet resultSet = methodWatcher.executeQuery("select * from "+spliceTableWatcher5.toString());
        Assert.assertTrue("Results returned!",!resultSet.next());
    }

//    @Test(timeout=10000)
    @Test
    public void testCanUpdateEntryIndexChanges() throws Exception{
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('sfines',2)",this.getTableReference(TABLE_NAME_6)));
    	new SpliceIndexWatcher(TABLE_NAME_6,spliceSchemaWatcher.schemaName,INDEX_61,spliceSchemaWatcher.schemaName,"(name)").starting(null);

        String oldName = "sfines";
        String newName = "jzhang";
        methodWatcher.getStatement().execute(String.format("update %s set name = '%s' where name = '%s'", this.getTableReference(TABLE_NAME_6),newName,oldName));

        ResultSet rs = methodWatcher.executeQuery(format("select * from %s where name = '%s'",this.getTableReference(TABLE_NAME_6),oldName));
        Assert.assertTrue("Rows returned incorrectly",!rs.next());

        assertSelectCorrect(spliceSchemaWatcher.schemaName,TABLE_NAME_6,newName,1);
    }

    @Test
    public void testUpdateEveryRowCorrect() throws Exception {
        methodWatcher.getStatement().execute(format("insert into %s (name,val) values ('sfines',2)",spliceTableWatcher7));
        new SpliceIndexWatcher(TABLE_NAME_7,spliceSchemaWatcher.schemaName,INDEX_62,spliceSchemaWatcher.schemaName,"(name)").starting(null);

        String newName = "jzhang";

        methodWatcher.getStatement().execute(String.format("update %s set name = '%s'", this.getTableReference(TABLE_NAME_7),newName));

        ResultSet rs = methodWatcher.executeQuery(format("select * from %s where name = '%s'",this.getTableReference(TABLE_NAME_7),"sfines"));
        Assert.assertTrue("Rows returned incorrectly",!rs.next());

        assertSelectCorrect(spliceSchemaWatcher.schemaName,TABLE_NAME_7,newName,1);
    }

    /**
     * Regression test for Bug 149. Confirm that we can add a view and then add an index
     * and stuff
     */
    @Test(timeout=10000)
    public void testCanAddIndexToViewedTable() throws Exception{
/*        rule.getStatement().execute("create view t_view as select name,val from t where val > 1");
        try{
            testCanUseIndex();
        }finally{
            rule.getStatement().execute("drop view t_view");
        }
        */
    }

    @Test
    public void testCanUpdateSecondFieldWithCompoundIndex() throws Exception {
        //Regression test for Bug 867.
        PreparedStatement ps = methodWatcher.prepareStatement("insert into "+spliceTableWatcher9+" (c1,c2,c3) values (?,?,?)");
        ps.setInt(1,6);
        ps.setInt(2,2);
        ps.setInt(3,8);
        ps.execute();

        SpliceIndexWatcher indexWatcher = new SpliceIndexWatcher(spliceTableWatcher9.tableName,spliceSchemaWatcher.schemaName,INDEX_91,spliceSchemaWatcher.schemaName,"(c1,c2 desc, c3)");
        indexWatcher.starting(null);
        try{
            methodWatcher.prepareStatement("update "+ spliceTableWatcher9+" set c2 = 11 where c3 = 7").executeUpdate();
        }finally{
            indexWatcher.drop();;
        }
    }

    private void assertSelectCorrect(String schemaName, String tableName, String name, int size) throws Exception{
        ResultSet resultSet = methodWatcher.executeQuery(format("select * from %s.%s where name = '%s'", schemaName,tableName,name));
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while(resultSet.next()){
            String retName = resultSet.getString(1);
            int val = resultSet.getInt(2);
            Assert.assertEquals("Incorrect name returned!", name, retName);
            results.add(String.format("name:%s,value:%d",retName,val));
        }
        Assert.assertEquals("Incorrect number of rows returned!",size,results.size());
    }
}
