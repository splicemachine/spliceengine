package org.apache.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
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
import java.util.List;

public class UpdateOperationIT extends SpliceUnitTest { 
	private static Logger LOG = Logger.getLogger(UpdateOperationIT.class);
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(UpdateOperationIT.class.getSimpleName());
	protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("LOCATION",spliceSchemaWatcher.schemaName,"(num int, addr varchar(50), zip char(5))");
    protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher("b",spliceSchemaWatcher.schemaName,"(num int, addr varchar(50), zip char(5))");
    protected static SpliceTableWatcher nullTableWatcher = new SpliceTableWatcher("NULL_TABLE",spliceSchemaWatcher.schemaName,"(addr varchar(50), zip char(5))");
    protected static SpliceTableWatcher spliceTableWatcher3 = new SpliceTableWatcher("c",spliceSchemaWatcher.schemaName,"(k int, l int)");
    protected static SpliceTableWatcher spliceTableWatcher4 = new SpliceTableWatcher("customer",spliceSchemaWatcher.schemaName,"(cust_id int, status boolean)");
    protected static SpliceTableWatcher spliceTableWatcher5 = new SpliceTableWatcher("shipment",spliceSchemaWatcher.schemaName,"(cust_id int)");
    protected static SpliceTableWatcher spliceTableWatcher6 = new SpliceTableWatcher("tab2",spliceSchemaWatcher.schemaName,"(c1 int not null primary key, c2 int, c3 int)");
    private static final SpliceTableWatcher nullIndexedTable = new SpliceTableWatcher("nt",spliceSchemaWatcher.schemaName,"(a int, b int)");
    private static final SpliceIndexWatcher nullIndex = new SpliceIndexWatcher(nullIndexedTable.tableName,spliceSchemaWatcher.schemaName,"nt_idx",spliceSchemaWatcher.schemaName,"(a)",true);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher)
            .around(spliceTableWatcher2)
            .around(spliceTableWatcher3)
            .around(nullTableWatcher)
            .around(spliceTableWatcher4)
            .around(spliceTableWatcher5)
            .around(spliceTableWatcher6)
            .around(nullIndexedTable)
            .around(nullIndex)
            .around(new SpliceDataWatcher() {
        @Override
        protected void starting(Description description) {
            try {
                PreparedStatement insertPs = spliceClassWatcher.prepareStatement("insert into " + spliceTableWatcher + " values(?,?,?)");
                PreparedStatement insertPs2 = spliceClassWatcher.prepareStatement("insert into " + spliceTableWatcher2 + " values(?,?,?)");

                insertPs.setInt(1, 100);
                insertPs.setString(2, "100");
                insertPs.setString(3, "94114");
                insertPs.executeUpdate();

                insertPs2.setInt(1, 250);
                insertPs2.setString(2, "100");
                insertPs2.setString(3, "94114");
                insertPs2.executeUpdate();

                insertPs.setInt(1, 200);
                insertPs.setString(2, "200");
                insertPs.setString(3, "94509");
                insertPs.executeUpdate();

                insertPs.setInt(1, 300);
                insertPs.setString(2, "300");
                insertPs.setString(3, "34166");
                insertPs.executeUpdate();

                // Insert into customer and shipment tables
                insertPs = spliceClassWatcher.prepareStatement("insert into " + spliceTableWatcher4 + " values(?,?)");
                insertPs.setInt(1, 1);
                insertPs.setBoolean(2, true);
                insertPs.executeUpdate();
                insertPs.setInt(1, 2);
                insertPs.setBoolean(2, true);
                insertPs.executeUpdate();
                insertPs.setInt(1, 3);
                insertPs.setBoolean(2, true);
                insertPs.executeUpdate();
                insertPs.setInt(1, 4);
                insertPs.setBoolean(2, true);
                insertPs.executeUpdate();
                insertPs.setInt(1, 5);
                insertPs.setBoolean(2, true);
                insertPs.executeUpdate();

                insertPs = spliceClassWatcher.prepareStatement("insert into " + spliceTableWatcher5 + " values(?)");
                insertPs.setInt(1, 2);
                insertPs.executeUpdate();
                insertPs.setInt(1, 4);
                insertPs.executeUpdate();

                insertPs = spliceClassWatcher.prepareStatement("insert into "+ spliceTableWatcher6 + " values(?,?,?)");
                insertPs.setInt(1,6);insertPs.setInt(2,2);insertPs.setInt(3,8); insertPs.execute(); //(6,2,8)
                insertPs.setInt(1,2);insertPs.setInt(2,8);insertPs.setInt(3,5); insertPs.execute(); //(2,8,5)
                insertPs.setInt(1,28);insertPs.setInt(2,5);insertPs.setInt(3,9); insertPs.execute(); //(28,5,9)
                insertPs.setInt(1,3);insertPs.setInt(2,12);insertPs.setInt(3,543); insertPs.execute(); //(3,12,543)
                insertPs.setInt(1,56);insertPs.setInt(2,2);insertPs.setInt(3,7); insertPs.execute(); //(56,2,7)
                insertPs.setInt(1,31);insertPs.setInt(2,5);insertPs.setInt(3,7); insertPs.execute(); //(31,5,7)
                insertPs.setInt(1,-12);insertPs.setInt(2,5);insertPs.setInt(3,2); insertPs.execute(); //(-12,5,2)

                PreparedStatement createIndex = spliceClassWatcher.prepareStatement("create index ti on " + spliceTableWatcher6 + "(c1,c2 desc,c3)");
                createIndex.execute();

                spliceClassWatcher.getStatement().execute(String.format("insert into %s (a,b) values (null,null)",nullIndexedTable));
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                spliceClassWatcher.closeAll();
            }
        }
    }).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try{
                        PreparedStatement ps = spliceClassWatcher.prepareStatement("insert into "+ spliceTableWatcher3 +" (k,l) values (?,?)");
                        ps.setInt(1,1);
                        ps.setInt(2,2);
                        ps.execute();

                        ps.setInt(1,3);
                        ps.setInt(2,4);
                        ps.execute();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }finally{
                        spliceClassWatcher.closeAll();
                    }
                }
            });
	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testUpdateSetNullLong() throws Exception {
        /*regression test for Bug 889*/
        int updated = methodWatcher.getStatement().executeUpdate("update "+ spliceTableWatcher3+" set k = NULL where l = 2");
        Assert.assertEquals("incorrect num rows updated!",1,updated);
        ResultSet rs = methodWatcher.executeQuery("select * from "+ spliceTableWatcher3);
        boolean nullFound=false;
        int count = 0;
        while(rs.next()){
            Integer k = rs.getInt(1);
            boolean nullK = rs.wasNull();
            Integer l =  rs.getInt(2);
            Assert.assertFalse("l should not be null!",rs.wasNull());
            if(nullK){
                Assert.assertFalse("Too many null records found!", nullFound);
                Assert.assertEquals("Incorrect row marked null!",2,l.intValue());
                nullFound = true;
            }
            count++;
        }
        Assert.assertEquals("Incorrect row count returned!",2,count);
    }

    @Test
	public void testUpdate() throws Exception {
		int updated= methodWatcher.getStatement().executeUpdate("update "+spliceTableWatcher+" set addr='240' where num=100");
		Assert.assertEquals("Incorrect num rows updated!",1,updated);
		ResultSet rs = methodWatcher.executeQuery("select * from "+spliceTableWatcher+" where num = 100");
		List<String> results = Lists.newArrayListWithCapacity(1);
		while(rs.next()){
			Integer num = rs.getInt(1);
			String addr = rs.getString(2);
			String zip = rs.getString(3);
			Assert.assertNotNull("no zip returned!",zip);
			Assert.assertEquals("Incorrect num returned!",100,num.intValue());
			Assert.assertEquals("Address incorrect","240",addr);
			results.add(String.format("num:%d,addr:%s,zip:%s",num,addr,zip));
		}
		for(String result:results){
			LOG.info(result);
		}
		Assert.assertEquals("Incorrect rows returned!",1,results.size());
	}

	@Test
	public void testUpdateMultipleColumns() throws Exception{
		int updated = methodWatcher.getStatement().executeUpdate("update "+spliceTableWatcher+" set addr='900',zip='63367' where num=300");
		Assert.assertEquals("incorrect number of records updated!",1,updated);
		ResultSet rs = methodWatcher.executeQuery("select * from "+ spliceTableWatcher+ " where num=300");
		List<String>results = Lists.newArrayList();
		while(rs.next()){
			Integer num = rs.getInt(1);
			String addr = rs.getString(2);
			String zip = rs.getString(3);
			Assert.assertEquals("incorrect num!",new Integer(300),num);
			Assert.assertEquals("Incorrect addr!","900",addr);
			Assert.assertEquals("incorrect zip!","63367",zip);
			results.add(String.format("num:%d,addr:%s,zip:%s",num,addr,zip));
		}
		Assert.assertEquals("Incorrect rows returned!",1,results.size());
	}

    @Test
    public void testWithoutWhere() throws Exception{
        methodWatcher.prepareStatement("insert into "+nullTableWatcher+" values (null,null)").execute();

        PreparedStatement preparedStatement = methodWatcher.prepareStatement("update " + nullTableWatcher + " set addr = ?, zip = ?");
        preparedStatement.setString(1,"2269 Concordia Drive");
        preparedStatement.setString(2,"65203");
        int updated = preparedStatement.executeUpdate();
        Assert.assertEquals("Incorrect number of records updated",1,updated);

        ResultSet rs = methodWatcher.executeQuery("select addr,zip from "+ nullTableWatcher);
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while(rs.next()){
            String addr = rs.getString(1);
            String zip = rs.getString(2);
            Assert.assertEquals("Incorrect address returned!","2269 Concordia Drive",addr);
            Assert.assertEquals("Incorrect zip returned!","65203",zip);
            results.add(String.format("addr=%s,zip=%s",addr,zip));
        }
        for(String result:results){
            LOG.info(result);
        }
        Assert.assertTrue("No row returned!",results.size()>0);
    }

    @Test
    public void testUpdateFromValues() throws Exception{
        /*
         * Regression test for Bug #286
         */
        int updated= methodWatcher.getStatement().executeUpdate("update " + spliceTableWatcher + " set addr=(values '5') where num=100");
        Assert.assertEquals("Incorrect num rows updated!", 1, updated);
        ResultSet rs = methodWatcher.executeQuery("select * from "+spliceTableWatcher+" where num = 100");
        List<String> results = Lists.newArrayListWithCapacity(1);
        while(rs.next()){
            Integer num = rs.getInt(1);
            String addr = rs.getString(2);
            String zip = rs.getString(3);
            Assert.assertNotNull("no zip returned!",zip);
            Assert.assertEquals("Incorrect num returned!",100,num.intValue());
            Assert.assertEquals("Address incorrect","5",addr);
            results.add(String.format("num:%d,addr:%s,zip:%s",num,addr,zip));
        }
        for(String result:results){
            LOG.info(result);
        }
        Assert.assertEquals("Incorrect rows returned!", 1, results.size());
    }

    @Test
    public void testUpdateFromSubquery() throws Exception {
        /* regression test for Bug 289 */
        int updated = methodWatcher.getStatement().executeUpdate("update "+ spliceTableWatcher2 +" set num=(select "+spliceTableWatcher+".num from "+spliceTableWatcher+" where "+spliceTableWatcher+".num = 100)");
        Assert.assertEquals("Incorrect num rows updated!",1,updated);
        ResultSet rs = methodWatcher.executeQuery("select * from "+ spliceTableWatcher2+" where num = 100");
        List<String> results = Lists.newArrayListWithCapacity(1);
        while(rs.next()){
            Integer num = rs.getInt(1);
            String addr = rs.getString(2);
            String zip = rs.getString(3);
            Assert.assertNotNull("no zip returned!",zip);
            Assert.assertEquals("Incorrect num returned!",100,num.intValue());
            Assert.assertEquals("Address incorrect","100",addr);
            results.add(String.format("num:%d,addr:%s,zip:%s",num,addr,zip));
        }
        for(String result:results){
            LOG.info(result);
        }
        Assert.assertEquals("Incorrect rows returned!", 1, results.size());
    }

    @Test
    public void testUpdateSetNullValues() throws Exception {
        /*
         * Regression test for Bug 682.
         */
        PreparedStatement ps = methodWatcher.prepareStatement("insert into "+ nullTableWatcher +" values (?,?)");
        ps.setString(1,"900 Green Meadows Road");
        ps.setString(2, "65201");
        ps.execute();

        //get initial count
        ResultSet rs = methodWatcher.executeQuery("select * from "+ nullTableWatcher+" where zip = '65201'");
        int originalCount=0;
        while(rs.next()){
            originalCount++;
        }

        //update to set a null entry
        int numChanged = methodWatcher.prepareStatement("update " + nullTableWatcher + " set zip = null where zip = '65201'").executeUpdate();
        Assert.assertEquals("Incorrect rows changed",1,numChanged);

        rs = methodWatcher.executeQuery("select * from "+ nullTableWatcher+" where zip is null");
        int count=0;
        while(rs.next()){
            String zip = rs.getString(2);
            Assert.assertNull("returned zip is not null!",zip);
            count++;
        }
        Assert.assertEquals("Incorrect row count returned",1,count);

        //make sure old value isn't there anymore
        rs = methodWatcher.executeQuery("select * from "+nullTableWatcher+" where zip = '65201'");
        int finalCount=0;
        while(rs.next()){
            finalCount++;
        }

        Assert.assertEquals("Row was not removed from original set",originalCount-1,finalCount);
    }

    @Test
    public void testUpdateOnAllColumnIndex() throws Exception {
        /*Regression test for DB-1481*/
        String query = "update " + spliceTableWatcher6 + " --DERBY-PROPERTIES index=TI\n" + //force the index just to make sure
                " set c2=11 where c3=7";
        int rows = methodWatcher.executeUpdate(query);
        Assert.assertEquals("incorrect num rows updated!", 2, rows);

        //make sure that every row with c3=7 is updated to be 11
        ResultSet rs = methodWatcher.executeQuery(String.format("select * from %s where c3=7", spliceTableWatcher6));
        int count =0;
        while(rs.next()){
            int c2 = rs.getInt(2);
            Assert.assertEquals("Incorrect value for c2!",11,c2);
            count++;
        }
        Assert.assertEquals("Incorrect returned count!",2,count);
    }

    @Test
    public void testUpdateOverNullIndexWorks() throws Exception {
        /*
         * Regression test for DB-2007. Assert that this doesn't explode, and that
         * the NULL,NULL row isn't modified, so the number of rows modified = 0
         */
        int modified = methodWatcher.executeUpdate("update "+ nullIndexedTable+" set a = a+ 1.1");
        Assert.assertEquals("Claimed to have modified a row!",0,modified);
    }

    // If you change one of the following 'update over join' tests,
    // you probably need to make a similar change to DeleteOperationIT.

    @Test
    public void testUpdateOverBroadcastJoin() throws Exception {
        TestConnection conn = methodWatcher.getOrCreateConnection();
        boolean oldAutoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);
        try{
            doTestUpdateOverJoin("BROADCAST",conn);
        }finally{
            conn.rollback();
            conn.setAutoCommit(oldAutoCommit);
        }
    }

    @Test
    public void testUpdateOverMergeSortJoin() throws Exception {
        TestConnection conn = methodWatcher.getOrCreateConnection();
        boolean oldAutoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);
        try{
            doTestUpdateOverJoin("SORTMERGE", conn);
        }finally{
            conn.rollback();
            conn.setAutoCommit(oldAutoCommit);
        }
    }

    private void doTestUpdateOverJoin(String hint,TestConnection connection) throws Exception {
    	StringBuffer sb = new StringBuffer(200);
    	sb.append("update %s %s set %s.status = 'false' \n");
    	sb.append("where not exists ( \n");
		sb.append("  select 1 \n");
		sb.append("  from %s %s --SPLICE-PROPERTIES joinStrategy=%s \n");                                                                           
		sb.append("  where %s.cust_id = %s.cust_id \n");
		sb.append(") \n");
		String query = String.format(sb.toString(), spliceTableWatcher4, "customer", "customer", spliceTableWatcher5, "shipment", hint, "customer", "shipment");
    	int rows = connection.createStatement().executeUpdate(query);
        Assert.assertEquals("incorrect num rows updated!", 3, rows);
    }
}
