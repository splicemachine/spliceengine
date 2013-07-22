package org.apache.derby.impl.sql.execute.operations;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import com.google.common.collect.Lists;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

public class UpdateOperationTest extends SpliceUnitTest {
	private static Logger LOG = Logger.getLogger(UpdateOperationTest.class);
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(UpdateOperationTest.class.getSimpleName());
	protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("LOCATION",spliceSchemaWatcher.schemaName,"(num int, addr varchar(50), zip char(5))");
    protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher("b",spliceSchemaWatcher.schemaName,"(num int, addr varchar(50), zip char(5))");
    protected static SpliceTableWatcher nullTableWatcher = new SpliceTableWatcher("NULL_TABLE",spliceSchemaWatcher.schemaName,"(addr varchar(50), zip char(5))");

	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher)
        .around(spliceTableWatcher2)
        .around(nullTableWatcher)
		.around(new SpliceDataWatcher() {
            @Override
            protected void starting(Description description) {
                try {
                    PreparedStatement insertPs = spliceClassWatcher.prepareStatement("insert into " + spliceTableWatcher + " values(?,?,?)");
                    PreparedStatement insertPs2 = spliceClassWatcher.prepareStatement("insert into "+ spliceTableWatcher2+" values(?,?,?)");

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
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    spliceClassWatcher.closeAll();
                }
            }
        });
	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

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
}
