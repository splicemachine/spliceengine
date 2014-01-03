package org.apache.derby.impl.sql.execute.operations;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Set;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import com.google.common.collect.Sets;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

/**
 * This tests basic table scans with and without projection/restriction
 */

public class DistinctScanOperationIT extends SpliceUnitTest {
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	private static Logger LOG = Logger.getLogger(DistinctScanOperationIT.class);
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(DistinctScanOperationIT.class.getSimpleName());	
	protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher("FOO",DistinctScanOperationIT.class.getSimpleName(),"(si int, sa varchar(40))");
    protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher("TS",DistinctScanOperationIT.class.getSimpleName(),"(si int, t timestamp)");
    protected static SpliceTableWatcher spliceTableWatcher3 = new SpliceTableWatcher("TAB",DistinctScanOperationIT.class.getSimpleName(),"(I INT, D DOUBLE)");
//	protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher("FOOBAR",DistinctScanOperationIT.class.getSimpleName(),"(name varchar(40), empId int)");
    private static int size = 10;
    private static long startTime;

	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher1)
		.around(spliceTableWatcher2)
		.around(new SpliceDataWatcher() {
            @Override
            protected void starting(Description description) {
                try {
                    spliceClassWatcher.setAutoCommit(true);
                    PreparedStatement ps = spliceClassWatcher.prepareStatement("insert into " + spliceTableWatcher1.toString() + " values (?,?)");
                    for (int i = 0; i < size; i++) {
                        ps.setInt(1, i);
                        ps.setString(2, Integer.toString(i + 1));
                        ps.executeUpdate();

                        if (i % 2 == 0) {
                            //add a duplicate row
                            ps.setInt(1, i);
                            ps.setString(2, Integer.toString(i + 1));
                            ps.executeUpdate();
                        }
                    }

                    ps = spliceClassWatcher.prepareStatement("insert into "+ spliceTableWatcher2.toString()+" values (?,?)");
                    startTime = System.currentTimeMillis();
                    for(int i=0;i<size;i++){
                        ps.setInt(1,i);
                        ps.setTimestamp(2,new Timestamp(System.currentTimeMillis()));
                        ps.execute();
                    }
                    ps = spliceClassWatcher.prepareStatement("insert into " + spliceTableWatcher3.toString() + " values (?,?)");
			        for(int i=0;i<10;i++){
					    ps.setInt(1,i);
					    ps.setDouble(2, i);
					    for(int j=0;j<100;j++){
							ps.addBatch();
					    }
					    ps.executeBatch();
			        }

                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    spliceClassWatcher.closeAll();
                }
            }

        });
	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();


    @Test
	public void testDistinctScanOperation() throws Exception {			
		ResultSet rs = methodWatcher.executeQuery("select distinct si from " + spliceTableWatcher1.toString());
        Set<Integer> priorResults = Sets.newHashSet();
		while (rs.next()) {
            int next = rs.getInt(1);
            System.out.printf("si=%d%n",next);
            Assert.assertTrue("Duplicate value "+ next+" returned!",!priorResults.contains(next));
            priorResults.add(next);
		}
		Assert.assertEquals(size,priorResults.size());
	}

    @Test
    public void testDistinctScanGetNextRowCore() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("select count(*) from (select distinct si from %s) a", spliceTableWatcher1.toString()));
        Assert.assertTrue("No rows returned, 1 row expected", rs.next());
        Assert.assertEquals("10 distinct results expected",10,rs.getInt(1));
        Assert.assertFalse("More than one row was returned", rs.next());
    }

    @Test
	public void testDistinctString() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select distinct sa from " + spliceTableWatcher1.toString());
        Set<String> priorResults = Sets.newHashSet();
        while (rs.next()) {
            String next = rs.getString(1);
            System.out.printf("si=%s%n",next);
            Assert.assertTrue("Duplicate value "+ next+" returned!",!priorResults.contains(next));
            priorResults.add(next);
        }
        Assert.assertEquals(size,priorResults.size());
	}

    @Test
    public void testDistinctTimestamp() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select distinct t from "+ spliceTableWatcher2.toString());
        Set<Timestamp> timestampSet = Sets.newHashSet();
        while(rs.next()){
            Timestamp ts = rs.getTimestamp(1);
            System.out.printf("t=%s%n",ts);
            Assert.assertTrue("Duplicate value "+ ts+" returned!", !timestampSet.contains(ts));
            Assert.assertTrue("Timestamp value returned too low!",startTime<=ts.getTime());
            timestampSet.add(ts);
        }

        Assert.assertEquals(size,timestampSet.size());

    }
    @Test
    public void testDistinctSequence() throws Exception {

    	ResultSet rs = methodWatcher.executeQuery("select distinct i from " + spliceTableWatcher3.toString());
        int i = 0;
        while(rs.next()){
        	i++;
        }
        Assert.assertEquals(i, 10);
        rs.close();

        rs = methodWatcher.executeQuery("select distinct i,d from " + spliceTableWatcher3.toString());

        i = 0;
        while(rs.next()){
        	i++;
        }
        Assert.assertEquals(i, 10);
        rs.close();
        rs = methodWatcher.executeQuery("select distinct d,i from " + spliceTableWatcher3.toString());

        i = 0;
        while(rs.next()){
        	i++;
        }
        Assert.assertEquals(i, 10);
        rs.close();
        rs = methodWatcher.executeQuery("select distinct d,i from " + spliceTableWatcher3.toString() + " order by i asc");

        i = 0;
        while(rs.next()){
        	Assert.assertEquals((int)rs.getDouble(1), i);
        	i++;
        }
        rs.close();
    }
}
