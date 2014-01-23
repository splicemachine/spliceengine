package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.*;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Unit Test for making sure MultiProbeTableScanOperation is logically correct.  Once we have metrics information, 
 * the test should be expanded to show that we only filter the records required.
 *
 */
public class MultiProbeTableScanOperatonIT { 
    public static final String CLASS_NAME = MultiProbeTableScanOperatonIT.class.getSimpleName();
    protected static SpliceWatcher spliceClassWatcher = new DefaultedSpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    protected static SpliceTableWatcher t1Watcher = new SpliceTableWatcher("user_groups",schemaWatcher.schemaName,"(user_id BIGINT NOT NULL,segment_id INT NOT NULL,unixtime BIGINT, primary key(segment_id, user_id))");
    private static int size = 10;
    
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(schemaWatcher)
            .around(t1Watcher)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        PreparedStatement ps = spliceClassWatcher.prepareStatement("insert into "+t1Watcher.toString()+" values (?,?,?)");
                        for(int i=0;i<size;i++){
                            ps.setInt(1,i);
                            ps.setInt(2,i);
                            ps.setLong(3, 1l);
                            ps.execute();
                        }

                        for(int i=0;i<size;i++){
                        	if ((i == 4) || (i==6)) {
	                            ps.setInt(1,size+i);
	                            ps.setInt(2,i);
	                            ps.setLong(3, 1l);
	                            ps.execute();
	                        }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }finally{
                        spliceClassWatcher.closeAll();
                    }
                }

            });

    @Rule public SpliceWatcher methodWatcher = new DefaultedSpliceWatcher(CLASS_NAME);

    @Test
    public void testMultiProbeTableScanScroll() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select user_id from "+t1Watcher+" where segment_id in (1,5,8,12)");
        int i = 0;
        while (rs.next()) {
        	i++;
        }
        Assert.assertEquals("Incorrect count returned!",3,i);
    }

    @Test
    public void testMultiProbeTableScanSink() throws Exception {
    	ResultSet rs = methodWatcher.executeQuery("select count(user_id) from (" + 
    			"select user_id, ("+
    			"max(case when segment_id = 7 then true else false end) " +
    			"or " +
    			"max(case when segment_id = 4 then true else false end)" + 
    			") as in_segment " +
    			"from "+t1Watcher+ " " +
    			"where segment_id in (7, 4) " +
    			"group by user_id) foo where in_segment = true");
        int i = 0;
        while (rs.next()) {
        	i++;
            Assert.assertEquals("Incorrect Distinct Customers",3,rs.getLong(1));        	
        }
        Assert.assertEquals("Incorrect records returned!",1,i);
    }




}
