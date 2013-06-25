package com.splicemachine.derby.impl.sql.execute.operations.microstrategy;

import com.google.common.collect.Sets;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.tables.SpliceOrderLineTable;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Set;

/**
 * Tests against Microstrategy Order Detail table.
 *
 * These are tests intended to make sure that things work against a real, actual data
 * set. Ensuring Groupings are unique, that sort of thing.
 *
 * WARNING: Most of these tests are extremely slow. They are working with a large data set. Beware
 *
 * @author Scott Fines
 *         Created on: 2/23/13
 */
public class MsOrderDetailTest extends SpliceUnitTest {

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	public static final String CLASS_NAME = MsOrderDetailTest.class.getSimpleName().toUpperCase();
	public static final String TABLE_NAME = "A";
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);	
	protected static SpliceOrderLineTable spliceTableWatcher = new SpliceOrderLineTable(TABLE_NAME,CLASS_NAME); 	
	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher);
	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

    /**
     * Test for Bug #230. The idea is to make sure that
     * when grouping up by a specific key, that there is only one entry per key.
     */
    @Test
    public void testGroupedAggregationsGroupUniquely() throws Exception{
        String userDir = System.getProperty("user.dir");
        if(!userDir.endsWith("structured_derby"))
            userDir = userDir+"/structured_derby/";
        PreparedStatement ps = methodWatcher.prepareStatement("call SYSCS_UTIL.SYSCS_IMPORT_DATA (?, ?, null,null,?,',',null,null,null,null)");
        ps.setString(1,CLASS_NAME);
        ps.setString(2,TABLE_NAME);        
        ps.setString(3,userDir+"/src/test/resources/order_detail_small.csv");
        ps.executeUpdate();
        ResultSet groupedRs = methodWatcher.executeQuery(format("select orl_customer_id, count(*) from %s group by orl_customer_id",this.getTableReference(TABLE_NAME)));
        Set<String> uniqueGroups = Sets.newHashSet();
        while(groupedRs.next()){
            String groupKey = groupedRs.getString(1);
            int groupCount = groupedRs.getInt(2);
            Assert.assertTrue("empty group count!",groupCount>0);
            Assert.assertTrue("Already seen key "+ groupKey, !uniqueGroups.contains(groupKey));
            uniqueGroups.add(groupKey);
        }
        Assert.assertTrue("No groups found!",uniqueGroups.size()>0);
    }
}
