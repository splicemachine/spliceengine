package com.splicemachine.derby.impl.sql.execute.operations.microstrategy;

import com.google.common.collect.Sets;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.tables.SpliceCustomerTable;
import com.splicemachine.test.suites.MicrostrategiesTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Set;

/**
 * @author Scott Fines
 * Created on: 2/24/13
 */
@Category(MicrostrategiesTests.class)
public class MicostrategiesCustomerIT extends SpliceUnitTest { 
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	public static final String CLASS_NAME = MicostrategiesCustomerIT.class.getSimpleName().toUpperCase();
	public static final String TABLE_NAME = "A";
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);	
	protected static SpliceCustomerTable spliceTableWatcher = new SpliceCustomerTable(TABLE_NAME,CLASS_NAME); 	
	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher);
	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

		@Test
		public void testRepeatedSelectDistincts() throws Exception {
			for(int i=0;i<100;i++){
					testSelectDistinctSelectsDistincts();
			}
		}

		@Test
    public void testSelectDistinctSelectsDistincts() throws Exception{
	    String userDir = System.getProperty("user.dir");
	    if(!userDir.endsWith("structured_derby"))
	    	userDir = userDir+"/structured_derby/";
	    PreparedStatement ps = methodWatcher.prepareStatement("call SYSCS_UTIL.SYSCS_IMPORT_DATA (?, ?, null,null,?,',',null,null,null,null)");
	    ps.setString(1,CLASS_NAME);
	    ps.setString(2,TABLE_NAME);  
	    ps.setString(3,userDir+"/src/test/resources/customer_iso.csv");
	    ps.executeUpdate();
	    
        ResultSet rs = methodWatcher.executeQuery(format("select distinct cst_city_id from %s",this.getTableReference(TABLE_NAME)));
        Set<Integer> cityIds = Sets.newHashSet();
        while(rs.next()){
            int city = rs.getInt(1);
            Assert.assertTrue("City already found!",!cityIds.contains(city));
            cityIds.add(city);
        }
        Assert.assertTrue("No City ids found!",cityIds.size()>0);
    }
}
