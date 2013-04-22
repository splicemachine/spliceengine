package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceFunctionWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.Assert;
import org.apache.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author Scott Fines
 *         Created on: 2/22/13
 */
public class FunctionTest extends SpliceUnitTest {
    private static final Logger LOG = Logger.getLogger(FunctionTest.class);
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(FunctionTest.class.getSimpleName());	
	protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("A",FunctionTest.class.getSimpleName(),"(data double)");
	protected static SpliceFunctionWatcher spliceFunctionWatcher = new SpliceFunctionWatcher("SIN",FunctionTest.class.getSimpleName(),"( data double) returns double external name 'java.lang.Math.sin' language java parameter style java");
	
	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher)
		.around(spliceTableWatcher)
		.around(spliceFunctionWatcher)
		.around(new SpliceDataWatcher(){
			@Override
			protected void starting(Description description) {
				try {
		        PreparedStatement ps = spliceClassWatcher.prepareStatement("insert into "+ FunctionTest.class.getSimpleName() + ".A (data) values (?)");
		        ps.setDouble(1,1.23d);
		        ps.executeUpdate();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				finally {
					spliceClassWatcher.closeAll();
				}
			}
			
		});
	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();


    @Test
    public void testSinFunction() throws Exception{
        ResultSet funcRs = methodWatcher.executeQuery("select SIN(data) from" + this.getPaddedTableReference("A"));
        int rows = 0;
        while(funcRs.next()){
            double sin = funcRs.getDouble(1);
            double correctSin = Math.sin(1.23d);
            Assert.assertEquals("incorrect sin!",correctSin,sin,1/100000d);
            LOG.info(funcRs.getDouble(1));
            rows++;
        }
        Assert.assertTrue("Incorrect rows returned!",rows>0);
    }
    /**
     * See Bug 266
     * 
     * @throws SQLException
     */
    @Test
    @Ignore("Protocol Error - Need to figure out - JL")
    public void testAcosFunction() throws Exception{
        ResultSet funcRs = methodWatcher.executeQuery("select acos(data) from" + this.getPaddedTableReference("A"));
        int rows = 0;
        while(funcRs.next()){
            double acos = funcRs.getDouble(1);
            double correctAcos = Math.acos(1.23d);
            Assert.assertEquals("incorrect acos!",correctAcos,acos,1/100000d);
            LOG.info(funcRs.getDouble(1));
            rows++;
        }
        Assert.assertTrue("Incorrect rows returned!",rows>0);
    }
    
}

