package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.*;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class StddevIT extends SpliceUnitTest {
    public static final String CLASS_NAME = StddevIT.class.getSimpleName().toUpperCase();

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String TABLE_NAME = "TAB";
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static String tableDef = "(I INT, D DOUBLE)";
    protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher(TABLE_NAME,CLASS_NAME, tableDef);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher).around(new SpliceDataWatcher() {
								@Override
								protected void starting(Description description) {
										PreparedStatement ps;
										try {
												ps = spliceClassWatcher.prepareStatement(
																String.format("insert into %s (i,d) values (?,?)", spliceTableWatcher));
												for(int i=0;i<10;i++){
														ps.setInt(1,i);
														ps.setDouble(2, i);
														for(int j=0;j<100;j++){
																ps.addBatch();
														}
														ps.executeBatch();
												}
												spliceClassWatcher.executeUpdate(
																String.format("create derby aggregate StddevIT.stddevpop for double external name \'com.splicemachine.derby.impl.sql.execute.operations.SpliceStddevPop\'"));

												spliceClassWatcher.executeUpdate(
																String.format("create derby aggregate StddevIT.stddevsamp for double external name \'com.splicemachine.derby.impl.sql.execute.operations.SpliceStddevSamp\'"));
										} catch (Exception e) {
												throw new RuntimeException(e);
										}
								}
						});

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    /**
     * This '@Before' method is ran before every '@Test' method
     */
    @Before
    public void setUp() throws Exception {
        ResultSet resultSet = methodWatcher.executeQuery(
                String.format("select * from %s", this.getTableReference(TABLE_NAME)));
        Assert.assertEquals(1000, resultSetSize(resultSet));
        resultSet.close();
    }
    
    @Test
    public void test() throws Exception {

    	ResultSet rs = methodWatcher.executeQuery(
                String.format("select SYSCS_UTIL.stddev_pop(i) from %s", this.getTableReference(TABLE_NAME)));

        while(rs.next()){
        	Assert.assertEquals((int)rs.getDouble(1), 2);
        }
        rs.close();

        rs = methodWatcher.executeQuery(
								String.format("select SYSCS_UTIL.stddev_samp(i) from %s", this.getTableReference(TABLE_NAME)));

        while(rs.next()) {
            Assert.assertEquals((int)rs.getDouble(1), 2);
        }

    }
}
