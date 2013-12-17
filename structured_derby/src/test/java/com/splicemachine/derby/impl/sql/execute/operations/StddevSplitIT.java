package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

import com.splicemachine.derby.utils.ConglomerateUtils;
import com.splicemachine.derby.utils.SpliceUtils;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;

public class StddevSplitIT extends SpliceUnitTest {
    public static final String CLASS_NAME = StddevSplitIT.class.getSimpleName().toUpperCase();

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String TABLE_NAME = "TAB";
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static String tableDef = "(I INT, D DOUBLE)";
    protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher(TABLE_NAME,CLASS_NAME, tableDef);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    /**
     * This '@Before' method is ran before every '@Test' method
     */
    @Before
    public void setUp() throws Exception {
        Connection conn = methodWatcher.createConnection();
        for (int j = 0 ; j < 100; ++j) {
            for (int i=0; i<10; i++) {
                conn.createStatement().execute(
                    String.format("insert into %s (i, d) values (%d, %f)",
                    		this.getTableReference(TABLE_NAME), i, i * 1.0));
            }
        }
        ResultSet resultSet = conn.createStatement().executeQuery(
                String.format("select * from %s", this.getTableReference(TABLE_NAME)));
        Assert.assertEquals(1000, resultSetSize(resultSet));
        resultSet.close();
        
        /*conn.createStatement().execute(
                String.format("create derby aggregate StddevSplitIT.stddevpop for double external name \'com.splicemachine.derby.impl.sql.execute.operations.SpliceStddevPop\'"));

        conn.createStatement().execute(
                String.format("create derby aggregate StddevSplitIT.stddevsamp for double external name \'com.splicemachine.derby.impl.sql.execute.operations.SpliceStddevSamp\'"));*/
        
        spliceClassWatcher.splitTable(TABLE_NAME, CLASS_NAME, 250);
        spliceClassWatcher.splitTable(TABLE_NAME, CLASS_NAME, 500);
        spliceClassWatcher.splitTable(TABLE_NAME, CLASS_NAME, 750);
    }

    
    @Test
    public void test() throws Exception {
    	Connection conn = methodWatcher.createConnection();
    	ResultSet rs = conn.createStatement().executeQuery(
                String.format("select SYSCS_UTIL.stddev_pop(i) from %s", this.getTableReference(TABLE_NAME)));

        while(rs.next()){
        	Assert.assertEquals((int)rs.getDouble(1), 2);
        }
        rs.close();

        rs = conn.createStatement().executeQuery(
                String.format("select SYSCS_UTIL.stddev_samp(i) from %s", this.getTableReference(TABLE_NAME)));

        while(rs.next()){
            Assert.assertEquals((int)rs.getDouble(1), 2);
        }
    }

    void wait(byte[] name) throws IOException, InterruptedException {

        HBaseAdmin admin = SpliceUtils.getAdmin();
        boolean isSplitting=true;
        while(isSplitting){
            isSplitting=false;
            List<HRegionInfo> regions = admin.getTableRegions(name);
            if (regions != null) {
                for(HRegionInfo region:regions){
                    if(region.isSplit()){
                        isSplitting=true;
                        break;
                    }
                }
            } else {
                isSplitting = true;
            }
            Thread.sleep(500);
        }
}    }
