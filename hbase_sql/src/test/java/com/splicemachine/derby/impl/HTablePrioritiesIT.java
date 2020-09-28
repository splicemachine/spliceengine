package com.splicemachine.derby.impl;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test.SerialTest;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category({SerialTest.class})
public class HTablePrioritiesIT {
    public static final String CLASS_NAME = HTablePrioritiesIT.class.getSimpleName().toUpperCase();
    protected static String TABLE = "T";
    protected static String TABLEA = "A";
    protected static String TABLED = "D";
    protected static String TABLEE = "E";

    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    @ClassRule
    public static TestRule classRule = spliceClassWatcher;

    @Rule
    public SpliceWatcher methodRule = new SpliceWatcher();

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher(TABLE, spliceSchemaWatcher
            .schemaName, "(name varchar(40), title varchar(40), age int)");
    protected static SpliceTableWatcher spliceTableAWatcher = new SpliceTableWatcher(TABLEA, spliceSchemaWatcher
            .schemaName, "(name varchar(40), title varchar(40), age int)");
    protected static SpliceTableWatcher spliceTableDWatcher = new SpliceTableWatcher(TABLED, spliceSchemaWatcher
            .schemaName, "(name varchar(40), title varchar(40), age int)");
    protected static SpliceTableWatcher spliceTableEWatcher = new SpliceTableWatcher(TABLEE, spliceSchemaWatcher
            .schemaName, "(name varchar(40), title varchar(40), age int)");
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher)
            .around(spliceTableAWatcher)
            .around(spliceTableDWatcher)
            .around(spliceTableEWatcher);

    @Test
    public void testTablesPriority() throws Exception {
        int prioNormal = 0, prioAdmin = 0, prioHigh = 0;
        try(Admin admin= ConnectionFactory.createConnection(new Configuration()).getAdmin()){
            HTableDescriptor[] tableDescriptors = admin.listTables();
            for( HTableDescriptor td : tableDescriptors) {
                String s = td.getValue("tableDisplayName");
                if( s == null ) {

                    String arr[] = {"splice:DROPPED_CONGLOMERATES", "splice:SPLICE_CONGLOMERATE",
                            "splice:SPLICE_MASTER_SNAPSHOTS", "splice:SPLICE_REPLICATION_PROGRESS",
                            "splice:SPLICE_SEQUENCES", "splice:SPLICE_TXN", "splice:TENTATIVE_DDL"};
                    Assert.assertTrue(td.toString(), ArrayUtils.contains(arr, td.getTableName().getNameAsString()));
                    Assert.assertEquals(td.toString(), HConstants.HIGH_QOS, td.getPriority());
                    prioHigh ++;
                }
                else
                {
                    if( s.startsWith("SYS") || s.startsWith("splice:") || s.equals("MON_GET_CONNECTION") ) {
                        Assert.assertEquals(td.toString(), HConstants.ADMIN_QOS, td.getPriority());
                        prioAdmin ++;
                    }
                    else {
                        Assert.assertEquals(td.toString(), 0, td.getPriority());
                        prioNormal ++;
                    }
                }
            }
        }
        // assert there's actually tables we are checking
        Assert.assertTrue( prioNormal >= 4 );
        Assert.assertTrue( prioAdmin > 5 );
        Assert.assertTrue( prioHigh > 5 );
    }

}
