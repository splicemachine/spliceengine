package com.splicemachine.derby.impl;

import com.splicemachine.access.hbase.HBasePartitionAdmin;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test.SerialTest;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.*;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

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
        try(Admin admin= ConnectionFactory.createConnection(new Configuration()).getAdmin()) {
            // we shouldn't have something to upgrade since we should've already created all tables correctly
            Assert.assertEquals( 0,
                    HBasePartitionAdmin.upgradeTablePrioritiesFromList(admin, admin.listTableDescriptors()) );

            testTablesPriority(admin);
        }
    }

    public void testTablesPriority(Admin admin) throws Exception {

        int prioNormal = 0, prioAdmin = 0, prioHigh = 0;
        List<TableDescriptor>  tableDescriptors = admin.listTableDescriptors();
        for( TableDescriptor td : tableDescriptors) {
            // test priority is a set in HBasePartitionAdmin.getPriorityShouldHave
            Assert.assertEquals(td.toString(), HBasePartitionAdmin.getPriorityShouldHave(td), td.getPriority());

            // this is a double-check, adjust this if you add tables
            String tdn = td.getValue("tableDisplayName");
            switch (td.getPriority()){
                case HConstants.ADMIN_QOS:
                    String arr[] = {"splice:DROPPED_CONGLOMERATES", "splice:SPLICE_CONGLOMERATE",
                            "splice:SPLICE_MASTER_SNAPSHOTS", "splice:SPLICE_REPLICATION_PROGRESS",
                            "splice:SPLICE_SEQUENCES", "splice:SPLICE_TXN", "splice:TENTATIVE_DDL"};
                    if( ArrayUtils.contains(arr, td.getTableName().getNameAsString()) ) {
                        Assert.assertEquals(null, tdn);
                    }
                    else {
                        Assert.assertTrue(tdn.startsWith("SYS") || tdn.startsWith("splice:") ||
                                tdn.equals("MON_GET_CONNECTION") );
                    }
                    prioAdmin++;
                    break;
                case HConstants.NORMAL_QOS:
                    prioNormal++;
                    break;
                default:
                    Assert.assertTrue("unknown priority", false);
            }
        }
        // assert there's actually tables we are checking
        Assert.assertTrue( prioNormal >= 4 );
        Assert.assertTrue( prioAdmin > 10 );
    }

    static public String getTableNameRepr(TableDescriptor td)
    {
        String s = td.getValue("tableDisplayName");
        s = s == null
                ? "N " + td.getTableName().getNameAsString()
                : "T " + s;
        String idx = td.getValue( "indexDisplayName");
        idx = idx == null ? "" : " INDEX " + idx;
        return s + idx;
    }

    // we have to further investigate this, but this conflicts with VacuumIT
    // maybe the enable/disable of the hbase tables here is conflicting
    @Ignore("DB-10230")
    @Test
    public void testTablesPriorityUpgrade() throws Exception {
        try(Admin admin= ConnectionFactory.createConnection(new Configuration()).getAdmin()) {
            List<TableDescriptor>  tdlist = admin.listTableDescriptors().stream()
                    .filter(td -> getTableNameRepr(td).startsWith("T SYSCONSTRAINTS"))
                    .collect(Collectors.toList());
            // list contains the table SYSCONSTRAINTS its indices (currently 1+3)
            Assert.assertTrue( tdlist.size() >= 4 );
            // change their priorities to PRIO = 0
            for( TableDescriptor td : tdlist ) {
                HBasePartitionAdmin.setHTablePriority(admin, td.getTableName(), td, 0 );
            }

            // do upgrade, assert we upgraded the priorities for the number of tables that we changed
            Assert.assertEquals( tdlist.size(),
                    HBasePartitionAdmin.upgradeTablePrioritiesFromList(admin, admin.listTableDescriptors()) );

            // now test if priorities are all correct
            testTablesPriority(admin);
        }
    }

}
