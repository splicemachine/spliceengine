package com.splicemachine.derby.hbase;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import com.splicemachine.test.SerialTest;
import com.splicemachine.test.SlowTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.utils.SpliceUtilities;

@Category(value = {SlowTest.class, SerialTest.class})
public class SaltedTempTableIT extends SpliceUnitTest {

    private static final String SCHEMA_NAME = SaltedTempTableIT.class.getSimpleName().toUpperCase();

    @Before
    public void setup() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(new Configuration());
        admin.disableTable(SpliceConstants.TEMP_TABLE_BYTES);
        admin.deleteTable(SpliceConstants.TEMP_TABLE_BYTES);
        SpliceUtilities.createTempTable(admin);
        admin.close();
    }

    private static String TABLE_NAME_1 = "selfjoin";
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(
            SCHEMA_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(
            SCHEMA_NAME);
    protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_NAME_1,
            SCHEMA_NAME, "(i int, j int)");
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher1)
            .around(new SpliceDataWatcher() {

                @Override
                protected void starting(Description description) {
                    try {
                        PreparedStatement ps = spliceClassWatcher.prepareStatement(String.format(
                                "insert into %s (i, j) values (?,?)", TABLE_NAME_1));
                        for (int i = 0; i < 200; i++) {
                            ps.setInt(1, i % 50); ps.setInt(2, i);
                            ps.executeUpdate();
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        spliceClassWatcher.closeAll();
                    }
                }
            });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA_NAME);

    @Test
    public void testGroupAggregateDistributesTempData() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                join(
                    "select a.i, avg(a.j), sum(a.j) from ",
                    TABLE_NAME_1 + " a ",
                    "group by a.i"));
        int j = 0;
        while (rs.next()) {
            j++;
        }
        Assert.assertEquals(50, j);
        assertDataInMostTempRegions();
    }

    private void assertDataInMostTempRegions() throws Exception {
        Configuration conf = new Configuration();
        HBaseAdmin admin = new HBaseAdmin(conf);
        List<HRegionInfo> regions = admin.getTableRegions(SpliceConstants.TEMP_TABLE_BYTES);
        int totalRegions = regions.size();
        HTable table = new HTable(conf, SpliceConstants.TEMP_TABLE_BYTES);
        List<HRegionInfo> emptyRegions = new ArrayList<HRegionInfo>();
        for (HRegionInfo region : regions) {
            if (!isThereDataInRegion(region, table)) {
                emptyRegions.add(region);
            }
        }
        admin.close();
        Assert.assertTrue("More than 25% of temp regions are empty. List of empty regions: " + emptyRegions,
                emptyRegions.size() < totalRegions * 0.75);
    }

    private boolean isThereDataInRegion(HRegionInfo region, HTable table) throws IOException {
        Scan scan = new Scan();
        scan.setRaw(true);
        scan.setStartRow(region.getStartKey());
        scan.setStopRow(region.getEndKey());
        ResultScanner scanner = table.getScanner(scan);
        int count = 0;
        while (scanner.next() != null) {
            count++;
        }
        return count > 0;
    }

    @Test
    public void testMergeSortJoinDistributesTempData() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                join(
                    "select avg(cast (a.j as double)), sum(a.j) from ",
                    TABLE_NAME_1 + " a ",
                    "inner join " + TABLE_NAME_1 + " b --SPLICE-PROPERTIES joinStrategy=SORTMERGE ",
                    "on a.i = b.i ",
                    "inner join " + TABLE_NAME_1 + " c --SPLICE-PROPERTIES joinStrategy=SORTMERGE ",
                    "on b.i = c.i"));
        boolean results = rs.next();
        Assert.assertTrue("No results", results);
        assertDataInMostTempRegions();
    }

    private String join(String... strings) {
        StringBuilder sb = new StringBuilder();
        for (String s : strings) {
            sb.append(s).append('\n');
        }
        return sb.toString();
    }

}
