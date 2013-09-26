package com.splicemachine.derby.hbase;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.test.framework.DefaultedSpliceWatcher;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

@Ignore("bug 830")
public class SplittingTempTableIT extends SpliceUnitTest {

    private static final String SCHEMA_NAME = SplittingTempTableIT.class.getSimpleName().toUpperCase();

    @BeforeClass
    public static void setup() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(new Configuration());
        HTableDescriptor htd = admin.getTableDescriptor(SpliceConstants.TEMP_TABLE_BYTES);
        // This parameters cause the Temp regions to split more often
        htd.setMaxFileSize(10 * 1024 * 1024); // 10 MB
        htd.setMemStoreFlushSize(10 * 1024 * 1024); // 10 MB
        admin.disableTable(SpliceConstants.TEMP_TABLE_BYTES);
        admin.modifyTable(SpliceConstants.TEMP_TABLE_BYTES, htd);
        admin.enableTable(SpliceConstants.TEMP_TABLE_BYTES);
        admin.close();
    }

    @AfterClass
    public static void cleanup() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(new Configuration());
        HTableDescriptor htd = admin.getTableDescriptor(SpliceConstants.TEMP_TABLE_BYTES);
        htd.remove(HTableDescriptor.MEMSTORE_FLUSHSIZE);
        htd.remove(HTableDescriptor.MAX_FILESIZE);
        admin.disableTable(SpliceConstants.TEMP_TABLE_BYTES);
        admin.modifyTable(SpliceConstants.TEMP_TABLE_BYTES, htd);
        admin.enableTable(SpliceConstants.TEMP_TABLE_BYTES);
        admin.close();
    }

    private static String TABLE_NAME_1 = "selfjoin";
    protected static DefaultedSpliceWatcher spliceClassWatcher = new DefaultedSpliceWatcher(
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
                        for (int i = 0; i < 3; i++) {
                            ps.setInt(1, 1); ps.setInt(2, 1);
                            ps.executeUpdate();
                            ps.setInt(1, 1); ps.setInt(2, 3);
                            ps.executeUpdate();
                            ps.setInt(1, 2); ps.setInt(2, 2);
                            ps.executeUpdate();
                            ps.setInt(1, 2); ps.setInt(2, 4);
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
    public SpliceWatcher methodWatcher = new DefaultedSpliceWatcher(SCHEMA_NAME);

    @Test
    public void testBigIntermediateResults() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                join(
                    "select a.i, avg(a.j), sum(a.j) from ",
                    TABLE_NAME_1 + " a ",
                    "inner join " + TABLE_NAME_1 + " b --DERBY-PROPERTIES joinStrategy=SORTMERGE ",
                    "on a.i = b.i ",
                    "inner join " + TABLE_NAME_1 + " c --DERBY-PROPERTIES joinStrategy=SORTMERGE ",
                    "on b.i = c.i ",
                    "inner join " + TABLE_NAME_1 + " d --DERBY-PROPERTIES joinStrategy=SORTMERGE ",
                    "on c.i = d.i ",
                    "inner join " + TABLE_NAME_1 + " e --DERBY-PROPERTIES joinStrategy=SORTMERGE ",
                    "on d.i = e.i ",
                    "inner join " + TABLE_NAME_1 + " f --DERBY-PROPERTIES joinStrategy=SORTMERGE ",
                    "on e.i = f.i ",
                    "inner join " + TABLE_NAME_1 + " g --DERBY-PROPERTIES joinStrategy=SORTMERGE ",
                    "on f.i = g.i ",
                    "inner join " + TABLE_NAME_1 + " h --DERBY-PROPERTIES joinStrategy=SORTMERGE ",
                    "on g.i = h.i ",
                    "group by a.i"));
        int j = 0;
        boolean saw1 = false;
        boolean saw2 = false;
        final int sumFor1 = 3359232; // (1+3)*3*(6**7);
        final int sumFor2 = 5038848; // (2+4)*3*(6**7);
        while (rs.next()) {
            j++;
            Assert.assertNotNull(rs.getInt(1));
            if (rs.getInt(1) == 1) {
                saw1 = true;
                Assert.assertEquals("sum for 1 doesn't add up", sumFor1, rs.getInt(3));
                Assert.assertEquals("avg for 1 doesn't add up", 2, rs.getInt(2));
            } else if (rs.getInt(1) == 2){
                saw2 = true;
                Assert.assertEquals("sum for 2 doesn't add up", sumFor2, rs.getInt(3));
                Assert.assertEquals("avg for 2 doesn't add up", 3, rs.getInt(2));
            } else {
                Assert.fail("Unrecognized value");
            }
        }
        Assert.assertEquals(2, j);
        Assert.assertEquals(saw1, saw2);
    }

    private String join(String... strings) {
        StringBuilder sb = new StringBuilder();
        for (String s : strings) {
            sb.append(s).append('\n');
        }
        return sb.toString();
    }

}
