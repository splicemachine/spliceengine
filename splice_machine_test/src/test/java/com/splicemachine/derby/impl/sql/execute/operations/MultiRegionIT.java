package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test.SlowTest;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import static java.lang.String.format;
import static org.junit.Assert.*;

/**
 * SerialTest because it clears the statement history table, SlowTests because it performs manual splits.
 */
@Category(value = {SerialTest.class, SlowTest.class})
public class MultiRegionIT {

    private static final String SCHEMA_NAME = MultiRegionIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA_NAME);
    private static final String TABLE1_NAME = "TAB1";
    private static final String TABLE2_NAME = "TAB2";
    private static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA_NAME);
    private static final SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE1_NAME, SCHEMA_NAME, "(I INT, D DOUBLE)");
    private static final SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE2_NAME, SCHEMA_NAME, "(I INT, D DOUBLE)");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher2)
            .around(spliceTableWatcher1).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    PreparedStatement ps;
                    try {
                        ps = spliceClassWatcher.prepareStatement(format("insert into %s (i, d) values (?, ?)", TABLE1_NAME));
                        for (int j = 0; j < 100; ++j) {
                            for (int i = 0; i < 10; i++) {
                                ps.setInt(1, i);
                                ps.setDouble(2, i * 1.0);
                                ps.execute();
                            }
                        }
                        spliceClassWatcher.splitTable(TABLE1_NAME, SCHEMA_NAME, 250);
                        spliceClassWatcher.splitTable(TABLE1_NAME, SCHEMA_NAME, 500);
                        spliceClassWatcher.splitTable(TABLE1_NAME, SCHEMA_NAME, 750);

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA_NAME);

    @Test
    public void testDistinctCount() throws Exception {
        Long count = methodWatcher.query(format("select count(distinct i) from %s", TABLE1_NAME));
        assertEquals(10, count.intValue());
    }

    @Test
    public void testInsertSelectLimit() throws Exception {
        int count = methodWatcher.executeUpdate(format("insert into %s select * from %s {limit 100}", TABLE2_NAME, TABLE1_NAME));
        assertEquals(100, count);

        count = methodWatcher.executeUpdate(format("insert into %s select * from %s OFFSET 10 ROWS FETCH NEXT 10 ROWS ONLY", TABLE2_NAME, TABLE1_NAME));
        assertEquals(10, count);

        count = methodWatcher.executeUpdate(format("insert into %s select * from %s OFFSET 100 ROWS FETCH NEXT 3000 ROWS ONLY", TABLE2_NAME, TABLE1_NAME));
        assertEquals(900, count);

        count = methodWatcher.executeUpdate(format("insert into %s select * from %s OFFSET 100 ROWS", TABLE2_NAME, TABLE1_NAME));
        assertEquals(900, count);

        ResultSet rs = methodWatcher.executeQuery(format("select count(*) from %s", TABLE2_NAME));
        assertTrue(rs.next());
        assertEquals(1910, rs.getInt(1));
    }

    private int getNumOfRegions(String schemaName, String tableName) throws Exception {
        long conglomId = spliceClassWatcher.getConglomId(tableName, schemaName);
        HBaseAdmin admin = SpliceUtils.getAdmin();
        List<HRegionInfo> regions = admin.getTableRegions(Bytes.toBytes(Long.toString(conglomId)));
        return regions.size();
    }
}
