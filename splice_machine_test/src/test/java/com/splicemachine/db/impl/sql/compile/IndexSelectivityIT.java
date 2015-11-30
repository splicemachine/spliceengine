package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.*;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 *
 *
 *
 */
public class IndexSelectivityIT extends SpliceUnitTest {
    public static final String CLASS_NAME = IndexSelectivityIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @BeforeClass
    public static void createDataSet() throws Exception {
        Connection conn = spliceClassWatcher.getOrCreateConnection();
        new TableCreator(conn)
                .withCreate("create table ts_low_cardinality (c1 int, c2 varchar(56), c3 timestamp, c4 boolean)")
                .withInsert("insert into ts_low_cardinality values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false),
                        row(null, null, null, null),
                        row(null, null, null, null),
                        row(null, null, null, null)))
                .withIndex("create index ts_low_cardinality_ix_1 on ts_low_cardinality(c1)")
                .withIndex("create index ts_low_cardinality_ix_2 on ts_low_cardinality(c2)")
                .withIndex("create index ts_low_cardinality_ix_3 on ts_low_cardinality(c1,c2)")
                .withIndex("create index ts_low_cardinality_ix_4 on ts_low_cardinality(c1,c2,c3)")
                .withIndex("create index ts_low_cardinality_ix_5 on ts_low_cardinality(c1,c2,c3,c4)")
                .withIndex("create index ts_low_cardinality_ix_6 on ts_low_cardinality(c2,c1)")
                .create();
        for (int i = 0; i < 10; i++) {
            spliceClassWatcher.executeUpdate("insert into ts_low_cardinality select * from ts_low_cardinality");
        }

        new TableCreator(conn)
                .withCreate("create table ts_high_cardinality (c1 int, c2 varchar(56), c3 timestamp, c4 boolean)")
                .withIndex("create index ts_high_cardinality_ix_1 on ts_high_cardinality(c1)")
                .withIndex("create index ts_high_cardinality_ix_2 on ts_high_cardinality(c2)")
                .withIndex("create index ts_high_cardinality_ix_3 on ts_high_cardinality(c1,c2)")
                .withIndex("create index ts_high_cardinality_ix_4 on ts_high_cardinality(c1,c2,c3)")
                .create();

        PreparedStatement insert = spliceClassWatcher.prepareStatement("insert into ts_high_cardinality values (?,?,?,?)");

        long time = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            insert.setInt(1,i);
            insert.setString(2, "" + i);
            insert.setTimestamp(3,new Timestamp(time-i));
            insert.setBoolean(4,false);
            insert.addBatch();
            if (i%100==0)
                insert.executeBatch();
        }
        insert.executeBatch();
        conn.commit();

        conn.createStatement().executeQuery(format(
                "call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS('%s',false)",
                spliceSchemaWatcher));
        conn.commit();
    }

    @Test
    public void testCoveringIndexScan() throws Exception {
        rowContainsQuery(3,"explain select c1 from ts_low_cardinality where c1 = 1","IndexScan[TS_LOW_CARDINALITY_IX_1",methodWatcher);
        rowContainsQuery(3,"explain select c1,c2 from ts_low_cardinality where c1 = 1","IndexScan[TS_LOW_CARDINALITY_IX_3",methodWatcher);
        rowContainsQuery(3,"explain select c2 from ts_low_cardinality where c2 = '1'","IndexScan[TS_LOW_CARDINALITY_IX_2",methodWatcher);
        rowContainsQuery(6,"explain select count(*) from ts_low_cardinality where c2 = '1'","IndexScan[TS_LOW_CARDINALITY_IX_2",methodWatcher);
    }

    @Test
    public void testSingleRowIndexLookup() throws Exception {
        rowContainsQuery(4,"explain select * from ts_high_cardinality where c1 = 1","IndexScan[TS_HIGH_CARDINALITY_IX_1",methodWatcher);
    }

    @Ignore("Obsoleted by testRangeIndexLookup1 and testRangeIndexLookup2")
    // Consider re-enabling this later, otherwise permanently remove it.
    public void testRangeIndexLookup() throws Exception {
        // 10/10000
        rowContainsQuery(4,"explain select * from ts_high_cardinality where c1 > 1 and c1 < 10","IndexScan[TS_HIGH_CARDINALITY_IX",methodWatcher);
        // 100/10000
        rowContainsQuery(4,"explain select * from ts_high_cardinality where c1 > 1 and c1 < 100","IndexScan[TS_HIGH_CARDINALITY_IX",methodWatcher);
        // 200/10000
        rowContainsQuery(3,"explain select * from ts_high_cardinality where c1 > 1 and c1 < 200","TableScan[TS_HIGH_CARDINALITY",methodWatcher);
        // 1000/10000
        rowContainsQuery(3,"explain select * from ts_high_cardinality where c1 > 1 and c1 < 1000","TableScan[TS_HIGH_CARDINALITY",methodWatcher);
        // 2000/10000
        rowContainsQuery(3,"explain select * from ts_high_cardinality where c1 > 1 and c1 < 2000","TableScan[TS_HIGH_CARDINALITY",methodWatcher);
        // 5000/10000
        rowContainsQuery(3,"explain select * from ts_high_cardinality where c1 > 1 and c1 < 5000","TableScan[TS_HIGH_CARDINALITY",methodWatcher);
    }
    
    @Test
    public void testRangeIndexLookup1() throws Exception {
    	// Instead of running unhinted explain and asserting that a TableScan or IndexScan
    	// is selected (which is what we used to do here), now we hint with a specific index
    	// and assert the more specific outcome of correct outputRows for both
    	// the IndexScan and IndexLookup.
    	
    	String index = "TS_HIGH_CARDINALITY_IX_1";
    	String query = "explain select * from ts_high_cardinality --SPLICE-PROPERTIES index=%s \n where c1 > 1 and c1 < %d";
        
    	// 10/10000
        rowContainsQuery(new int[]{3, 3, 4, 4},
        	format(query, index, 10),
            methodWatcher,
            "IndexLookup","outputRows=8",
            "IndexScan[TS_HIGH_CARDINALITY_IX_1","outputRows=8");
        
        // 100/10000
        rowContainsQuery(new int[]{3, 3, 4, 4},
        	format(query, index, 100),
            methodWatcher,
            "IndexLookup","outputRows=98",
            "IndexScan[TS_HIGH_CARDINALITY_IX_1","outputRows=98");
        
        // 200/10000
        rowContainsQuery(new int[]{3, 3, 4, 4},
        	format(query, index, 200),
            methodWatcher,
            "IndexLookup","outputRows=198",
            "IndexScan[TS_HIGH_CARDINALITY_IX_1","outputRows=198");
        
        // 1000/10000
        rowContainsQuery(new int[]{3, 3, 4, 4},
        	format(query, index, 1000),
            methodWatcher,
            "IndexLookup","outputRows=998",
            "IndexScan[TS_HIGH_CARDINALITY_IX_1","outputRows=998");
        
        // 2000/10000
        rowContainsQuery(new int[]{3, 3, 4, 4},
        	format(query, index, 2000),
            methodWatcher,
            "IndexLookup","outputRows=1998",
            "IndexScan[TS_HIGH_CARDINALITY_IX_1","outputRows=1998");
        
        // 5000/10000
        rowContainsQuery(new int[]{3, 3, 4, 4},
        	format(query, index, 5000),
            methodWatcher,
            "IndexLookup","outputRows=4998",
            "IndexScan[TS_HIGH_CARDINALITY_IX_1","outputRows=4998");
    }

    @Test
    public void testRangeIndexLookup2() throws Exception {
        // Similar to testRangeIndexLookup1 except use index 2 (TS_HIGH_CARDINALITY_IX_2)
    	// even though we still filter by C1. This will introduce ProjectRestrict
    	// into the plan, but it should still have the correct outputRows.
    	// DB-3872 caused the ProjectRestrict outputRows to be incorrect.

    	String index2 = "TS_HIGH_CARDINALITY_IX_2";
    	String query = "explain select * from ts_high_cardinality --SPLICE-PROPERTIES index=%s \n where c1 > 1 and c1 < %d";
        
    	// 10/10000
        rowContainsQuery(new int[]{3, 3},
        	format(query, index2, 10),
            methodWatcher,
            "ProjectRestrict", "outputRows=8");
        
        // 100/10000
        rowContainsQuery(new int[]{3, 3},
        	format(query, index2, 100),
            methodWatcher,
            "ProjectRestrict", "outputRows=98");
        
        // 200/10000
        rowContainsQuery(new int[]{3, 3},
        	format(query, index2, 200),
            methodWatcher,
            "ProjectRestrict", "outputRows=198");
        
        // 1000/10000
        rowContainsQuery(new int[]{3, 3},
        	format(query, index2, 1000),
            methodWatcher,
            "ProjectRestrict", "outputRows=998");
        
        // 2000/10000
        rowContainsQuery(new int[]{3, 3},
        	format(query, index2, 2000),
            methodWatcher,
            "ProjectRestrict", "outputRows=1998");
        
        // 5000/10000
        rowContainsQuery(new int[]{3, 3},
        	format(query, index2, 5000),
            methodWatcher,
            "ProjectRestrict", "outputRows=4998");
    }

    @Test
    public void testNonCoveringIndexScan() throws Exception {

    }

    @Test
    public void testMostSelectiveIndexChosen() throws Exception {

    }

    @Test
    public void test1PercentRangeScan() throws Exception {

    }

    @Test
    public void test20PercentRangeScan() throws Exception {

    }

    @Test
    public void testIndexScanSelectivity() throws Exception {
        String query = "explain select * from --SPLICE-PROPERTIES joinOrder=FIXED\n" +
                "ts_high_cardinality t1, ts_low_cardinality t2 --SPLICE-PROPERTIES index=%s, joinStrategy=SORTMERGE\n" +
                "where t1.c1=t2.c1 and t2.c2='1'";
        ResultSet rs = methodWatcher.executeQuery(String.format(query, "ts_low_cardinality_ix_3"));
        double c1 = getIndexScanCost(rs);
        rs = methodWatcher.executeQuery(String.format(query, "ts_low_cardinality_ix_6"));
        double c2 = getIndexScanCost(rs);
        Assert.assertTrue(c1>c2);
    }

    private double getIndexScanCost(ResultSet rs) throws SQLException {
        String s=null;
        while(rs.next()) {
            s = rs.getString(1).trim();
            if (s.contains("IndexScan")) break;
        }
        Assert.assertNotNull(s);
        String[] indexScan = s.split(",");
        for (String s1 : indexScan) {
            if (s1.contains("totalCost")) {
                String[] s2 = s1.split("=");
                return Double.parseDouble(s2[1]);
            }
        }
        return 0;
    }

}
