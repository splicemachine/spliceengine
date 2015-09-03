package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_tools.TableCreator;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 *
 * 
 *
 */
public class SelectivityIT extends SpliceUnitTest {
    private static Logger LOG = Logger.getLogger(SelectivityIT.class);
    public static final String CLASS_NAME = SelectivityIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public static void createData(Connection conn, String schemaName) throws Exception {

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
                .create();
        for (int i = 0; i < 10; i++) {
            spliceClassWatcher.executeUpdate("insert into ts_low_cardinality select * from ts_low_cardinality");
        }

        new TableCreator(conn)
                .withCreate("create table ts_high_cardinality (c1 int, c2 varchar(56), c3 timestamp, c4 boolean)")
                .create();

        PreparedStatement insert = spliceClassWatcher.prepareStatement("insert into ts_high_cardinality values (?,?,?,?)");

        long time = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            insert.setInt(1,i);
            insert.setString(2, "" + i);
            insert.setTimestamp(3,new Timestamp(time-i));
            insert.setBoolean(4,false);
            insert.addBatch();
            if (1%100==0)
                insert.executeBatch();
        }
        insert.executeBatch();

        new TableCreator(conn)
                .withCreate("create table ts_nulls (c1 int, c2 varchar(56), c3 timestamp, c4 boolean)")
                .withInsert("insert into ts_nulls values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false),
                        row(null, null, null, null),
                        row(null, null, null, null),
                        row(null, null, null, null)))
                        .create();
        new TableCreator(conn)
                .withCreate("create table ts_nonulls (c1 int, c2 varchar(56), c3 timestamp, c4 boolean)")
                .withInsert("insert into ts_nonulls values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false))).create();

        new TableCreator(conn)
                .withCreate("create table ts_notnulls (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null)")
                .withInsert("insert into ts_notnulls values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false))).create();

        new TableCreator(conn)
                .withCreate("create table ts_singlepk (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null, primary key (c1))")
                .withInsert("insert into ts_singlepk values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false))).create();

        new TableCreator(conn)
                .withCreate("create table ts_multiplepk (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null, primary key (c1,c2))")
                .withInsert("insert into ts_multiplepk values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false))).create();

        conn.createStatement().executeQuery(format(
                "call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS('%s',false)",
                schemaName));

        conn.commit();
        new TableCreator(conn)
                .withCreate("create table tns_nulls (c1 int, c2 varchar(56), c3 timestamp, c4 boolean)")
                .withInsert("insert into tns_nulls values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false),
                        row(null, null, null, null),
                        row(null, null, null, null),
                        row(null, null, null, null))).create();
        new TableCreator(conn)
                .withCreate("create table tns_nonulls (c1 int, c2 varchar(56), c3 timestamp, c4 boolean)")
                .withInsert("insert into tns_nonulls values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false))).create();

        new TableCreator(conn)
                .withCreate("create table tns_notnulls (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null)")
                .withInsert("insert into tns_notnulls values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false))).create();
        new TableCreator(conn)
                .withCreate("create table tns_singlepk (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null, primary key (c1))")
                .withInsert("insert into tns_singlepk values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false))).create();

        new TableCreator(conn)
                .withCreate("create table tns_multiplepk (c1 int not null, c2 varchar(56) not null, c3 timestamp not null, c4 boolean not null, primary key (c1,c2))")
                .withInsert("insert into tns_multiplepk values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false))).create();

        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }


    @Test
    public void testNullSelectivity() throws Exception {
        // with stats
        rowContainsQuery(3,"explain select * from ts_nulls where c1 is null","outputRows=3,",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_nonulls where c1 is null","outputRows=1,",methodWatcher); // clamps to 1
        rowContainsQuery(3,"explain select * from ts_notnulls where c1 is null","outputRows=1,",methodWatcher); // clamps to 1
        rowContainsQuery(3,"explain select * from ts_singlepk where c1 is null","outputRows=1,",methodWatcher); // clamps to 1
        rowContainsQuery(3,"explain select * from ts_multiplepk where c1 is null","outputRows=1,",methodWatcher); // clamps to 1
        // without stats
        rowContainsQuery(3,"explain select * from tns_nulls where c1 is null","outputRows=2,",methodWatcher);
        rowContainsQuery(3,"explain select * from tns_nonulls where c1 is null","outputRows=2,",methodWatcher);
        rowContainsQuery(3,"explain select * from tns_notnulls where c1 is null","outputRows=2,",methodWatcher);
        rowContainsQuery(3,"explain select * from tns_singlepk where c1 is null","outputRows=1,",methodWatcher);
        rowContainsQuery(3,"explain select * from tns_multiplepk where c1 is null","outputRows=2,",methodWatcher);
    }

    @Test
    public void testNotNullSelectivity() throws Exception {
        // with stats
        rowContainsQuery(3,"explain select * from ts_nulls where c1 is not null","outputRows=5,",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_nonulls where c1 is not null","outputRows=5,",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_notnulls where c1 is not null","outputRows=5,",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_singlepk where c1 is not null","outputRows=5,",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_multiplepk where c1 is not null","outputRows=5,",methodWatcher);
        // without stats
        rowContainsQuery(3,"explain select * from tns_nulls where c1 is not null","outputRows=18,",methodWatcher);
        rowContainsQuery(3,"explain select * from tns_nonulls where c1 is not null","outputRows=18,",methodWatcher);
        rowContainsQuery(3,"explain select * from tns_notnulls where c1 is not null","outputRows=18,",methodWatcher);
        rowContainsQuery(3,"explain select * from tns_singlepk where c1 is not null","outputRows=18,",methodWatcher);
        rowContainsQuery(3,"explain select * from tns_multiplepk where c1 is not null","outputRows=18,",methodWatcher);
    }

        @Test
    @Ignore("DB-3629")
    public void testInSelectivity() throws Exception {
        // with stats
        secondRowContainsQuery("explain select * from ts_nulls where c1 in (1,2,3)","outputRows=3",methodWatcher);
        secondRowContainsQuery("explain select * from ts_nonulls where c1 in (1,2,3)","outputRows=3",methodWatcher);
        secondRowContainsQuery("explain select * from ts_notnulls where c1 in (1,2,3)","outputRows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_singlepk where c1 in (1,2,3)","outputRows=3",methodWatcher);
        firstRowContainsQuery("explain select * from ts_multiplepk where c1 in (1,2,3)","outputRows=3",methodWatcher);
        // without stats
        secondRowContainsQuery("explain select * from tns_nulls where c1 in (1,2,3)","outputRows=2",methodWatcher);
        secondRowContainsQuery("explain select * from tns_nonulls where c1 in (1,2,3)","outputRows=3",methodWatcher);
        secondRowContainsQuery("explain select * from tns_notnulls where c1 in (1,2,3)","outputRows=0",methodWatcher);
        secondRowContainsQuery("explain select * from tns_singlepk where c1 in (1,2,3)","outputRows=3",methodWatcher);
        secondRowContainsQuery("explain select * from tns_multiplepk where c1 in (1,2,3)","outputRows=3",methodWatcher);

    }

    @Test
    public void testWildcardLikeSelectivity() throws Exception {
        // with stats
        rowContainsQuery(3,"explain select * from ts_nulls where c2 like '%1'","outputRows=4,",methodWatcher); // ?JL
        rowContainsQuery(3,"explain select * from ts_nonulls where c2 like '%1'","outputRows=3,",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_notnulls where c2 like '%1'","outputRows=3,",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_singlepk where c2 like '%1'","outputRows=3,",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_multiplepk where c2 like '%1'","outputRows=3,",methodWatcher);
        // without stats
        rowContainsQuery(3,"explain select * from tns_nulls where c2 like '%1'","outputRows=10,",methodWatcher);
        rowContainsQuery(3,"explain select * from tns_nonulls where c2 like '%1'","outputRows=10,",methodWatcher);
        rowContainsQuery(3,"explain select * from tns_notnulls where c2 like '%1'","outputRows=10,",methodWatcher);
        rowContainsQuery(3,"explain select * from tns_singlepk where c2 like '%1'","outputRows=10,",methodWatcher);
        rowContainsQuery(3,"explain select * from tns_multiplepk where c2 like '%1'","outputRows=10,",methodWatcher);

    }

    @Test
    public void testAndSelectivity() throws Exception {
    }

    @Test
    public void testOrSelectivity() throws Exception {
    }

    @Test
    public void testNotSelectivity() throws Exception {

    }

    @Test
    public void testDB3635Between() throws Exception {
        rowContainsQuery(3,"explain select * from ts_singlepk where c1 < 3 and c1< 6 and c1>4 and c1>5","outputRows=1,",methodWatcher);
    }

    @Test
    // 6 is out of bounds, 3 valid
    public void testSinglePKMultiprobeScan() throws Exception {
        rowContainsQuery(3,"explain select * from ts_singlepk where c1 in (3,6,4,5)","outputRows=3,",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_singlepk where c1 in (3,6,4,5)","MultiProbeTableScan",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_singlepk where c1 in (3,6,4,5) and c1 = 3","outputRows=1,",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_singlepk where c1 in (3,6,4,5) and c1 = 3","MultiProbeTableScan",methodWatcher);
    }

    @Test
    public void testMultiplePKMultiprobeScan() throws Exception {
        rowContainsQuery(3,"explain select * from ts_multiplepk where c1 in (3,6,4,5) and c2 = '3'","outputRows=1",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_multiplepk where c1 in (3,6,4,5) and c2 = '3'","MultiProbeTableScan",methodWatcher);
    }

    @Test
    public void testLTSelectivity() throws Exception {

        // with stats
        rowContainsQuery(3,"explain select * from ts_nulls where c1 < 3", "outputRows=2,", methodWatcher);
        rowContainsQuery(3,"explain select * from ts_nonulls where c1 < 3", "outputRows=2,", methodWatcher);
        rowContainsQuery(3,"explain select * from ts_notnulls where c1 < 3", "outputRows=2,", methodWatcher);
        rowContainsQuery(3,"explain select * from ts_singlepk where c1 < 3","outputRows=2,",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_multiplepk where c1 < 3","outputRows=2,",methodWatcher);
        // without stats
        rowContainsQuery(3,"explain select * from tns_nulls where c1 < 3","outputRows=18",methodWatcher);
        rowContainsQuery(3,"explain select * from tns_nonulls where c1 < 3","outputRows=18",methodWatcher);
        rowContainsQuery(3,"explain select * from tns_notnulls where c1 < 3","outputRows=18",methodWatcher);
        rowContainsQuery(3,"explain select * from tns_singlepk where c1 < 3","outputRows=18",methodWatcher);
        rowContainsQuery(3,"explain select * from tns_multiplepk where c1 < 3","outputRows=18",methodWatcher);

    }

    @Test
    public void testLTESelectivity() throws Exception {
        // with stats
        rowContainsQuery(3,"explain select * from ts_nulls where c1 <= 3","outputRows=3,",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_nonulls where c1 <= 3","outputRows=3,",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_notnulls where c1 <= 3","outputRows=3,",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_singlepk where c1 <= 3","outputRows=3,",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_multiplepk where c1 <= 3","outputRows=3,",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_high_cardinality where c1 <= 500","outputRows=501,",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_high_cardinality where c1 <= 1000","outputRows=1001,",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_high_cardinality where c1 <= 2000","outputRows=2001,",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_high_cardinality where c1 <= 8000","outputRows=8001,",methodWatcher);

        // without stats
/*        firstRowContainsQuery("explain select * from tns_nulls where c1 <= 3","outputRows=10",methodWatcher);
        firstRowContainsQuery("explain select * from tns_nonulls where c1 <= 3","outputRows=10",methodWatcher);
        firstRowContainsQuery("explain select * from tns_notnulls where c1 <= 3","outputRows=10",methodWatcher);
        firstRowContainsQuery("explain select * from tns_singlepk where c1 <= 3","outputRows=10",methodWatcher);
        firstRowContainsQuery("explain select * from tns_multiplepk where c1 <= 3","outputRows=10",methodWatcher);
        */
    }


    @Test
    public void testGTSelectivity() throws Exception {

    }

    @Test
    public void testGTESelectivity() throws Exception {

    }

    @Test
    public void testBetweenSelectivity() throws Exception {
        rowContainsQuery(3,"explain select * from ts_high_cardinality where c1 <= 500 and c1 >= 0","outputRows=501,",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_high_cardinality where c1 <= 1000 and c1 >= 500","outputRows=501,",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_high_cardinality where c1 <= 2000 and c1 >= 500","outputRows=1501,",methodWatcher);
        rowContainsQuery(3,"explain select * from ts_high_cardinality where c1 <= 8000 and c1 >= 1000","outputRows=7001,",methodWatcher);
    }


    @Test
    @Ignore
    public void testOutOfBoundsPredicates() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("explain select * from ts_nulls where date(c3)='0000-01-01'");
        rs.next();
        Assert.assertTrue("row count inaccurate " + rs.getString(1), rs.getString(1).contains("outputRows=5"));
    }


    @Test
    @Ignore("10% Selectivity")
    public void testProjectionSelectivity() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("explain select * from ts_nulls where c2 like '%1%'");
        rs.next();
        Assert.assertTrue("row count inaccurate " + rs.getString(1), rs.getString(1).contains("outputRows=1"));
    }

    @Test
    @Ignore
    public void testExtractOperatorNodeSelectivity() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("explain select * from ts_low_cardinality where month(c3) = 1");
        rs.next();
        Assert.assertTrue("row count inaccurate " + rs.getString(1), rs.getString(1).contains("outputRows=1"));
    }

    @Test
    public void testLimitCosting() throws Exception {
        rowsContainsQuery("explain select * from ts_low_cardinality {limit 10}", new Contains().add(1,"rows=10,").add(2,"totalCost=1978.24"), methodWatcher);
    }


}