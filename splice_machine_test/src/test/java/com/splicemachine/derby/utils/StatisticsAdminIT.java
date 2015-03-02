package com.splicemachine.derby.utils;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.test.SerialTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author Scott Fines
 *         Date: 3/2/15
 */
@Category(SerialTest.class)
public class StatisticsAdminIT {
    private static SpliceWatcher classWatcher = new SpliceWatcher();
    private static final String CLASSNAME = StatisticsAdminIT.class.getSimpleName().toUpperCase();

    private static final SpliceSchemaWatcher schema = new SpliceSchemaWatcher(CLASSNAME);

    private static final SpliceTableWatcher emptyTable = new SpliceTableWatcher("EMPTY",CLASSNAME,"(a int)");
    private static final SpliceTableWatcher occupiedTable = new SpliceTableWatcher("OCCUPIED",CLASSNAME,"(a int)");

    private static TestConnection conn;

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schema)
            .around(emptyTable)
            .around(occupiedTable)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        PreparedStatement ps = classWatcher.prepareStatement("insert into "+occupiedTable+"(a) values (?)");
                        ps.setInt(1,1);
                        ps.execute();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });

    @BeforeClass
    public static void setupClass() throws Exception{
        conn = classWatcher.getOrCreateConnection();
    }

    @AfterClass
    public static void tearDownClass() throws Exception{
        conn.close();
    }

    @Before
    public void setUp() throws Exception {
        conn.setAutoCommit(false);
    }

    @After
    public void tearDown() throws Exception {
        conn.rollback(); //rollback any modifications made
        conn.reset();
    }

    @Test
    public void testTableStatisticsAreCorrectForEmptyTable() throws Exception {
        CallableStatement callableStatement = conn.prepareCall("call SYSCS_UTIL.COLLECT_TABLE_STATISTICS(?,?,?)");
        callableStatement.setString(1,schema.schemaName);
        callableStatement.setString(2,emptyTable.tableName);
        callableStatement.setBoolean(3,false);

        callableStatement.execute();
        /*
         * Now we need to make sure that the statistics were properly recorded.
         *
         * There are 2 tables in particular of interest: the Column stats, and the row stats. Since
         * the emptyTable is not configured to collect any column stats, we only check the row stats for
         * values.
         */
        long conglomId = SpliceAdmin.getConglomids(conn,schema.schemaName, emptyTable.tableName)[0];
        PreparedStatement check = conn.prepareStatement("select * from sys.systablestatistics where conglomerateId = ?");
        check.setLong(1, conglomId);
        ResultSet resultSet = check.executeQuery();
        Assert.assertTrue("Unable to find statistics for table!", resultSet.next());
        Assert.assertEquals("Incorrect row count!", 0l, resultSet.getLong(6));
        Assert.assertEquals("Incorrect partition size!",0l,resultSet.getLong(7));
        Assert.assertEquals("Incorrect row width!",0l,resultSet.getInt(8));
    }

    @Test
    public void testTableStatisticsCorrectForOccupiedTable() throws Exception {
        CallableStatement callableStatement = conn.prepareCall("call SYSCS_UTIL.COLLECT_TABLE_STATISTICS(?,?,?)");
        callableStatement.setString(1,schema.schemaName);
        callableStatement.setString(2,occupiedTable.tableName);
        callableStatement.setBoolean(3,false);

        callableStatement.execute();
        /*
         * Now we need to make sure that the statistics were properly recorded.
         *
         * There are 2 tables in particular of interest: the Column stats, and the row stats. Since
         * the emptyTable is not configured to collect any column stats, we only check the row stats for
         * values.
         */
        long conglomId = SpliceAdmin.getConglomids(conn,schema.schemaName, occupiedTable.tableName)[0];
        PreparedStatement check = conn.prepareStatement("select * from sys.systablestatistics where conglomerateId = ?");
        check.setLong(1, conglomId);
        ResultSet resultSet = check.executeQuery();
        Assert.assertTrue("Unable to find statistics for table!", resultSet.next());
        Assert.assertEquals("Incorrect row count!", 1l, resultSet.getLong(6));
        /*
         * We would love to assert specifics about the size of the partition and the width
         * of the row, but doing so results in a fragile test--the size of the row changes after the
         * transaction system performed read resolution, so if you wait for long enough (i.e. have a slow
         * enough system) this test will end up breaking. However, we do know that there is only a single
         * row in this table, so the partition size should be the same as the avgRowWidth
         */
        long partitionSize = resultSet.getLong(7);
        long rowWidth = resultSet.getLong(8);
        Assert.assertTrue("partition size != row width!",partitionSize==rowWidth);
    }
}

