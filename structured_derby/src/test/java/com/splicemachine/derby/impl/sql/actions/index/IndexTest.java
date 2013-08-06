package com.splicemachine.derby.impl.sql.actions.index;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import org.apache.commons.dbutils.DbUtils;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.io.PrintStream;
import java.sql.*;
import java.util.Map;

/**
 * Test for index stuff, more or less encompassing:
 *
 *  nulls
 *  Multiple keys
 *  Different Data Sets
 *  Natural Column Order Changes
 *  Ascending/Descending
 *  Use in scans, inserts, updates
 *
 *  Indexes are created and dropped in each test so that tables can be reused.
 *
 * @author Jeff Cunningham
 *         Date: 7/31/13
 */
public class IndexTest extends SpliceUnitTest {
    private static final String SCHEMA_NAME = IndexTest.class.getSimpleName().toUpperCase();
    public static final String CUSTOMER_OORDER_JOIN = "select %1$s.c.c_last, %1$s.c.c_first, %1$s.o.o_id, %1$s.o.o_entry_d from %1$s.%2$s c, %1$s.%3$s o where c.c_id = o.o_c_id";
    public static final String SELECT_UNIQUE_CUSTOMER = "select * from %s.%s c where c.c_last = 'ESEPRIANTI' and c.c_id = 2365";
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();

    // taken from tpc-c
    private static final String CUSTOMER_BY_NAME = String.format("SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city, "
            + "c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, "
            + "c_balance, c_ytd_payment, c_payment_cnt, c_since FROM %s.%s"
            + " WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first", SCHEMA_NAME, CustomerTable.TABLE_NAME);

    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA_NAME);
    protected static CustomerTable customerTableWatcher = new CustomerTable(CustomerTable.TABLE_NAME,SCHEMA_NAME) {
        @Override
        protected void starting(Description description) {
            super.starting(description);
            importData(getResourceDirectory()+ "/index/customer.csv");
        }
    };

//    protected static SpliceIndexWatcher customerIndexWatcher =
//            new SpliceIndexWatcher(CustomerTable.TABLE_NAME,SCHEMA_NAME,CustomerTable.INDEX_NAME,SCHEMA_NAME,CustomerTable.INDEX_DEF, false);

    protected static OrderTable orderTableWatcher = new OrderTable(OrderTable.TABLE_NAME,SCHEMA_NAME) {
        @Override
        protected void starting(Description description) {
            super.starting(description);
            importData(getResourceDirectory()+ "/index/order-with-nulls.csv");
        }
    };

    protected static NewOrderTable newOrderTableWatcher = new NewOrderTable(NewOrderTable.TABLE_NAME,SCHEMA_NAME) {
        @Override
        protected void starting(Description description) {
            super.starting(description);
            importData(getResourceDirectory()+ "/index/new-order.csv");
        }
    };

//    protected static SpliceIndexWatcher orderIndexWatcher =
//            new SpliceIndexWatcher(OrderTable.TABLE_NAME,SCHEMA_NAME,OrderTable.INDEX_NAME,SCHEMA_NAME,OrderTable.INDEX_DEF, false);

    protected static OrderLineTable orderLineTableWatcher = new OrderLineTable(OrderLineTable.TABLE_NAME,SCHEMA_NAME) {
        @Override
        protected void starting(Description description) {
            super.starting(description);
            importData(getResourceDirectory()+ "/index/order-line.csv");
        }
    };

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(customerTableWatcher)
//            .around(customerIndexWatcher)
            .around(newOrderTableWatcher)
            .around(orderTableWatcher)
            .around(orderLineTableWatcher);
//            .around(orderIndexWatcher);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    // ===============================================================================
    // Query Tests
    // ===============================================================================

    @Test
    public void testQueryCustomerById() throws Exception {
        String query = String.format(SELECT_UNIQUE_CUSTOMER,
                SCHEMA_NAME,CustomerTable.TABLE_NAME);
        ResultSet rs = methodWatcher.executeQuery(query);
        Assert.assertEquals(1, count(rs));
    }

    @Test
    @Ignore("Create bug for this")
    public void testQueryCustomerByIdWithIndex() throws Exception {
        try {
            createIndex(methodWatcher.createConnection(), SCHEMA_NAME, CustomerTable.TABLE_NAME, CustomerTable.INDEX_NAME, CustomerTable.INDEX_ORDER_DEF_ASC, false);
        String query = String.format(SELECT_UNIQUE_CUSTOMER,
                SCHEMA_NAME,CustomerTable.TABLE_NAME);
        ResultSet rs = methodWatcher.executeQuery(query);
        Assert.assertEquals(1, count(rs));
        } finally {
            dropIndex(SCHEMA_NAME,CustomerTable.INDEX_NAME);
        }
    }

    @Test
    public void testQueryOrder() throws Exception {
        String query = String.format("select * from %s.%s o where o.o_w_id = 1 and o.o_d_id = 1 and o.o_id = 1", SCHEMA_NAME, OrderTable.TABLE_NAME);
        long start = System.currentTimeMillis();
        ResultSet rs = methodWatcher.executeQuery(query);

        String duration = getDuration(start, System.currentTimeMillis());
//        int cnt = printResult(query, rs, System.out);
        int cnt = count(rs);
        System.out.println("Rows returned: "+cnt+" in "+duration);

        Assert.assertEquals(1, cnt);
    }

    @Test
    public void testQueryCustomerByName() throws Exception {
        PreparedStatement ps = methodWatcher.prepareStatement(CUSTOMER_BY_NAME);
        ps.setInt(1, 1); // c_w_id
        ps.setInt(2, 7); // c_d_id
        ps.setString(3, "ESEPRIANTI");  // c_last
        long start = System.currentTimeMillis();
        ResultSet rs = ps.executeQuery();

        String duration = getDuration(start, System.currentTimeMillis());
        int cnt = count(rs);
        System.out.println("Rows returned: "+cnt+" in "+duration);
        Assert.assertEquals(8,cnt);
    }

    @Test
    @Ignore("TODO: Create bug for this")
    public void testQueryCustomerByNameWithIndex() throws Exception {
        try {
            createIndex(methodWatcher.createConnection(), SCHEMA_NAME, CustomerTable.TABLE_NAME, CustomerTable.INDEX_NAME, CustomerTable.INDEX_DEF, false);

            PreparedStatement ps = methodWatcher.prepareStatement(CUSTOMER_BY_NAME);
            // this column combo is the PK
            ps.setInt(1, 1); // c_w_id
            ps.setInt(2, 7); // c_d_id
            ps.setString(3, "ESEPRIANTI");  // c_last
            long start = System.currentTimeMillis();
            ResultSet rs = ps.executeQuery();

            String duration = getDuration(start, System.currentTimeMillis());
            int cnt = count(rs);
            System.out.println("Rows returned: "+cnt+" in "+duration);
            Assert.assertEquals(8,cnt);
        } finally {
            dropIndex(SCHEMA_NAME, CustomerTable.INDEX_NAME);
        }
    }

    @Test
    public void testDistinctCustomerByID() throws Exception {
        String query = String.format("select distinct c_id from %s.%s", SCHEMA_NAME, CustomerTable.TABLE_NAME);
        long start = System.currentTimeMillis();
        ResultSet rs = methodWatcher.executeQuery(query);

        String duration = getDuration(start, System.currentTimeMillis());
        int cnt = count(rs);
        System.out.println("Rows returned: "+cnt+" in "+duration);
        Assert.assertTrue(cnt >= 3000);
    }

    @Test
    public void testDistinctCustomerByIDWithIndex() throws Exception {
        try {
            createIndex(methodWatcher.createConnection(), SCHEMA_NAME, CustomerTable.TABLE_NAME, CustomerTable.INDEX_NAME, CustomerTable.INDEX_DEF, false);

            String query = String.format("select distinct c_id from %s.%s", SCHEMA_NAME, CustomerTable.TABLE_NAME);
            long start = System.currentTimeMillis();
            ResultSet rs = methodWatcher.executeQuery(query);

            String duration = getDuration(start, System.currentTimeMillis());
            int cnt = count(rs);
            System.out.println("Rows returned: "+cnt+" in "+duration);
            Assert.assertTrue(cnt >= 3000);
        } finally {
            dropIndex(SCHEMA_NAME, CustomerTable.INDEX_NAME);
        }
    }

    @Test
    public void testJoinCustomerOrders() throws Exception {
        String query = String.format(CUSTOMER_OORDER_JOIN,
                SCHEMA_NAME, CustomerTable.TABLE_NAME, OrderTable.TABLE_NAME);
        long start = System.currentTimeMillis();
        ResultSet rs = methodWatcher.executeQuery(query);
        String duration = getDuration(start, System.currentTimeMillis());
        int cnt = count(rs);
        System.out.println("Rows returned: "+cnt+" in "+duration);
        Assert.assertTrue(cnt >= 300000);
    }

    @Test
    public void testJoinCustomerOrdersWithIndex() throws Exception {
        try {
            createIndex(methodWatcher.createConnection(), SCHEMA_NAME, CustomerTable.TABLE_NAME, CustomerTable.INDEX_NAME, CustomerTable.INDEX_DEF, false);
            createIndex(methodWatcher.createConnection(), SCHEMA_NAME, OrderTable.TABLE_NAME, OrderTable.INDEX_NAME, OrderTable.INDEX_DEF, false);

            String query = String.format(CUSTOMER_OORDER_JOIN,
                    SCHEMA_NAME, CustomerTable.TABLE_NAME, OrderTable.TABLE_NAME);
            long start = System.currentTimeMillis();
            ResultSet rs = methodWatcher.executeQuery(query);
            String duration = getDuration(start, System.currentTimeMillis());
            int cnt = count(rs);
            System.out.println("Rows returned: "+cnt+" in "+duration);
            Assert.assertTrue(cnt >= 300000);
        } finally {
            dropIndex(SCHEMA_NAME,CustomerTable.INDEX_NAME);
            dropIndex(SCHEMA_NAME,OrderTable.INDEX_NAME);
        }
    }

    @Test
    public void testJoinCustomerOrdersWithIndexNotInColumnOrder() throws Exception {
        try {
            createIndex(methodWatcher.createConnection(), SCHEMA_NAME, CustomerTable.TABLE_NAME, CustomerTable.INDEX_NAME, CustomerTable.INDEX_ORDER_DEF, false);
            createIndex(methodWatcher.createConnection(), SCHEMA_NAME, OrderTable.TABLE_NAME, OrderTable.INDEX_NAME, OrderTable.INDEX_ORDER_DEF, false);

            String query = String.format(CUSTOMER_OORDER_JOIN,
                    SCHEMA_NAME, CustomerTable.TABLE_NAME, OrderTable.TABLE_NAME);
            long start = System.currentTimeMillis();
            ResultSet rs = methodWatcher.executeQuery(query);
            String duration = getDuration(start, System.currentTimeMillis());
            int cnt = count(rs);
            System.out.println("Rows returned: "+cnt+" in "+duration);
            Assert.assertTrue(cnt >= 300000);
        } finally {
            dropIndex(SCHEMA_NAME,CustomerTable.INDEX_NAME);
            dropIndex(SCHEMA_NAME,OrderTable.INDEX_NAME);
        }
    }

    @Test(timeout=1000*60*3)  // Time out after 3 min
    @Ignore("Always times out")
    public void testJoinCustomerOrdersOrderLineWithIndexNotInColumnOrder() throws Exception {
        try {
            createIndex(methodWatcher.createConnection(), SCHEMA_NAME, CustomerTable.TABLE_NAME, CustomerTable.INDEX_NAME, CustomerTable.INDEX_ORDER_DEF, false);
            createIndex(methodWatcher.createConnection(), SCHEMA_NAME, OrderTable.TABLE_NAME, OrderTable.INDEX_NAME, OrderTable.INDEX_ORDER_DEF, false);
            createIndex(methodWatcher.createConnection(), SCHEMA_NAME, OrderLineTable.TABLE_NAME, OrderLineTable.INDEX_NAME, OrderLineTable.INDEX_ORDER_DEF, false);

            String query = String.format("select %1$s.c.c_last, %1$s.c.c_first, %1$s.o.o_id, %1$s.o.o_entry_d, %1$s.ol.ol_amount from %1$s.%2$s c, %1$s.%3$s o, %1$s.%4$s ol where c.c_id = o.o_c_id and ol.ol_o_id = o.o_id and c.c_last = 'ESEPRIANTI'",
                    SCHEMA_NAME, CustomerTable.TABLE_NAME, OrderTable.TABLE_NAME, OrderLineTable.TABLE_NAME);
            long start = System.currentTimeMillis();
            ResultSet rs = methodWatcher.executeQuery(query);
            String duration = getDuration(start, System.currentTimeMillis());
//        Assert.assertTrue(printResult(query, rs, System.out) > 0);
            int cnt = count(rs);
            System.out.println("Rows returned: "+cnt+" in "+duration);
            Assert.assertTrue(cnt>0);
        } finally {
            dropIndex(SCHEMA_NAME,CustomerTable.INDEX_NAME);
            dropIndex(SCHEMA_NAME,OrderTable.INDEX_NAME);
            dropIndex(SCHEMA_NAME,OrderLineTable.INDEX_NAME);
        }
    }

    // ===============================================================================
    // Update Tests
    // ===============================================================================

    @Test
    public void testUpdateCustomer() throws Exception {
        String query = String.format(SELECT_UNIQUE_CUSTOMER, SCHEMA_NAME,CustomerTable.TABLE_NAME);
        ResultSet rs = methodWatcher.executeQuery(query);
        Assert.assertEquals(1, count(rs));

        int nCols = methodWatcher.getStatement().executeUpdate(String.format("update %s.%s c set c.c_credit_lim = %s where %s",
                SCHEMA_NAME,CustomerTable.TABLE_NAME, "3000.00", "c.c_w_id = 1 and c.c_d_id = 10 and c.c_id = 2365"));
        Assert.assertEquals(1, nCols);

        query = String.format("select * from %s.%s c where c.c_last = 'ESEPRIANTI' and c.c_id = 2365 and c.c_credit_lim = 3000.00",
                SCHEMA_NAME,CustomerTable.TABLE_NAME);
        rs = methodWatcher.executeQuery(query);
        Assert.assertEquals(1, count(rs));
    }

    @Test
    @Ignore("Create bug for this")
    public void testUpdateCustomerWithIndex() throws Exception {
        try {
            createIndex(methodWatcher.createConnection(), SCHEMA_NAME, CustomerTable.TABLE_NAME, CustomerTable.INDEX_NAME, CustomerTable.INDEX_ORDER_DEF_ASC, false);

            String query = String.format(SELECT_UNIQUE_CUSTOMER, SCHEMA_NAME,CustomerTable.TABLE_NAME);
            ResultSet rs = methodWatcher.executeQuery(query);
            Assert.assertEquals(1, count(rs));

            int nCols = methodWatcher.getStatement().executeUpdate(String.format("update %s.%s c set c.c_credit_lim = %s where %s",
                    SCHEMA_NAME,CustomerTable.TABLE_NAME, "3000.00", "c.c_w_id = 1 and c.c_d_id = 10 and c.c_id = 2365"));
            Assert.assertEquals(1, nCols);

            query = String.format("select * from %s.%s c where c.c_last = 'ESEPRIANTI' and c.c_id = 2365 and c.c_credit_lim = 3000.00",
                    SCHEMA_NAME,CustomerTable.TABLE_NAME);
            rs = methodWatcher.executeQuery(query);
            Assert.assertEquals(1, count(rs));
        } finally {
            dropIndex(SCHEMA_NAME,CustomerTable.INDEX_NAME);
        }
    }

    // ===============================================================================
    // Insert Tests
    // ===============================================================================

    @Test
    public void testInsertNewOrder() throws Exception {
        int nCols = methodWatcher.getStatement().executeUpdate(String.format("insert into %s.%s values (%s)",
                SCHEMA_NAME,NewOrderTable.TABLE_NAME, "1,1,2001"));
        Assert.assertEquals(1, nCols);
    }

    @Test
    public void testInsertNewOrderWithIndex() throws Exception {
        try {
            createIndex(methodWatcher.createConnection(), SCHEMA_NAME, NewOrderTable.TABLE_NAME, NewOrderTable.INDEX_NAME, NewOrderTable.INDEX_ORDER_DEF, false);
            int nCols = methodWatcher.getStatement().executeUpdate(String.format("insert into %s.%s values (%s)",
                    SCHEMA_NAME,NewOrderTable.TABLE_NAME, "1,2,2001"));
            Assert.assertEquals(1, nCols);
        } finally {
            dropIndex(SCHEMA_NAME,NewOrderTable.INDEX_NAME);
        }
    }

    @Test
    public void testInsertOrder() throws Exception {
        int nCols = methodWatcher.getStatement().executeUpdate(String.format("insert into %s.%s values (%s)",
                SCHEMA_NAME,OrderTable.TABLE_NAME, "3001,1,1,1268,10,6,1,'2013-08-01 10:33:44.813'"));
        Assert.assertEquals(1, nCols);
    }

    private static final String CUST_INSERT1 = "1,1,3001,0.1221,'GC','CUNNINGHAM','Jeff',50000.0,-10.0,10.0,1,0,'qmvfraakwixzcrqxt','mamrbljycaxrh','bcsygfxkurug','VH','843711111','3185126927164979','2013-08-01 10:33:44.813','OE','uufvvwszkmxfrxerjxiekbsf'";
    @Test
    public void testInsertCustomer() throws Exception {
        String query = String.format("select c.c_last, c.c_first, c.c_since from %s.%s c where c.c_last = 'CUNNINGHAM'",
                SCHEMA_NAME, CustomerTable.TABLE_NAME);
        ResultSet rs = methodWatcher.executeQuery(query);
        Assert.assertEquals(0, count(rs));

        int nCols = methodWatcher.getStatement().executeUpdate(String.format("insert into %s.%s values (%s)",
                SCHEMA_NAME,CustomerTable.TABLE_NAME, CUST_INSERT1));
        // C_LAST: CUNNINGHAM
        // C_FIRST: Jeff
        // C_SINCE: 2013-08-01 10:33:44.813
        Assert.assertEquals(1, nCols);

        rs = methodWatcher.executeQuery(query);
        Assert.assertEquals(1, count(rs));
    }

    private static final String CUST_INSERT2 = "1,1,3002,0.1221,'GC','Jones','Fred',50000.0,-10.0,10.0,1,0,'qmvfraakwixzcrqxt','mamrsdfdsycaxrh','bcsyfdsdkurug','VL','843211111','3185126927164979','2013-08-01 10:33:44.813','OE','tktkdcbjqxbewxllutwigcdmzenarkhiklzfkaybefrppwtvdmecqfqaa'";
    @Test
    @Ignore("Create bug")
    public void testInsertCustomerWithIndex() throws Exception {
        try {
            createIndex(methodWatcher.createConnection(), SCHEMA_NAME, CustomerTable.TABLE_NAME, CustomerTable.INDEX_NAME, CustomerTable.INDEX_ORDER_DEF, false);

            String query = String.format("select c.c_last, c.c_first, c.c_since from %s.%s c where c.c_last = 'Jones'",
                    SCHEMA_NAME, CustomerTable.TABLE_NAME);
            ResultSet rs = methodWatcher.executeQuery(query);
            Assert.assertEquals(0, count(rs));

            int nCols = methodWatcher.getStatement().executeUpdate(String.format("insert into %s.%s values (%s)",
                    SCHEMA_NAME,CustomerTable.TABLE_NAME, CUST_INSERT2));
            Assert.assertEquals(1, nCols);

            rs = methodWatcher.executeQuery(query);
            Assert.assertEquals(1, count(rs));
        } finally {
            dropIndex(SCHEMA_NAME,CustomerTable.INDEX_NAME);
        }
    }

    // ===============================================================================
    // Helpers
    // ===============================================================================

    private static final String SELECT_SPECIFIC_INDEX = "select c.conglomeratename from sys.sysconglomerates c inner join sys.sysschemas s on " +
            "c.schemaid = s.schemaid where c.isindex = 'TRUE' and s.schemaname = ? and c.conglomeratename = ?";
    private static void createIndex(Connection connection, String schemaName, String tableName, String indexName, String definition, boolean unique) throws Exception {
        PreparedStatement statement = null;
        Statement statement2 = null;
        ResultSet rs = null;
        try {
            connection = SpliceNetConnection.getConnection();
            statement = connection.prepareStatement(SELECT_SPECIFIC_INDEX);
            statement.setString(1, schemaName);
            statement.setString(2, indexName);
            rs = statement.executeQuery();
            if (rs.next()) {
                SpliceIndexWatcher.executeDrop(schemaName,indexName);
            }
            connection.commit();
            statement2 = connection.createStatement();
            statement2.execute(String.format("create "+(unique?"unique":"")+" index %s.%s on %s.%s %s",
                    schemaName,indexName,schemaName,tableName,definition));
            connection.commit();
        } catch (Exception e) {
            throw new RuntimeException("Create index failed.", e);
        } finally {
            DbUtils.closeQuietly(rs);
            DbUtils.closeQuietly(statement);
            DbUtils.closeQuietly(statement2);
            DbUtils.commitAndCloseQuietly(connection);
        }
    }

    private static void dropIndex(String schemaName, String indexName) throws Exception {
        SpliceIndexWatcher.executeDrop(schemaName,indexName);
    }

    private static int count(ResultSet rs) throws Exception{
        int cnt = 0;
        while (rs.next()) {
            ++cnt;
        }
        return cnt;
    }

    private synchronized int printResult(String statement, ResultSet rs, PrintStream out) throws SQLException {
        if (rs.isClosed()) {
            return 0;
        }
        int count = 0;
        out.println();
        out.println(statement);
        for (Map map : TestUtils.resultSetToMaps(rs)) {
            out.println("--- "+(++count));
            for (Object entryObj : map.entrySet()) {
                Map.Entry entry = (Map.Entry) entryObj;
                out.println(entry.getKey() + ": " + entry.getValue());
            }
        }
        return count;
    }

    /**
     * Calculate and return the string duration of the given start and end times (in milliseconds)
     * @param startMilis the starting time of the duration given by <code>System.currentTimeMillis()</code>
     * @param stopMilis the ending time of the duration given by <code>System.currentTimeMillis()</code>
     * @return example <code>0 hrs 04 min 41 sec 337 mil</code>
     */
    public static String getDuration(long startMilis, long stopMilis) {

        long secondInMillis = 1000;
        long minuteInMillis = secondInMillis * 60;
        long hourInMillis = minuteInMillis * 60;

        long diff = stopMilis - startMilis;
        long elapsedHours = diff / hourInMillis;
        diff = diff % hourInMillis;
        long elapsedMinutes = diff / minuteInMillis;
        diff = diff % minuteInMillis;
        long elapsedSeconds = diff / secondInMillis;
        diff = diff % secondInMillis;

        return String.format("%d hrs %02d min %02d sec %03d mil", elapsedHours, elapsedMinutes, elapsedSeconds, diff);
    }
}
