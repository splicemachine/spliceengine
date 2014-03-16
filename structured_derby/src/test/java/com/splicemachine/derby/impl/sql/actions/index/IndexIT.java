package com.splicemachine.derby.impl.sql.actions.index;

import com.splicemachine.derby.test.framework.SpliceIndexWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

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
public class IndexIT extends SpliceUnitTest { 
    private static final Logger LOG = Logger.getLogger(IndexIT.class);

    private static final String SCHEMA_NAME = IndexIT.class.getSimpleName().toUpperCase();
    public static final String CUSTOMER_ORDER_JOIN = "select %1$s.c.c_last, %1$s.c.c_first, %1$s.o.o_id, %1$s.o.o_entry_d from %1$s.%2$s c, %1$s.%3$s o where c.c_id = o.o_c_id";
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

    protected static OrderLineTable orderLineTableWatcher = new OrderLineTable(OrderLineTable.TABLE_NAME,SCHEMA_NAME) {
        @Override
        protected void starting(Description description) {
            super.starting(description);
            importData(getResourceDirectory()+ "/index/order-line.csv");
        }
    };

    protected static HeaderTable headerTableWatcher = new HeaderTable(HeaderTable.TABLE_NAME,SCHEMA_NAME) {
        @Override
        protected void starting(Description description) {
            super.starting(description);
            try {
                spliceClassWatcher.executeUpdate(String.format("INSERT INTO %s.%s VALUES" +
                        "(0, 8888, DATE('2011-12-28'))", SCHEMA_NAME, HeaderTable.TABLE_NAME));
            } catch (Exception e) {
                LOG.error("Error inserting into HEADER table", e);
            }
        }
    };

    protected static EmailableTable emailableTableWatcher = new EmailableTable(EmailableTable.TABLE_NAME,SCHEMA_NAME){
        @Override
        protected void starting(Description description) {
            super.starting(description);
            try {
                spliceClassWatcher.executeUpdate(String.format("INSERT INTO %s.%s VALUES" +
                        "(8888)", SCHEMA_NAME, EmailableTable.TABLE_NAME));
            } catch (Exception e) {
                LOG.error("Error inserting into EMAILABLE table", e);
            }
        }
    };

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(customerTableWatcher)
            .around(newOrderTableWatcher)
            .around(orderTableWatcher)
            .around(orderLineTableWatcher)
            .around(headerTableWatcher)
            .around(emailableTableWatcher);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();
    
    // ===============================================================================
    // Create Index Tests
    // ===============================================================================
    
    @Test
    public void createIndexesInOrderNonUnique() throws Exception {
        try {
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, CustomerTable.TABLE_NAME, CustomerTable.INDEX_NAME, CustomerTable.INDEX_DEF, false);
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, OrderTable.TABLE_NAME, OrderTable.INDEX_NAME, OrderTable.INDEX_DEF, false);
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, OrderLineTable.TABLE_NAME, OrderLineTable.INDEX_NAME, OrderLineTable.INDEX_DEF, false);
        } finally {
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,CustomerTable.INDEX_NAME);
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,OrderTable.INDEX_NAME);
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,OrderLineTable.INDEX_NAME);
        }
    }
    
    @Test
    public void createIndexesInOrderUnique() throws Exception {
        try {
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, CustomerTable.TABLE_NAME, CustomerTable.INDEX_NAME, CustomerTable.INDEX_DEF, true);
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, OrderTable.TABLE_NAME, OrderTable.INDEX_NAME, OrderTable.INDEX_DEF, true);
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, OrderLineTable.TABLE_NAME, OrderLineTable.INDEX_NAME, OrderLineTable.INDEX_DEF, true);
        } finally {
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,CustomerTable.INDEX_NAME);
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,OrderTable.INDEX_NAME);
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,OrderLineTable.INDEX_NAME);
        }
    }
    
    @Test
    public void createIndexesOutOfOrderNonUnique() throws Exception {
        try {
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, CustomerTable.TABLE_NAME, CustomerTable.INDEX_NAME, CustomerTable.INDEX_ORDER_DEF, false);
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, OrderTable.TABLE_NAME, OrderTable.INDEX_NAME, OrderTable.INDEX_ORDER_DEF, false);
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, OrderLineTable.TABLE_NAME, OrderLineTable.INDEX_NAME, OrderLineTable.INDEX_ORDER_DEF, false);
        } finally {
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,CustomerTable.INDEX_NAME);
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,OrderTable.INDEX_NAME);
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,OrderLineTable.INDEX_NAME);
        }
    }
    
    @Test
    public void createIndexesOutOfOrderUnique() throws Exception {
        try {
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, CustomerTable.TABLE_NAME, CustomerTable.INDEX_NAME, CustomerTable.INDEX_ORDER_DEF, true);
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, OrderTable.TABLE_NAME, OrderTable.INDEX_NAME, OrderTable.INDEX_ORDER_DEF, true);
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, OrderLineTable.TABLE_NAME, OrderLineTable.INDEX_NAME, OrderLineTable.INDEX_ORDER_DEF, true);
        } finally {
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,CustomerTable.INDEX_NAME);
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,OrderTable.INDEX_NAME);
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,OrderLineTable.INDEX_NAME);
        }
    }
    
    @Test
    public void createIndexesAscNonUnique() throws Exception {
        try {
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, CustomerTable.TABLE_NAME, CustomerTable.INDEX_NAME, CustomerTable.INDEX_ORDER_DEF_ASC, false);
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, OrderTable.TABLE_NAME, OrderTable.INDEX_NAME, OrderTable.INDEX_ORDER_DEF_ASC, false);
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, OrderLineTable.TABLE_NAME, OrderLineTable.INDEX_NAME, OrderLineTable.INDEX_ORDER_DEF_ASC, false);
        } finally {
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,CustomerTable.INDEX_NAME);
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,OrderTable.INDEX_NAME);
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,OrderLineTable.INDEX_NAME);
        }
    }
    
    @Test
    public void createIndexesAscUnique() throws Exception {
        try {
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, CustomerTable.TABLE_NAME, CustomerTable.INDEX_NAME, CustomerTable.INDEX_ORDER_DEF_ASC, true);
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, OrderTable.TABLE_NAME, OrderTable.INDEX_NAME, OrderTable.INDEX_ORDER_DEF_ASC, true);
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, OrderLineTable.TABLE_NAME, OrderLineTable.INDEX_NAME, OrderLineTable.INDEX_ORDER_DEF_ASC, true);
        } finally {
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,CustomerTable.INDEX_NAME);
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,OrderTable.INDEX_NAME);
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,OrderLineTable.INDEX_NAME);
        }
    }
    
    @Test
    public void createIndexesDescNonUnique() throws Exception {
        try {
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, CustomerTable.TABLE_NAME, CustomerTable.INDEX_NAME, CustomerTable.INDEX_ORDER_DEF_DESC, false);
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, OrderTable.TABLE_NAME, OrderTable.INDEX_NAME, OrderTable.INDEX_ORDER_DEF_DESC, false);
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, OrderLineTable.TABLE_NAME, OrderLineTable.INDEX_NAME, OrderLineTable.INDEX_ORDER_DEF_DESC, false);
        } finally {
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,CustomerTable.INDEX_NAME);
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,OrderTable.INDEX_NAME);
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,OrderLineTable.INDEX_NAME);
        }
    }
    
    @Test
    public void createIndexesDescUnique() throws Exception {
        try {
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, CustomerTable.TABLE_NAME, CustomerTable.INDEX_NAME, CustomerTable.INDEX_ORDER_DEF_DESC, true);
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, OrderTable.TABLE_NAME, OrderTable.INDEX_NAME, OrderTable.INDEX_ORDER_DEF_DESC, true);
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, OrderLineTable.TABLE_NAME, OrderLineTable.INDEX_NAME, OrderLineTable.INDEX_ORDER_DEF_DESC, true);
        } finally {
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,CustomerTable.INDEX_NAME);
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,OrderTable.INDEX_NAME);
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,OrderLineTable.INDEX_NAME);
        }
    }

    // ===============================================================================
    // Query Tests
    // ===============================================================================

    @Test
    public void testQueryCustomerById() throws Exception {
        String query = String.format(SELECT_UNIQUE_CUSTOMER,
                SCHEMA_NAME,CustomerTable.TABLE_NAME);
        ResultSet rs = methodWatcher.executeQuery(query);
        Assert.assertEquals(1, resultSetSize(rs));
    }

    @Test
    public void testQueryCustomerByIdWithIndex() throws Exception {
        try {
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, CustomerTable.TABLE_NAME, CustomerTable.INDEX_NAME, CustomerTable.INDEX_ORDER_DEF_ASC, false);
            String query = String.format(SELECT_UNIQUE_CUSTOMER, SCHEMA_NAME,CustomerTable.TABLE_NAME);
            ResultSet rs = methodWatcher.executeQuery(query);
            Assert.assertEquals(1, resultSetSize(rs));
        } finally {
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,CustomerTable.INDEX_NAME);
        }
    }

    @Test
    public void testQueryOrder() throws Exception {
        String query = String.format("select * from %s.%s o where o.o_w_id = 1 and o.o_d_id = 1 and o.o_id = 1", SCHEMA_NAME, OrderTable.TABLE_NAME);
        long start = System.currentTimeMillis();
        ResultSet rs = methodWatcher.executeQuery(query);

        String duration = TestUtils.getDuration(start, System.currentTimeMillis());
//        int cnt = printResult(query, rs, System.out);
        int cnt = resultSetSize(rs);
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

        String duration = TestUtils.getDuration(start, System.currentTimeMillis());
        int cnt = resultSetSize(rs);
        System.out.println("Rows returned: "+cnt+" in "+duration);
        Assert.assertEquals(8,cnt);
    }

    @Test
    public void testQueryCustomerByNameWithIndex() throws Exception {
        try {
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, CustomerTable.TABLE_NAME, CustomerTable.INDEX_NAME, CustomerTable.INDEX_DEF, false);

            PreparedStatement ps = methodWatcher.prepareStatement(CUSTOMER_BY_NAME);
            // this column combo is the PK
            ps.setInt(1, 1); // c_w_id
            ps.setInt(2, 7); // c_d_id
            ps.setString(3, "ESEPRIANTI");  // c_last
            long start = System.currentTimeMillis();
            ResultSet rs = ps.executeQuery();

            String duration = TestUtils.getDuration(start, System.currentTimeMillis());
            int cnt = resultSetSize(rs);
            System.out.println("Rows returned: "+cnt+" in "+duration);
            Assert.assertEquals(8,cnt);
        } finally {
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME, CustomerTable.INDEX_NAME);
        }
    }

    @Test
    public void testDistinctCustomerByID() throws Exception {
        String query = String.format("select distinct c_id from %s.%s", SCHEMA_NAME, CustomerTable.TABLE_NAME);
        long start = System.currentTimeMillis();
        ResultSet rs = methodWatcher.executeQuery(query);

        String duration = TestUtils.getDuration(start, System.currentTimeMillis());
        int cnt = resultSetSize(rs);
        System.out.println("Rows returned: "+cnt+" in "+duration);
        Assert.assertTrue(cnt >= 3000);
    }

    @Test
    public void testDistinctCustomerByIDWithIndex() throws Exception {
        try {
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, CustomerTable.TABLE_NAME, CustomerTable.INDEX_NAME, CustomerTable.INDEX_DEF, false);

            String query = String.format("select distinct c_id from %s.%s", SCHEMA_NAME, CustomerTable.TABLE_NAME);
            long start = System.currentTimeMillis();
            ResultSet rs = methodWatcher.executeQuery(query);

            String duration = TestUtils.getDuration(start, System.currentTimeMillis());
            int cnt = resultSetSize(rs);
            System.out.println("Rows returned: "+cnt+" in "+duration);
            Assert.assertTrue(cnt >= 3000);
        } finally {
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME, CustomerTable.INDEX_NAME);
        }
    }

    @Test
    public void testJoinCustomerOrders() throws Exception {
        String query = String.format(CUSTOMER_ORDER_JOIN,
                SCHEMA_NAME, CustomerTable.TABLE_NAME, OrderTable.TABLE_NAME);
        long start = System.currentTimeMillis();
        ResultSet rs = methodWatcher.executeQuery(query);
        String duration = TestUtils.getDuration(start, System.currentTimeMillis());
        int cnt = resultSetSize(rs);
        System.out.println("Rows returned: "+cnt+" in "+duration);
        Assert.assertTrue(cnt >= 300000);
    }

    @Test
    public void testJoinCustomerOrdersWithIndex() throws Exception {
        try {
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, CustomerTable.TABLE_NAME, CustomerTable.INDEX_NAME, CustomerTable.INDEX_DEF, false);
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, OrderTable.TABLE_NAME, OrderTable.INDEX_NAME, OrderTable.INDEX_DEF, false);

            String query = String.format(CUSTOMER_ORDER_JOIN,
                    SCHEMA_NAME, CustomerTable.TABLE_NAME, OrderTable.TABLE_NAME);
            long start = System.currentTimeMillis();
            ResultSet rs = methodWatcher.executeQuery(query);
            String duration = TestUtils.getDuration(start, System.currentTimeMillis());
            int cnt = resultSetSize(rs);
            System.out.println("Rows returned: "+cnt+" in "+duration);
            Assert.assertTrue(cnt >= 300000);
        } finally {
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,CustomerTable.INDEX_NAME);
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,OrderTable.INDEX_NAME);
        }
    }

    public static final String HEADER_JOIN = String.format("select distinct a.customer_master_id\n" +
                "from %s.%s a, %s.%s b --SPLICE-PROPERTIES joinStrategy=SORTMERGE\n" +
                "where transaction_dt >= DATE('2011-12-28') and transaction_dt <=\n" +
                "DATE('2012-03-03')\n" +
                "and b.customer_master_id=a.customer_master_id",
                SCHEMA_NAME, HeaderTable.TABLE_NAME, SCHEMA_NAME, EmailableTable.TABLE_NAME);

    @Test
    public void testHeaderJoinMSJ() throws Exception {
        long start = System.currentTimeMillis();
        ResultSet rs = methodWatcher.executeQuery(HEADER_JOIN);
        String duration = TestUtils.getDuration(start, System.currentTimeMillis());
        int cnt = resultSetSize(rs);
        System.out.println("Rows returned: "+cnt+" in "+duration);
        Assert.assertTrue(cnt == 1);
    }

    @Test
    public void testHeaderJoinMSJWithIndex() throws Exception {
        try {
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, HeaderTable.TABLE_NAME, HeaderTable.INDEX_NAME, HeaderTable.INDEX_DEF, false);
            long start = System.currentTimeMillis();
            ResultSet rs = methodWatcher.executeQuery(HEADER_JOIN);
            String duration = TestUtils.getDuration(start, System.currentTimeMillis());
            int cnt = resultSetSize(rs);
            System.out.println("Rows returned: "+cnt+" in "+duration);
            Assert.assertTrue(cnt == 1);
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,HeaderTable.INDEX_NAME);
        }
    }

    @Test
    public void testJoinCustomerOrdersWithIndexNotInColumnOrder() throws Exception {
        try {
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, CustomerTable.TABLE_NAME, CustomerTable.INDEX_NAME, CustomerTable.INDEX_ORDER_DEF, false);
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, OrderTable.TABLE_NAME, OrderTable.INDEX_NAME, OrderTable.INDEX_ORDER_DEF, false);

            String query = String.format(CUSTOMER_ORDER_JOIN,
                    SCHEMA_NAME, CustomerTable.TABLE_NAME, OrderTable.TABLE_NAME);
            long start = System.currentTimeMillis();
            ResultSet rs = methodWatcher.executeQuery(query);
            String duration = TestUtils.getDuration(start, System.currentTimeMillis());
            int cnt = resultSetSize(rs);
            System.out.println("Rows returned: "+cnt+" in "+duration);
            Assert.assertTrue(cnt >= 300000);
        } finally {
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,CustomerTable.INDEX_NAME);
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,OrderTable.INDEX_NAME);
        }
    }

    @Test(timeout=1000*60*3)  // Time out after 3 min
    public void testJoinCustomerOrdersOrderLineWithIndexNotInColumnOrder() throws Exception {
        try {
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, CustomerTable.TABLE_NAME, CustomerTable.INDEX_NAME, CustomerTable.INDEX_ORDER_DEF, false);
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, OrderTable.TABLE_NAME, OrderTable.INDEX_NAME, OrderTable.INDEX_ORDER_DEF, false);
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, OrderLineTable.TABLE_NAME, OrderLineTable.INDEX_NAME, OrderLineTable.INDEX_ORDER_DEF, false);

            String query = String.format("select %1$s.c.c_last, %1$s.c.c_first, %1$s.o.o_id, %1$s.o.o_entry_d, %1$s.ol.ol_amount from %1$s.%2$s c, %1$s.%3$s o, %1$s.%4$s ol where c.c_id = o.o_c_id and ol.ol_o_id = o.o_id and c.c_last = 'ESEPRIANTI'",
                    SCHEMA_NAME, CustomerTable.TABLE_NAME, OrderTable.TABLE_NAME, OrderLineTable.TABLE_NAME);
            long start = System.currentTimeMillis();
            ResultSet rs = methodWatcher.executeQuery(query);
            String duration = TestUtils.getDuration(start, System.currentTimeMillis());
//        Assert.assertTrue(printResult(query, rs, System.out) > 0);
            int cnt = resultSetSize(rs);
            System.out.println("Rows returned: "+cnt+" in "+duration);
            Assert.assertTrue(cnt>0);
        } finally {
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,CustomerTable.INDEX_NAME);
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,OrderTable.INDEX_NAME);
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,OrderLineTable.INDEX_NAME);
        }
    }

    // ===============================================================================
    // Update Tests
    // ===============================================================================

    @Test
    public void testUpdateCustomer() throws Exception {
        String query = String.format(SELECT_UNIQUE_CUSTOMER, SCHEMA_NAME,CustomerTable.TABLE_NAME);
        ResultSet rs = methodWatcher.executeQuery(query);
        Assert.assertEquals(1, resultSetSize(rs));

        int nCols = methodWatcher.getStatement().executeUpdate(String.format("update %s.%s c set c.c_credit_lim = %s where %s",
                SCHEMA_NAME,CustomerTable.TABLE_NAME, "3000.00", "c.c_w_id = 1 and c.c_d_id = 10 and c.c_id = 2365"));
        Assert.assertEquals(1, nCols);

        query = String.format("select * from %s.%s c where c.c_last = 'ESEPRIANTI' and c.c_id = 2365 and c.c_credit_lim = 3000.00",
                SCHEMA_NAME,CustomerTable.TABLE_NAME);
        rs = methodWatcher.executeQuery(query);
        Assert.assertEquals(1, resultSetSize(rs));
    }

    @Test
    public void testUpdateCustomerWithIndex() throws Exception {
        try {
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, CustomerTable.TABLE_NAME, CustomerTable.INDEX_NAME, CustomerTable.INDEX_ORDER_DEF_ASC, false);

            String query = String.format(SELECT_UNIQUE_CUSTOMER, SCHEMA_NAME,CustomerTable.TABLE_NAME);
            ResultSet rs = methodWatcher.executeQuery(query);
            Assert.assertEquals(1, resultSetSize(rs));

            int nCols = methodWatcher.getStatement().executeUpdate(String.format("update %s.%s c set c.c_credit_lim = %s where %s",
                    SCHEMA_NAME,CustomerTable.TABLE_NAME, "3000.00", "c.c_w_id = 1 and c.c_d_id = 10 and c.c_id = 2365"));
            Assert.assertEquals(1, nCols);

            query = String.format("select * from %s.%s c where c.c_last = 'ESEPRIANTI' and c.c_id = 2365 and c.c_credit_lim = 3000.00",
                    SCHEMA_NAME,CustomerTable.TABLE_NAME);
            rs = methodWatcher.executeQuery(query);
            Assert.assertEquals(1, resultSetSize(rs));
        } finally {
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,CustomerTable.INDEX_NAME);
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
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, NewOrderTable.TABLE_NAME, NewOrderTable.INDEX_NAME, NewOrderTable.INDEX_ORDER_DEF, false);
            int nCols = methodWatcher.getStatement().executeUpdate(String.format("insert into %s.%s values (%s)",
                    SCHEMA_NAME,NewOrderTable.TABLE_NAME, "1,2,2001"));
            Assert.assertEquals(1, nCols);
        } finally {
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,NewOrderTable.INDEX_NAME);
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
        Assert.assertEquals(0, resultSetSize(rs));

        int nCols = methodWatcher.getStatement().executeUpdate(String.format("insert into %s.%s values (%s)",
                SCHEMA_NAME,CustomerTable.TABLE_NAME, CUST_INSERT1));
        // C_LAST: CUNNINGHAM
        // C_FIRST: Jeff
        // C_SINCE: 2013-08-01 10:33:44.813
        Assert.assertEquals(1, nCols);

        rs = methodWatcher.executeQuery(query);
        Assert.assertEquals(1, resultSetSize(rs));
    }

    @Test
    public void testIndexAfterInserts() throws Exception {
        try {
            methodWatcher.prepareStatement("drop index ia").execute();
        } catch (Exception e1) {
            // ignore
        }
        try {
            methodWatcher.prepareStatement(String.format("drop table %s.a", SCHEMA_NAME)).execute();
        } catch (Exception e1) {
            // ignore
        }
        methodWatcher.prepareStatement(String.format("create table %s.a (i int)", SCHEMA_NAME)).execute();
        PreparedStatement ps = methodWatcher.prepareStatement(String.format("insert into %s.a values 1", SCHEMA_NAME));
        ps.execute();
        ps.execute();
        ps.execute();
        try {
            methodWatcher.prepareStatement(String.format("create unique index ia on %s.a (i)", SCHEMA_NAME)).execute();
            Assert.fail("Index should have raised an exception");
        } catch (SQLException se) {
        }
    }

    @Test
    public void testFailedIndexRollbacks() throws Exception {
        try {
            methodWatcher.prepareStatement("drop index ib").execute();
        } catch (Exception e1) {
            // ignore
        }
        try {
            methodWatcher.prepareStatement(String.format("drop table %s.b", SCHEMA_NAME)).execute();
        } catch (Exception e1) {
            // ignore
        }
        methodWatcher.prepareStatement(String.format("create table %s.b (i int)", SCHEMA_NAME)).execute();
        PreparedStatement ps = methodWatcher.prepareStatement(String.format("insert into %s.b values 1", SCHEMA_NAME));
        ps.execute();
        ps.execute();
        ps.execute();
        for (int i = 0; i < 5; ++i) {
            try {
                methodWatcher.prepareStatement(String.format("create unique index ib on %s.b (i)", SCHEMA_NAME)).execute();
                Assert.fail("Index should have raised an exception");
            } catch (SQLException se) {
                Assert.assertEquals("Expected constraint violation exception", "23505", se.getSQLState());
            }
        }
    }

    @Test
    public void testRolledBackIndexDisappears() throws Exception {
        try {
            methodWatcher.prepareStatement("drop index ic").execute();
        } catch (Exception e1) {
            // ignore
        }
        try {
            methodWatcher.prepareStatement(String.format("drop table if exists %s.c", SCHEMA_NAME)).execute();
        } catch (Exception e1) {
            // ignore
        }
        methodWatcher.prepareStatement(String.format("create table %s.c (i int)", SCHEMA_NAME)).execute();
        Connection connection1 = methodWatcher.createConnection();
        connection1.setAutoCommit(false);
        PreparedStatement ps = connection1.prepareStatement(String.format("insert into %s.c values 1", SCHEMA_NAME));
        PreparedStatement createStmt = connection1.prepareStatement(String.format("create index ic on %s.c (i)", SCHEMA_NAME));
        PreparedStatement countStmt = connection1.prepareStatement(String.format("select count(*) from %s.c", SCHEMA_NAME));
        for (int i = 0; i < 5; ++i) {
            createStmt.execute();
            ps.execute();
            ps.execute();
            ps.execute();
            ResultSet rs = countStmt.executeQuery();
            Assert.assertTrue(rs.next());
            Assert.assertEquals(3, rs.getInt(1));
            connection1.rollback();
        }
    }

    private static final String CUST_INSERT2 = "1,1,3002,0.1221,'GC','Jones','Fred',50000.0,-10.0,10.0,1,0,'qmvfraakwixzcrqxt','mamrsdfdsycaxrh','bcsyfdsdkurug','VL','843211111','3185126927164979','2013-08-01 10:33:44.813','OE','tktkdcbjqxbewxllutwigcdmzenarkhiklzfkaybefrppwtvdmecqfqaa'";
    @Test
    public void testInsertCustomerWithIndex() throws Exception {
        try {
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, CustomerTable.TABLE_NAME, CustomerTable.INDEX_NAME, CustomerTable.INDEX_ORDER_DEF, false);

            String query = String.format("select c.c_last, c.c_first, c.c_since from %s.%s c where c.c_last = 'Jones'",
                    SCHEMA_NAME, CustomerTable.TABLE_NAME);
            ResultSet rs = methodWatcher.executeQuery(query);
            Assert.assertEquals(0, resultSetSize(rs));

            int nCols = methodWatcher.getStatement().executeUpdate(String.format("insert into %s.%s values (%s)",
                    SCHEMA_NAME,CustomerTable.TABLE_NAME, CUST_INSERT2));
            Assert.assertEquals(1, nCols);

            rs = methodWatcher.executeQuery(query);
            Assert.assertEquals(1, resultSetSize(rs));
        } finally {
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,CustomerTable.INDEX_NAME);
        }
    }
}
