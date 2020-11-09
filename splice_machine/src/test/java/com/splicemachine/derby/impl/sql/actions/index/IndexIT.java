/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.actions.index;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.LongerThanTwoMinutes;
import com.splicemachine.test.SerialTest;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import splice.com.google.common.base.Joiner;

import java.sql.*;
import java.util.List;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Test for index stuff, more or less encompassing:
 * <p/>
 * nulls
 * Multiple keys
 * Different Data Sets
 * Natural Column Order Changes
 * Ascending/Descending
 * Use in scans, inserts, updates
 * <p/>
 * Indexes are created and dropped in each test so that tables can be reused.
 *
 * @author Jeff Cunningham
 *         Date: 7/31/13
 */
@Category(value = {SerialTest.class, LongerThanTwoMinutes.class})
public class IndexIT extends SpliceUnitTest{
    private static final Logger LOG=Logger.getLogger(IndexIT.class);

    private static final String SCHEMA_NAME=IndexIT.class.getSimpleName().toUpperCase();
    private static final String CUSTOMER_ORDER_JOIN="select %1$s.c.c_last, %1$s.c.c_first, %1$s.o.o_id, %1$s.o.o_entry_d from %1$s.%2$s c, %1$s.%3$s o where c.c_id = o.o_c_id";
    private static final String SELECT_UNIQUE_CUSTOMER="select * from %s.%s c where c.c_last = 'ESEPRIANTI' and c.c_id = 2365";
    protected static SpliceWatcher spliceClassWatcher=new SpliceWatcher();

    // taken from tpc-c
    private static final String CUSTOMER_BY_NAME=String.format("SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city, "
            +"c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, "
            +"c_balance, c_ytd_payment, c_payment_cnt, c_since FROM %s.%s"
            +" WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first",SCHEMA_NAME,CustomerTable.TABLE_NAME);

    protected static SpliceSchemaWatcher spliceSchemaWatcher=new SpliceSchemaWatcher(SCHEMA_NAME);
    private static CustomerTable customerTableWatcher=new CustomerTable(CustomerTable.TABLE_NAME,SCHEMA_NAME){
        @Override
        protected void starting(Description description){
            super.starting(description);
            importData(getResourceDirectory()+"/index/customer.csv","yyyy-MM-dd HH:mm:ss.SSS");
        }
    };

    private static OrderTable orderTableWatcher=new OrderTable(OrderTable.TABLE_NAME,SCHEMA_NAME){
        @Override
        protected void starting(Description description){
            super.starting(description);
            importData(getResourceDirectory()+"/index/order-with-nulls.csv","yyyy-MM-dd HH:mm:ss.SSS");
        }
    };

    private static NewOrderTable newOrderTableWatcher=new NewOrderTable(NewOrderTable.TABLE_NAME,SCHEMA_NAME){
        @Override
        protected void starting(Description description){
            super.starting(description);
            importData(getResourceDirectory()+"/index/new-order.csv","yyyy-MM-dd HH:mm:ss.SSS");
        }
    };

    private static OrderLineTable orderLineTableWatcher=new OrderLineTable(OrderLineTable.TABLE_NAME,SCHEMA_NAME){
        @Override
        protected void starting(Description description){
            super.starting(description);
            importData(getResourceDirectory()+"/index/order-line-small.csv","yyyy-MM-dd HH:mm:ss.SSS");
        }
    };

    private static HeaderTable headerTableWatcher=new HeaderTable(HeaderTable.TABLE_NAME,SCHEMA_NAME){
        @Override
        protected void starting(Description description){
            super.starting(description);
            try{
                spliceClassWatcher.executeUpdate(String.format("INSERT INTO %s.%s VALUES"+
                        "(0, 8888, DATE('2011-12-28'))",SCHEMA_NAME,HeaderTable.TABLE_NAME));
            }catch(Exception e){
                LOG.error("Error inserting into HEADER table",e);
            }
        }
    };

    private static EmailableTable emailableTableWatcher=new EmailableTable(EmailableTable.TABLE_NAME,SCHEMA_NAME){
        @Override
        protected void starting(Description description){
            super.starting(description);
            try{
                spliceClassWatcher.executeUpdate(String.format("INSERT INTO %s.%s VALUES"+
                        "(8888)",SCHEMA_NAME,EmailableTable.TABLE_NAME));
            }catch(Exception e){
                LOG.error("Error inserting into EMAILABLE table",e);
                throw new RuntimeException(e);
            }
        }
    };

    private static final String A_TABLE_NAME="A";
    private static final SpliceTableWatcher A_TABLE=new SpliceTableWatcher("A",SCHEMA_NAME,"(i int)");
    private static final SpliceTableWatcher descIndex=new SpliceTableWatcher("descTable",SCHEMA_NAME,"(a int, b varchar(20),PRIMARY KEY(b))"){
        @Override
        protected void starting(Description description){
            super.starting(description);
            try{
                spliceClassWatcher.executeUpdate(String.format("INSERT INTO %s VALUES (80,'EFGH')",descIndex));
            }catch(Exception e){
                throw new RuntimeException(e);
            }
        }
    };

    private static final SpliceTableWatcher cchar = new SpliceTableWatcher("CCHAR",SCHEMA_NAME,"(cint int, cchar char(10), ctime int)"){
        @Override
        protected void starting(Description description){
            super.starting(description);
            try(Statement s = spliceClassWatcher.getOrCreateConnection().createStatement()){
                s.executeUpdate("insert into "+cchar+" values (11,'11',1)");

                s.executeUpdate("create index b1 on "+cchar+" (cchar, cint)");
            }catch(SQLException e){
                throw new RuntimeException(e);
            }
        }
    };

    @ClassRule
    public static TestRule chain=RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(customerTableWatcher)
            .around(newOrderTableWatcher)
            .around(orderTableWatcher)
            .around(orderLineTableWatcher)
            .around(headerTableWatcher)
            .around(A_TABLE)
            .around(emailableTableWatcher)
            .around(cchar)
            .around(descIndex);

    @Rule
    public SpliceWatcher methodWatcher=new SpliceWatcher(SCHEMA_NAME);

    private Connection conn;

    @Before
    public void setUpTest() throws Exception{
        conn=methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
    }

    @After
    public void tearDownTest() throws Exception{
        conn.rollback();
    }

    // ===============================================================================
    // Create Index Tests
    // ===============================================================================

    @Test
    public void createIndexesInOrderNonUnique() throws Exception{
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,CustomerTable.TABLE_NAME,CustomerTable.INDEX_NAME,CustomerTable.INDEX_DEF,false);
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,OrderTable.TABLE_NAME,OrderTable.INDEX_NAME,OrderTable.INDEX_DEF,false);
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,OrderLineTable.TABLE_NAME,OrderLineTable.INDEX_NAME,OrderLineTable.INDEX_DEF,false);
    }

    @Test
    public void createIndexesInOrderUnique() throws Exception{
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,CustomerTable.TABLE_NAME,CustomerTable.INDEX_NAME,CustomerTable.INDEX_DEF,true);
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,OrderTable.TABLE_NAME,OrderTable.INDEX_NAME,OrderTable.INDEX_DEF,true);
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,OrderLineTable.TABLE_NAME,OrderLineTable.INDEX_NAME,OrderLineTable.INDEX_DEF,true);
    }

    @Test
    public void createIndexesOutOfOrderNonUnique() throws Exception{
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,CustomerTable.TABLE_NAME,CustomerTable.INDEX_NAME,CustomerTable.INDEX_ORDER_DEF,false);
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,OrderTable.TABLE_NAME,OrderTable.INDEX_NAME,OrderTable.INDEX_ORDER_DEF,false);
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,OrderLineTable.TABLE_NAME,OrderLineTable.INDEX_NAME,OrderLineTable.INDEX_ORDER_DEF,false);
    }

    @Test
    public void createIndexesOutOfOrderUnique() throws Exception{
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,CustomerTable.TABLE_NAME,CustomerTable.INDEX_NAME,CustomerTable.INDEX_ORDER_DEF,true);
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,OrderTable.TABLE_NAME,OrderTable.INDEX_NAME,OrderTable.INDEX_ORDER_DEF,true);
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,OrderLineTable.TABLE_NAME,OrderLineTable.INDEX_NAME,OrderLineTable.INDEX_ORDER_DEF,true);
    }

    @Test
    public void createIndexesAscNonUnique() throws Exception{
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,CustomerTable.TABLE_NAME,CustomerTable.INDEX_NAME,CustomerTable.INDEX_ORDER_DEF_ASC,false);
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,OrderTable.TABLE_NAME,OrderTable.INDEX_NAME,OrderTable.INDEX_ORDER_DEF_ASC,false);
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,OrderLineTable.TABLE_NAME,OrderLineTable.INDEX_NAME,OrderLineTable.INDEX_ORDER_DEF_ASC,false);
    }

    @Test
    public void createIndexesAscUnique() throws Exception{
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,CustomerTable.TABLE_NAME,CustomerTable.INDEX_NAME,CustomerTable.INDEX_ORDER_DEF_ASC,true);
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,OrderTable.TABLE_NAME,OrderTable.INDEX_NAME,OrderTable.INDEX_ORDER_DEF_ASC,true);
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,OrderLineTable.TABLE_NAME,OrderLineTable.INDEX_NAME,OrderLineTable.INDEX_ORDER_DEF_ASC,true);
    }

    @Test
    public void createIndexesDescNonUnique() throws Exception{
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,CustomerTable.TABLE_NAME,CustomerTable.INDEX_NAME,CustomerTable.INDEX_ORDER_DEF_DESC,false);
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,OrderTable.TABLE_NAME,OrderTable.INDEX_NAME,OrderTable.INDEX_ORDER_DEF_DESC,false);
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,OrderLineTable.TABLE_NAME,OrderLineTable.INDEX_NAME,OrderLineTable.INDEX_ORDER_DEF_DESC,false);
    }

    @Test
    public void createIndexesDescUnique() throws Exception{
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,CustomerTable.TABLE_NAME,CustomerTable.INDEX_NAME,CustomerTable.INDEX_ORDER_DEF_DESC,true);
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,OrderTable.TABLE_NAME,OrderTable.INDEX_NAME,OrderTable.INDEX_ORDER_DEF_DESC,true);
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,OrderLineTable.TABLE_NAME,OrderLineTable.INDEX_NAME,OrderLineTable.INDEX_ORDER_DEF_DESC,true);
    }

    @Test
    public void createIndexesOnExpressions() throws Exception{
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,CustomerTable.TABLE_NAME,CustomerTable.INDEX_NAME,CustomerTable.EXPR_INDEX_DEF,false);
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,OrderTable.TABLE_NAME,OrderTable.INDEX_NAME,OrderTable.EXPR_INDEX_DEF,true);
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,OrderLineTable.TABLE_NAME,OrderLineTable.INDEX_NAME,OrderLineTable.EXPR_INDEX_DEF,false);
    }

    @Test
    public void createIndexesOnExpressionsNoDuplicateIndex() throws Exception{
        String tableName = "TEST_IDX_DUP";
        methodWatcher.executeUpdate(format("create table %s (vc varchar(10), i int not null)", tableName));

        String checkQuery = "select count(*) from sys.sysconglomerates where conglomeratename='%s'";
        String expected1 = "1 |\n" +
                "----\n" +
                " 1 |";
        String expected0 = "1 |\n" +
                "----\n" +
                " 0 |";

        methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (i, vc)", tableName, tableName));
        try(ResultSet rs = methodWatcher.executeQuery(format(checkQuery, tableName + "_IDX"))) {
            assertEquals(expected1, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }

        // an index on the same set of columns but with expressions is not a duplicate
        methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX_1 ON %s (i * 4, upper(vc))", tableName, tableName));
        try(ResultSet rs = methodWatcher.executeQuery(format(checkQuery, tableName + "_IDX_1"))) {
            assertEquals(expected1, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }

        // this is a duplicate of _IDX_1 (can't catch exception since it's just a warning)
        methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX_2 ON %s (i * 4, upper(vc))", tableName, tableName));
        try(ResultSet rs = methodWatcher.executeQuery(format(checkQuery, tableName + "_IDX_2"))) {
            // should be 0 because this index is not created
            assertEquals(expected0, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }

        methodWatcher.executeUpdate(format("DROP INDEX %s_IDX", tableName));
        try(ResultSet rs = methodWatcher.executeQuery(format(checkQuery, tableName + "_IDX"))) {
            assertEquals(expected0, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }

        // not a duplicate of _IDX_1
        methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (i, vc)", tableName, tableName));
        try(ResultSet rs = methodWatcher.executeQuery(format(checkQuery, tableName + "_IDX"))) {
            assertEquals(expected1, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    // ===============================================================================
    // Query Tests
    // ===============================================================================


    @Test
    public void testCanUpdateIndexTwiceCorrectlyWhenUpdatingToItself() throws Exception{
        try(Statement s = conn.createStatement()){
            int updateCount = s.executeUpdate("update "+cchar+" set cint = cint");
            Assert.assertEquals("Incorrect update count!",1,updateCount);
            updateCount = s.executeUpdate("update "+ cchar+" set cchar = 's'");
            Assert.assertEquals("Incorrect update count!",1,updateCount);
            try(ResultSet rs = s.executeQuery("select cchar, cint from "+cchar+" --SPLICE-PROPERTIES index=B1\n")){
                Assert.assertTrue("No rows returned!",rs.next());
                Assert.assertEquals("Incorrect value for cchar!","s",rs.getString(1).trim());
                Assert.assertEquals("Incorrect value for cint!",11,rs.getInt(2));
                Assert.assertFalse("Too many rows returned!",rs.next());
            }
        }
    }

    @Test
    public void testDescendingIndexOverPrimaryKeyedColumn() throws Exception{
        /*
         * Regression test for DB-3845. Believe it or not, this simple test failed before the fix
         * was implemented :)
         *
         * The idea is simple: create a descending index over a table with a primary key column, and
         * make sure that it returns results.
         */
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,descIndex.tableName,"t_idx","(b desc)",false);
        String query="select * from "+descIndex+" --SPLICE-PROPERTIES index=T_IDX\n";
        query+="where b = 'EFGH'";
        try(Statement s=conn.createStatement()){
            try(ResultSet rs=s.executeQuery(query)){
                Assert.assertTrue("No row returned!",rs.next());
                String b=rs.getString("b");
                Assert.assertFalse("Returned null!",rs.wasNull());
                Assert.assertEquals("Incorrect returned value!","EFGH",b);
                Assert.assertFalse("More than one row returned!",rs.next());
            }
        }
    }

    @Test
    public void testQueryCustomerById() throws Exception{
        String query=String.format(SELECT_UNIQUE_CUSTOMER,SCHEMA_NAME,CustomerTable.TABLE_NAME);
        ResultSet rs=methodWatcher.executeQuery(query);
        Assert.assertEquals(1,resultSetSize(rs));
    }

    @Test
    public void testQueryCustomerByIdWithIndex() throws Exception{
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,CustomerTable.TABLE_NAME,CustomerTable.INDEX_NAME,CustomerTable.INDEX_ORDER_DEF_ASC,false);
        String query=String.format(SELECT_UNIQUE_CUSTOMER,SCHEMA_NAME,CustomerTable.TABLE_NAME);
        ResultSet rs=methodWatcher.executeQuery(query);
        Assert.assertEquals(1,resultSetSize(rs));
    }

    @Test
    public void testQueryOrder() throws Exception{
        String query=String.format("select * from %s.%s o where o.o_w_id = 1 and o.o_d_id = 1 and o.o_id = 1",SCHEMA_NAME,OrderTable.TABLE_NAME);
        long start=System.currentTimeMillis();
        ResultSet rs=methodWatcher.executeQuery(query);

        String duration=TestUtils.getDuration(start,System.currentTimeMillis());
//        int cnt = printResult(query, rs, System.out);
        int cnt=resultSetSize(rs);
        System.out.println("Rows returned: "+cnt+" in "+duration);

        Assert.assertEquals(1,cnt);
    }

    @Test
    public void testQueryCustomerByName() throws Exception{
        PreparedStatement ps=methodWatcher.prepareStatement(CUSTOMER_BY_NAME);
        ps.setInt(1,1); // c_w_id
        ps.setInt(2,7); // c_d_id
        ps.setString(3,"ESEPRIANTI");  // c_last
        long start=System.currentTimeMillis();
        ResultSet rs=ps.executeQuery();

        String duration=TestUtils.getDuration(start,System.currentTimeMillis());
        int cnt=resultSetSize(rs);
        System.out.println("Rows returned: "+cnt+" in "+duration);
        Assert.assertEquals(8,cnt);
    }

    @Test
    public void testQueryCustomerByNameWithIndex() throws Exception{
        // Test for DB-1620
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,CustomerTable.TABLE_NAME,CustomerTable.INDEX_NAME,CustomerTable.INDEX_DEF,false);

        String idxQuery=String.format("SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city, "
                +"c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, "
                +"c_balance, c_ytd_payment, c_payment_cnt, c_since FROM %s.%s --SPLICE-PROPERTIES index=%s \n"
                +" WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first",SCHEMA_NAME,CustomerTable.TABLE_NAME,CustomerTable.INDEX_NAME);
        PreparedStatement ps=methodWatcher.prepareStatement(idxQuery);
        // this column combo is the PK
        ps.setInt(1,1); // c_w_id
        ps.setInt(2,7); // c_d_id
        ps.setString(3,"ESEPRIANTI");  // c_last
        long start=System.currentTimeMillis();
        ResultSet rs=ps.executeQuery();

        String duration=TestUtils.getDuration(start,System.currentTimeMillis());
        int cnt=resultSetSize(rs);
        System.out.println("Rows returned: "+cnt+" in "+duration);
        Assert.assertEquals(8,cnt);
    }

    @Test
    public void testQueryCustomerByNameWithTpccIndex() throws Exception {
        // DB-4894: this tpcc index on customer table caused an NPE in AbstractTimeDescriptorSerializer
        // because of open/close in indexToBaseRow.  The close nulled out the ThreadLocal<Calendar>

        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,CustomerTable.TABLE_NAME,CustomerTable.INDEX_NAME,CustomerTable.INDEX_NAME_DEF,false);
        PreparedStatement ps=methodWatcher.prepareStatement(CUSTOMER_BY_NAME);
        ps.setInt(1,1); // c_w_id
        ps.setInt(2,7); // c_d_id
        ps.setString(3,"ESEPRIANTI");  // c_last
        ResultSet rs=ps.executeQuery();
        Assert.assertEquals(8,resultSetSize(rs));
    }

    @Test
    public void testQueryCustomerSinceWithIndex() throws Exception{
        // Test for DB-1620
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,CustomerTable.TABLE_NAME,CustomerTable.INDEX_NAME,CustomerTable.INDEX_DEF,false);

        String idxQuery=String.format(
                "SELECT c_since FROM %s.%s --SPLICE-PROPERTIES index=%s",
                SCHEMA_NAME,CustomerTable.TABLE_NAME,CustomerTable.INDEX_NAME);
        long start=System.currentTimeMillis();
        ResultSet rs=methodWatcher.executeQuery(idxQuery);

        String duration=TestUtils.getDuration(start,System.currentTimeMillis());
        int cnt=resultSetSize(rs);
        System.out.println("Rows returned: "+cnt+" in "+duration);
        Assert.assertEquals(30000,cnt);
    }

    @Test
    public void testDistinctCustomerByID() throws Exception{
        String query=String.format("select distinct c_id from %s.%s",SCHEMA_NAME,CustomerTable.TABLE_NAME);
        long start=System.currentTimeMillis();
        ResultSet rs=methodWatcher.executeQuery(query);

        String duration=TestUtils.getDuration(start,System.currentTimeMillis());
        int cnt=resultSetSize(rs);
        System.out.println("Rows returned: "+cnt+" in "+duration);
        Assert.assertTrue(cnt>=3000);
    }

    @Test
    public void testDistinctCustomerByIDWithIndex() throws Exception{
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,CustomerTable.TABLE_NAME,CustomerTable.INDEX_NAME,CustomerTable.INDEX_DEF,false);

        String query=String.format("select distinct c_id from %s.%s",SCHEMA_NAME,CustomerTable.TABLE_NAME);
        long start=System.currentTimeMillis();
        ResultSet rs=methodWatcher.executeQuery(query);

        String duration=TestUtils.getDuration(start,System.currentTimeMillis());
        int cnt=resultSetSize(rs);
        System.out.println("Rows returned: "+cnt+" in "+duration);
        Assert.assertTrue(cnt>=3000);
    }

    @Test
    public void testJoinCustomerOrders() throws Exception{
        String query=String.format(CUSTOMER_ORDER_JOIN,
                SCHEMA_NAME,CustomerTable.TABLE_NAME,OrderTable.TABLE_NAME);
        long start=System.currentTimeMillis();
        ResultSet rs=methodWatcher.executeQuery(query);
        String duration=TestUtils.getDuration(start,System.currentTimeMillis());
        int cnt=resultSetSize(rs);
        System.out.println("Rows returned: "+cnt+" in "+duration);
        Assert.assertTrue(cnt>=300000);
    }

    @Test
    public void testJoinCustomerOrdersWithIndex() throws Exception{
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,CustomerTable.TABLE_NAME,CustomerTable.INDEX_NAME,CustomerTable.INDEX_DEF,false);
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,OrderTable.TABLE_NAME,OrderTable.INDEX_NAME,OrderTable.INDEX_DEF,false);

        String query=String.format(CUSTOMER_ORDER_JOIN,
                SCHEMA_NAME,CustomerTable.TABLE_NAME,OrderTable.TABLE_NAME);
        long start=System.currentTimeMillis();
        ResultSet rs=methodWatcher.executeQuery(query);
        String duration=TestUtils.getDuration(start,System.currentTimeMillis());
        int cnt=resultSetSize(rs);
        System.out.println("Rows returned: "+cnt+" in "+duration);
        Assert.assertTrue(cnt>=300000);
    }

    public static final String HEADER_JOIN=String.format("select distinct a.customer_master_id\n"+
                    "from %s.%s a, %s.%s b --SPLICE-PROPERTIES joinStrategy=SORTMERGE\n"+
                    "where transaction_dt >= DATE('2011-12-28') and transaction_dt <=\n"+
                    "DATE('2012-03-03')\n"+
                    "and b.customer_master_id=a.customer_master_id",
            SCHEMA_NAME,HeaderTable.TABLE_NAME,SCHEMA_NAME,EmailableTable.TABLE_NAME);

    @Test
    public void testHeaderJoinMSJ() throws Exception{
        long start=System.currentTimeMillis();
        ResultSet rs=methodWatcher.executeQuery(HEADER_JOIN);
        String duration=TestUtils.getDuration(start,System.currentTimeMillis());
        int cnt=resultSetSize(rs);
        System.out.println("Rows returned: "+cnt+" in "+duration);
        Assert.assertTrue(cnt==1);
    }

    @Test
    public void testHeaderJoinMSJWithIndex() throws Exception{
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,HeaderTable.TABLE_NAME,HeaderTable.INDEX_NAME,HeaderTable.INDEX_DEF,false);
        long start=System.currentTimeMillis();
        ResultSet rs=methodWatcher.executeQuery(HEADER_JOIN);
        String duration=TestUtils.getDuration(start,System.currentTimeMillis());
        int cnt=resultSetSize(rs);
        System.out.println("Rows returned: "+cnt+" in "+duration);
        Assert.assertTrue(cnt==1);
    }

    @Test
    public void testJoinCustomerOrdersWithIndexNotInColumnOrder() throws Exception{
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,CustomerTable.TABLE_NAME,CustomerTable.INDEX_NAME,CustomerTable.INDEX_ORDER_DEF,false);
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,OrderTable.TABLE_NAME,OrderTable.INDEX_NAME,OrderTable.INDEX_ORDER_DEF,false);

        String query=String.format(CUSTOMER_ORDER_JOIN,
                SCHEMA_NAME,CustomerTable.TABLE_NAME,OrderTable.TABLE_NAME);
        long start=System.currentTimeMillis();
        ResultSet rs=methodWatcher.executeQuery(query);
        String duration=TestUtils.getDuration(start,System.currentTimeMillis());
        int cnt=resultSetSize(rs);
        System.out.println("Rows returned: "+cnt+" in "+duration);
        Assert.assertTrue(cnt>=300000);
    }

    @Test(timeout=1000*60*5)  // Time out after 3 min
    public void testJoinCustomerOrdersOrderLineWithIndexNotInColumnOrder() throws Exception{
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,CustomerTable.TABLE_NAME,CustomerTable.INDEX_NAME,CustomerTable.INDEX_ORDER_DEF,false);
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,OrderTable.TABLE_NAME,OrderTable.INDEX_NAME,OrderTable.INDEX_ORDER_DEF,false);
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,OrderLineTable.TABLE_NAME,OrderLineTable.INDEX_NAME,OrderLineTable.INDEX_ORDER_DEF,false);

        String query=String.format("select "+
                        "%1$s.c.c_last, %1$s.c.c_first, %1$s.o.o_id, %1$s.o.o_entry_d, %1$s.ol.ol_amount "+
                        "from "+
                        "%1$s.%2$s c, %1$s.%3$s o, %1$s.%4$s ol "+
                        "where "+
                        "c.c_id = o.o_c_id and ol.ol_o_id = o.o_id and c.c_last = 'ESEPRIANTI'",
                SCHEMA_NAME,CustomerTable.TABLE_NAME,OrderTable.TABLE_NAME,OrderLineTable.TABLE_NAME);
        long start=System.currentTimeMillis();
        System.out.println(query);
        ResultSet rs=conn.prepareStatement(query).executeQuery();
        String duration=TestUtils.getDuration(start,System.currentTimeMillis());
//        Assert.assertTrue(printResult(query, rs, System.out) > 0);
        int cnt=resultSetSize(rs);
        System.out.println("Rows returned: "+cnt+" in "+duration);
        Assert.assertTrue(cnt>0);
    }

    @Test
    public void testSysPropIndexEqualNull() throws Exception{
        // todo Test for DB-1636 and DB-857
        String indexName="IA";
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,A_TABLE_NAME,indexName,"(i)",false);
        methodWatcher.executeUpdate(String.format("insert into %s.%s values 1,2,3,4,5",SCHEMA_NAME,A_TABLE_NAME));
        ResultSet rs=methodWatcher.executeQuery(String.format("select count(*) from %s.%s --SPLICE-PROPERTIES index=%s",SCHEMA_NAME,A_TABLE_NAME,indexName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(5,rs.getInt(1));

        // Test for DB-1636 - used to get NPE here
        rs=methodWatcher.executeQuery(String.format("select count(*) from %s.%s --SPLICE-PROPERTIES index=NULL",SCHEMA_NAME,A_TABLE_NAME));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(5,rs.getInt(1));

        // Test for DB-857 - trunc'ing table did not update index
        methodWatcher.executeUpdate(String.format("truncate table %s.%s",SCHEMA_NAME,A_TABLE_NAME));
        methodWatcher.executeUpdate(String.format("insert into %s.%s values 6,7,8,9,10,11", SCHEMA_NAME,A_TABLE_NAME));

        // query base table directly
        String sqlText = String.format("select * from %s.%s --SPLICE-PROPERTIES index=%s\n order by 1",SCHEMA_NAME,A_TABLE_NAME, "NULL");
        rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "I |\n" +
                "----\n" +
                " 6 |\n" +
                " 7 |\n" +
                " 8 |\n" +
                " 9 |\n" +
                "10 |\n" +
                "11 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));

        // query using index
        sqlText = String.format("select * from %s.%s --SPLICE-PROPERTIES index=%s",SCHEMA_NAME,A_TABLE_NAME, indexName);
        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
    }

    /* DB-3699: Tests a plan with a join over two index scans of compound indexes */
    @Test
    public void joinOverTwoCompoundIndexScans() throws Exception{
        methodWatcher.executeUpdate("CREATE TABLE PERSON_ADDRESS (PID INTEGER, ADDR_ID INTEGER)");
        methodWatcher.executeUpdate("CREATE TABLE ADDRESS (ADDR_ID INTEGER, STD_STATE_PROVENCE VARCHAR(30))");
        methodWatcher.executeUpdate("CREATE INDEX a_idx ON ADDRESS (std_state_provence, addr_id)");
        methodWatcher.executeUpdate("CREATE INDEX pa_idx ON PERSON_ADDRESS (pid, addr_id)");
        methodWatcher.executeUpdate("INSERT INTO ADDRESS VALUES (100, 'MO'),(200, 'IA'),(300,'NY'),(400,'FL'),(500,'AL')");
        methodWatcher.executeUpdate("INSERT INTO PERSON_ADDRESS VALUES (10, 100),(20, 200),(30,300),(40, 400),(50,500)");

        ResultSet resultSet=methodWatcher.executeQuery(""+
                "select pa.pid\n"+
                "from PERSON_ADDRESS pa --SPLICE-PROPERTIES index=pa_idx\n"+
                "join ADDRESS a         --SPLICE-PROPERTIES index=a_idx, joinStrategy=broadcast\n"+
                "  on pa.addr_id=a.addr_id\n"+
                "where a.std_state_provence in ('IA', 'FL', 'NY')");

        assertEquals(""+
                "PID |\n"+
                "------\n"+
                " 20  |\n"+
                " 30  |\n"+
                " 40  |",TestUtils.FormattedResult.ResultFactory.toString(resultSet));
    }


    // ===============================================================================
    // Update Tests
    // ===============================================================================

    @Test
    public void testUpdateCustomer() throws Exception{
        String query=String.format(SELECT_UNIQUE_CUSTOMER,SCHEMA_NAME,CustomerTable.TABLE_NAME);
        ResultSet rs=methodWatcher.executeQuery(query);
        Assert.assertEquals(1,resultSetSize(rs));

        int nCols=methodWatcher.getStatement().executeUpdate(String.format("update %s.%s c set c.c_credit_lim = %s where %s",
                SCHEMA_NAME,CustomerTable.TABLE_NAME,"3001.00","c.c_w_id = 1 and c.c_d_id = 10 and c.c_id = 2365"));
        Assert.assertEquals(1,nCols);

        query=String.format("select * from %s.%s c where c.c_last = 'ESEPRIANTI' and c.c_id = 2365 and c.c_credit_lim = 3001.00",
                SCHEMA_NAME,CustomerTable.TABLE_NAME);
        rs=methodWatcher.executeQuery(query);
        Assert.assertEquals(1,resultSetSize(rs));
    }

    @Test
    public void testUpdateCustomerWithIndex() throws Exception{
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,CustomerTable.TABLE_NAME,CustomerTable.INDEX_NAME,CustomerTable.INDEX_ORDER_DEF_ASC,false);

        String query=String.format(SELECT_UNIQUE_CUSTOMER,SCHEMA_NAME,CustomerTable.TABLE_NAME);
        ResultSet rs=methodWatcher.executeQuery(query);
        Assert.assertEquals(1,resultSetSize(rs));

        int nCols=methodWatcher.getStatement().executeUpdate(String.format("update %s.%s c set c.c_credit_lim = %s where %s",
                SCHEMA_NAME,CustomerTable.TABLE_NAME,"3000.00","c.c_w_id = 1 and c.c_d_id = 10 and c.c_id = 2365"));
        Assert.assertEquals(1,nCols);

        query=String.format("select * from %s.%s c where c.c_last = 'ESEPRIANTI' and c.c_id = 2365 and c.c_credit_lim = 3000.00",
                SCHEMA_NAME,CustomerTable.TABLE_NAME);
        rs=methodWatcher.executeQuery(query);
        Assert.assertEquals(1,resultSetSize(rs));
    }

    @Test
    public void testUpdateCustomerWithExpressionBasedIndex() throws Exception{
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,CustomerTable.TABLE_NAME,CustomerTable.INDEX_NAME,CustomerTable.EXPR_INDEX_DEF,false);

        String query=String.format(SELECT_UNIQUE_CUSTOMER,SCHEMA_NAME,CustomerTable.TABLE_NAME);
        ResultSet rs=methodWatcher.executeQuery(query);
        Assert.assertEquals(1,resultSetSize(rs));

        int nRows=methodWatcher.getStatement().executeUpdate(String.format("update %s.%s c set c.c_last = %s where %s",
                SCHEMA_NAME,CustomerTable.TABLE_NAME,"'new_value'","(c.c_w_id + 2) * 4 = 12 and c.c_d_id = 10 and c.c_id = 510"));
        Assert.assertEquals(1,nRows);

        query=String.format("select * from %s.%s c where upper(c_last) = 'NEW_VALUE'",
                SCHEMA_NAME,CustomerTable.TABLE_NAME);
        rs=methodWatcher.executeQuery(query);
        Assert.assertEquals(1,resultSetSize(rs));
    }

    // ===============================================================================
    // Insert Tests
    // ===============================================================================

    @Test
    public void testInsertNewOrder() throws Exception{
        int nCols=methodWatcher.getStatement().executeUpdate(String.format("insert into %s.%s values (%s)",
                SCHEMA_NAME,NewOrderTable.TABLE_NAME,"1,1,2001"));
        Assert.assertEquals(1,nCols);
    }

    @Test
    public void testInsertNewOrderWithIndex() throws Exception{
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,NewOrderTable.TABLE_NAME,NewOrderTable.INDEX_NAME,NewOrderTable.INDEX_ORDER_DEF,false);
        int nCols=methodWatcher.getStatement().executeUpdate(String.format("insert into %s.%s values (%s)",
                SCHEMA_NAME,NewOrderTable.TABLE_NAME,"1,2,2001"));
        Assert.assertEquals(1,nCols);
    }

    @Test
    public void testInsertOrder() throws Exception{
        int nCols=methodWatcher.getStatement().executeUpdate(String.format("insert into %s.%s values (%s)",
                SCHEMA_NAME,OrderTable.TABLE_NAME,"3001,1,1,1268,10,6,1,'2013-08-01 10:33:44.813'"));
        Assert.assertEquals(1,nCols);
    }

    private static final String CUST_INSERT1="1,1,3001,0.1221,'GC','CUNNINGHAM','Jeff',50000.0,-10.0,10.0,1,0,'qmvfraakwixzcrqxt','mamrbljycaxrh','bcsygfxkurug','VH','843711111','3185126927164979','2013-08-01 10:33:44.813','OE','uufvvwszkmxfrxerjxiekbsf'";

    @Test
    public void testInsertCustomer() throws Exception{
        String query=String.format("select c.c_last, c.c_first, c.c_since from %s.%s c where c.c_last = 'CUNNINGHAM'",
                SCHEMA_NAME,CustomerTable.TABLE_NAME);
        ResultSet rs=methodWatcher.executeQuery(query);
        Assert.assertEquals(0,resultSetSize(rs));

        int nCols=methodWatcher.getStatement().executeUpdate(String.format("insert into %s.%s values (%s)",
                SCHEMA_NAME,CustomerTable.TABLE_NAME,CUST_INSERT1));
        // C_LAST: CUNNINGHAM
        // C_FIRST: Jeff
        // C_SINCE: 2013-08-01 10:33:44.813
        Assert.assertEquals(1,nCols);

        rs=methodWatcher.executeQuery(query);
        Assert.assertEquals(1,resultSetSize(rs));
    }

    @Test(expected=SQLException.class)
    public void testIndexAfterInserts() throws Exception{
        try{
            methodWatcher.prepareStatement("drop index ia").execute();
        }catch(Exception e1){
            // ignore
        }
        try{
            methodWatcher.prepareStatement(String.format("drop table %s.a",SCHEMA_NAME)).execute();
        }catch(Exception e1){
            // ignore
        }
        methodWatcher.prepareStatement(String.format("create table %s.a (i int)",SCHEMA_NAME)).execute();
        PreparedStatement ps=methodWatcher.prepareStatement(String.format("insert into %s.a values 1",SCHEMA_NAME));
        ps.execute();
        ps.execute();
        ps.execute();
        methodWatcher.prepareStatement(String.format("create unique index ia on %s.a (i)",SCHEMA_NAME)).execute();
        Assert.fail("Index should have raised an exception");
    }

    @Test
    public void testFailedIndexRollbacks() throws Exception{
        try{
            conn.prepareStatement("drop index ib").execute();
        }catch(Exception e1){
            // ignore
        }
        try{
            conn.prepareStatement(String.format("drop table %s.b",SCHEMA_NAME)).execute();
        }catch(Exception e1){
            // ignore
        }
        conn.prepareStatement(String.format("create table %s.b (i int)",SCHEMA_NAME)).execute();
        PreparedStatement ps=methodWatcher.prepareStatement(String.format("insert into %s.b values 1",SCHEMA_NAME));
        ps.execute();
        ps.execute();
        ps.execute();
        for(int i=0;i<5;++i){
            try{
                conn.prepareStatement(String.format("create unique index ib on %s.b (i)",SCHEMA_NAME)).execute();
                Assert.fail("Index should have raised an exception");
            }catch(SQLException se){
                Assert.assertEquals("Expected constraint violation exception","23505",se.getSQLState());
            }
        }
    }

    private static final String CUST_INSERT2="1,1,3002,0.1221,'GC','Jones','Fred',50000.0,-10.0,10.0,1,0,'qmvfraakwixzcrqxt','mamrsdfdsycaxrh','bcsyfdsdkurug','VL','843211111','3185126927164979','2013-08-01 10:33:44.813','OE','tktkdcbjqxbewxllutwigcdmzenarkhiklzfkaybefrppwtvdmecqfqaa'";

    @Test
    public void testInsertCustomerWithIndex() throws Exception{
        SpliceIndexWatcher.createIndex(conn,SCHEMA_NAME,CustomerTable.TABLE_NAME,CustomerTable.INDEX_NAME,CustomerTable.INDEX_ORDER_DEF,false);

        String query=String.format("select c.c_last, c.c_first, c.c_since from %s.%s c where c.c_last = 'Jones'",
                SCHEMA_NAME,CustomerTable.TABLE_NAME);
        ResultSet rs=methodWatcher.executeQuery(query);
        Assert.assertEquals(0,resultSetSize(rs));

        int nCols=methodWatcher.getStatement().executeUpdate(String.format("insert into %s.%s values (%s)",
                SCHEMA_NAME,CustomerTable.TABLE_NAME,CUST_INSERT2));
        Assert.assertEquals(1,nCols);

        rs=methodWatcher.executeQuery(query);
        Assert.assertEquals(1,resultSetSize(rs));
    }


    @Test
    public void testCost() throws Exception{
        methodWatcher.execute("drop table if exists address");
        methodWatcher.execute("create table ADDRESS (addr_id int, std_state_provence varchar(5))");
        methodWatcher.execute("drop table if exists person_address");
        methodWatcher.execute("create table person_address (pid int, addr_id int)");

        methodWatcher.execute("insert into address values (1,'CA'),(2,'CO'),(3,'FL'),(4,'IL'),(5,'IL'),(6,'IA'),(7,'WI'),(8,'WY'),(9,'IA'),(10,'WI')");
        methodWatcher.execute("insert into person_address values (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10)");

        methodWatcher.execute("create index address_ix on address(addr_id)");
        methodWatcher.execute("create index address_ix4 on address(addr_id,std_state_provence)");
        methodWatcher.execute("create index b_idx1 on person_address(pid, addr_id)");

        //methodWatcher.executeQuery("elapsedtime on");

        // should have higher cost - needs index to base row lookup
        String sql1="explain exclude no statistics SELECT count(a4.PID) FROM --splice-properties joinOrder=FIXED \n"+
                " PERSON_ADDRESS a4 --splice-properties index=B_IDX1 \n"+
                " INNER JOIN ADDRESS a5 --splice-properties joinStrategy=SORTMERGE,index=ADDRESS_IX \n"+
                " ON a4.ADDR_ID = a5.ADDR_ID \n"+
                " WHERE  a5.STD_STATE_PROVENCE IN ('IA', 'WI', 'IL')";

        List<String> arr1=methodWatcher.queryList(sql1);

        // should have lower cost
        String sql2="explain exclude no statistics SELECT count(a4.PID) FROM --splice-properties joinOrder=FIXED \n"+
                " PERSON_ADDRESS a4 --splice-properties index=B_IDX1 \n"+
                " INNER JOIN ADDRESS a5 --splice-properties joinStrategy=SORTMERGE,index=ADDRESS_IX4 \n"+
                " ON a4.ADDR_ID = a5.ADDR_ID \n"+
                " WHERE  a5.STD_STATE_PROVENCE IN ('IA', 'WI', 'IL')";

        List<String> arr2=methodWatcher.queryList(sql2);

        assertTrue("Plan #1 has small size",arr1.size()==10);
        assertTrue("Plan #1 must be longer that plan #2",arr1.size()>arr2.size());

        String messages=Joiner.on("").join(arr1);
        assertTrue("IndexLookup cannot appear anywhere in the plan.",messages.contains("IndexLookup"));

        // check for total cost
        double cost1=SpliceUnitTest.parseTotalCost(arr1.get(1));
        double cost2=SpliceUnitTest.parseTotalCost(arr2.get(1));
        assertTrue("Cost #1 must be bigger than cost #2", Double.compare(cost1, cost2) > 0);
    }

    @Test
    // DB-5160
    public void testCreateIndexWithUpdatedData() throws Exception {
        methodWatcher.executeUpdate("create table w (a int, b int, primary key (a))");
        methodWatcher.executeUpdate("insert into w (a,b) values (1,1),(2,1),(3,2)");
        methodWatcher.executeUpdate("update w set a = a-1 where b = 1");
        methodWatcher.executeUpdate("create index widx on w (b)");
        rowContainsQuery(new int[]{1,2,3},"select b from w --splice-properties index=widx",methodWatcher,
                "1","1","2");
    }

    @Test
    // DB-5029
    public void testCreateIndexAndUpdateDataViaIndexScan() throws Exception {
        methodWatcher.executeUpdate("create table double_INDEXES1(column1 DOUBLE)");
        methodWatcher.executeUpdate("INSERT INTO double_INDEXES1(column1) VALUES (-1.79769E+308),(1.79769E+308),(0)");
        methodWatcher.executeUpdate("CREATE INDEX doubleIndex2 ON double_INDEXES1(column1 ASC)");
        methodWatcher.executeUpdate("update double_INDEXES1 set column1 = -1.79769E+308 where column1 = -1.79769E+308");
        rowContainsQuery(new int[]{1,2,3},"select column1 from double_INDEXES1 --splice-properties index=doubleIndex2\n order by column1",methodWatcher,
                "-1.79769E308","0","1.79769E308");

    }

    // ===============================================================================
    // Index expression tests - create, insert, and update
    // ===============================================================================

    @Test
    public void testCreateIndexAndInsertWithExpressionsOfNumericFunctions() throws Exception {
        String tableName = "TEST_IDX_NUMERIC_FN";
        methodWatcher.executeUpdate(format("create table %s (d1 double, d2 double, i int)", tableName));
        methodWatcher.executeUpdate(format("insert into %s values (3.14159, 4.54321, 1), (2.71828, 2.12345, 2)", tableName));
        methodWatcher.executeUpdate(
                format("CREATE INDEX %s_IDX ON %s " +
                       "(abs(d1), ceil(d1), exp(d1), floor(d1), ln(d1), log10(d1), max(i, 3), min(3, i), mod(i, 2), " +
                       "round(d1), sign(d1), sqrt(d1), trunc(i, 1))",
                        tableName, tableName));

        methodWatcher.executeUpdate(format("insert into %s values (1.41421, 3.43215, 3)", tableName));
        methodWatcher.executeUpdate(format("update %s set d1 = 1.61803 where d2 = 4.54321", tableName));
        rowContainsQuery(new int[]{1,2,3},format("select d1 from %s --splice-properties index=%s_IDX\n order by d1", tableName, tableName),methodWatcher,
                "1.41421","1.61803","2.71828");
    }

    @Test
    public void testCreateIndexAndInsertWithExpressionsOfStringFunctions() throws Exception {
        String tableName = "TEST_IDX_STRING_FN";
        methodWatcher.executeUpdate(format("create table %s (c1 char(5), c2 varchar(5))", tableName));
        methodWatcher.executeUpdate(format("insert into %s values ('foo', 'bar'), ('hello', 'world')", tableName));

        methodWatcher.executeUpdate(
                format("CREATE INDEX %s_IDX ON %s " +
                       "(c1 || '.test', initcap(c1), lcase(c1), length(c1), " +
                       "ltrim(c1), repeat(c1, 2), replace(c1, 'o', '0'), rtrim(c1), " +
                       "substr(c1, 2), trim(c1), ucase(c1))",
                        tableName, tableName));

        methodWatcher.executeUpdate(format("insert into %s values ('abc', 'xyz')", tableName));
        methodWatcher.executeUpdate(format("update %s set c1 = 'this' where c2 = 'world'", tableName));
        rowContainsQuery(new int[]{1,2,3},format("select c1 from %s --splice-properties index=%s_IDX\n order by c1", tableName, tableName),methodWatcher,
                "abc","foo","this");
    }

    @Test
    public void testCreateIndexAndInsertWithExpressionsOfTrigonometricFunctions() throws Exception {
        String tableName = "TEST_IDX_TRIGONOMETRIC_FN";
        methodWatcher.executeUpdate(format("create table %s (d1 double, d2 double)", tableName));
        methodWatcher.executeUpdate(format("insert into %s values (30, 0.4), (60, 0.6)", tableName));
        methodWatcher.executeUpdate(
                format("CREATE INDEX %s_IDX ON %s " +
                       "(acos(d2), asin(d2), atan(d2), atan2(d2, 0), cos(d1), cosh(d1), cot(d1), degrees(d1), " +
                       "radians(d1), pi() + d2, sin(d1), sinh(d1), tan(d1), tanh(d1))",
                        tableName, tableName));

        methodWatcher.executeUpdate(format("insert into %s values (90, 0.8)", tableName));
        methodWatcher.executeUpdate(format("update %s set d1 = 70 where d2 = 0.6", tableName));
        rowContainsQuery(new int[]{1,2,3},format("select d1 from %s --splice-properties index=%s_IDX\n order by d1", tableName, tableName),methodWatcher,
                "30","70","90");
    }

    @Test
    public void testCreateIndexAndInsertWithExpressionsOfDateTimeFunctions() throws Exception {
        String tableName = "TEST_IDX_DATETIME_FN";
        methodWatcher.executeUpdate(format("create table %s (d date, t time, ts timestamp)", tableName));
        methodWatcher.executeUpdate(
                format("insert into %s values " +
                       "('2014-01-28', '22:00:05', '2014-01-31 22:00:05')," +
                       "('2016-05-04', '12:04:30', '2016-05-01 12:04:30')"
                        , tableName));
        methodWatcher.executeUpdate(
                format("CREATE INDEX %s_IDX ON %s " +
                       "(add_months(d, 5), date(ts), day(d), extract(Week FROM d), hour(t), day(d), minute(t), " +
                       "month(d), month_between(d, '2018-01-01'), monthname(d), quarter(d), second(t), " +
                       "time(ts), timestamp(d, t), timestampadd(SQL_TSI_DAY, 2, ts), " +
                       "timestampdiff(SQL_TSI_DAY, timestamp(d, t), ts), trunc(d, 'month'), week(d), year(d))",
                        tableName, tableName));

        methodWatcher.executeUpdate(format("insert into %s values ('2017-09-10', '08:55:06', '2017-09-12 08:45:06')", tableName));
        methodWatcher.executeUpdate(format("update %s set d = '2016-05-05' where t = '12:04:30'", tableName));
        rowContainsQuery(new int[]{1,2,3},format("select d from %s --splice-properties index=%s_IDX\n order by d", tableName, tableName),methodWatcher,
                "2014-01-28","2016-05-05","2017-09-10");
    }

    @Test
    public void testCreateIndexAndInsertWithExpressionsOfConversionFunctions() throws Exception {
        String tableName = "TEST_IDX_CONVERSION_FN";
        methodWatcher.executeUpdate(format("create table %s (vc varchar(10), i int not null)", tableName));
        methodWatcher.executeUpdate(format("insert into %s values ('50', 10), ('100', 20)", tableName));
        methodWatcher.executeUpdate(
                format("CREATE INDEX %s_IDX ON %s " +
                       "(bigint(vc), cast(vc as integer), char(i), double(vc), integer(vc), smallint(vc), " +
                       "tinyint(vc), to_char(date(i),'yy'), to_date(date(i), 'yyyy-MM-dd'), varchar(vc || 'x'))",
                        tableName, tableName));

        methodWatcher.executeUpdate(format("insert into %s values ('120', 30)", tableName));
        methodWatcher.executeUpdate(format("update %s set vc = '80' where i = 10", tableName));
        rowContainsQuery(new int[]{1,2,3},format("select vc from %s --splice-properties index=%s_IDX\n order by vc", tableName, tableName),methodWatcher,
                "100","120","80");
    }

    @Test
    public void testCreateIndexAndInsertWithExpressionsOfMiscFunctions() throws Exception {
        String tableName = "TEST_IDX_MISC_FN";
        methodWatcher.executeUpdate(format("create table %s (vc varchar(10), i int)", tableName));
        methodWatcher.executeUpdate(format("insert into %s values ('abc', 10), ('def', 20)", tableName));
        methodWatcher.executeUpdate(
                format("CREATE INDEX %s_IDX ON %s " +
                                "(coalesce(vc, 'aa'), nvl(vc, 'bb'), nullif(vc, 'def'))",
                        tableName, tableName));

        methodWatcher.executeUpdate(format("insert into %s values ('ghi', 30)", tableName));
        methodWatcher.executeUpdate(format("update %s set vc = 'xyz' where i = 10", tableName));
        rowContainsQuery(new int[]{1,2,3},format("select vc from %s --splice-properties index=%s_IDX\n order by vc", tableName, tableName),methodWatcher,
                "def","ghi","xyz");
    }

    @Test
    public void testCreateIndexAndInsertWithExpressionsCaseExpression() throws Exception {
        String tableName = "TEST_IDX_CASE_EXPR";
        methodWatcher.executeUpdate(format("create table %s (i int)", tableName));
        methodWatcher.executeUpdate(format("insert into %s values (10), (11)", tableName));

        methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (case mod(i, 2) when 0 then i else 0 end)", tableName, tableName));

        methodWatcher.executeUpdate(format("insert into %s values (35)", tableName));
        methodWatcher.executeUpdate(format("update %s set i = 18 where i = 10", tableName));
        rowContainsQuery(new int[]{1,2,3},format("select i from %s --splice-properties index=%s_IDX\n order by i", tableName, tableName),methodWatcher,
                "11","18","35");
    }

    @Test
    public void testCreateIndexAndInsertWithExpressionsNullInput() throws Exception {
        String tableName = "TEST_IDX_NULL_INPUT";
        methodWatcher.executeUpdate(format("create table %s (c char(4), i int)", tableName));
        methodWatcher.executeUpdate(format("insert into %s values (NULL, 10), ('abc', NULL)", tableName));

        methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (upper(c), nullif(i, 0))", tableName, tableName));

        methodWatcher.executeUpdate(format("insert into %s values (NULL, NULL)", tableName));
        methodWatcher.executeUpdate(format("update %s set c = 'def' where i = 10", tableName));

        String query = format("select i from %s --splice-properties index=%s_IDX\n order by i nulls last", tableName, tableName);
        String expected = "I  |\n" +
                "------\n" +
                " 10  |\n" +
                "NULL |\n" +
                "NULL |";

        try (ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    // DB-10431
    @Test
    public void testCreateIndexAndInsertWithExpressionsBatchWithFullNullRow() throws Exception {
        String tableName = "TEST_IDX_BATCH_INSERT";
        methodWatcher.executeUpdate(format("create table %s (c char(4), i int, d double)", tableName));
        methodWatcher.executeUpdate(format("insert into %s values ('abc', 10, 1.1), ('def', 20, 2.2)", tableName));

        methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (upper(c), i + 2)", tableName, tableName));

        // Without the fix, the following insert fails sporadically. It depends on the order of inserting rows. If
        // (NULL, NULL, NULL) is inserted first, it fails. With the fix, it should never fail.
        methodWatcher.executeUpdate(format("insert into %s values ('jkl', 30, 2.2), (NULL, NULL, NULL)", tableName));
        methodWatcher.executeUpdate(format("update %s set c = 'def' where i = 10", tableName));

        String query = format("select c from %s order by c nulls last", tableName);
        String expected = "C  |\n" +
                "------\n" +
                " def |\n" +
                " def |\n" +
                " jkl |\n" +
                "NULL |";

        try (ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testCreateIndexAndInsertWithExpressionsOfNotAllowedBuiltInFunctions() throws Exception {
        String tableName = "TEST_IDX_NOT_ALLOWED_FN";
        methodWatcher.executeUpdate(format("create table %s (vc varchar(10), i int not null)", tableName));

        try {
            methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (i + rand(3))", tableName, tableName));
            Assert.fail("expect exception of invalid index expression");
        } catch (SQLException e) {
            Assert.assertEquals("429BX", e.getSQLState());
        }

        try {
            methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (i + random())", tableName, tableName));
            Assert.fail("expect exception of invalid index expression");
        } catch (SQLException e) {
            Assert.assertEquals("429BX", e.getSQLState());
        }

        try {
            methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (regexp_like(vc, 'o'))", tableName, tableName));
            Assert.fail("expect exception of invalid index expression");
        } catch (SQLException e) {
            Assert.assertEquals("429BX", e.getSQLState());
        }

        try {
            methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (instr(vc, 'o'))", tableName, tableName));
            Assert.fail("expect exception of invalid index expression");
        } catch (SQLException e) {
            Assert.assertEquals("429BX", e.getSQLState());
        }

        try {
            methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (locate('o', vc))", tableName, tableName));
            Assert.fail("expect exception of invalid index expression");
        } catch (SQLException e) {
            Assert.assertEquals("429BX", e.getSQLState());
        }

        try {
            methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (to_char(current_date, 'yy') || vc)", tableName, tableName));
            Assert.fail("expect exception of invalid index expression");
        } catch (SQLException e) {
            Assert.assertEquals("429BX", e.getSQLState());
        }

        try {
            methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (cast(current_time as varchar(32)) || vc)", tableName, tableName));
            Assert.fail("expect exception of invalid index expression");
        } catch (SQLException e) {
            Assert.assertEquals("429BX", e.getSQLState());
        }

        try {
            methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (to_char(date(current_timestamp), 'yy') || vc)", tableName, tableName));
            Assert.fail("expect exception of invalid index expression");
        } catch (SQLException e) {
            Assert.assertEquals("429BX", e.getSQLState());
        }

        try {
            methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (to_char(date(now()), 'yy') || vc)", tableName, tableName));
            Assert.fail("expect exception of invalid index expression");
        } catch (SQLException e) {
            Assert.assertEquals("429BX", e.getSQLState());
        }

        try {
            methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (vc || current_role)", tableName, tableName));
            Assert.fail("expect exception of invalid index expression");
        } catch (SQLException e) {
            Assert.assertEquals("429BX", e.getSQLState());
        }

        try {
            methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (vc || (current schema))", tableName, tableName));
            Assert.fail("expect exception of invalid index expression");
        } catch (SQLException e) {
            Assert.assertEquals("429BX", e.getSQLState());
        }

        try {
            methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (vc || current_user)", tableName, tableName));
        } catch (SQLException e) {
            Assert.assertEquals("429BX", e.getSQLState());
        }

        try {
            methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (vc || group_user)", tableName, tableName));
            Assert.fail("expect exception of invalid index expression");
        } catch (SQLException e) {
            Assert.assertEquals("429BX", e.getSQLState());
        }

        try {
            methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (vc || session_user)", tableName, tableName));
            Assert.fail("expect exception of invalid index expression");
        } catch (SQLException e) {
            Assert.assertEquals("429BX", e.getSQLState());
        }

        try {
            methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (vc || user)", tableName, tableName));
            Assert.fail("expect exception of invalid index expression");
        } catch (SQLException e) {
            Assert.assertEquals("429BX", e.getSQLState());
        }
    }

    @Test
    public void testCreateIndexAndInsertWithExpressionsOfAggregateFunctions() throws Exception {
        String tableName = "TEST_IDX_AGGR_FN";
        methodWatcher.executeUpdate(format("create table %s (vc varchar(10), i int)", tableName));

        try {
            methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (avg(i))", tableName, tableName));
            Assert.fail("expect exception of invalid index expression");
        } catch (SQLException e) {
            Assert.assertEquals("429BX", e.getSQLState());
        }

        try {
            methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (count(i))", tableName, tableName));
            Assert.fail("expect exception of invalid index expression");
        } catch (SQLException e) {
            Assert.assertEquals("429BX", e.getSQLState());
        }

        try {
            methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (count(*) + i)", tableName, tableName));
            Assert.fail("expect exception of invalid index expression");
        } catch (SQLException e) {
            Assert.assertEquals("429BX", e.getSQLState());
        }

        try {
            methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (max(i))", tableName, tableName));
            Assert.fail("expect exception of invalid index expression");
        } catch (SQLException e) {
            Assert.assertEquals("429BX", e.getSQLState());
        }

        try {
            methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (min(i))", tableName, tableName));
            Assert.fail("expect exception of invalid index expression");
        } catch (SQLException e) {
            Assert.assertEquals("429BX", e.getSQLState());
        }

        try {
            methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (stddev_pop(i))", tableName, tableName));
            Assert.fail("expect exception of invalid index expression");
        } catch (SQLException e) {
            Assert.assertEquals("429BX", e.getSQLState());
        }

        try {
            methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (stddev_samp(i))", tableName, tableName));
            Assert.fail("expect exception of invalid index expression");
        } catch (SQLException e) {
            Assert.assertEquals("429BX", e.getSQLState());
        }

        try {
            methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (sum(i))", tableName, tableName));
            Assert.fail("expect exception of invalid index expression");
        } catch (SQLException e) {
            Assert.assertEquals("429BX", e.getSQLState());
        }
    }

    @Test
    public void testCreateIndexAndInsertWithExpressionsOfWindowFunctions() throws Exception {
        String tableName = "TEST_IDX_WINDOW_FN";
        methodWatcher.executeUpdate(format("create table %s (vc varchar(10), i int)", tableName));

        try {
            methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (dense_rank() over (partition by i order by i) + i)", tableName, tableName));
            Assert.fail("expect exception of invalid index expression");
        } catch (SQLException e) {
            Assert.assertEquals("429BX", e.getSQLState());
        }

        try {
            methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (rank() over (partition by i order by i) + i)", tableName, tableName));
            Assert.fail("expect exception of invalid index expression");
        } catch (SQLException e) {
            Assert.assertEquals("429BX", e.getSQLState());
        }

        try {
            methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (row_number() over (partition by i order by i) + i)", tableName, tableName));
            Assert.fail("expect exception of invalid index expression");
        } catch (SQLException e) {
            Assert.assertEquals("429BX", e.getSQLState());
        }

        try {
            methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (first_value(vc) over (partition by i order by i))", tableName, tableName));
            Assert.fail("expect exception of invalid index expression");
        } catch (SQLException e) {
            Assert.assertEquals("429BX", e.getSQLState());
        }

        try {
            methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (last_value(vc) over (partition by i order by i))", tableName, tableName));
            Assert.fail("expect exception of invalid index expression");
        } catch (SQLException e) {
            Assert.assertEquals("429BX", e.getSQLState());
        }

        try {
            methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (lead(vc) over (partition by i order by i))", tableName, tableName));
            Assert.fail("expect exception of invalid index expression");
        } catch (SQLException e) {
            Assert.assertEquals("429BX", e.getSQLState());
        }

        try {
            methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (lag(vc) over (partition by i order by i))", tableName, tableName));
            Assert.fail("expect exception of invalid index expression");
        } catch (SQLException e) {
            Assert.assertEquals("429BX", e.getSQLState());
        }
    }

    @Test
    public void testCreateIndexAndInsertWithExpressionsSubquery() throws Exception {
        String tableName = "TEST_IDX_UDF";
        methodWatcher.executeUpdate(format("create table %s (i int)", tableName));

        try {
            methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (case mod(i, 2) when 0 then (select count(*) from %s) else 0 end)",
                    tableName, tableName, tableName));
            Assert.fail("expect exception of invalid index expression");
        } catch (SQLException e) {
            Assert.assertEquals("429BX", e.getSQLState());
        }
    }

    @Test
    public void testCreateIndexAndInsertWithExpressionsOnUDF() throws Exception {
        String tableName = "TEST_IDX_UDF";
        methodWatcher.executeUpdate(format("create table %s (d double)", tableName));
        methodWatcher.executeUpdate("CREATE FUNCTION try_some_UDF( RADIANS DOUBLE )\n" +
                "  RETURNS DOUBLE\n" +
                "  PARAMETER STYLE JAVA\n" +
                "  NO SQL\n" +
                "  LANGUAGE JAVA\n" +
                "  EXTERNAL NAME 'java.lang.Math.toDegrees'");

        try {
            methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (try_some_UDF(d))", tableName, tableName));
            Assert.fail("expect exception of invalid index expression");
        } catch (SQLException e) {
            Assert.assertEquals("429BX", e.getSQLState());
        }
    }

    @Test
    public void testCreateIndexAndInsertWithExpressionsNoColumnReference() throws Exception {
        String tableName = "TEST_IDX_NO_COLUMN_REFERENCE";
        methodWatcher.executeUpdate(format("create table %s (vc varchar(10), i int not null)", tableName));

        try {
            methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (lower(vc), 2 + 4)", tableName, tableName));
            Assert.fail("expect exception of invalid index expression");
        } catch (SQLException e) {
            Assert.assertEquals("429BX", e.getSQLState());
            Assert.assertTrue(e.getMessage().contains("2 + 4"));
        }
    }

    // ===============================================================================
    // Index expression tests - qualify and query
    // ===============================================================================

    @Test
    public void testFullScanViaExpressionBasedIndex() throws Exception {
        String tableName = "TEST_FULL_SCAN_VIA_EXPR_INDEX";
        methodWatcher.executeUpdate(format("create table %s (c char(4), i int)", tableName));
        methodWatcher.executeUpdate(format("insert into %s values ('foo', 0), ('bar', 1)", tableName));

        methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (UPPER(C))", tableName, tableName));
        methodWatcher.executeUpdate(format("insert into %s values ('abc', 2)", tableName));

        String query = format("select c from %s --splice-properties index=%s_IDX\n order by c", tableName, tableName);
        String expected = "C  |\n" +
                "-----\n" +
                "abc |\n" +
                "bar |\n" +
                "foo |";

        try (ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    @Test
    public void testScansStarStopKeyFromIndexExpressionPredicate() throws Exception {
        String tableName = "TEST_START_STOP_KEY_EXPR_INDEX";
        methodWatcher.executeUpdate(format("create table %s (c char(4), i int)", tableName));
        methodWatcher.executeUpdate(format("insert into %s values ('foo', 0), ('bar', 1)", tableName));

        methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (UPPER(C))", tableName, tableName));
        methodWatcher.executeUpdate(format("insert into %s values ('abc', 2)", tableName));

        // same start and stop keys
        String query = format("select c from %s --splice-properties index=%s_IDX\n where upper(c) = 'BAR'", tableName, tableName);
        String expected = "C  |\n" +
                "-----\n" +
                "bar |";

        try (ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        // start key
        query = format("select c from %s --splice-properties index=%s_IDX\n where upper(c) > 'BAR'", tableName, tableName);
        expected = "C  |\n" +
                "-----\n" +
                "foo |";

        try (ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        // stop key
        query = format("select c from %s --splice-properties index=%s_IDX\n where upper(c) < 'BAR'", tableName, tableName);
        expected = "C  |\n" +
                "-----\n" +
                "abc |";

        try (ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        // different start and stop keys
        query = format("select c from %s --splice-properties index=%s_IDX\n where upper(c) > 'ABA' and upper(c) < 'BAT'", tableName, tableName);
        expected = "C  |\n" +
                "-----\n" +
                "abc |\n" +
                "bar |";

        try (ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        // ISNULL, start key == stop key == NULL
        methodWatcher.executeUpdate(format("insert into %s values (NULL, 100)", tableName));
        query = format("select c from %s --splice-properties index=%s_IDX\n where upper(c) is null", tableName, tableName);
        expected = "C  |\n" +
                "------\n" +
                "NULL |";

        try (ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        // In-list probe
        query = format("select c from %s --splice-properties index=%s_IDX\n where upper(c) in ('ABA', 'BAR')", tableName, tableName);
        expected = "C  |\n" +
                "-----\n" +
                "bar |";

        try (ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    @Test
    public void testScansQualifierFromIndexExpressionPredicate() throws Exception {
        String tableName = "TEST_START_STOP_KEY_EXPR_INDEX";
        methodWatcher.executeUpdate(format("create table %s (c char(4), i int, d double)", tableName));
        methodWatcher.executeUpdate(format("insert into %s values ('foo', 10, 0.1), ('bar', 20, 0.3)", tableName));

        methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (UPPER(C), MOD(i, 3), d)", tableName, tableName));
        methodWatcher.executeUpdate(format("insert into %s values ('abc', 30, 0.5)", tableName));

        // NOT EQUAL
        String query = format("select c from %s --splice-properties index=%s_IDX\n where upper(c) != 'BAR'", tableName, tableName);
        String expected = "C  |\n" +
                "-----\n" +
                "abc |\n" +
                "foo |";

        try (ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        // IS NOT NULL
        methodWatcher.executeUpdate(format("insert into %s values (NULL, 100, 0.9)", tableName));
        query = format("select c from %s --splice-properties index=%s_IDX\n where upper(c) is not null", tableName, tableName);
        expected = "C  |\n" +
                "-----\n" +
                "abc |\n" +
                "bar |\n" +
                "foo |";

        try (ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        // In-list no probe
        query = format("select c from %s --splice-properties index=%s_IDX\n where upper(c) not in ('ABC', 'BAR')", tableName, tableName);
        expected = "C  |\n" +
                "-----\n" +
                "foo |";

        try (ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        // gap from the first index column
        query = format("select c from %s --splice-properties index=%s_IDX\n where mod(i,3) = 0", tableName, tableName);
        expected = "C  |\n" +
                "-----\n" +
                "abc |";

        try (ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    @Test
    public void testIndexExpressionPredicateNotQualifier() throws Exception {
        String tableName = "TEST_NOT_QUALIFIER_EXPR_INDEX";
        methodWatcher.executeUpdate(format("create table %s (c char(4), i int)", tableName));
        methodWatcher.executeUpdate(format("insert into %s values ('foo', 0), ('bar', 1)", tableName));

        methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (UPPER(C))", tableName, tableName));
        methodWatcher.executeUpdate(format("insert into %s values ('abc', 2)", tableName));

        String query = format("select c from %s --splice-properties index=%s_IDX\n where upper(c) between 'AAA' and 'BAT' order by c", tableName, tableName);
        String expected = "C  |\n" +
                "-----\n" +
                "abc |\n" +
                "bar |";

        try (ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    @Test
    public void testIndexExpressionTernaryOperatorRewriting() throws Exception {
        String tableName = "TEST_TERNARY_OP_EXPR";
        methodWatcher.executeUpdate(format("create table %s (vc varchar(32), ts timestamp)", tableName));
        methodWatcher.executeUpdate(
                format("insert into %s values " +
                                "('abc  ', '2014-01-01 22:00:05')," +
                                "('def  ', '2016-05-01 12:04:30')"
                        , tableName));
        methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (timestampadd(SQL_TSI_DAY, 2, ts), rtrim(vc))", tableName, tableName));

        rowContainsQuery(new int[]{1}, format("select rtrim(vc) from %s --splice-properties index=%s_IDX\n" +
                        " where timestampadd(SQL_TSI_DAY, 2, ts) = '2014-01-03 22:00:05' and rtrim(vc) like 'ab_%%'", tableName, tableName), methodWatcher,
                "abc");
    }

    @Test
    public void testIndexExpressionOnMultipleColumns() throws Exception {
        String tableName = "TEST_INDEX_EXPR_MULTIPLE_COLUMNS";
        methodWatcher.executeUpdate(format("create table %s (c char(4), i int, d double)", tableName));
        methodWatcher.executeUpdate(format("insert into %s values ('abc', 11, 1.1), ('def', 20, 2.2), ('jkl', 21, 2.2), ('ghi', 30, 3.3)", tableName));

        methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (D + I)", tableName, tableName));
        methodWatcher.executeUpdate(format("insert into %s values ('xyz', 40, 3.3)", tableName));

        String query = format("select c from %s --splice-properties index=%s_IDX\n where d + i > 30", tableName, tableName);
        String expected = "C  |\n" +
                "-----\n" +
                "ghi |\n" +
                "xyz |";

        try (ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    @Test
    public void testIndexExpressionOnDeterministicFunction() throws Exception {
        String tableName = "TEST_INDEX_EXPR_DETERMINISTIC_FN";
        methodWatcher.executeUpdate(format("create table %s (c char(4), i int, d double)", tableName));
        methodWatcher.executeUpdate(format("insert into %s values ('abc', 11, 1.1), ('def', 20, 2.2), ('jkl', 21, 2.2), ('ghi', 30, 3.3)", tableName));

        methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (ln(i), log10(i) + sqrt(d))", tableName, tableName));
        methodWatcher.executeUpdate(format("insert into %s values ('xyz', 40, 3.3)", tableName));

        // top node of an index expression is a method call
        String query = format("select c from %s --splice-properties index=%s_IDX\n where ln(i) > 3.2", tableName, tableName);

        String[] expectedOps = new String[] {
                "IndexLookup",
                "IndexScan",
                " > 3.2"            // should be on the same line as IndexScan
        };
        rowContainsQuery(new int[]{4, 5, 5}, "explain " + query, methodWatcher, expectedOps);

        String expected = "C  |\n" +
                "-----\n" +
                "ghi |\n" +
                "xyz |";

        try (ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        // an index expression containing method calls (also, note operands' order of plus)
        query = format("select c from %s --splice-properties index=%s_IDX\n where sqrt(d) + log10(i) > 3.2", tableName, tableName);

        rowContainsQuery(new int[]{4, 5, 5}, "explain " + query, methodWatcher, expectedOps);

        try (ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    @Test
    public void testIndexExpressionInListRightHandSide() throws Exception {
        methodWatcher.executeUpdate("create table TEST_IN_LIST_RHS(a1 int, a2 int)");
        methodWatcher.executeUpdate("insert into TEST_IN_LIST_RHS values(0,0),(1,10),(2,20),(3,30),(4,40),(5,50)");
        methodWatcher.executeUpdate("create index TEST_IN_LIST_RHS_IDX on TEST_IN_LIST_RHS(a1 * 3, a2)");

        methodWatcher.executeUpdate("create table TEST_IN_LIST_RHS_2(b1 int, b2 int)");
        methodWatcher.executeUpdate("insert into TEST_IN_LIST_RHS_2 values(0,0),(3,30),(5,50)");

        String query = "select b1, a2 from TEST_IN_LIST_RHS --splice-properties index=TEST_IN_LIST_RHS_IDX\n, " +
                "TEST_IN_LIST_RHS_2 where b1 in (1, a1 * 3)";

        String[] expectedOps = new String[]{
                "ProjectRestrict",
                "[(B1[2:1] IN (1,TEST_IN_LIST_RHS_IDX_col1[1:1]))]",
                "TableScan[TEST_IN_LIST_RHS_2",
                "IndexScan[TEST_IN_LIST_RHS_IDX"
        };
        rowContainsQuery(new int[]{5,5,6,7}, "explain " + query, methodWatcher, expectedOps);

        String expected = "B1 |A2 |\n" +
                "--------\n" +
                " 0 | 0 |\n" +
                " 3 |10 |";

        try (ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    @Test
    public void testIndexOnExpressionCountAsterisk() throws Exception {
        methodWatcher.executeUpdate("create table TEST_COUNT_STAR(a1 int, a2 int)");
        methodWatcher.executeUpdate("insert into TEST_COUNT_STAR values(0,0),(1,10),(2,20),(3,30),(4,40),(5,50)");
        methodWatcher.executeUpdate("create index TEST_COUNT_STAR_IDX on TEST_COUNT_STAR(mod(a1, 2))");

        String query = "select count(*) from TEST_COUNT_STAR --splice-properties index=TEST_COUNT_STAR_IDX";

        /* check plan */
        try(ResultSet rs = methodWatcher.executeQuery("explain " + query)) {
            String explainPlanText = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            Assert.assertTrue(explainPlanText.contains("IndexScan[TEST_COUNT_STAR_IDX"));
            Assert.assertFalse(explainPlanText.contains("IndexLookup"));
        }

        /* check result */
        String expected = "1 |\n" +
                "----\n" +
                " 6 |";

        try (ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    @Test
    public void testJoinOnTheSameIndexExpressionText() throws Exception {
        String tableName_1 = "TEST_SAME_EXPR_TEXT_EXPR_INDEX_1";
        methodWatcher.executeUpdate(format("create table %s (c char(4))", tableName_1));
        methodWatcher.executeUpdate(format("insert into %s values ('foo'), ('bar'), ('abb')", tableName_1));
        methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (UPPER(C))", tableName_1, tableName_1));

        String tableName_2 = "TEST_SAME_EXPR_TEXT_EXPR_INDEX_2";
        methodWatcher.executeUpdate(format("create table %s (c char(4))", tableName_2));
        methodWatcher.executeUpdate(format("insert into %s values ('foo'), ('not'), ('abb')", tableName_2));
        methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (UPPER(C))", tableName_2, tableName_2));

        // For both indexes, index expression is "upper(c)" and their AST would be the same. By setting their
        // table number correctly, we should not have a problem in matching index expressions in predicate.
        String query = format("select * from %s tbl1 --splice-properties index=%s_IDX\n " +
                " inner join %s tbl2 --splice-properties index=%s_IDX\n " +
                " on upper(tbl1.c) = upper(tbl2.c)",
                tableName_1, tableName_1, tableName_2, tableName_2);

        String expected = "C  | C  |\n" +
                "----------\n" +
                "abb |abb |\n" +
                "foo |foo |";

        try (ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    @Test
    public void testCoveringExpressionBasedIndex() throws Exception {
        String tableName = "TEST_COVERING_EXPR_INDEX";
        methodWatcher.executeUpdate(format("create table %s (c char(4), i int)", tableName));
        methodWatcher.executeUpdate(format("insert into %s values ('foo', 0), ('bar', 1)", tableName));

        methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (UPPER(C), mod(i, 2) + 1)", tableName, tableName));
        methodWatcher.executeUpdate(format("insert into %s values ('abc', 2)", tableName));

        ///////////////////////////////////////
        // test select list and where clause //
        ///////////////////////////////////////

        String query = format("select upper(c), mod(i,2)+1 from %s --splice-properties index=%s_IDX\n where upper(c) = 'BAR'", tableName, tableName);

        /* check plan */
        try(ResultSet rs = methodWatcher.executeQuery("explain " + query)) {
            String explainPlanText = TestUtils.FormattedResult.ResultFactory.toString(rs);
            Assert.assertTrue(explainPlanText.contains("IndexScan"));
            Assert.assertFalse(explainPlanText.contains("IndexLookup"));     // no base row retrieving
            Assert.assertFalse(explainPlanText.contains("ProjectRestrict")); // no final upper(c) computation
        }

        /* check result */
        String expected = "1  | 2 |\n" +
                "---------\n" +
                "BAR | 2 |";

        try (ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        ///////////////////////////////////////
        // test group by and having clause   //
        ///////////////////////////////////////

        query = format("select upper(c) from %s --splice-properties index=%s_IDX\n group by upper(c) having upper(c) > 'BAN'", tableName, tableName);

        /* check plan */
        try(ResultSet rs = methodWatcher.executeQuery("explain " + query)) {
            String explainPlanText = TestUtils.FormattedResult.ResultFactory.toString(rs);
            Assert.assertTrue(explainPlanText.contains("IndexScan"));
            Assert.assertFalse(explainPlanText.contains("IndexLookup"));  // no base row retrieving
            Assert.assertTrue(explainPlanText.contains("GroupBy"));
            Assert.assertFalse(explainPlanText.contains("upper"));        // ProjectRestrict is needed because of having, but no need to compute upper()
        }

        /* check result */
        expected = "1  |\n" +
                "-----\n" +
                "BAR |\n" +
                "FOO |";

        try (ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        ///////////////////////////////////////
        // test order by clause              //
        ///////////////////////////////////////

        query = format("select upper(c) from %s --splice-properties index=%s_IDX\n order by upper(c)", tableName, tableName);

        /* check plan */
        try(ResultSet rs = methodWatcher.executeQuery("explain " + query)) {
            String explainPlanText = TestUtils.FormattedResult.ResultFactory.toString(rs);
            Assert.assertTrue(explainPlanText.contains("IndexScan"));
            Assert.assertFalse(explainPlanText.contains("IndexLookup"));  // no base row retrieving
            Assert.assertTrue(explainPlanText.contains("OrderBy"));
            Assert.assertFalse(explainPlanText.contains("upper"));
        }

        /* check result */
        expected = "1  |\n" +
                "-----\n" +
                "ABC |\n" +
                "BAR |\n" +
                "FOO |";

        try (ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    @Test
    public void testCoveringExpressionBasedIndexWithAggregates() throws Exception {
        String tableName = "TEST_COVERING_EXPR_INDEX_AGGR";
        methodWatcher.executeUpdate(format("create table %s (c char(4), i int, d double)", tableName));
        methodWatcher.executeUpdate(format("insert into %s values ('abc', 11, 1.1), ('def', 20, 2.2), ('jkl', 21, 2.2), ('ghi', 30, 3.3), ('xyz', 40, 3.3)", tableName));

        methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (UPPER(C), mod(i, 2), d)", tableName, tableName));

        ///////////////////////////////////////
        // test aggregates with group by     //
        ///////////////////////////////////////

        String query = format("select d, sum(mod(i,2)) from %s --splice-properties index=%s_IDX\n group by d having sum(mod(i,2)) > 0", tableName, tableName);

        /* check plan */
        try (ResultSet rs = methodWatcher.executeQuery("explain " + query)) {
            String explainPlanText = TestUtils.FormattedResult.ResultFactory.toString(rs);
            Assert.assertTrue(explainPlanText.contains("IndexScan"));
            Assert.assertFalse(explainPlanText.contains("IndexLookup")); // no base row retrieving
            Assert.assertFalse(explainPlanText.contains("mod"));
        }

        /* check result */
        String expected = "D  | 2 |\n" +
                "---------\n" +
                "1.1 | 1 |\n" +
                "2.2 | 1 |";

        try (ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        ///////////////////////////////////////
        // test scalar aggregates            //
        ///////////////////////////////////////

        query = format("select max(d), sum(mod(i,2)) from %s --splice-properties index=%s_IDX\n", tableName, tableName);

        /* check plan */
        try (ResultSet rs = methodWatcher.executeQuery("explain " + query)) {
            String explainPlanText = TestUtils.FormattedResult.ResultFactory.toString(rs);
            Assert.assertTrue(explainPlanText.contains("IndexScan"));
            Assert.assertFalse(explainPlanText.contains("IndexLookup")); // no base row retrieving
            Assert.assertFalse(explainPlanText.contains("mod"));
        }

        /* check result */
        expected = "1  | 2 |\n" +
                "---------\n" +
                "3.3 | 2 |";

        try (ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    @Test
    public void testCoveringIndexOnExpressionsWindowFunction() throws Exception {
        String tableName = "TEST_COVERING_EXPR_INDEX_WINDOW";
        methodWatcher.executeUpdate(format("create table %s (c char(4), i int, d double)", tableName));
        methodWatcher.executeUpdate(format("insert into %s values ('abc', 1, 1.0), ('def', 1, 2.0), ('jkl', 1, 3.0), ('ghi', 2, 3.3), ('xyz', 2, 4.3)", tableName));

        methodWatcher.executeUpdate(format("CREATE INDEX %s_IDX ON %s (i + 1, d)", tableName, tableName));

        String query = format("select i + 1, rank() over (partition by i + 1 order by d) " +
                "from %s --splice-properties index=%s_IDX", tableName, tableName);

        /* check plan */
        try (ResultSet rs = methodWatcher.executeQuery("explain " + query)) {
            String explainPlanText = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            Assert.assertTrue(explainPlanText.contains("IndexScan"));
            Assert.assertFalse(explainPlanText.contains("IndexLookup")); // no base row retrieving
        }

        /* check result */
        String expected = "1 | 2 |\n" +
                "--------\n" +
                " 2 | 1 |\n" +
                " 2 | 2 |\n" +
                " 2 | 3 |\n" +
                " 3 | 1 |\n" +
                " 3 | 2 |";

        try (ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        query = format("select i + 1, sum(d) over (partition by i + 1), avg(d) over (partition by i + 1) " +
                "from %s --splice-properties index=%s_IDX", tableName, tableName);

        /* check plan */
        try (ResultSet rs = methodWatcher.executeQuery("explain " + query)) {
            String explainPlanText = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            Assert.assertTrue(explainPlanText.contains("IndexScan"));
            Assert.assertFalse(explainPlanText.contains("IndexLookup")); // no base row retrieving
        }

        /* check result */
        expected = "1 | 2  | 3  |\n" +
                "--------------\n" +
                " 2 |6.0 |2.0 |\n" +
                " 2 |6.0 |2.0 |\n" +
                " 2 |6.0 |2.0 |\n" +
                " 3 |7.6 |3.5 |\n" +
                " 3 |7.6 |3.5 |";

        try (ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    @Test
    public void testJoinOverTwoCompoundExpressionBasedIndexes() throws Exception {
        methodWatcher.executeUpdate("CREATE TABLE PERSON_ADDRESS_1 (PID INTEGER, ADDR_ID INTEGER)");
        methodWatcher.executeUpdate("CREATE TABLE ADDRESS_1 (ADDR_ID INTEGER, STD_STATE_PROVENCE VARCHAR(30))");
        methodWatcher.executeUpdate("CREATE INDEX pa_idx_1 ON PERSON_ADDRESS_1 (pid, max(addr_id, 300))");
        methodWatcher.executeUpdate("CREATE INDEX a_idx_1 ON ADDRESS_1 (lower(std_state_provence), abs(addr_id))");
        methodWatcher.executeUpdate("INSERT INTO PERSON_ADDRESS_1 VALUES (10, 100),(20, 200),(30,300),(40, 400),(50,500)");
        methodWatcher.executeUpdate("INSERT INTO ADDRESS_1 VALUES (100, 'MO'),(200, 'IA'),(300,'NY'),(400,'FL'),(500,'AL')");

        ///////////////////
        // both covering //
        ///////////////////

        String query = "select pa.pid, lower(a.std_state_provence)\n"+
                " from PERSON_ADDRESS_1 pa --SPLICE-PROPERTIES index=pa_idx_1\n"+
                " join ADDRESS_1 a         --SPLICE-PROPERTIES index=a_idx_1 %s\n"+
                "  on max(pa.addr_id, 300) = abs(a.addr_id)\n"+
                " where lower(a.std_state_provence) in ('ia', 'fl', 'al')";

        /* check plan */
        try (ResultSet rs = methodWatcher.executeQuery("explain " + format(query, ""))) {
            String explainPlanText = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            Assert.assertTrue(explainPlanText.contains("MultiProbeIndexScan[A_IDX_1")); // in-list for ADDRESS_1
            Assert.assertTrue(explainPlanText.contains("IndexScan[PA_IDX_1"));
            Assert.assertFalse(explainPlanText.contains("IndexLookup")); // no base row retrieving
        }

        /* check result */
        String expected = "PID | 2 |\n" +
                "----------\n" +
                " 40  |fl |\n" +
                " 50  |al |";

        testJoinStrategy(query, "", expected);  // optimizer's choice
        testJoinStrategy(query, "nestedloop", expected);
        testJoinStrategy(query, "broadcast", expected);
        testJoinStrategy(query, "cross, useSpark=true", expected);  // on spark
        // merge and sortmerge are not feasible

        ///////////////////////////////
        // only pa_idx_1 is covering //
        ///////////////////////////////

        query = "select pa.pid, a.std_state_provence\n"+
                " from PERSON_ADDRESS_1 pa --SPLICE-PROPERTIES index=pa_idx_1\n"+
                " join ADDRESS_1 a         --SPLICE-PROPERTIES index=a_idx_1 %s\n"+
                "  on max(pa.addr_id, 300) = abs(a.addr_id)\n"+
                " where lower(a.std_state_provence) in ('ia', 'fl', 'al')";

        /* check plan */
        String[] expectedOps = new String[] {
                "IndexScan[PA_IDX_1",
                "IndexLookup",                 // base row retrieving for ADDRESS_1
                "MultiProbeIndexScan[A_IDX_1", // in-list for ADDRESS_1
        };
        rowContainsQuery(new int[]{6,8,9}, "explain " + format(query, ""), methodWatcher, expectedOps);

        /* check result */
        expected = "PID |STD_STATE_PROVENCE |\n" +
                "--------------------------\n" +
                " 40  |        FL         |\n" +
                " 50  |        AL         |";
        testJoinStrategy(query, "", expected);  // optimizer's choice

        ///////////////////////////////
        // only a_idx_1 is covering  //
        ///////////////////////////////

        query = "select pa.pid + 1, abs(a.addr_id)\n"+
                " from PERSON_ADDRESS_1 pa --SPLICE-PROPERTIES index=pa_idx_1\n"+
                " join ADDRESS_1 a         --SPLICE-PROPERTIES index=a_idx_1 %s\n"+
                "  on max(pa.addr_id, 300) = abs(a.addr_id)\n"+
                " where lower(a.std_state_provence) in ('ia', 'fl', 'al')";

        /* check plan */
        expectedOps = new String[] {
                "IndexLookup",                 // base row retrieving for ADDRESS_1
                "IndexScan[PA_IDX_1",
                "MultiProbeIndexScan[A_IDX_1", // in-list for ADDRESS_1
        };
        rowContainsQuery(new int[]{5,6,7}, "explain " + format(query, ""), methodWatcher, expectedOps);

        /* check result */
        expected = "1 | 2  |\n" +
                "---------\n" +
                "41 |400 |\n" +
                "51 |500 |";
        testJoinStrategy(query, "", expected);  // optimizer's choice

        //////////////////////////
        // neither is covering  //
        //////////////////////////

        query = "select *\n"+
                " from PERSON_ADDRESS_1 pa --SPLICE-PROPERTIES index=pa_idx_1\n"+
                " join ADDRESS_1 a         --SPLICE-PROPERTIES index=a_idx_1 %s\n"+
                "  on max(pa.addr_id, 300) = abs(a.addr_id)\n"+
                " where lower(a.std_state_provence) in ('ia', 'fl', 'al')";

        /* check plan */
        expectedOps = new String[] {
                "IndexLookup",                 // base row retrieving for ADDRESS_1
                "IndexScan[PA_IDX_1",
                "IndexLookup",                 // base row retrieving for PERSON_ADDRESS_1
                "MultiProbeIndexScan[A_IDX_1", // in-list for ADDRESS_1
        };
        rowContainsQuery(new int[]{5,6,7,8}, "explain " + format(query, ""), methodWatcher, expectedOps);

        /* check result */
        expected = "PID | ADDR_ID | ADDR_ID |STD_STATE_PROVENCE |\n" +
                "----------------------------------------------\n" +
                " 40  |   400   |   400   |        FL         |\n" +
                " 50  |   500   |   500   |        AL         |";
        testJoinStrategy(query, "", expected);  // optimizer's choice
    }

    private void testJoinStrategy(String queryFormat, String joinStrategy, String expected) throws Exception {
        String query = format(queryFormat, joinStrategy.isEmpty() ? "" : ", joinStrategy=" + joinStrategy);

        try(ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    @Test
    public void testCoveringIndexOnExpressionsOuterJoin() throws Exception {
        methodWatcher.executeUpdate("create table TEST_INDEX_EXPR_FOJ_1 (a1 int, b1 int, c1 int, d1 int)");
        methodWatcher.executeUpdate("create index TEST_INDEX_EXPR_FOJ_1_IDX on TEST_INDEX_EXPR_FOJ_1(a1+1, b1+1)");
        methodWatcher.executeUpdate("create table TEST_INDEX_EXPR_FOJ_2 (a2 int, b2 int, c2 int)");

        methodWatcher.executeUpdate("insert into TEST_INDEX_EXPR_FOJ_1 values (1,2,2,2), (2,3,3,3), (3,4,4,4)");
        methodWatcher.executeUpdate("insert into TEST_INDEX_EXPR_FOJ_2 values (1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5)");

        // full outer join, two tables
        String query = "select a1+1 as X, b2 from TEST_INDEX_EXPR_FOJ_1 --splice-properties index=TEST_INDEX_EXPR_FOJ_1_IDX\n " +
                "full join TEST_INDEX_EXPR_FOJ_2 on b1+1=b2";

        /* check plan */
        try (ResultSet rs = methodWatcher.executeQuery("explain " + query)) {
            String explainPlanText = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            Assert.assertTrue(explainPlanText.contains("IndexScan[TEST_INDEX_EXPR_FOJ_1_IDX"));
            Assert.assertFalse(explainPlanText.contains("IndexLookup")); // no base row retrieving
        }

        /* check result */
        String expected = "X  |B2 |\n" +
                "----------\n" +
                "  2  | 3 |\n" +
                "  3  | 4 |\n" +
                "  4  | 5 |\n" +
                "NULL | 1 |\n" +
                "NULL | 2 |";
        try(ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        // full outer join, three tables
        query = "select t11.b1+1 as X, t12.a1+1 as Y, b2 from " +
                "TEST_INDEX_EXPR_FOJ_1 t11 --splice-properties index=TEST_INDEX_EXPR_FOJ_1_IDX\n " +
                "full join TEST_INDEX_EXPR_FOJ_2 on t11.b1+1=b2 " +
                "full join TEST_INDEX_EXPR_FOJ_1 t12 --splice-properties index=TEST_INDEX_EXPR_FOJ_1_IDX\n " +
                " on t12.a1+1=b2";

        /* check plan */
        try (ResultSet rs = methodWatcher.executeQuery("explain " + query)) {
            String explainPlanText = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            Assert.assertTrue(explainPlanText.contains("IndexScan[TEST_INDEX_EXPR_FOJ_1_IDX"));
            Assert.assertFalse(explainPlanText.contains("IndexLookup")); // no base row retrieving
        }

        /* check result */
        expected = "X  |  Y  |B2 |\n" +
                "----------------\n" +
                "  3  |  3  | 3 |\n" +
                "  4  |  4  | 4 |\n" +
                "  5  |NULL | 5 |\n" +
                "NULL |  2  | 2 |\n" +
                "NULL |NULL | 1 |";
        try(ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    @Test
    public void testCoveringIndexOnExpressionsDerivedTable() throws Exception {
        methodWatcher.executeUpdate("create table TEST_DERIVED_TABLE(a1 int, a2 int)");
        methodWatcher.executeUpdate("insert into TEST_DERIVED_TABLE values(0,0),(1,10),(2,20),(3,30),(4,40),(5,50)");
        methodWatcher.executeUpdate("create index TEST_DERIVED_TABLE_IDX on TEST_DERIVED_TABLE(a1 * 3, a2)");

        methodWatcher.executeUpdate("create table TEST_DERIVED_TABLE_2(b1 int, b2 int)");
        methodWatcher.executeUpdate("insert into TEST_DERIVED_TABLE_2 values(1,10),(3,30),(5,50)");

        // index is not covering for derived table, but query should be flattened to
        // "select a1*3 from TEST_DERIVED_TABLE where a1*3 < 8;"
        String query = "select a1*3 from " +
                "(select a1 from TEST_DERIVED_TABLE --splice-properties index=TEST_DERIVED_TABLE_IDX\n) dt" +
                " where a1 * 3 < 8";

        /* check plan */
        try (ResultSet rs = methodWatcher.executeQuery("explain " + query)) {
            String explainPlanText = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            Assert.assertTrue(explainPlanText.contains("IndexScan[TEST_DERIVED_TABLE_IDX"));
            Assert.assertFalse(explainPlanText.contains("IndexLookup")); // no base row retrieving
        }

        /* check result */
        String expected = "1 |\n" +
                "----\n" +
                " 0 |\n" +
                " 3 |\n" +
                " 6 |";
        testJoinStrategy(query, "", expected);  // optimizer's choice

        // index is covering for derived table, outer level references aliases, y + 1 can use
        // a1 * 3 column from index directly, so still no IndexLookup
        // Note that this derived table cannot be flattened because not all select items are
        // cloneable (an expression other than plain column reference is mostly not cloneable).
        query = "select y + 1 from " +
                "(select a2 as x, a1 * 3 as y from TEST_DERIVED_TABLE --splice-properties index=TEST_DERIVED_TABLE_IDX\n) dt" +
                " where x < 25";

        /* check plan */
        try (ResultSet rs = methodWatcher.executeQuery("explain " + query)) {
            String explainPlanText = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            Assert.assertTrue(explainPlanText.contains("IndexScan[TEST_DERIVED_TABLE_IDX"));
            Assert.assertFalse(explainPlanText.contains("IndexLookup")); // no base row retrieving
        }

        /* check result */
        expected = "1 |\n" +
                "----\n" +
                " 1 |\n" +
                " 4 |\n" +
                " 7 |";
        testJoinStrategy(query, "", expected);  // optimizer's choice

        // join inside derived table, then join
        // derived table is not flattenable because fromList.size() > 1
        query = "select x, t2.b2 from (select 3*a1 as x, b2 from TEST_DERIVED_TABLE --splice-properties index=TEST_DERIVED_TABLE_IDX\n" +
                ", TEST_DERIVED_TABLE_2 where a1*3 = b1) dt, TEST_DERIVED_TABLE_2 t2\n" +
                "where X=3";

        /* check plan */
        try (ResultSet rs = methodWatcher.executeQuery("explain " + query)) {
            String explainPlanText = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            Assert.assertTrue(explainPlanText.contains("IndexScan[TEST_DERIVED_TABLE_IDX"));
            Assert.assertTrue(explainPlanText.contains("TableScan[TEST_DERIVED_TABLE_2"));
            Assert.assertFalse(explainPlanText.contains("IndexLookup")); // no base row retrieving
        }

        /* check result */
        expected = "X |B2 |\n" +
                "--------\n" +
                " 3 |10 |\n" +
                " 3 |30 |\n" +
                " 3 |50 |";
        testJoinStrategy(query, "", expected);  // optimizer's choice

        // outer join inside derived table, then outer join
        // derived table is not flattenable because fromList.size() > 1
        query = "select dt.x, dt.b2, t1.a2 from " +
                "  (select 3*a1 as x, b2 from TEST_DERIVED_TABLE --splice-properties index=TEST_DERIVED_TABLE_IDX\n " +
                "                             right join TEST_DERIVED_TABLE_2 on a1*3 = b1) as dt " +
                "  full join TEST_DERIVED_TABLE t1 --splice-properties index=TEST_DERIVED_TABLE_IDX\n on dt.b2 = t1.a2 " +
                "where dt.x is null";

        /* check plan */
        try (ResultSet rs = methodWatcher.executeQuery("explain " + query)) {
            String explainPlanText = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            Assert.assertTrue(explainPlanText.contains("IndexScan[TEST_DERIVED_TABLE_IDX"));
            Assert.assertTrue(explainPlanText.contains("TableScan[TEST_DERIVED_TABLE_2"));
            Assert.assertFalse(explainPlanText.contains("IndexLookup")); // no base row retrieving
        }

        /* check result */
        expected = "X  | B2  |A2 |\n" +
                "----------------\n" +
                "NULL | 10  |10 |\n" +
                "NULL | 50  |50 |\n" +
                "NULL |NULL | 0 |\n" +
                "NULL |NULL |20 |\n" +
                "NULL |NULL |40 |";
        testJoinStrategy(query, "", expected);  // optimizer's choice

        // corner case: no inner base table column is referenced, not flattenable because of aggregation
        query = "select x, t2.b1 from (select count(*) as x from TEST_DERIVED_TABLE --splice-properties index=TEST_DERIVED_TABLE_IDX\n) t1, " +
                "TEST_DERIVED_TABLE_2 t2";

        /* check plan */
        try (ResultSet rs = methodWatcher.executeQuery("explain " + query)) {
            String explainPlanText = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            Assert.assertTrue(explainPlanText.contains("IndexScan[TEST_DERIVED_TABLE_IDX"));
            Assert.assertTrue(explainPlanText.contains("TableScan[TEST_DERIVED_TABLE_2"));
            Assert.assertFalse(explainPlanText.contains("IndexLookup")); // no base row retrieving
        }

        /* check result */
        expected = "X |B1 |\n" +
                "--------\n" +
                " 6 | 1 |\n" +
                " 6 | 3 |\n" +
                " 6 | 5 |";
        testJoinStrategy(query, "", expected);  // optimizer's choice
    }

    @Test
    public void testCoveringIndexOnExpressionsUnionAll() throws Exception {
        methodWatcher.executeUpdate("create table TEST_INDEX_EXPR_UNION_1 (a1 int, b1 int, c1 int, d1 int)");
        methodWatcher.executeUpdate("create index TEST_INDEX_EXPR_UNION_1_IDX on TEST_INDEX_EXPR_UNION_1(a1+1, b1+1)");
        methodWatcher.executeUpdate("create table TEST_INDEX_EXPR_UNION_2 (a2 int, b2 int, c2 int)");

        methodWatcher.executeUpdate("insert into TEST_INDEX_EXPR_UNION_1 values (1,2,2,2), (2,3,3,3), (3,4,4,4)");
        methodWatcher.executeUpdate("insert into TEST_INDEX_EXPR_UNION_2 values (1,1,1), (2,2,2), (3,3,3), (4,4,4), (5,5,5)");

        String query = "select b1+1 from TEST_INDEX_EXPR_UNION_1 --splice-properties index=TEST_INDEX_EXPR_UNION_1_IDX\n where a1 + 1 = 2 " +
                "union all " +
                "select a2 from TEST_INDEX_EXPR_UNION_2 where b2 = 4";

        /* check plan */
        try (ResultSet rs = methodWatcher.executeQuery("explain " + query)) {
            String explainPlanText = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            Assert.assertTrue(explainPlanText.contains("IndexScan[TEST_INDEX_EXPR_UNION_1_IDX"));
            Assert.assertFalse(explainPlanText.contains("IndexLookup")); // no base row retrieving
        }

        /* check result */
        String expected = "1 |\n" +
                "----\n" +
                " 3 |\n" +
                " 4 |";
        try (ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    @Test
    public void testCoveringExpressionBasedIndexSubquery() throws Exception {
        methodWatcher.executeUpdate("create table TEST_SUBQ_A(a1 int, a2 int)");
        methodWatcher.executeUpdate("create table TEST_SUBQ_B(b1 int, b2 int)");
        methodWatcher.executeUpdate("insert into TEST_SUBQ_A values(0,0),(1,10),(2,20),(3,30),(4,40),(5,50)");
        methodWatcher.executeUpdate("insert into TEST_SUBQ_B values(0,0),(0,0),(1,10),(1,10),(2,20),(2,20),(3,30),(3,30),(4,40),(4,40),(5,50),(5,50)");
        methodWatcher.executeUpdate("create index TEST_SUBQ_A_IDX on TEST_SUBQ_A(a1 * 3, a2)");
        methodWatcher.executeUpdate("create index TEST_SUBQ_B_IDX on TEST_SUBQ_B(b1 * 3, ln(b2+1))");

        ///////////////////////////////////////
        // subquery can be flattened to join //
        ///////////////////////////////////////

        // uncorrelated
        String query = "select a1 * 3, a2 from " +
                " TEST_SUBQ_A --SPLICE-PROPERTIES index=TEST_SUBQ_A_IDX\n" +
                " where a1 * 3 in " +
                "  (select b1 * 3 from TEST_SUBQ_B --SPLICE-PROPERTIES index=TEST_SUBQ_B_IDX\n" +
                "   where ln(b2 + 1) > 3.2)";

        // Don't really care about result order, let it be how ResultFactory.toString() sorts.
        String expected = "1 |A2 |\n" +
                "--------\n" +
                "12 |40 |\n" +
                "15 |50 |\n" +
                " 9 |30 |";

        try(ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        // correlated
        query = "select a2 from TEST_SUBQ_A --SPLICE-PROPERTIES index=TEST_SUBQ_A_IDX\n" +
                " where a2 > (select sum(b1*3) from TEST_SUBQ_B --SPLICE-PROPERTIES index=TEST_SUBQ_B_IDX\n" +
                "              where ln(b2+1) < a1 * 3)";

        expected = "A2 |\n" +
                "----\n" +
                "10 |";

        try(ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        // SSQ
        query = "select b1 * 3, (select distinct a2 from TEST_SUBQ_A --SPLICE-PROPERTIES index=TEST_SUBQ_A_IDX\n" +
                " where a1*3 > ln(b2+1) and a1 * 3 < 6) from TEST_SUBQ_B --SPLICE-PROPERTIES index=TEST_SUBQ_B_IDX\n" +
                " where b1 * 3 between 3 and 6";

        expected = "1 |  2  |\n" +
                "----------\n" +
                " 3 | 10  |\n" +
                " 3 | 10  |\n" +
                " 6 |NULL |\n" +
                " 6 |NULL |";

        try(ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        //////////////////////////////////
        // subquery cannot be flattened //
        //////////////////////////////////

        query = "select a1 * 3, a2 from " +
                " TEST_SUBQ_A --SPLICE-PROPERTIES index=TEST_SUBQ_A_IDX\n" +
                " where a1 * 3 between 0 and 3 or a1 * 3 in " +
                "  (select b1 * 3 from TEST_SUBQ_B --SPLICE-PROPERTIES index=TEST_SUBQ_B_IDX\n" +
                "   where ln(b2 + 1) > 3.2)";

        // Don't really care about result order, let it be how ResultFactory.toString() sorts.
        expected = "1 |A2 |\n" +
                "--------\n" +
                " 0 | 0 |\n" +
                "12 |40 |\n" +
                "15 |50 |\n" +
                " 3 |10 |\n" +
                " 9 |30 |";

        try(ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }
}
