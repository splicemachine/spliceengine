/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
import com.splicemachine.test.SerialTest;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.spark_project.guava.base.Joiner;

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
@Category(value = {SerialTest.class})
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
                "join ADDRESS a         --SPLICE-PROPERTIES index=a_idx\n"+
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
        String sql1="explain SELECT count(a4.PID) FROM --splice-properties joinOrder=FIXED \n"+
                " PERSON_ADDRESS a4 --splice-properties index=B_IDX1 \n"+
                " INNER JOIN ADDRESS a5 --splice-properties joinStrategy=SORTMERGE,index=ADDRESS_IX \n"+
                " ON a4.ADDR_ID = a5.ADDR_ID \n"+
                " WHERE  a5.STD_STATE_PROVENCE IN ('IA', 'WI', 'IL')";

        List<String> arr1=methodWatcher.queryList(sql1);

        // should have lower cost
        String sql2="explain SELECT count(a4.PID) FROM --splice-properties joinOrder=FIXED \n"+
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
        methodWatcher.executeUpdate("CREATE INDEX doubleIndex2 ON double_INDEXES1(column1 ASC)");
        methodWatcher.executeUpdate("update double_INDEXES1 set column1 = -1.79769E+308 where column1 = -1.79769E+308");
        rowContainsQuery(new int[]{1,2,3},"select column1 from double_INDEXES1 --splice-properties index=doubleIndex2\n order by column1",methodWatcher,
                "-1.79769E308","0","1.79769E308");

    }

}
