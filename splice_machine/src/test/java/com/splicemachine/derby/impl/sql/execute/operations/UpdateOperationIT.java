/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.junit.Assert;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import java.math.BigDecimal;
import java.sql.*;
import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

public class UpdateOperationIT {

    private static final String SCHEMA = UpdateOperationIT.class.getSimpleName().toUpperCase();

    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);
    private static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);

    @Rule
    public final SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createSharedTables() throws Exception {

        Connection connection = spliceClassWatcher.getOrCreateConnection();

        new TableCreator(connection)
                .withCreate("create table LOCATION (num int, addr varchar(50), zip char(5))")
                .withInsert("insert into LOCATION values(?,?,?)")
                .withRows(rows(
                                row(100, "100", "94114"),
                                row(200, "200", "94509"),
                                row(300, "300", "34166"))
                )
                .create();

        new TableCreator(connection)
                .withCreate("create table NULL_TABLE (addr varchar(50), zip char(5))")
                .create();

        new TableCreator(connection)
                .withCreate("create table SHIPMENT (cust_id int)")
                .withInsert("insert into SHIPMENT values(?)")
                .withRows(rows(row(102),row(104)))
                .create();

        new TableCreator(connection)
                .withCreate("create table CUSTOMER (cust_id int, status boolean, level int)")
                .withInsert("insert into CUSTOMER values(?,?,?)")
                .withRows(rows(row(101, true, 1), row(102, true, 2), row(103, true, 3), row(104, true, 4), row(105, true, 5)))
                .create();

        new TableCreator(connection)
                .withCreate("create table CUSTOMER_TEMP (cust_id int, status boolean, level int)")
                .withInsert("insert into CUSTOMER_TEMP values(?,?,?)")
                .withRows(rows(row(101, true, 1), row(102, true, 2), row(103, true, 3), row(104, true, 4), row(105, true, 5)))
                .create();

        new TableCreator(connection)
                .withCreate("create table NULL_TABLE2 (col1 varchar(50), col2 char(5), col3 int)")
                .create();

        new TableCreator(connection)
                .withCreate("create table BOOL_TABLE(c boolean, i int)")
                .withIndex("create index idx_bool on BOOL_TABLE(c desc)")
                .withInsert("insert into bool_table values(?,?)")
                .withRows(rows(row(null, 0), row(true, 1), row(false, 2)))
                .create();

        try(CallableStatement cs = connection.prepareCall("call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS(?,false)")){
            cs.setString(1,SCHEMA);
            cs.execute();
        }
    }

    /*regression test for DB-2204*/
    @Test
    public void testUpdateDoubleIndexFieldWorks() throws Exception {

        TestConnection conn = methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);

        try(Statement s = conn.createStatement()){
            s.executeUpdate("create table dc (dc decimal(10,2))");
            s.executeUpdate("insert into dc values (10)");
        }
        conn.commit();

        try(Statement s = conn.createStatement()){
            s.execute("create index didx on dc (dc)");
            s.execute("create unique index duniq on dc (dc)");

            int updated=s.executeUpdate("update dc set dc = dc+1.1");
            //make base table and index have expected number of rows
            assertEquals("Incorrect number of reported updates!",1,updated);

            assertEquals(1L,conn.count(s,"select * from dc"));
            assertEquals(1L,conn.count(s,"select * from dc --SPLICE-PROPERTIES index=didx"));
            assertEquals(1L,conn.count(s,"select * from dc --SPLICE-PROPERTIES index=duniq"));

            //make sure that the update worked
            BigDecimal expected=new BigDecimal("11.10");
            assertEquals("Incorrect value returned!",expected,methodWatcher.query("select dc from dc"));
            assertEquals("Incorrect value returned!",expected,methodWatcher.query("select dc from dc --SPLICE-PROPERTIES index=didx"));
            assertEquals("Incorrect value returned!",expected,methodWatcher.query("select dc from dc --SPLICE-PROPERTIES index=duniq"));
        }
    }

    /*regression test for Bug 889*/
    @Test
    public void testUpdateSetNullLong() throws Exception {

        new TableCreator(methodWatcher.getOrCreateConnection())
                .withCreate("create table c (k int, l int)")
                .withInsert("insert into c (k,l) values (?,?)")
                .withRows(rows(row(1, 2), row(2, 4)))
                .create();

        int updated = methodWatcher.executeUpdate("update c set k = NULL where l = 2");

        assertEquals("incorrect num rows updated!", 1L, updated);
        assertEquals("incorrect num rows present!", 2L, (long)methodWatcher.query("select count(*) from c"));

        ResultSet rs = methodWatcher.executeQuery("select * from c");
        assertEquals("" +
                        "K  | L |\n" +
                        "----------\n" +
                        "  2  | 4 |\n" +
                        "NULL | 2 |",
                TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testUpdate() throws Exception {
        try(Statement s=methodWatcher.getStatement()){
            int updated=s.executeUpdate("update LOCATION set addr='240' where num=100");
            assertEquals("Incorrect num rows updated!",1,updated);
            try(ResultSet rs=s.executeQuery("select * from LOCATION where num = 100")){
                assertEquals(""+
                                "NUM |ADDR | ZIP  |\n"+
                                "-------------------\n"+
                                " 100 | 240 |94114 |",
                        TestUtils.FormattedResult.ResultFactory.toString(rs));
            }
        }
    }

    @Test
    public void testUpdateMultipleColumns() throws Exception {
        int updated = methodWatcher.getStatement().executeUpdate("update LOCATION set addr='900',zip='63367' where num=300");
        assertEquals("incorrect number of records updated!", 1, updated);
        ResultSet rs = methodWatcher.executeQuery("select * from LOCATION where num=300");
        assertEquals("" +
                        "NUM |ADDR | ZIP  |\n" +
                        "-------------------\n" +
                        " 300 | 900 |63367 |",
                TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testWithoutWhere() throws Exception {
        methodWatcher.prepareStatement("insert into NULL_TABLE values (null,null)").execute();

        PreparedStatement preparedStatement = methodWatcher.prepareStatement("update NULL_TABLE set addr = ?, zip = ?");
        preparedStatement.setString(1, "2269 Concordia Drive");
        preparedStatement.setString(2, "65203");
        int updated = preparedStatement.executeUpdate();
        assertEquals("Incorrect number of records updated", 1, updated);

        ResultSet rs = methodWatcher.executeQuery("select addr,zip from NULL_TABLE");
        assertEquals("" +
                        "ADDR         | ZIP  |\n" +
                        "-----------------------------\n" +
                        "2269 Concordia Drive |65203 |",
                TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    /* Regression test for Bug #286 */
    @Test
    public void testUpdateFromValues() throws Exception {
        int updated = methodWatcher.getStatement().executeUpdate("update LOCATION set addr=(values '5') where num=100");
        assertEquals("Incorrect num rows updated!", 1, updated);
        ResultSet rs = methodWatcher.executeQuery("select * from LOCATION where num = 100");
        assertEquals("" +
                        "NUM |ADDR | ZIP  |\n" +
                        "-------------------\n" +
                        " 100 |  5  |94114 |",
                TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    /* regression test for Bug 289 */
    @Test
    public void testUpdateFromSubquery() throws Exception {
        new TableCreator(methodWatcher.getOrCreateConnection())
                .withCreate("create table b (num int, addr varchar(50), zip char(5))")
                .withInsert("insert into b values(?,?,?)")
                .withRows(rows(row(25, "100", "94114")))
                .create();

        int updated = methodWatcher.getStatement().executeUpdate("update b set num=(select LOCATION.num from LOCATION where LOCATION.num = 100)");
        assertEquals("Incorrect num rows updated!", 1, updated);
        ResultSet rs = methodWatcher.executeQuery("select * from b where num = 100");
        assertEquals("" +
                        "NUM |ADDR | ZIP  |\n" +
                        "-------------------\n" +
                        " 100 | 100 |94114 |",
                TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    /* Regression test for Bug 682. */
    @Test
    public void testUpdateSetNullValues() throws Exception {
        PreparedStatement ps = methodWatcher.prepareStatement("insert into NULL_TABLE values (?,?)");
        ps.setString(1, "900 Green Meadows Road");
        ps.setString(2, "65201");
        ps.execute();

        //get initial count
        Long originalCount = methodWatcher.query("select count(*) from NULL_TABLE where zip = '65201'");

        //update to set a null entry
        int numChanged = methodWatcher.executeUpdate("update NULL_TABLE set zip = null where zip = '65201'");
        assertEquals("Incorrect rows changed", 1, numChanged);

        ResultSet rs = methodWatcher.executeQuery("select * from NULL_TABLE where zip is null");
        assertEquals("" +
                        "ADDR          | ZIP |\n" +
                        "------------------------------\n" +
                        "900 Green Meadows Road |NULL |",
                TestUtils.FormattedResult.ResultFactory.toString(rs));

        //make sure old value isn't there anymore
        Long finalCount = methodWatcher.query("select count(*) from NULL_TABLE where zip = '65201'");
        assertEquals("Row was not removed from original set", originalCount.longValue() - 1L, finalCount.longValue());
    }

    /* Regression test for Bug 2572. */
    @Test
    public void testUpdateSetALLNullValues() throws Exception {
        PreparedStatement ps = methodWatcher.prepareStatement("insert into NULL_TABLE2 values (?,?,?)");
        ps.setString(1, "900 Green Meadows Road");
        ps.setString(2, "65201");
        ps.setInt(3, 10);
        ps.execute();

        //get initial count
        Long count = methodWatcher.query("select count(*) from NULL_TABLE2 where col3 = 10");

        //update to set 1st column null
        int numChanged = methodWatcher.executeUpdate("update NULL_TABLE2 set col1 = null where col3 = 10");
        assertEquals("Incorrect rows changed", count.longValue(), numChanged);

        ResultSet rs = methodWatcher.executeQuery("select * from NULL_TABLE2 where col3 = 10");
        assertEquals("" +
                "COL1 |COL2  |COL3 |\n" +
                "-------------------\n" +
                "NULL |65201 | 10  |",
                TestUtils.FormattedResult.ResultFactory.toString(rs));

        numChanged = methodWatcher.executeUpdate("update NULL_TABLE2 set col2 = null where col3 = 10");
        assertEquals("Incorrect rows changed", count.longValue(), numChanged);
        rs = methodWatcher.executeQuery("select * from NULL_TABLE2 where col3 = 10");
        assertEquals("" +
                        "COL1 |COL2 |COL3 |\n" +
                        "------------------\n" +
                        "NULL |NULL | 10  |",
                TestUtils.FormattedResult.ResultFactory.toString(rs));

        numChanged = methodWatcher.executeUpdate("update NULL_TABLE2 set col3 = null where col3 = 10");
        assertEquals("Incorrect rows changed", count.longValue(), numChanged);
        rs = methodWatcher.executeQuery("select * from NULL_TABLE2 where col3 is null");
        assertEquals("" +
                        "COL1 |COL2 |COL3 |\n" +
                        "------------------\n" +
                        "NULL |NULL |NULL |",
                TestUtils.FormattedResult.ResultFactory.toString(rs));

    }

    /* Regression test for DB-1481, DB-2189 */
    @Test
    public void testUpdateOnAllColumnIndex() throws Exception {
        new TableCreator(methodWatcher.getOrCreateConnection())
                .withCreate("create table tab2 (c1 int not null primary key, c2 int, c3 int)")
                .withIndex("create index ti on tab2 (c1,c2 desc,c3)")
                .withInsert("insert into tab2 values(?,?,?)")
                .withRows(rows(
                        row(1, 10, 1),
                        row(2, 20, 1),
                        row(3, 30, 2),
                        row(4, 40, 2),
                        row(5, 50, 3),
                        row(6, 60, 3),
                        row(7, 70, 3)
                ))
                .create();

        int rows = methodWatcher.executeUpdate("update tab2 set c2=99 where c3=2");

        assertEquals("incorrect num rows updated!", 2L, rows);
        assertEquals("incorrect num rows present!", 7L, (long)methodWatcher.query("select count(*) from tab2"));
        assertEquals("incorrect num rows present!", 7L, (long)methodWatcher.query("select count(*) from tab2 --SPLICE-PROPERTIES index=ti"));

        ResultSet rs1 = methodWatcher.executeQuery("select * from tab2 where c3=2");
        ResultSet rs2 = methodWatcher.executeQuery("select * from tab2 --SPLICE-PROPERTIES index=ti \n where c3=2");

        String expected = "" +
                "C1 |C2 |C3 |\n" +
                "------------\n" +
                " 3 |99 | 2 |\n" +
                " 4 |99 | 2 |";

        assertEquals("verify without index", expected, TestUtils.FormattedResult.ResultFactory.toString(rs1));
        assertEquals("verify using index", expected, TestUtils.FormattedResult.ResultFactory.toString(rs2));
    }

    /* Test for DB-3160 */
    @Test
    public void testUpdateCompoundPrimaryKeyWithMatchingCompoundIndex() throws Exception {
        methodWatcher.executeUpdate("create table tab4 (c1 int, c2 int, c3 int, c4 int, primary key(c1, c2, c3))");
        methodWatcher.executeUpdate("create index tab4_idx on tab4 (c1,c2,c3)");
        methodWatcher.executeUpdate("insert into tab4 values (10,10,10,10),(20,20,20,20),(30,30,30,30)");

        int rows = methodWatcher.executeUpdate("update tab4 set c2=88, c3=888, c4=8888 where c1=10 or c1 = 20");

        assertEquals("incorrect num rows updated!", 2L, rows);
        assertEquals("incorrect num rows present!", 3L, (long)methodWatcher.query("select count(*) from tab4"));
        assertEquals("incorrect num rows present!", 3L, (long)methodWatcher.query("select count(*) from tab4 --SPLICE-PROPERTIES index=tab4_idx"));
        assertEquals("expected this row to be deleted from index", 0L, (long)methodWatcher.query("select count(*) from tab4 --SPLICE-PROPERTIES index=tab4_idx\n where c2=10"));

        ResultSet rs1 = methodWatcher.executeQuery("select * from tab4 where c1=10 or c1 = 20");
        ResultSet rs2 = methodWatcher.executeQuery("select * from tab4 --SPLICE-PROPERTIES index=tab4_idx \n where c1=10 or c1 = 20");

        String expected = "" +
                "C1 |C2 |C3  | C4  |\n" +
                "-------------------\n" +
                "10 |88 |888 |8888 |\n" +
                "20 |88 |888 |8888 |";

        assertEquals("verify without index", expected, TestUtils.FormattedResult.ResultFactory.toString(rs1));
        assertEquals("verify using index", expected, TestUtils.FormattedResult.ResultFactory.toString(rs2));
    }

    @Test
    public void updateColumnWhichIsDescendingInIndexAndIsAlsoPrimaryKey() throws Exception {
        new TableCreator(methodWatcher.getOrCreateConnection())
                .withCreate("create table tab3 (a int primary key, b int, c int, d int)")
                .withIndex("create index index_tab3 on tab3 (a desc, b desc, c)")
                .withInsert("insert into tab3 values(?,?,?,?)")
                .withRows(rows(
                        row(10, 10, 10, 10),
                        row(20, 20, 10, 20),
                        row(30, 30, 20, 30)
                ))
                .create();

        int rows = methodWatcher.executeUpdate("update tab3 set a=99,b=99,c=99 where d=30");

        assertEquals("incorrect num rows updated!", 1L, rows);
        assertEquals("incorrect num rows present!", 3L, (long)methodWatcher.query("select count(*) from tab3"));
        assertEquals("incorrect num rows present!", 3L, (long)methodWatcher.query("select count(*) from tab3 --SPLICE-PROPERTIES index=index_tab3"));

        ResultSet rs1 = methodWatcher.executeQuery("select * from tab3 where d=30");
        ResultSet rs2 = methodWatcher.executeQuery("select * from tab3 --SPLICE-PROPERTIES index=index_tab3 \n where d=30");

        String expected = "" +
                "A | B | C | D |\n" +
                "----------------\n" +
                "99 |99 |99 |30 |";

        assertEquals("verify without index", expected, TestUtils.FormattedResult.ResultFactory.toString(rs1));
        assertEquals("verify using index", expected, TestUtils.FormattedResult.ResultFactory.toString(rs2));
    }

    /*
     * Regression test for DB-2007. Assert that this doesn't explode, and that
     * the NULL,NULL row isn't modified, so the number of rows modified = 0
     */
    @Test
    @Ignore("issue with unique index behavior")
    public void testUpdateOverNullIndexWorks() throws Exception {
        new TableCreator(methodWatcher.createConnection())
                .withCreate("create table nt (id int primary key, a int, b int)")
                .withInsert("insert into nt values(?,?,?)")
                .withRows(rows(row(1, null, null), row(2, null, null), row(3, null, null), row(4, 1, 1)))
                .withIndex("create unique index nt_idx on nt (a)").create();


        // updates to null should remain null.  So we are testing setting null to null on base and index conglom.
        // Even though null to null rows are not physically changed they should still contribute to row count.
        assertEquals(4L, (long)methodWatcher.executeUpdate("update NT set a = a + 9"));

        // same thing, but updating using primary key
        assertEquals(1L, (long)methodWatcher.executeUpdate("update NT set a = a + 9 where id=1"));

        assertEquals(4L, (long)methodWatcher.query("select count(*) from nt"));
        assertEquals(4L, (long)methodWatcher.query("select count(*) from nt --SPLICE-PROPERTIES index=nt_idx"));
        assertEquals(3L, (long)methodWatcher.query("select count(*) from nt where a is null"));
        assertEquals(3L, (long)methodWatcher.query("select count(*) from nt --SPLICE-PROPERTIES index=nt_idx\n where a is null"));
    }

    // If you change one of the following 'update over join' tests,
    // you probably need to make a similar change to DeleteOperationIT.

    @Test
    public void testUpdateOverNestedLoopJoin() throws Exception {
        TestConnection conn = methodWatcher.getOrCreateConnection();
        boolean oldAutoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);
        try {
            doTestUpdateOverJoin("NESTEDLOOP", conn);
        } finally {
            conn.rollback();
            conn.setAutoCommit(oldAutoCommit);
        }
    }

    private void doTestUpdateOverJoin(String hint, TestConnection connection) throws Exception {
        StringBuilder sb = new StringBuilder(200);
        sb.append("update CUSTOMER customer set CUSTOMER.status = 'false' \n");
        sb.append("where not exists ( \n");
        sb.append("  select 1 \n");
        sb.append("  from SHIPMENT shipment --SPLICE-PROPERTIES joinStrategy=%s \n");
        sb.append("  where CUSTOMER.cust_id = SHIPMENT.cust_id \n");
        sb.append(") \n");
        String query = String.format(sb.toString(), hint);
        int rows = connection.createStatement().executeUpdate(query);
        assertEquals("incorrect num rows updated!", 3, rows);
    }

    @Test
    public void testUpdateMultiColumnMultiSubSyntax() throws Exception {
        StringBuilder sb = new StringBuilder(200);
        sb.append("update customer \n");
        sb.append("  set status = 'false', \n");
        sb.append("  level = ( \n");
        sb.append("    select level \n");
        sb.append("    from customer_temp custtemp \n");
        sb.append("    where custtemp.cust_id = customer.cust_id \n");
        sb.append("  )");
        String query =sb.toString();
        int rows = methodWatcher.executeUpdate(query);
        Assert.assertEquals("incorrect num rows updated!", 5, rows);
    }

    @Test
    public void testUpdateMultiColumnOneSubSyntaxNoOuterWhere() throws Exception {
        int rows = doTestUpdateMultiColumnOneSubSyntax(null);
        Assert.assertEquals("incorrect num rows updated!", 5, rows);
    }

    @Test
    public void testUpdateMultiColumnOneSubSyntaxWithOuterWhere() throws Exception {
        int rows = doTestUpdateMultiColumnOneSubSyntax(" where customer.cust_id <> 105");
        Assert.assertEquals("incorrect num rows updated!", 4, rows);
    }

    @Test
    public void testUpdateUsingBoolDescIndex() throws Exception {

        String sql =  "update bool_table --SPLICE-PROPERTIES index=idx_bool\n" +
                "set c=false where c=true";
        int n = methodWatcher.executeUpdate(sql);
        Assert.assertEquals("Incorrect number of rows updated", 1, n);
    }

    // Used by previous tests (testUpdateMultiColumnOneSub*)
    private int doTestUpdateMultiColumnOneSubSyntax(String outerWhere) throws Exception {
        Connection conn = methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
        try{
            StringBuilder sb=new StringBuilder(200);
            sb.append("update customer \n");
            sb.append("  set (status, level) = ( \n");
            sb.append("    select customer_temp.status, customer_temp.level \n");
            sb.append("    from customer_temp \n");
            sb.append("    where customer_temp.cust_id = customer.cust_id \n");
            sb.append("  ) ");
            if(outerWhere!=null){
                sb.append(outerWhere);
            }
            String query=sb.toString();
            try(Statement statement=conn.createStatement()){
                return statement.executeUpdate(query);
            }
        }finally{
            conn.rollback();
        }
    }

}
