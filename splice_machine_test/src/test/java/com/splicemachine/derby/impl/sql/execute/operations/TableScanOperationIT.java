package com.splicemachine.derby.impl.sql.execute.operations;

import java.math.BigDecimal;
import java.sql.*;
import java.util.List;

import com.google.common.collect.Lists;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_dao.JDBCTemplate;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;

import static java.lang.String.format;
import static org.junit.Assert.*;

/**
 * This tests basic table scans with and without projection/restriction
 */
public class TableScanOperationIT {

    private static Logger LOG = Logger.getLogger(TableScanOperationIT.class);

    private static SpliceWatcher spliceClassWatcher = new SpliceWatcher();

    private static final String SCHEMA = TableScanOperationIT.class.getSimpleName().toUpperCase();
    private static final String TABLE_NAME = "A";
    private static final String TABLE_NAME2 = "AB";

    private static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);
    private static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher(TABLE_NAME, SCHEMA, "(si varchar(40),sa character varying(40),sc varchar(40),sd int,se float,sf decimal(5))");
    private static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE_NAME2, SCHEMA, "(si varchar(40),sa character varying(40),sc varchar(40),sd1 int, sd2 smallint, sd3 bigint, se1 float, se2 double, se3 decimal(4,2), se4 REAL)");
    private static SpliceTableWatcher spliceTableWatcher4 = new SpliceTableWatcher("T1", SCHEMA, "(c1 int, c2 int)");
    private static SpliceTableWatcher spliceTableWatcher5 = new SpliceTableWatcher("CHICKEN", SCHEMA, "(c1 timestamp, c2 date, c3 time)");
    private static SpliceTableWatcher spliceTableWatcher6 = new SpliceTableWatcher("CHICKEN1", SCHEMA, "(c1 timestamp, c2 date, c3 time, primary key (c1))");
    private static SpliceTableWatcher spliceTableWatcher7 = new SpliceTableWatcher("CHICKEN2", SCHEMA, "(c1 timestamp, c2 date, c3 time, primary key (c2))");
    private static SpliceTableWatcher spliceTableWatcher8 = new SpliceTableWatcher("CHICKEN3", SCHEMA, "(c1 timestamp, c2 date, c3 time, primary key (c3))");
    private static SpliceTableWatcher spliceTableWatcher9 = new SpliceTableWatcher("NUMBERS", SCHEMA, "(i int, l bigint, s smallint, d double precision, r real, dc decimal(10,2), PRIMARY KEY(i))");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher)
            .around(spliceTableWatcher2)
            .around(spliceTableWatcher4)
            .around(spliceTableWatcher5)
            .around(spliceTableWatcher6)
            .around(spliceTableWatcher7)
            .around(spliceTableWatcher8)
            .around(spliceTableWatcher9)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        PreparedStatement ps = spliceClassWatcher.prepareStatement(format("insert into %s.%s (si, sa, sc,sd,se,sf) values (?,?,?,?,?,?)", SCHEMA, TABLE_NAME));
                        for (int i = 0; i < 10; i++) {
                            ps.setString(1, "" + i);
                            ps.setString(2, "i");
                            ps.setString(3, "" + i * 10);
                            ps.setInt(4, i);
                            ps.setFloat(5, 10.0f * i);
                            ps.setBigDecimal(6, i % 2 == 0 ? BigDecimal.valueOf(i).negate() : BigDecimal.valueOf(i)); //make sure we have some negative values
                            ps.executeUpdate();
                        }
                        spliceClassWatcher.executeUpdate(format("insert into %s.%s values (null, null), (1,1), (null, null), (2,1), (3,1),(10,10)", SCHEMA, "T1"));
                        spliceClassWatcher.executeUpdate(format("insert into %s.%s values (timestamp('2012-05-01 00:00:00.0'), date('2010-01-01'), time('00:00:00'))", SCHEMA, "CHICKEN"));
                        spliceClassWatcher.executeUpdate(format("insert into %s.%s values (timestamp('2012-05-01 00:00:00.0'), date('2010-01-01'), time('00:00:00'))", SCHEMA, "CHICKEN1"));
                        spliceClassWatcher.executeUpdate(format("insert into %s.%s values (timestamp('2012-05-01 00:00:00.0'), date('2010-01-01'), time('00:00:00'))", SCHEMA, "CHICKEN2"));
                        spliceClassWatcher.executeUpdate(format("insert into %s.%s values (timestamp('2012-05-01 00:00:00.0'), date('2010-01-01'), time('00:00:00'))", SCHEMA, "CHICKEN3"));

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        spliceClassWatcher.closeAll();
                    }
                }

            }).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        PreparedStatement ps = spliceClassWatcher.prepareStatement(format("insert into %s.%s (si, sa, sc,sd1, sd2, sd3,se1,se2,se3,se4) values (?,?,?,?,?,?,?,?,?,?)", SCHEMA, TABLE_NAME2));
                        for (int i = 0; i < 10; i++) {
                            ps.setString(1, "" + i);
                            ps.setString(2, "i");
                            ps.setString(3, "" + i * 10);
                            ps.setInt(4, i);
                            ps.setInt(5, i);
                            ps.setInt(6, i);

                            ps.setFloat(7, 10.0f * i);
                            ps.setFloat(8, 10.0f * i);
                            ps.setFloat(9, 10.0f * i);
                            ps.setFloat(10, 10.0f * i);
                            ps.executeUpdate();
                        }

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        spliceClassWatcher.closeAll();
                    }
                }

            }).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        PreparedStatement ps = spliceClassWatcher.prepareStatement(format("insert into %s.%s (i, l, s, d, r, dc) values (?,?,?,?,?,?)", SCHEMA, "NUMBERS"));
                        for (int i = 0; i < 10; i++) {
                            ps.setInt(1, i);
                            ps.setInt(2, i);
                            ps.setInt(3, i);
                            ps.setDouble(4, 10.0);
                            ps.setDouble(5, 10.0);
                            if (i == 0) {
                                ps.setNull(6, Types.DECIMAL);
                            } else {
                                ps.setBigDecimal(6, BigDecimal.valueOf(i));
                            }
                            ps.executeUpdate();
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        spliceClassWatcher.closeAll();
                    }
                }

            }).around(new SpliceDataWatcher(){
                @Override
                protected void starting(Description description){
                    try(CallableStatement cs = spliceClassWatcher.prepareCall("call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS(?,false)")){
                        cs.setString(1,"SYS");
                        cs.execute();
                    }catch(Exception e){
                        throw new RuntimeException(e);
                    }
                }
            });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @Test
    public void testZeroFilledColumnsAreNotNull() throws Exception {
        //regression test for Bug 562
        methodWatcher.executeUpdate("create table NT (c1 character(4), c2 character(6), c3 numeric(5), c4 numeric(5))");
        PreparedStatement ps = methodWatcher.prepareStatement("insert into NT values (?,?,?,?)");
        ps.setString(1, "II");
        ps.setString(2, "KK");
        ps.setBigDecimal(3, new BigDecimal("9.0"));
        ps.setBigDecimal(4, BigDecimal.ZERO);
        ps.execute();

        ResultSet rs = methodWatcher.executeQuery("select * from NT");
        assertTrue(rs.next());
        assertEquals("expected database to add padding here", "II  ", rs.getString(1));
        assertEquals("expected database to add padding here", "KK    ", rs.getString(2));
        assertEquals(new BigDecimal("9"), rs.getBigDecimal(3));
        assertEquals(BigDecimal.ZERO, rs.getBigDecimal(4));
        assertFalse(rs.next());
    }

    @Test
    public void testSimpleTableScan() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("select * from %s", TABLE_NAME));
        int i = 0;
        while (rs.next()) {
            i++;
            assertNotNull(rs.getString(1));
            assertNotNull(rs.getString(2));
            assertNotNull(rs.getString(3));
            assertNotNull(rs.getBigDecimal(6));
        }
        assertEquals(10, i);
    }


    @Test
    public void testScanForNullEntries() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("select si from %s where si is null", TABLE_NAME));
        boolean hasRows = false;
        List<String> results = Lists.newArrayList();
        while (rs.next()) {
            hasRows = true;
            results.add(format("si=%s", rs.getString(1)));
        }

        if (hasRows) {
            for (String row : results) {
                LOG.info(row);
            }
            Assert.fail("Rows returned! expected 0 but was " + results.size());
        }
    }

    @Test
    public void testQualifierTableScanPreparedStatement() throws Exception {
        PreparedStatement stmt = methodWatcher.prepareStatement(format("select * from %s where si = ?", TABLE_NAME));
        stmt.setString(1, "5");
        ResultSet rs = stmt.executeQuery();
        int i = 0;
        while (rs.next()) {
            i++;
            LOG.info("a.si=" + rs.getString(1) + ",b.si=" + rs.getString(2) + ",c.si=" + rs.getString(3));
            assertNotNull(rs.getString(1));
            assertNotNull(rs.getString(2));
            assertNotNull(rs.getString(3));
        }
        assertEquals(1, i);
    }

    @Test
    public void testQualifierTableScanPreparedStatementRepeated() throws Exception {
        PreparedStatement stmt = methodWatcher.prepareStatement(format("select * from %s where si = ?", TABLE_NAME));
        for (int iter = 0; iter < 10; iter++) {
            stmt.setString(1, Integer.toString(iter));
            ResultSet rs = stmt.executeQuery();
            int i = 0;
            while (rs.next()) {
                i++;
                LOG.info("a.si=" + rs.getString(1) + ",b.si=" + rs.getString(2) + ",c.si=" + rs.getString(3));
                assertNotNull(rs.getString(1));
                assertNotNull(rs.getString(2));
                assertNotNull(rs.getString(3));
            }
            assertEquals(1, i);
        }
    }

    @Test
    public void testOrQualifiedTableScanPreparedStatement() throws Exception {
        PreparedStatement stmt = methodWatcher.prepareStatement(format("select * from %s where si = ? or si = ?", TABLE_NAME));
        stmt.setString(1, "5");
        stmt.setString(2, "4");
        ResultSet rs = stmt.executeQuery();
        int i = 0;
        while (rs.next()) {
            i++;
            LOG.info("a.si=" + rs.getString(1) + ",b.si=" + rs.getString(2) + ",c.si=" + rs.getString(3));
            assertNotNull(rs.getString(1));
            assertNotNull(rs.getString(2));
            assertNotNull(rs.getString(3));
        }
        assertEquals(2, i);
    }

    @Test
    public void testQualifierTableScan() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("select * from %s where si = '5'", TABLE_NAME));
        int i = 0;
        while (rs.next()) {
            i++;
            LOG.info("a.si=" + rs.getString(1) + ",b.si=" + rs.getString(2) + ",c.si=" + rs.getString(3));
            assertNotNull(rs.getString(1));
            assertNotNull(rs.getString(2));
            assertNotNull(rs.getString(3));
        }
        assertEquals(1, i);
    }

    @Test
    public void testRestrictedTableScan() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select si,sc from A");
        int i = 0;
        while (rs.next()) {
            i++;
            LOG.info("a.si=" + rs.getString(1) + ",c.si=" + rs.getString(2));
            assertNotNull("a.si is null!", rs.getString(1));
            assertNotNull("c.si is null!", rs.getString(2));
        }
        assertEquals(10, i);
    }

    /*
     * Char columns are a special case. Values are padded as stored in hbase and we must account for this when
     * evaluating qualifiers or constructing scan start/stop keys.
     */
    @Test
    public void testScanChar() throws Exception {
        methodWatcher.executeUpdate("create table char_table(a char(9), b char(9), c char(9), primary key(a))");
        methodWatcher.executeUpdate("create unique index char_table_index on char_table(c)");
        methodWatcher.executeUpdate("insert into char_table values('aaa', 'aaa', 'aaa'),('aa', 'aa', 'aa'),('a', 'a', 'a'),('', '', '')");

        // where char column IS primary key
        assertEquals(1L, methodWatcher.query("select count(*) from char_table where a = 'aaa'"));
        assertEquals(1L, methodWatcher.query("select count(*) from char_table where a = 'aaa   '"));
        assertEquals(1L, methodWatcher.query("select count(*) from char_table where a = 'aaa      '"));
        assertEquals(1L, methodWatcher.query("select count(*) from char_table where 'aaa' = a"));

        // where char column is not primary key
        assertEquals(1L, methodWatcher.query("select count(*) from char_table where b = 'aaa'"));
        assertEquals(1L, methodWatcher.query("select count(*) from char_table where b = 'aaa   '"));
        assertEquals(1L, methodWatcher.query("select count(*) from char_table where b = 'aaa      '"));
        assertEquals(1L, methodWatcher.query("select count(*) from char_table where 'aaa' = b"));

        // where char column is unique index
        assertEquals(1L, methodWatcher.query("select count(*) from char_table --splice-properties index=char_table_index\n where c = 'aaa'"));
        assertEquals(1L, methodWatcher.query("select count(*) from char_table --splice-properties index=char_table_index\n where c = 'aaa   '"));
        assertEquals(1L, methodWatcher.query("select count(*) from char_table --splice-properties index=char_table_index\n where c = 'aaa      '"));
        assertEquals(1L, methodWatcher.query("select count(*) from char_table --splice-properties index=char_table_index\n where 'aaa' = c"));
    }

    /* DB-3367: scans where the start/stop keys were prepared statement bind variables compared to SQLChar would fail. */
    @Test
    public void testScanCharPreparedStatement() throws Exception {
        methodWatcher.executeUpdate("create table char_table_ps(a char(9), b char(9), c char(9), primary key(a))");
        methodWatcher.executeUpdate("create unique index char_table_ps_index on char_table_ps(c)");
        methodWatcher.executeUpdate("insert into char_table_ps values('aaa', 'aaa', 'aaa'),('aa', 'aa', 'aa'),('a', 'a', 'a'),('', '', '')");

        JDBCTemplate template = new JDBCTemplate(methodWatcher.getOrCreateConnection());

        // where char column IS primary key
        assertEquals(1L, template.query("select count(*) from char_table_ps where a = ?", "aaa").get(0));
        assertEquals(1L, template.query("select count(*) from char_table_ps where a = ?", "aaa   ").get(0));
        assertEquals(1L, template.query("select count(*) from char_table_ps where a = ?", "aaa      ").get(0));
        assertEquals(1L, template.query("select count(*) from char_table_ps where ? = a", "aaa").get(0));

        // where char column is not primary key
        assertEquals(1L, template.query("select count(*) from char_table_ps where b = ?", "aaa").get(0));
        assertEquals(1L, template.query("select count(*) from char_table_ps where b = ?", "aaa   ").get(0));
        assertEquals(1L, template.query("select count(*) from char_table_ps where b = ?", "aaa      ").get(0));
        assertEquals(1L, template.query("select count(*) from char_table_ps where ? = b", "aaa").get(0));

        // where char column is unique index
        assertEquals(1L, template.query("select count(*) from char_table_ps --splice-properties index=char_table_ps_index\n where c = ?", "aaa").get(0));
        assertEquals(1L, template.query("select count(*) from char_table_ps --splice-properties index=char_table_ps_index\n where c = ?", "aaa   ").get(0));
        assertEquals(1L, template.query("select count(*) from char_table_ps --splice-properties index=char_table_ps_index\n where c = ?", "aaa      ").get(0));
        assertEquals(1L, template.query("select count(*) from char_table_ps --splice-properties index=char_table_ps_index\n where ? = c", "aaa").get(0));
    }

    @Test
    public void testScanIntWithLessThanOperator() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select  sd from A where sd < 5");
        List<String> results = Lists.newArrayList();
        while (rs.next()) {
            int sd = rs.getInt(1);
            assertTrue("incorrect sd returned!", sd < 5);
            results.add(format("sd:%d", sd));
        }
        for (String result : results) {
            LOG.info(result);
        }
        assertEquals("Incorrect rows returned!", 5, results.size());
    }

    @Test
    public void testScanIntWithLessThanEqualOperator() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select sd from A where sd <= 5");
        List<String> results = Lists.newArrayList();
        while (rs.next()) {
            int sd = rs.getInt(1);
            assertTrue("incorrect sd returned!", sd <= 5);
            results.add(format("sd:%d", sd));
        }
        for (String result : results) {
            LOG.info(result);
        }
        assertEquals("Incorrect rows returned!", 6, results.size());
    }

    @Test
    public void testScanIntWithGreaterThanOperator() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select  sd from A where sd > 5");
        List<String> results = Lists.newArrayList();
        while (rs.next()) {
            int sd = rs.getInt(1);
            assertTrue("incorrect sd returned!", sd > 5);
            results.add(format("sd:%d", sd));
        }
        for (String result : results) {
            LOG.info(result);
        }
        assertEquals("Incorrect rows returned!", 4, results.size());
    }

    @Test
    public void testScanIntWithGreaterThanEqualsOperator() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select sd from A where sd >= 5");
        List<String> results = Lists.newArrayList();
        while (rs.next()) {
            int sd = rs.getInt(1);
            assertTrue("incorrect sd returned!", sd >= 5);
            results.add(format("sd:%d", sd));
        }
        for (String result : results) {
            LOG.info(result);
        }
        assertEquals("Incorrect rows returned!", 5, results.size());
    }

    @Test
    public void testScanIntWithNotEqualsOperator() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select  sd from A where sd != 5");
        List<String> results = Lists.newArrayList();
        while (rs.next()) {
            int sd = rs.getInt(1);
            assertTrue("incorrect sd returned!", sd != 5);
            results.add(format("sd:%d", sd));
        }
        for (String result : results) {
            LOG.info(result);
        }
        assertEquals("Incorrect rows returned!", 9, results.size());
    }


    @Test
    public void testScanFloatWithLessThanOperator() throws Exception {
        float correctCompare = 50f;
        ResultSet rs = methodWatcher.executeQuery("select se from A where se < " + correctCompare);
        List<String> results = Lists.newArrayList();
        while (rs.next()) {
            float se = rs.getFloat(1);
            assertTrue("incorrect se returned!", se < correctCompare);
            results.add(format("se:%f", se));
        }
        for (String result : results) {
            LOG.info(result);
        }
        assertEquals("Incorrect rows returned!", 5, results.size());
    }

    @Test
    public void testScanFloatWithLessThanEqualOperator() throws Exception {
        float correctCompare = 50f;
        ResultSet rs = methodWatcher.executeQuery("select  se from A where se <= " + correctCompare);
        List<String> results = Lists.newArrayList();
        while (rs.next()) {
            float se = rs.getFloat(1);
//            Assert.assertTrue("incorrect se returned!se:"+se,se<=correctCompare);
            results.add(format("se:%f", se));
        }
        for (String result : results) {
            LOG.warn(result);
        }
        assertEquals("Incorrect rows returned!", 6, results.size());
    }

    @Test
    public void testScanFloatWithGreaterThanOperator() throws Exception {
        float correctCompare = 50f;
        ResultSet rs = methodWatcher.executeQuery("select  se from A where se > " + correctCompare);
        List<String> results = Lists.newArrayList();
        while (rs.next()) {
            float se = rs.getFloat(1);
            assertTrue("incorrect se returned!", se > 5);
            results.add(format("se:%f", se));
        }
        for (String result : results) {
            LOG.info(result);
        }
        assertEquals("Incorrect rows returned!", 4, results.size());
    }

    @Test
    public void testScanFloatWithGreaterThanEqualsOperator() throws Exception {
        float correctCompare = 50f;
        ResultSet rs = methodWatcher.executeQuery("select  se from A where se >= " + correctCompare);
        List<String> results = Lists.newArrayList();
        while (rs.next()) {
            float se = rs.getFloat(1);
            assertTrue("incorrect se returned!", se >= correctCompare);
            results.add(format("se:%f", se));
        }
        for (String result : results) {
            LOG.info(result);
        }
        assertEquals("Incorrect rows returned!", 5, results.size());
    }

    @Test
    public void testScanFloatWithNotEqualsOperator() throws Exception {
        float correctCompare = 50f;
        ResultSet rs = methodWatcher.executeQuery("select  se from A where se != " + correctCompare);
        List<String> results = Lists.newArrayList();
        while (rs.next()) {
            float se = rs.getFloat(1);
            assertTrue("incorrect se returned!", se != correctCompare);
            results.add(format("se:%f", se));
        }
        for (String result : results) {
            LOG.info(result);
        }
        assertEquals("Incorrect rows returned!", 9, results.size());
    }

    @Test
    public void testScanFloatWithEqualsOperator() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select se1 from AB where se1 = 50.0");

        rs.next();

        float res = rs.getFloat(1);
        assertEquals(50.0f, res, 0.0);

        assertFalse(rs.next());
    }

    @Test
    public void testScanDoubleWithEqualsOperator() throws Exception {
        /*
         * Choose to scan for 0.0 to check for Bug 738 simultaneously.
         */
        ResultSet rs = methodWatcher.executeQuery("select se2 from AB where se2 = 0.0");

        rs.next();

        double res = rs.getDouble(1);
        assertEquals(0.0, res, 0.0);

        assertFalse(rs.next());
    }

    @Test
    public void testScanDecimalWithEqualsOperator() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select se3 from AB where se3 = 50.0");

        assertTrue("No rows returned!", rs.next());

        double res = rs.getDouble(1);
        assertEquals(50.0, res, 0.0);

        assertFalse(rs.next());
    }

    @Test
    public void testScanRealWithEqualsOperation() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select se4 from AB where se4 = 0e0");

        rs.next();

        float res = rs.getFloat(1);
        assertEquals(0.0, res, 0.0);

        assertFalse(rs.next());
    }

    @Test
    public void testScanIntWithEqualsOperator() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select sd1 from AB where sd1 = 5");

        rs.next();
        int sd = rs.getInt(1);
        assertEquals(sd, 5);
        assertFalse(rs.next());
    }

    @Test
    public void testScanSmallIntWithEqualsOperator() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select sd2 from AB where sd2 = 5");

        rs.next();
        int sd = rs.getInt(1);
        assertEquals(sd, 5);
        assertFalse(rs.next());
    }

    @Test
    public void testScanBigIntWithEqualsOperator() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select sd3 from AB where sd3 = 5");

        rs.next();
        int sd = rs.getInt(1);
        assertEquals(sd, 5);
        assertFalse(rs.next());
    }

    @Test
    public void testScanIntWithFloatInEquals() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select sd1 from AB where sd1 = 5.0");

        assertTrue("No results returned", rs.next());
        int sd = rs.getInt(1);
        assertEquals(sd, 5);
        assertFalse(rs.next());
    }

    @Test
    public void testScanFloatWithIntInEquals() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select se1 from AB where se1 = 50");

        assertTrue("No results returned", rs.next());
        float sd = rs.getFloat(1);
        assertEquals(sd, 50.0, 0.0);
        assertFalse(rs.next());
    }

    @Test
    public void testScanWithNoColumns() throws Exception {
        // In order to produce a table scan in which no columns are actually read, run a cross join where no columns from
        // the right-hand table are referenced
        ResultSet rs = methodWatcher.executeQuery(format("select o.se1 from %s o, %s t", "AB", "A"));

        List results = TestUtils.resultSetToArrays(rs);

        assertEquals(100, results.size());
    }

    @Test
    public void testWithOrCriteria() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select * from " + spliceTableWatcher + " where si = '2' or sc = '30'");
        int count = 0;
        while (rs.next()) {
            String si = rs.getString(1);
            String sc = rs.getString(3);

            if (!"2".equals(si) && !"30".equals(sc))
                Assert.fail("Either si !=2 or sc !=30. si=" + si + ", sc=" + sc);

            count++;
        }

        assertEquals("Incorrect count returned", 2, count);
    }

    @Test
    public void testWithLikeCriteria() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select * from " + spliceTableWatcher + " where si like '2%'");
        int count = 0;
        while (rs.next()) {
            String si = rs.getString(1);
            String sc = rs.getString(3);

            assertEquals("Incorrect si value", "2", si);

            count++;
        }

        assertEquals("Incorrect count returned", 1, count);
    }

    @Test
    public void testBooleanDataTypeOnScan() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("select 1 in (1,2) from %s", spliceTableWatcher4));
        int count = 0;
        while (rs.next()) {
            count++;
            assertEquals(true, rs.getBoolean(1));
        }
        assertEquals(6, count);
    }

    @Test
    public void testCanScanDoubleEdgeCase() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select 1,1.797e+308,-1.797e+308,'This query should work' from " + spliceTableWatcher);
        int count = 0;
        while (rs.next()) {
            int first = rs.getInt(1);
            double second = rs.getDouble(2);
            double third = rs.getDouble(3);
            String fourth = rs.getString(4);

            assertEquals("Incorrect first field!", 1, first);
            assertEquals("Incorrect second field!", 1.797e+308, second, 0.0000000001);
            assertEquals("Incorrect third field!", -1.797e+308, third, 0.0000000001);
            assertEquals("Incorrect fourth field!", "This query should work", fourth);
            count++;
        }

        assertEquals("Incorrect count returned!", 10, count);
    }

    @Test
    public void testScanOfTimestampQualifiedByString() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("select * from %s where c1 = '2012-05-01 00:00:00.0'", spliceTableWatcher5));
        int count = 0;
        while (rs.next()) {
            count++;
        }
        assertEquals("Incorrect count returned!", 1, count);
    }

    @Test
    public void testScanOfDateQualifiedByString() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("select * from %s where c2 = '2010-01-01'", spliceTableWatcher5));
        int count = 0;
        while (rs.next()) {
            count++;
        }
        assertEquals("Incorrect count returned!", 1, count);
    }

    @Test
    public void testScanOfTimeQualifiedByString() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("select * from %s where c3 = '00:00:00'", spliceTableWatcher5));
        int count = 0;
        while (rs.next()) {
            count++;
        }
        assertEquals("Incorrect count returned!", 1, count);
    }

    @Test
    public void testScanOfTimestampPrimaryKeyQualifiedByString() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("select * from %s where c1 = '2012-05-01 00:00:00.0'", spliceTableWatcher6));
        int count = 0;
        while (rs.next()) {
            count++;
        }
        assertEquals("Incorrect count returned!", 1, count);
    }

    @Test
    public void testScanOfDatePrimaryKeyQualifiedByString() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("select * from %s where c2 = '2010-01-01'", spliceTableWatcher7));
        int count = 0;
        while (rs.next()) {
            count++;
        }
        assertEquals("Incorrect count returned!", 1, count);
    }

    @Test
    public void testScanOfTimePrimaryKeyQualifiedByString() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("select * from %s where c3 = '00:00:00'", spliceTableWatcher8));
        int count = 0;
        while (rs.next()) {
            count++;
        }
        assertEquals("Incorrect count returned!", 1, count);
    }

    @Test
    // Test for DB-1101
    public void testScanOfNumericalTypeIsNotNull() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("select dc from %s", spliceTableWatcher9));
        int nulls = 0;
        while (rs.next()) {
            BigDecimal bd = rs.getBigDecimal(1);
            if (bd == null) {
                nulls++;
            }
        }
        assertEquals("Incorrect number of nulls returned!", 1, nulls);
    }
}
