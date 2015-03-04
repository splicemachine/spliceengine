package com.splicemachine.derby.impl.stats;

import com.splicemachine.derby.test.framework.*;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.math.BigDecimal;
import java.sql.*;

/**
 * Tests for data type correctness w.r.t different column types
 *
 * @author Scott Fines
 *         Date: 3/3/15
 */
public class StatisticsDataTypeIT {
    private static final SpliceWatcher classWatcher = new SpliceWatcher();
    private static final SpliceSchemaWatcher schema = new SpliceSchemaWatcher(StatisticsDataTypeIT.class.getSimpleName().toUpperCase());

    private static final String BASE_SCHEMA="b smallint,c int,d bigint,e real,f double,g numeric(5,2),h char(5),i varchar(10)";
    private static final SpliceTableWatcher allDataTypes    = new SpliceTableWatcher("DT"        ,schema.schemaName,"("+BASE_SCHEMA+")");
    private static final SpliceTableWatcher smallintPk      = new SpliceTableWatcher("smallintPk",schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(b))");
    private static final SpliceTableWatcher intPk           = new SpliceTableWatcher("intPk"     ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(b,c))");
    private static final SpliceTableWatcher bigintPk        = new SpliceTableWatcher("bigintPk"  ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(b,c,d))");
    private static final SpliceTableWatcher realPk          = new SpliceTableWatcher("realPk"    ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(b,c,d,e))");
    private static final SpliceTableWatcher doublePk        = new SpliceTableWatcher("doublePk"  ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(b,c,d,e,f))");
    private static final SpliceTableWatcher numericPk       = new SpliceTableWatcher("numericPk" ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(b,c,d,e,f,g))");
    private static final SpliceTableWatcher charPk          = new SpliceTableWatcher("charPk"    ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(b,c,d,e,f,g,h))");
    private static final SpliceTableWatcher varcharPk       = new SpliceTableWatcher("varcharPk" ,schema.schemaName,"("+BASE_SCHEMA+",PRIMARY KEY(b,c,d,e,f,g,h,i))");

    @ClassRule public static final TestRule rule = RuleChain.outerRule(classWatcher)
            .around(schema)
            .around(allDataTypes)
            .around(smallintPk)
            .around(intPk)
            .around(bigintPk)
            .around(realPk)
            .around(doublePk)
            .around(numericPk)
            .around(charPk)
            .around(varcharPk)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        PreparedStatement adtPs = classWatcher.prepareStatement("insert into " + allDataTypes + " (b,c,d,e,f,g,h,i) values (?,?,?,?,?,?,?,?)");
                        PreparedStatement siPs  = classWatcher.prepareStatement("insert into " + smallintPk + " (b,c,d,e,f,g,h,i) values (?,?,?,?,?,?,?,?)");
                        PreparedStatement iPs   = classWatcher.prepareStatement("insert into " + intPk + " (b,c,d,e,f,g,h,i) values (?,?,?,?,?,?,?,?)");
                        PreparedStatement biPs  = classWatcher.prepareStatement("insert into " + bigintPk + " (b,c,d,e,f,g,h,i) values (?,?,?,?,?,?,?,?)");
                        PreparedStatement rPs   = classWatcher.prepareStatement("insert into " + realPk + " (b,c,d,e,f,g,h,i) values (?,?,?,?,?,?,?,?)");
                        PreparedStatement dPs   = classWatcher.prepareStatement("insert into " + doublePk + " (b,c,d,e,f,g,h,i) values (?,?,?,?,?,?,?,?)");
                        PreparedStatement nPs   = classWatcher.prepareStatement("insert into " + numericPk + " (b,c,d,e,f,g,h,i) values (?,?,?,?,?,?,?,?)");
                        PreparedStatement cPs   = classWatcher.prepareStatement("insert into " + charPk + " (b,c,d,e,f,g,h,i) values (?,?,?,?,?,?,?,?)");
                        PreparedStatement vcPs  = classWatcher.prepareStatement("insert into " + varcharPk + " (b,c,d,e,f,g,h,i) values (?,?,?,?,?,?,?,?)");
                        short bVal = (short) 0;
                        int cVal = 0;
                        long dVal = 0;
                        float eVal = 0;
                        double fVal = 0;
                        BigDecimal hVal = BigDecimal.ZERO;
                        String iVal = Integer.toString(0);
                        String jVal = Integer.toString(0);
                        for (int i = 0; i < 512; i++) {
                            setInsertValues(adtPs, bVal, cVal, dVal, eVal, fVal, hVal, iVal, jVal);
                            setInsertValues(siPs, bVal, cVal, dVal, eVal, fVal, hVal, iVal, jVal);
                            setInsertValues(iPs, bVal, cVal, dVal, eVal, fVal, hVal, iVal, jVal);
                            setInsertValues(biPs, bVal, cVal, dVal, eVal, fVal, hVal, iVal, jVal);
                            setInsertValues(rPs, bVal, cVal, dVal, eVal, fVal, hVal, iVal, jVal);
                            setInsertValues(dPs, bVal, cVal, dVal, eVal, fVal, hVal, iVal, jVal);
                            setInsertValues(nPs, bVal, cVal, dVal, eVal, fVal, hVal, iVal, jVal);
                            setInsertValues(cPs, bVal, cVal, dVal, eVal, fVal, hVal, iVal, jVal);
                            setInsertValues(vcPs, bVal, cVal, dVal, eVal, fVal, hVal, iVal, jVal);
                            if (i % 100 == 0) {
                                adtPs.executeBatch();
                                siPs.executeBatch();
                                iPs.executeBatch();
                                biPs.executeBatch();
                                rPs.executeBatch();
                                dPs.executeBatch();
                                nPs.executeBatch();
                                cPs.executeBatch();
                                vcPs.executeBatch();
                            }

                            bVal++;
                            if (i % 2 == 0) cVal++;
                            if (i % 4 == 0) dVal++;
                            if (i % 8 == 0) eVal += 1.5f;
                            if (i % 16 == 0) fVal += .75d;
                            if (i % 32 == 0) hVal = hVal.add(BigDecimal.ONE);
                            if (i % 64 == 0) iVal = Integer.toString(Integer.parseInt(iVal) + 1);
                            if (i % 128 == 0) jVal = Integer.toString(Integer.parseInt(jVal) + 2);

                        }
                        adtPs.executeBatch();
                        siPs.executeBatch();
                        iPs.executeBatch();
                        biPs.executeBatch();
                        rPs.executeBatch();
                        dPs.executeBatch();
                        nPs.executeBatch();
                        cPs.executeBatch();
                        vcPs.executeBatch();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });

    private static void setInsertValues(PreparedStatement adtPs, short bVal, int cVal, long dVal, float eVal, double fVal, BigDecimal hVal, String iVal, String jVal) throws SQLException {
        adtPs.setShort(1, bVal);
        adtPs.setInt(2, cVal);
        adtPs.setLong(3, dVal);
        adtPs.setFloat(4, eVal);
        adtPs.setDouble(5, fVal);
        adtPs.setBigDecimal(6, hVal);
        adtPs.setString(7, iVal);
        adtPs.setString(8, jVal);

        adtPs.addBatch();
    }

    private static Connection conn;

    private static final TestColumn siCol = new TestColumn("b",false);
    private static final TestColumn iCol = new TestColumn("c",false);
    private static final TestColumn biCol = new TestColumn("d",false);
    private static final TestColumn rCol = new TestColumn("e",true);
    private static final TestColumn dCol = new TestColumn("f",true);
    private static final TestColumn nCol = new TestColumn("g",false);
    private static final TestColumn cCol = new TestColumn("h",false);
    private static final TestColumn vcCol = new TestColumn("i",false);

    @BeforeClass
    public static void setUpClass() throws Exception {
        conn = classWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        ((TestConnection)conn).reset();
    }

    @After
    public void tearDown() throws Exception {
        conn.rollback();
    }

    @Test public void noPk_smallint() throws Exception { testCorrect(allDataTypes.tableName, siCol); }
    @Test public void noPk_smallint_int()       throws Exception{ testCorrect(allDataTypes.tableName,siCol,iCol); }
    @Test public void noPk_smallint_bigint()    throws Exception{ testCorrect(allDataTypes.tableName,siCol,biCol); }
    @Test public void noPk_smallint_real()      throws Exception{ testCorrect(allDataTypes.tableName,siCol,rCol); }
    @Test public void noPk_smallint_double()    throws Exception{ testCorrect(allDataTypes.tableName,siCol,dCol); }
    @Test public void noPk_smallint_numeric()   throws Exception{ testCorrect(allDataTypes.tableName,siCol,nCol); }
    @Test public void noPk_smallint_char()      throws Exception{ testCorrect(allDataTypes.tableName,siCol,cCol); }
    @Test public void noPk_smallint_varchar()   throws Exception{ testCorrect(allDataTypes.tableName,siCol,vcCol); }

    @Test public void noPk_int() throws Exception { testCorrect(allDataTypes.tableName, iCol); }
    @Test public void noPk_int_smallint()  throws Exception{ testCorrect(allDataTypes.tableName,iCol,siCol); }
    @Test public void noPk_int_bigint()    throws Exception{ testCorrect(allDataTypes.tableName,iCol,biCol); }
    @Test public void noPk_int_real()      throws Exception{ testCorrect(allDataTypes.tableName,iCol,rCol); }
    @Test public void noPk_int_double()    throws Exception{ testCorrect(allDataTypes.tableName,iCol,dCol); }
    @Test public void noPk_int_numeric()   throws Exception{ testCorrect(allDataTypes.tableName,iCol,nCol); }
    @Test public void noPk_int_char()      throws Exception{ testCorrect(allDataTypes.tableName,iCol,cCol); }
    @Test public void noPk_int_varchar()   throws Exception{ testCorrect(allDataTypes.tableName,iCol,vcCol); }

    @Test public void noPk_bigint() throws Exception { testCorrect(allDataTypes.tableName, biCol); }
    @Test public void noPk_bigint_smallint()  throws Exception{ testCorrect(allDataTypes.tableName,biCol,siCol); }
    @Test public void noPk_bigint_int()       throws Exception{ testCorrect(allDataTypes.tableName,biCol,iCol); }
    @Test public void noPk_bigint_real()      throws Exception{ testCorrect(allDataTypes.tableName,biCol,rCol); }
    @Test public void noPk_bigint_double()    throws Exception{ testCorrect(allDataTypes.tableName,biCol,dCol); }
    @Test public void noPk_bigint_numeric()   throws Exception{ testCorrect(allDataTypes.tableName,biCol,nCol); }
    @Test public void noPk_bigint_char()      throws Exception{ testCorrect(allDataTypes.tableName,biCol,cCol); }
    @Test public void noPk_bigint_varchar()   throws Exception{ testCorrect(allDataTypes.tableName,biCol,vcCol); }

    @Test public void noPk_real() throws Exception { testCorrect(allDataTypes.tableName, rCol); }
    @Test public void noPk_real_smallint()  throws Exception{ testCorrect(allDataTypes.tableName,rCol,siCol); }
    @Test public void noPk_real_int()       throws Exception{ testCorrect(allDataTypes.tableName,rCol,iCol); }
    @Test public void noPk_real_bigint()    throws Exception{ testCorrect(allDataTypes.tableName,rCol,biCol); }
    @Test public void noPk_real_double()    throws Exception{ testCorrect(allDataTypes.tableName,rCol,dCol); }
    @Test public void noPk_real_numeric()   throws Exception{ testCorrect(allDataTypes.tableName,rCol,nCol); }
    @Test public void noPk_real_char()      throws Exception{ testCorrect(allDataTypes.tableName,rCol,cCol); }
    @Test public void noPk_real_varchar()   throws Exception{ testCorrect(allDataTypes.tableName,rCol,vcCol); }

    @Test public void noPk_double() throws Exception { testCorrect(allDataTypes.tableName, dCol); }
    @Test public void noPk_double_smallint()throws Exception{ testCorrect(allDataTypes.tableName,dCol,siCol); }
    @Test public void noPk_double_int()     throws Exception{ testCorrect(allDataTypes.tableName,dCol,iCol); }
    @Test public void noPk_double_bigint()  throws Exception{ testCorrect(allDataTypes.tableName,dCol,biCol); }
    @Test public void noPk_double_real()    throws Exception{ testCorrect(allDataTypes.tableName,dCol,rCol); }
    @Test public void noPk_double_numeric() throws Exception{ testCorrect(allDataTypes.tableName,dCol,nCol); }
    @Test public void noPk_double_char()    throws Exception{ testCorrect(allDataTypes.tableName,dCol,cCol); }
    @Test public void noPk_double_varchar() throws Exception{ testCorrect(allDataTypes.tableName,dCol,vcCol); }

    @Test public void noPk_numeric() throws Exception { testCorrect(allDataTypes.tableName, nCol); }
    @Test public void noPk_numeric_smallint()throws Exception{ testCorrect(allDataTypes.tableName,nCol,siCol); }
    @Test public void noPk_numeric_int()     throws Exception{ testCorrect(allDataTypes.tableName,nCol,iCol); }
    @Test public void noPk_numeric_bigint()  throws Exception{ testCorrect(allDataTypes.tableName,nCol,biCol); }
    @Test public void noPk_numeric_real()    throws Exception{ testCorrect(allDataTypes.tableName,nCol,rCol); }
    @Test public void noPk_numeric_double()  throws Exception{ testCorrect(allDataTypes.tableName,nCol,dCol); }
    @Test public void noPk_numeric_char()    throws Exception{ testCorrect(allDataTypes.tableName,nCol,cCol); }
    @Test public void noPk_numeric_varchar() throws Exception{ testCorrect(allDataTypes.tableName,nCol,vcCol); }

    @Test public void noPk_char() throws Exception { testCorrect(allDataTypes.tableName, cCol); }
    @Test public void noPk_char_smallint()throws Exception{ testCorrect(allDataTypes.tableName,cCol,siCol); }
    @Test public void noPk_char_int()     throws Exception{ testCorrect(allDataTypes.tableName,cCol,iCol); }
    @Test public void noPk_char_bigint()  throws Exception{ testCorrect(allDataTypes.tableName,cCol,biCol); }
    @Test public void noPk_char_real()    throws Exception{ testCorrect(allDataTypes.tableName,cCol,rCol); }
    @Test public void noPk_char_double()  throws Exception{ testCorrect(allDataTypes.tableName,cCol,dCol); }
    @Test public void noPk_char_numeric() throws Exception{ testCorrect(allDataTypes.tableName,cCol,nCol); }
    @Test public void noPk_char_varchar() throws Exception{ testCorrect(allDataTypes.tableName,cCol,vcCol); }

    @Test public void noPk_varchar() throws Exception { testCorrect(allDataTypes.tableName, vcCol); }
    @Test public void noPk_varchar_smallint()throws Exception{ testCorrect(allDataTypes.tableName,vcCol,siCol); }
    @Test public void noPk_varchar_int()     throws Exception{ testCorrect(allDataTypes.tableName,vcCol,iCol); }
    @Test public void noPk_varchar_bigint()  throws Exception{ testCorrect(allDataTypes.tableName,vcCol,biCol); }
    @Test public void noPk_varchar_real()    throws Exception{ testCorrect(allDataTypes.tableName,vcCol,rCol); }
    @Test public void noPk_varchar_double()  throws Exception{ testCorrect(allDataTypes.tableName,vcCol,dCol); }
    @Test public void noPk_varchar_numeric() throws Exception{ testCorrect(allDataTypes.tableName,vcCol,nCol); }
    @Test public void noPk_varchar_char()    throws Exception{ testCorrect(allDataTypes.tableName,vcCol,cCol); }


    @Test public void pk_smallint() throws Exception { testCorrect(smallintPk.tableName, siCol); }
    @Test public void pk_smallint_int()       throws Exception{ testCorrect(smallintPk.tableName,siCol,iCol); }
    @Test public void pk_smallint_bigint()    throws Exception{ testCorrect(smallintPk.tableName,siCol,biCol); }
    @Test public void pk_smallint_real()      throws Exception{ testCorrect(smallintPk.tableName,siCol,rCol); }
    @Test public void pk_smallint_double()    throws Exception{ testCorrect(smallintPk.tableName,siCol,dCol); }
    @Test public void pk_smallint_numeric()   throws Exception{ testCorrect(smallintPk.tableName,siCol,nCol); }
    @Test public void pk_smallint_char()      throws Exception{ testCorrect(smallintPk.tableName,siCol,cCol); }
    @Test public void pk_smallint_varchar()   throws Exception{ testCorrect(smallintPk.tableName,siCol,vcCol); }

    @Test public void pk_integer() throws Exception { testCorrect(intPk.tableName, iCol); }
    @Test public void pk_int_smallint()  throws Exception{ testCorrect(intPk.tableName,iCol,siCol); }
    @Test public void pk_int_bigint()    throws Exception{ testCorrect(intPk.tableName,iCol,biCol); }
    @Test public void pk_int_real()      throws Exception{ testCorrect(intPk.tableName,iCol,rCol); }
    @Test public void pk_int_double()    throws Exception{ testCorrect(intPk.tableName,iCol,dCol); }
    @Test public void pk_int_numeric()   throws Exception{ testCorrect(intPk.tableName,iCol,nCol); }
    @Test public void pk_int_char()      throws Exception{ testCorrect(intPk.tableName,iCol,cCol); }
    @Test public void pk_int_varchar()   throws Exception{ testCorrect(intPk.tableName,iCol,vcCol); }

    @Test public void pk_bigint() throws Exception { testCorrect(bigintPk.tableName, biCol); }
    @Test public void pk_bigint_smallint()  throws Exception{ testCorrect(bigintPk.tableName,biCol,siCol); }
    @Test public void pk_bigint_int()       throws Exception{ testCorrect(bigintPk.tableName,biCol,iCol); }
    @Test public void pk_bigint_real()      throws Exception{ testCorrect(bigintPk.tableName,biCol,rCol); }
    @Test public void pk_bigint_double()    throws Exception{ testCorrect(bigintPk.tableName,biCol,dCol); }
    @Test public void pk_bigint_numeric()   throws Exception{ testCorrect(bigintPk.tableName,biCol,nCol); }
    @Test public void pk_bigint_char()      throws Exception{ testCorrect(bigintPk.tableName,biCol,cCol); }
    @Test public void pk_bigint_varchar()   throws Exception{ testCorrect(bigintPk.tableName,biCol,vcCol); }

    @Test public void pk_real() throws Exception { testCorrect(realPk.tableName, rCol); }
    @Test public void pk_real_smallint()  throws Exception{ testCorrect(realPk.tableName,rCol,siCol); }
    @Test public void pk_real_int()       throws Exception{ testCorrect(realPk.tableName,rCol,iCol); }
    @Test public void pk_real_bigint()    throws Exception{ testCorrect(realPk.tableName,rCol,biCol); }
    @Test public void pk_real_double()    throws Exception{ testCorrect(realPk.tableName,rCol,dCol); }
    @Test public void pk_real_numeric()   throws Exception{ testCorrect(realPk.tableName,rCol,nCol); }
    @Test public void pk_real_char()      throws Exception{ testCorrect(realPk.tableName,rCol,cCol); }
    @Test public void pk_real_varchar()   throws Exception{ testCorrect(realPk.tableName,rCol,vcCol); }

    @Test public void pk_double() throws Exception { testCorrect(doublePk.tableName, dCol); }
    @Test public void pk_double_smallint()throws Exception{ testCorrect(doublePk.tableName,dCol,siCol); }
    @Test public void pk_double_int()     throws Exception{ testCorrect(doublePk.tableName,dCol,iCol); }
    @Test public void pk_double_bigint()  throws Exception{ testCorrect(doublePk.tableName,dCol,biCol); }
    @Test public void pk_double_real()    throws Exception{ testCorrect(doublePk.tableName,dCol,rCol); }
    @Test public void pk_double_numeric() throws Exception{ testCorrect(doublePk.tableName,dCol,nCol); }
    @Test public void pk_double_char()    throws Exception{ testCorrect(doublePk.tableName,dCol,cCol); }
    @Test public void pk_double_varchar() throws Exception{ testCorrect(doublePk.tableName,dCol,vcCol); }

    @Test public void pk_numeric() throws Exception { testCorrect(numericPk.tableName, nCol); }
    @Test public void pk_numeric_smallint()throws Exception{ testCorrect(numericPk.tableName,nCol,siCol); }
    @Test public void pk_numeric_int()     throws Exception{ testCorrect(numericPk.tableName,nCol,iCol); }
    @Test public void pk_numeric_bigint()  throws Exception{ testCorrect(numericPk.tableName,nCol,biCol); }
    @Test public void pk_numeric_real()    throws Exception{ testCorrect(numericPk.tableName,nCol,rCol); }
    @Test public void pk_numeric_double()  throws Exception{ testCorrect(numericPk.tableName,nCol,dCol); }
    @Test public void pk_numeric_char()    throws Exception{ testCorrect(numericPk.tableName,nCol,cCol); }
    @Test public void pk_numeric_varchar() throws Exception{ testCorrect(numericPk.tableName,nCol,vcCol); }

    @Test public void pk_char() throws Exception { testCorrect(charPk.tableName, cCol); }
    @Test public void pk_char_smallint()throws Exception{ testCorrect(charPk.tableName,cCol,siCol); }
    @Test public void pk_char_int()     throws Exception{ testCorrect(charPk.tableName,cCol,iCol); }
    @Test public void pk_char_bigint()  throws Exception{ testCorrect(charPk.tableName,cCol,biCol); }
    @Test public void pk_char_real()    throws Exception{ testCorrect(charPk.tableName,cCol,rCol); }
    @Test public void pk_char_double()  throws Exception{ testCorrect(charPk.tableName,cCol,dCol); }
    @Test public void pk_char_numeric() throws Exception{ testCorrect(charPk.tableName,cCol,nCol); }
    @Test public void pk_char_varchar() throws Exception{ testCorrect(charPk.tableName,cCol,vcCol); }

    @Test public void pk_varchar() throws Exception { testCorrect(varcharPk.tableName, vcCol); }
    @Test public void pk_varchar_smallint()throws Exception{ testCorrect(varcharPk.tableName,vcCol,siCol); }
    @Test public void pk_varchar_int()     throws Exception{ testCorrect(varcharPk.tableName,vcCol,iCol); }
    @Test public void pk_varchar_bigint()  throws Exception{ testCorrect(varcharPk.tableName,vcCol,biCol); }
    @Test public void pk_varchar_real()    throws Exception{ testCorrect(varcharPk.tableName,vcCol,rCol); }
    @Test public void pk_varchar_double()  throws Exception{ testCorrect(varcharPk.tableName,vcCol,dCol); }
    @Test public void pk_varchar_numeric() throws Exception{ testCorrect(varcharPk.tableName,vcCol,nCol); }
    @Test public void pk_varchar_char()    throws Exception{ testCorrect(varcharPk.tableName,vcCol,cCol); }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void enable(String tableName,String columnName) throws SQLException {
        CallableStatement enableCall = conn.prepareCall("call SYSCS_UTIL.ENABLE_COLUMN_STATISTICS(?,?,?)");
        enableCall.setString(1,schema.schemaName);
        enableCall.setString(2,tableName);
        enableCall.setString(3,columnName.toUpperCase());
        enableCall.execute();
    }

    private void assertCorrectCollectResults(String tableName,ResultSet results) throws SQLException {
        Assert.assertTrue("No rows returned!",results.next());
        Assert.assertEquals("Incorrect schema name!", schema.schemaName, results.getString(1));
        Assert.assertEquals("Incorrect table name!", tableName, results.getString(2));
        Assert.assertEquals("Incorrect # of Regions collected!", 1, results.getInt(3));
        Assert.assertEquals("Incorrect # of tasks executed!", 1, results.getInt(4));
        Assert.assertEquals("Incorrect # of rows collected!",512,results.getInt(5));
        Assert.assertFalse("More than one row returned!",results.next());
    }

    private void assertCorrectTableStatistics(String tableName) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("select * from sys.systablestatistics where schemaname = ? and tablename = ?")) {
            ps.setString(1, schema.schemaName);
            ps.setString(2, tableName);

            try(ResultSet resultSet = ps.executeQuery()) {
                Assert.assertTrue("No rows returned!", resultSet.next());
                Assert.assertEquals("Incorrect schema returned!", schema.schemaName, resultSet.getString(1));
                Assert.assertEquals("Incorrect table returned!", tableName, resultSet.getString(2));
                Assert.assertEquals("Incorrect total row count!", 512, resultSet.getInt(3));
                Assert.assertEquals("Incorrect average row count!", 512, resultSet.getInt(4));
                Assert.assertEquals("Incorrect number of partitions!", 1, resultSet.getInt(6));
                Assert.assertFalse("More than one row returned!", resultSet.next());
            }
        }
    }

    private void assertColumnStatsCorrect(String tableName,String colName,boolean useFloatStrings) throws SQLException {
        colName = colName.toUpperCase();
        try(PreparedStatement ps = conn.prepareStatement("select * from sys.syscolumnstatistics where schemaname = ? and tablename = ? and columnName=?")){
            ps.setString(1,schema.schemaName);
            ps.setString(2,tableName);
            ps.setString(3,colName);

            try(ResultSet rs = ps.executeQuery()){
                Assert.assertTrue("No rows returned!",rs.next());
                Assert.assertEquals("Incorrect schema!", schema.schemaName, rs.getString(1));
                Assert.assertEquals("Incorrect table!", tableName, rs.getString(2));
                Assert.assertEquals("Incorrect Column!", colName, rs.getString(3));
                Assert.assertEquals("Incorrect Null Count!",0,rs.getLong(5));
                if(useFloatStrings)
                    Assert.assertEquals("Incorrect Min!","0.0",rs.getString(7).trim());
                else
                    Assert.assertEquals("Incorrect Min!","0",rs.getString(7).trim());
                Assert.assertFalse("More than one row returned!",rs.next());
            }
        }
    }

    private void testCorrect(String tableName, TestColumn... columns) throws SQLException {
        for(TestColumn column:columns) {
            enable(tableName, column.columnName);
        }
        try(CallableStatement collectCall = conn.prepareCall("call SYSCS_UTIL.COLLECT_TABLE_STATISTICS(?,?,false)")) {
            collectCall.setString(1, schema.schemaName);
            collectCall.setString(2, tableName);
            collectCall.execute();
            try(ResultSet results = collectCall.getResultSet()) {
                assertCorrectCollectResults(tableName, results);
            }
        }

        //now check that tableStats are correct
        assertCorrectTableStatistics(tableName);

        //now check that the column stats are correct
        for(TestColumn column:columns) {
            assertColumnStatsCorrect(tableName, column.columnName,column.useFloatStrings);
        }
    }

    private static class TestColumn{
        String columnName;
        boolean useFloatStrings;

        public static TestColumn create(String columnName,boolean useFloatStrings){
            return new TestColumn(columnName,useFloatStrings);
        }
        public TestColumn(String columnName, boolean useFloatStrings) {
            this.columnName = columnName;
            this.useFloatStrings = useFloatStrings;
        }
    }

}
