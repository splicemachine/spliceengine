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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.apache.commons.dbutils.DbUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.fail;

/**
 * @author Jeff Cunningham
 *         Date: 2/19/15
 */
public class SimpleDateArithmeticIT {
    private static SpliceWatcher spliceClassWatcher = new SpliceWatcher();

    private static SpliceSchemaWatcher schemaWatcher =
        new SpliceSchemaWatcher(SimpleDateArithmeticIT.class.getSimpleName().toUpperCase());

    private static final String QUALIFIED_TABLE_NAME = schemaWatcher.schemaName + ".date_add_test";
    private static final String QUALIFIED_TABLE_NAME2 = schemaWatcher.schemaName + ".old_date_test";
    private static final String QUALIFIED_TIME_TABLE_NAME = schemaWatcher.schemaName + ".time_test";

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
                                            .around(schemaWatcher);

    @BeforeClass
    public static void createTables() throws Exception {
        new TableCreator(spliceClassWatcher.getOrCreateConnection())
        .withCreate(String.format("create table %s (s varchar(15), d date, t timestamp, t2 timestamp)", QUALIFIED_TABLE_NAME))
        .withInsert(String.format("insert into %s values(?,?,?,?)", QUALIFIED_TABLE_NAME))
        .withRows(rows(
        row("2012-05-23", new SimpleDateFormat("yyyy-MM-dd").parse("1988-12-26"), Timestamp.valueOf("2000-06-07 17:12:30"), Timestamp.valueOf("2012-12-13 00:00:00"))))
        .create();

        // Insert nulls into old_date_test to test DB-8290, which fixes a bug where
        // MERGE_DATA_FROM_FILE will not overwrite null columns in a PK table.
        new TableCreator(spliceClassWatcher.getOrCreateConnection())
        .withCreate(String.format("create table %s (int_pk int, d date, primary key(int_pk))", QUALIFIED_TABLE_NAME2))
        .withInsert(String.format("insert into %s values(?,?)", QUALIFIED_TABLE_NAME2))
        .withRows(rows(row(1, null), row(2, null), row(3, null),
                       row(4, null), row(5, null), row(6, null)))
        .create();
        spliceClassWatcher.commit();

        String dataDir = SpliceUnitTest.getResourceDirectory() + "date.csv";

        String sqlText = String.format("CALL SYSCS_UTIL.MERGE_DATA_FROM_FILE('" + schemaWatcher.schemaName + "','OLD_DATE_TEST',null,'%s',null,null,'yyyy-MM-dd HH:mm:ss.SSS','yyyy-MM-dd',null,0,'/BAD',false,null)", dataDir);
        try {
            CallableStatement cs = spliceClassWatcher.prepareCall(sqlText);
            ResultSet rs = cs.executeQuery();
            DbUtils.closeQuietly(rs);
        }
        catch (Exception e) {
            Assert.fail("Failed to merge data to table old_date_test.");
        }

        dataDir = SpliceUnitTest.getResourceDirectory() + "time.csv";
        new TableCreator(spliceClassWatcher.getOrCreateConnection())
        .withCreate(String.format("create table %s (t time, primary key(t))", QUALIFIED_TIME_TABLE_NAME))
        .create();
        spliceClassWatcher.commit();

        dataDir = SpliceUnitTest.getResourceDirectory() + "time.csv";

        sqlText = String.format("CALL SYSCS_UTIL.MERGE_DATA_FROM_FILE('" + schemaWatcher.schemaName + "','TIME_TEST',null,'%s',null,null,'yyyy-MM-dd HH:mm:ss.SSS','yyyy-MM-dd','HH:mm:ss',0,'/BAD',false,null)", dataDir);
        try {
            CallableStatement cs = spliceClassWatcher.prepareCall(sqlText);
            ResultSet rs = cs.executeQuery();
            DbUtils.closeQuietly(rs);
        }
        catch (Exception e) {
            Assert.fail("Failed to merge data to table time_test.");
        }
        spliceClassWatcher.commit();

    }

    //=========================================================================================================
    // Date column
    //=========================================================================================================

    @Test
    public void testOldDates() throws Exception {
        String sqlText =
        String.format("select d from %s order by 1", QUALIFIED_TABLE_NAME2);

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
        "D     |\n" +
        "------------\n" +
        "1453-05-29 |\n" +
        "1775-04-19 |\n" +
        "1776-07-04 |\n" +
        "1783-09-03 |\n" +
        "1791-12-05 |\n" +
        "1861-04-12 |\n" +
        "1865-05-13 |";
        Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testPlusDateColumn() throws Exception {
        String sqlText =
            String.format("select d + 1 from  %s", QUALIFIED_TABLE_NAME);

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1     |\n" +
                "------------\n" +
                "1988-12-27 |";
        Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testMinusDateColumn() throws Exception {
        String sqlText =
            String.format("select d + 1 from  %s", QUALIFIED_TABLE_NAME);

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1     |\n" +
                "------------\n" +
                "1988-12-27 |";
        Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testPlusDateColumnCommutative() throws Exception {
        String sqlText =
            String.format("select 4 + d from  %s", QUALIFIED_TABLE_NAME);

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1     |\n" +
                "------------\n" +
                "1988-12-30 |";
        Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }
    @Test
    public void testMinusDateColumnNotCommutative() throws Exception {
        String sqlText =
            String.format("select 4 - d from  %s", QUALIFIED_TABLE_NAME);

        try (ResultSet rs = spliceClassWatcher.executeQuery(sqlText)) {
            fail("Expected exception.");
        } catch (Exception e) {
            Assert.assertEquals("The '-' operator with a left operand type of 'INTEGER' and a right operand type of 'DATE' is not supported.",
                                e.getLocalizedMessage());
        }
    }

    @Test
    public void testCurrentDateMinusDateColumn() throws Exception {
        String sqlText = String.format("select current_date - d from  %s", QUALIFIED_TABLE_NAME);
        int rows = 0;
        try (ResultSet rs = spliceClassWatcher.executeQuery(sqlText)) {
            // can't use static result compare with CURRENT_DATE funct. Best we can do is count rows.
            ++rows;
        }
        Assert.assertTrue("\n" + sqlText + "\nExpected at least one row.", rows > 0);
    }

    @Test
    public void testDateColumnMinusCurrentDate() throws Exception {
        String sqlText = String.format("select d - current_date from  %s", QUALIFIED_TABLE_NAME);
        int rows = 0;
        try (ResultSet rs = spliceClassWatcher.executeQuery(sqlText)) {
            // can't use static result compare with CURRENT_DATE funct. Best we can do is count rows.
            ++rows;
        }
        Assert.assertTrue("\n" + sqlText + "\nExpected at least one row.", rows > 0);
    }

    //=========================================================================================================
    // Values Date
    //=========================================================================================================

    @Test
    public void testPlusDateValues() throws Exception {
        String sqlText = "values  date('2011-12-26') + 1";

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1     |\n" +
                "------------\n" +
                "2011-12-27 |";
        Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testMinusDateValues() throws Exception {
        String sqlText = "values  date('2011-12-26') - 1";

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1     |\n" +
                "------------\n" +
                "2011-12-25 |";
        Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testDateMinusDatePositiveValues() throws Exception {
        String sqlText = "values  date('2011-12-26') - date('2011-06-05')";

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1  |\n" +
                "-----\n" +
                "204 |";
        Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testDateMinusDateNegativeValues() throws Exception {
        String sqlText = "values  date('2011-06-05') - date('2011-12-26')";

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1  |\n" +
                "------\n" +
                "-204 |";
        Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testDatePlusDateValuesError() throws Exception {
        String sqlText = "values  date('2011-12-26') + date('2011-06-05')";

        try (ResultSet rs = spliceClassWatcher.executeQuery(sqlText)) {
            fail("Expected exception.");
        } catch (Exception e) {
            Assert.assertEquals("DATEs cannot be added. The operation is undefined.",
                                e.getLocalizedMessage());
        }
    }

    @Test
    public void testMultiplyPlusDateValuesError() throws Exception {
        String sqlText = "values  date('2011-12-26') * date('2011-06-05')";

        try (ResultSet rs = spliceClassWatcher.executeQuery(sqlText)) {
            fail("Expected exception.");
        } catch (Exception e) {
            Assert.assertEquals("DATEs cannot be multiplied or divided. The operation is undefined.",
                                e.getLocalizedMessage());
        }
    }

    @Test
    public void testDateDivideDateValuesError() throws Exception {
        String sqlText = "values  date('2011-12-26') / date('2011-06-05')";

        try (ResultSet rs = spliceClassWatcher.executeQuery(sqlText)) {
            fail("Expected exception.");
        } catch (Exception e) {
            Assert.assertEquals("DATEs cannot be multiplied or divided. The operation is undefined.",
                                e.getLocalizedMessage());
        }
    }

    @Test
    public void testDateWithCurrentDateValues() throws Exception {
        String sqlText = "values  (current_date - 1) - current_date + 2";
        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1 |\n" +
                "----\n" +
                " 1 |";
        Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    //=========================================================================================================
    // Timestamp column
    //=========================================================================================================

    @Test
    public void testPlusTimestampColumn() throws Exception {
        String sqlText =
            String.format("select t + 1 from  %s", QUALIFIED_TABLE_NAME);

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1           |\n" +
                "-----------------------\n" +
                "2000-06-08 17:12:30.0 |";
        Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testMinusTimestampColumn() throws Exception {
        String sqlText =
            String.format("select t - 1 from  %s", QUALIFIED_TABLE_NAME);

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1           |\n" +
                "-----------------------\n" +
                "2000-06-06 17:12:30.0 |";
        Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testPlusTimestampColumnCommutative() throws Exception {
        String sqlText =
            String.format("select 4 + t from  %s", QUALIFIED_TABLE_NAME);

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1           |\n" +
                "-----------------------\n" +
                "2000-06-11 17:12:30.0 |";
        Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }
    @Test
    public void testMinusTimestampColumnNotCommutative() throws Exception {
        String sqlText =
            String.format("select 4 - t from  %s", QUALIFIED_TABLE_NAME);

        try (ResultSet rs = spliceClassWatcher.executeQuery(sqlText)) {
            fail("Expected exception.");
        } catch (Exception e) {
            Assert.assertEquals("The '-' operator with a left operand type of 'INTEGER' and a right operand type of 'TIMESTAMP' is not supported.",
                                e.getLocalizedMessage());
        }
    }

    //=========================================================================================================
    // Values Timestamp
    //=========================================================================================================

    @Test
    public void testPlusTimestampValues() throws Exception {
        String sqlText = "values  timestamp('2011-12-26', '17:13:30') + 1";

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1           |\n" +
                "-----------------------\n" +
                "2011-12-27 17:13:30.0 |";
        Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testMinusTimestampValues() throws Exception {
        String sqlText = "values  timestamp('2011-12-26', '17:13:30') - 1";

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1           |\n" +
                "-----------------------\n" +
                "2011-12-25 17:13:30.0 |";
        Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTimestampMinusTimestampPositiveValues() throws Exception {
        String sqlText = "values  timestamp('2011-12-26', '17:13:30') - timestamp('2011-06-05', '05:06:00')";

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1  |\n" +
                "-----\n" +
                "204 |";
        Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTimestampMinusTimestampNegativeValues() throws Exception {
        String sqlText = "values timestamp('2011-06-05', '05:06:00') - timestamp('2011-12-26', '17:13:30')";

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1  |\n" +
                "------\n" +
                "-204 |";
        Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testIntegerPlusTimestampValues() throws Exception {
        // DB-2943
        String sqlText = "values 1+ timestamp('2011-06-05', '05:06:00')";

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1           |\n" +
                "-----------------------\n" +
                "2011-06-06 05:06:00.0 |";
        Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTimestampPlusTimestampValuesError() throws Exception {
        String sqlText = "values  timestamp('2011-12-26', '17:13:30') + timestamp('2011-06-05', '05:06:00')";

        try (ResultSet rs = spliceClassWatcher.executeQuery(sqlText)) {
            fail("Expected exception.");
        } catch (Exception e) {
            Assert.assertEquals("TIMESTAMPs cannot be added. The operation is undefined.",
                                e.getLocalizedMessage());
        }
    }

    @Test
    public void testMultiplyPlusTimestampValuesError() throws Exception {
        String sqlText = "values  timestamp('2011-12-26', '17:13:30') * timestamp('2011-06-05', '05:06:00')";

        try (ResultSet rs = spliceClassWatcher.executeQuery(sqlText)) {
            fail("Expected exception.");
        } catch (Exception e) {
            Assert.assertEquals("TIMESTAMPs cannot be multiplied or divided. The operation is undefined.",
                                e.getLocalizedMessage());
        }
    }

    @Test
    public void testTimestampDivideTimestampValuesError() throws Exception {
        String sqlText = "values  timestamp('2011-12-26', '17:13:30') / timestamp('2011-06-05', '05:06:00')";

        try (ResultSet rs = spliceClassWatcher.executeQuery(sqlText)) {
            fail("Expected exception.");
        } catch (Exception e) {
            Assert.assertEquals("TIMESTAMPs cannot be multiplied or divided. The operation is undefined.",
                                e.getLocalizedMessage());
        }
    }

    //=========================================================================================================
    // Values Timestamp/Date mixed and interesting edge cases
    //=========================================================================================================

    @Test
    public void testDateMinusTimestampValues() throws Exception {
        String sqlText = "values  date('2011-06-04') - timestamp('2011-06-05', '05:06:00')";

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1 |\n" +
                "----\n" +
                "-1 |";
        Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTimestampMinusDateValues() throws Exception {
        String sqlText = "values  timestamp('2011-06-05', '05:06:00') - date('2011-12-26')";

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1  |\n" +
                "------\n" +
                "-203 |";
        Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testAddDateOnLeapYear() throws Exception {
        String sqlText = "values  Date('2016-02-28') + 1";

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1     |\n" +
                "------------\n" +
                "2016-02-29 |";
        Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testAddDateOnLeapYear2() throws Exception {
        String sqlText = "values Date('2016-02-28') + 2 - 1";

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1     |\n" +
                "------------\n" +
                "2016-02-29 |";
        Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testAddDateOnLeapYear3() throws Exception {
        String sqlText = "values timestamp('2016-03-28', '22:13:13') - 28";

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1           |\n" +
                "-----------------------\n" +
                "2016-02-29 22:13:13.0 |";
        Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTimestampGTDateCompare() throws Exception {
        String sqlText = String.format("select t from %s where t > '2000-04-01'", QUALIFIED_TABLE_NAME);

        try (ResultSet rs = spliceClassWatcher.executeQuery(sqlText)) {

            String expected =
                "T           |\n" +
                    "-----------------------\n" +
                    "2000-06-07 17:12:30.0 |";
            Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testTimestampLTDateCompare() throws Exception {
        String sqlText =  String.format("select t from %s where t < '2000-07-01'", QUALIFIED_TABLE_NAME);

        try (ResultSet rs = spliceClassWatcher.executeQuery(sqlText)) {

            String expected =
                "T           |\n" +
                    "-----------------------\n" +
                    "2000-06-07 17:12:30.0 |";
            Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testTimestampNETDateCompare() throws Exception {
        String sqlText =  String.format("select t from %s where t != '2000-06-07'", QUALIFIED_TABLE_NAME);

        try (ResultSet rs = spliceClassWatcher.executeQuery(sqlText)) {

            String expected =
                "T           |\n" +
                    "-----------------------\n" +
                    "2000-06-07 17:12:30.0 |";
            Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testTimestampETDateCompare() throws Exception {
        String sqlText =  String.format("select t2 from %s where t2 = '2012-12-13'", QUALIFIED_TABLE_NAME);

        try (ResultSet rs = spliceClassWatcher.executeQuery(sqlText)) {

            String expected =
                "T2           |\n" +
                    "-----------------------\n" +
                    "2012-12-13 00:00:00.0 |";
            Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testDateGTTimestampCompareColummn() throws Exception {
        String sqlText =  String.format("select d from %s where d > '1988-11-24 00:00:00'", QUALIFIED_TABLE_NAME);

        try (ResultSet rs = spliceClassWatcher.executeQuery(sqlText)) {

            String expected =
                "D     |\n" +
                    "------------\n" +
                    "1988-12-26 |";
            Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testDateGTTimestampCompareValues() throws Exception {
        String sqlText = "values date('2088-12-26') > '2088-12-25 00:00:00'";

        try (ResultSet rs = spliceClassWatcher.executeQuery(sqlText)) {

            String expected =
                "1  |\n" +
                    "------\n" +
                    "true |";
            Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testDateLTTimestampCompare() throws Exception {
        String sqlText =  String.format("select d from %s where d < '1988-12-29 00:00:00'", QUALIFIED_TABLE_NAME);

        try (ResultSet rs = spliceClassWatcher.executeQuery(sqlText)) {

            String expected =
                "D     |\n" +
                    "------------\n" +
                    "1988-12-26 |";
            Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testDateNETTimestampCompare() throws Exception {
        String sqlText =  String.format("select d from %s where d != '1988-12-25 00:00:00'", QUALIFIED_TABLE_NAME);

        try (ResultSet rs = spliceClassWatcher.executeQuery(sqlText)) {

            String expected =
                "D     |\n" +
                    "------------\n" +
                    "1988-12-26 |";
            Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testDateETTimestampCompareColumn() throws Exception {
        String sqlText =  String.format("select d from %s where d = '1988-12-26 00:00:00'", QUALIFIED_TABLE_NAME);

        try (ResultSet rs = spliceClassWatcher.executeQuery(sqlText)) {

            String expected =
                "D     |\n" +
                    "------------\n" +
                    "1988-12-26 |";
            Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testDateETTimestampCompareValues() throws Exception {
        String sqlText = "values date('2088-12-26') = '2088-12-26 00:00:00'";

        try (ResultSet rs = spliceClassWatcher.executeQuery(sqlText)) {

            String expected =
                "1  |\n" +
                    "------\n" +
                    "true |";
            Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testCastToTimestamp() throws Exception {
        String sqlText = "values cast(add_months('1993-07-01',3) as timestamp)";

        try (ResultSet rs = spliceClassWatcher.executeQuery(sqlText)) {

            String expected =
                "1           |\n" +
                    "-----------------------\n" +
                    "1993-10-01 00:00:00.0 |";
            Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testCastToDate() throws Exception {
        String sqlText = "values cast(add_months('1993-07-01 00:00:00',3) as date)";

        try (ResultSet rs = spliceClassWatcher.executeQuery(sqlText)) {

            String expected =
                "1     |\n" +
                    "------------\n" +
                    "1993-10-01 |";
            Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }


    //=========================================================================================================
    // Time column
    //=========================================================================================================

    @Test
    public void testTimeColumn() throws Exception {
        String sqlText =
        String.format("select t from %s order by 1", QUALIFIED_TIME_TABLE_NAME);

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
        "T    |\n" +
        "----------\n" +
        "00:00:00 |\n" +
        "00:00:01 |\n" +
        "00:00:59 |\n" +
        "00:01:01 |\n" +
        "01:01:01 |\n" +
        "05:55:55 |\n" +
        "12:01:01 |\n" +
        "15:09:02 |\n" +
        "20:15:59 |\n" +
        "22:01:00 |\n" +
        "23:00:01 |\n" +
        "23:59:59 |";
        Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

}

