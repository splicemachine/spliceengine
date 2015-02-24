package com.splicemachine.derby.impl.sql.execute.operations;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;

/**
 * @author Jeff Cunningham
 *         Date: 2/11/15
 */
public class TruncateFunctionIT {
    private static SpliceWatcher spliceClassWatcher = new SpliceWatcher();

    private static SpliceSchemaWatcher schemaWatcher =
        new SpliceSchemaWatcher(TruncateFunctionIT.class.getSimpleName().toUpperCase());

    private static final String QUALIFIED_TABLE_NAME = schemaWatcher.schemaName + ".trunctest";

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
                                            .around(schemaWatcher);

    @BeforeClass
    public static void createTable() throws Exception {
        new TableCreator(spliceClassWatcher.getOrCreateConnection())
            .withCreate(String.format("create table %s (s varchar(15), d date, t timestamp, n decimal(15, 7), i integer)", QUALIFIED_TABLE_NAME))
            .withInsert(String.format("insert into %s values(?,?,?,?, ?)", QUALIFIED_TABLE_NAME))
            .withRows(rows(
                row("2012-05-23", new SimpleDateFormat("yyyy-MM-dd").parse("1988-12-26"), Timestamp.valueOf("2000-06-07 17:12:30"), 12345.6789, 123321)))
            .create();
    }

    @Test
    public void testSelect() throws Exception {
        String sqlText =
            String.format("SELECT * from %s", QUALIFIED_TABLE_NAME);

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
                "S     |     D     |          T           |      N       |   I   |\n" +
                    "----------------------------------------------------------------------\n" +
                    "2012-05-23 |1988-12-26 |2000-06-07 17:12:30.0 |12345.6789000 |123321 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    //=========================================================================================================
    // Date column
    //=========================================================================================================

    @Test
    public void testTruncDateColumn_Year() throws Exception {
        String sqlText =
            String.format("select trunc(d, 'year') as \"truncd\", d, t, n from %s", QUALIFIED_TABLE_NAME);

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "truncd   |     D     |          T           |      N       |\n" +
                "--------------------------------------------------------------\n" +
                "1988-01-01 |1988-12-26 |2000-06-07 17:12:30.0 |12345.6789000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTruncDateColumn_Month() throws Exception {
        String sqlText =
            String.format("select trunc(d, 'month') as \"truncd\", d, t, n from %s", QUALIFIED_TABLE_NAME);

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "truncd   |     D     |          T           |      N       |\n" +
                "--------------------------------------------------------------\n" +
                "1988-12-01 |1988-12-26 |2000-06-07 17:12:30.0 |12345.6789000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTruncDateColumn_Day() throws Exception {
        String sqlText =
            String.format("select trunc(d, 'day') as \"truncd\", d, t, n from %s", QUALIFIED_TABLE_NAME);

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "truncd   |     D     |          T           |      N       |\n" +
                "--------------------------------------------------------------\n" +
                "1988-12-26 |1988-12-26 |2000-06-07 17:12:30.0 |12345.6789000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTruncDateColumn_Default() throws Exception {
        // defaults to DAY
        String sqlText =
            String.format("select trunc(d) as \"truncd\", d, t, n from %s", QUALIFIED_TABLE_NAME);

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "truncd   |     D     |          T           |      N       |\n" +
                "--------------------------------------------------------------\n" +
                "1988-12-26 |1988-12-26 |2000-06-07 17:12:30.0 |12345.6789000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTruncDateColumn_Hour_invalid() throws Exception {
        String sqlText =
            String.format("select trunc(d, 'hour') as \"truncd\", d, t, n from %s", QUALIFIED_TABLE_NAME);

        try (ResultSet rs = spliceClassWatcher.executeQuery(sqlText)) {
            fail("Expected exception.");
        } catch (Exception e) {
            Assert.assertEquals("The truncate function got an invalid right-side trunc value for operand type DATE: 'HOUR'.",
                                e.getLocalizedMessage());
        }
    }

    //=========================================================================================================
    // Values Date
    //=========================================================================================================

    @Test
    public void testTruncDateValues_Year() throws Exception {
        String sqlText = "values truncate(date('2011-12-26'), 'YEAR')";

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1     |\n" +
                "------------\n" +
                "2011-01-01 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTruncDateValues_Month() throws Exception {
        String sqlText = "values truncate(date('2011-12-26'), 'month')";

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1     |\n" +
                "------------\n" +
                "2011-12-01 |";
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTruncDateValues_Day() throws Exception {
        String sqlText = "values truncate(date('2011-12-26'), 'day')";

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1     |\n" +
                "------------\n" +
                "2011-12-26 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTruncDateValues_Default() throws Exception {
        // defauls to DAY
        String sqlText = "values truncate(date('2011-12-26'))";

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1     |\n" +
                "------------\n" +
                "2011-12-26 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTruncDateValues_Hour_invalid() throws Exception {
        String sqlText = "values truncate(date('2011-12-26'), 'hour')";

        try (ResultSet rs = spliceClassWatcher.executeQuery(sqlText)) {
            fail("Expected exception.");
        } catch (Exception e) {
            Assert.assertEquals("The truncate function got an invalid right-side trunc value for operand type DATE: 'HOUR'.",
                                e.getLocalizedMessage());
        }
    }

    //=========================================================================================================
    // Timestamp column
    //=========================================================================================================

    @Test
    public void testTruncTimstampColumn_Year() throws Exception {
        String sqlText =
            String.format("select trunc(t, 'year') as \"truncd\", d, t, n from %s", QUALIFIED_TABLE_NAME);

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "truncd         |     D     |          T           |      N       |\n" +
                "-------------------------------------------------------------------------\n" +
                "2000-01-01 00:00:00.0 |1988-12-26 |2000-06-07 17:12:30.0 |12345.6789000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTruncTimestampColumn_Month() throws Exception {
        String sqlText =
            String.format("select trunc(t, 'month') as \"truncd\", d, t, n from %s", QUALIFIED_TABLE_NAME);

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "truncd         |     D     |          T           |      N       |\n" +
                "-------------------------------------------------------------------------\n" +
                "2000-06-01 00:00:00.0 |1988-12-26 |2000-06-07 17:12:30.0 |12345.6789000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTruncTimestampColumn_Day() throws Exception {
        String sqlText =
            String.format("select trunc(t, 'day') as \"truncd\", d, t, n from %s", QUALIFIED_TABLE_NAME);

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "truncd         |     D     |          T           |      N       |\n" +
                "-------------------------------------------------------------------------\n" +
                "2000-06-07 00:00:00.0 |1988-12-26 |2000-06-07 17:12:30.0 |12345.6789000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTruncTimestampColumn_Default() throws Exception {
        // defaults to DAY
        String sqlText =
            String.format("select trunc(t) as \"truncd\", d, t, n from %s", QUALIFIED_TABLE_NAME);

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "truncd         |     D     |          T           |      N       |\n" +
                "-------------------------------------------------------------------------\n" +
                "2000-06-07 00:00:00.0 |1988-12-26 |2000-06-07 17:12:30.0 |12345.6789000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTruncTimestampColumn_Hour() throws Exception {
        String sqlText =
            String.format("select trunc(t, 'hour') as \"truncd\", d, t, n from %s", QUALIFIED_TABLE_NAME);

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "truncd         |     D     |          T           |      N       |\n" +
                "-------------------------------------------------------------------------\n" +
                "2000-06-07 17:00:00.0 |1988-12-26 |2000-06-07 17:12:30.0 |12345.6789000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTruncTimestampColumnMin() throws Exception {
        String sqlText =
            String.format("select trunc(t, 'MINUTE') as \"truncd\", d, t, n from %s", QUALIFIED_TABLE_NAME);

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "truncd         |     D     |          T           |      N       |\n" +
                "-------------------------------------------------------------------------\n" +
                "2000-06-07 17:12:00.0 |1988-12-26 |2000-06-07 17:12:30.0 |12345.6789000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTruncTimestampColumn_Min_invalid() throws Exception {
        String sqlText =
            String.format("select trunc(t, 'minuite') as \"truncd\", d, t, n from %s", QUALIFIED_TABLE_NAME);

        try (ResultSet rs = spliceClassWatcher.executeQuery(sqlText)) {
            fail("Expected exception.");
        } catch (Exception e) {
            Assert.assertEquals("The truncate function got an unknown right-side trunc value for operand type 'TIMESTAMP or DATE': 'MINUITE'. Acceptable values are: 'YEAR, YR, MONTH, MON, MO, DAY, HOUR, HR, MINUTE, MIN, SECOND, SEC, MILLISECOND, MILLI, NANOSECOND'.",
                                e.getLocalizedMessage());
        }
    }

    @Test
    public void testTruncTimestampColumn_Sec() throws Exception {
        String sqlText =
            String.format("select trunc(t, 'second') as \"truncd\", d, t, n from %s", QUALIFIED_TABLE_NAME);

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "truncd         |     D     |          T           |      N       |\n" +
                "-------------------------------------------------------------------------\n" +
                "2000-06-07 17:12:30.0 |1988-12-26 |2000-06-07 17:12:30.0 |12345.6789000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    //=========================================================================================================
    // Values Timestamp
    //=========================================================================================================

    @Test
    public void testTruncTimestampValues_Year() throws Exception {
        String sqlText = "values truncate(timestamp('2011-12-26', '17:13:30'), 'YEAR')";

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1           |\n" +
                "-----------------------\n" +
                "2011-01-01 00:00:00.0 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTruncTimestampValues_Month() throws Exception {
        String sqlText = "values truncate(timestamp('2011-12-26', '17:13:30'), 'month')";

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1           |\n" +
                "-----------------------\n" +
                "2011-12-01 00:00:00.0 |";
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTruncTimestampValues_Default() throws Exception {
        // defaults to DAY
        String sqlText = "values truncate(timestamp('2011-12-26', '17:13:30'))";

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1           |\n" +
                "-----------------------\n" +
                "2011-12-26 00:00:00.0 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTruncTimestampValues_Day() throws Exception {
        String sqlText = "values truncate(timestamp('2011-12-26', '17:13:30'), 'day')";

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1           |\n" +
                "-----------------------\n" +
                "2011-12-26 00:00:00.0 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTruncTimestampValues_Hour() throws Exception {
        String sqlText = "values truncate(timestamp('2011-12-26', '17:13:30'), 'hour')";

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1           |\n" +
                "-----------------------\n" +
                "2011-12-26 17:00:00.0 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTruncTimestampValues_Minute() throws Exception {
        String sqlText = "values truncate(timestamp('2011-12-26', '17:13:30'), 'minute')";

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1           |\n" +
                "-----------------------\n" +
                "2011-12-26 17:13:00.0 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTruncTimestampValues_Second() throws Exception {
        String sqlText = "values truncate(timestamp('2011-12-26', '17:13:30'), 'second')";

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1           |\n" +
                "-----------------------\n" +
                "2011-12-26 17:13:30.0 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testAlternativeTruncValues() throws Exception {
        // just making sure we don't get an exception using these alternative trunc value strings
        String sqlText = "values truncate(timestamp('2011-12-26', '17:13:30'), 'yr')";
        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);
        rs.close();

        sqlText = "values truncate(timestamp('2011-12-26', '17:13:30'), 'mon')";
        rs = spliceClassWatcher.executeQuery(sqlText);
        rs.close();

        sqlText = "values truncate(timestamp('2011-12-26', '17:13:30'), 'mo')";
        rs = spliceClassWatcher.executeQuery(sqlText);
        rs.close();

        sqlText = "values truncate(timestamp('2011-12-26', '17:13:30'), 'hr')";
        rs = spliceClassWatcher.executeQuery(sqlText);
        rs.close();

        sqlText = "values truncate(timestamp('2011-12-26', '17:13:30'), 'min')";
        rs = spliceClassWatcher.executeQuery(sqlText);
        rs.close();

        sqlText = "values truncate(timestamp('2011-12-26', '17:13:30'), 'sec')";
        rs = spliceClassWatcher.executeQuery(sqlText);
        rs.close();
    }

    //=========================================================================================================
    // Column Decimal
    //=========================================================================================================

    @Test
    public void testTruncDecimalColumn1() throws Exception {
        String sqlText =
            String.format("select trunc(n, 1), n from %s", QUALIFIED_TABLE_NAME);

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1       |      N       |\n" +
                "------------------------------\n" +
                "12345.6000000 |12345.6789000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTruncDecimalColumn2() throws Exception {
        String sqlText =
            String.format("select trunc(n, 2), n from %s", QUALIFIED_TABLE_NAME);

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1       |      N       |\n" +
                "------------------------------\n" +
                "12345.6700000 |12345.6789000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTruncDecimalColumnMinus2() throws Exception {
        String sqlText =
            String.format("select trunc(n, -2), n from %s", QUALIFIED_TABLE_NAME);

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1       |      N       |\n" +
                "------------------------------\n" +
                "12300.0000000 |12345.6789000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTruncDecimalColumnZero() throws Exception {
        String sqlText =
            String.format("select trunc(n, 0), n from %s", QUALIFIED_TABLE_NAME);

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1       |      N       |\n" +
                "------------------------------\n" +
                "12345.0000000 |12345.6789000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTruncDecimalColumnEmpty() throws Exception {
        String sqlText =
            String.format("select trunc(n, ), n from %s", QUALIFIED_TABLE_NAME);

        try (ResultSet rs = spliceClassWatcher.executeQuery(sqlText)) {
            fail("Expected exception.");
        } catch (Exception e) {
            Assert.assertEquals("Syntax error: Encountered \")\" at line 1, column 17.",
                                e.getLocalizedMessage());
        }
    }

    @Test
    public void testTruncDecimalColumnNull() throws Exception {
        String sqlText =
            String.format("select trunc(n, null), n from %s", QUALIFIED_TABLE_NAME);

        try (ResultSet rs = spliceClassWatcher.executeQuery(sqlText)) {
            fail("Expected exception.");
        } catch (Exception e) {
            Assert.assertEquals("Syntax error: Encountered \"null\" at line 1, column 17.",
                                e.getLocalizedMessage());
        }
    }

    @Test
    public void testTruncDecimalColumnNonNumeric() throws Exception {
        String sqlText =
            String.format("select trunc(n, x), n from %s", QUALIFIED_TABLE_NAME);

        try (ResultSet rs = spliceClassWatcher.executeQuery(sqlText)) {
            fail("Expected exception.");
        } catch (Exception e) {
            Assert.assertEquals("Column 'X' is either not in any table in the FROM list or appears within a join " +
                                    "specification and is outside the scope of the join specification or appears in " +
                                    "a HAVING clause and is not in the GROUP BY list. If this is a CREATE or " +
                                    "ALTER TABLE  statement then 'X' is not a column in the target table.",
                                e.getLocalizedMessage());
        }
    }

    @Test
    public void testTruncDecimalColumnGreaterThanPrecision() throws Exception {
        String sqlText =
            String.format("select trunc(n, 8), n from %s", QUALIFIED_TABLE_NAME);

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1       |      N       |\n" +
                "------------------------------\n" +
                "12345.6789000 |12345.6789000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTruncDecimalColumnNegativeGreaterThanPrecision() throws Exception {
        String sqlText = String.format("select trunc(n, -5), n from %s", QUALIFIED_TABLE_NAME);

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1  |      N       |\n" +
                "---------------------\n" +
                "0E-7 |12345.6789000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTruncIntegerColumn1() throws Exception {
        String sqlText =
            String.format("select trunc(i, 1), i from %s", QUALIFIED_TABLE_NAME);

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1   |   I   |\n" +
                "----------------\n" +
                "123321 |123321 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTruncIntegerColumnNegative1() throws Exception {
        String sqlText =
            String.format("select trunc(i, -1), i from %s", QUALIFIED_TABLE_NAME);

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1   |   I   |\n" +
                "----------------\n" +
                "123320 |123321 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    //=========================================================================================================
    // Value Decimal
    //=========================================================================================================

    @Test
    public void testTruncDecimalValue1() throws Exception {
        String sqlText =  "values truncate(12345.6789, 1)";

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1     |\n" +
                "------------\n" +
                "12345.6000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTruncDecimalValue2() throws Exception {
        String sqlText =  "values truncate(12345.6789, 2)";

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1     |\n" +
                "------------\n" +
                "12345.6700 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTruncDecimalValueNegative3() throws Exception {
        String sqlText =  "values truncate(12345.6789, -3)";

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1     |\n" +
                "------------\n" +
                "12000.0000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTruncIntegerValue3() throws Exception {
        String sqlText =  "values truncate(123321, 3)";

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1   |\n" +
                "--------\n" +
                "123321 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTruncIntegerValueNegative3() throws Exception {
        String sqlText =  "values truncate(123321, -3)";

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1   |\n" +
                "--------\n" +
                "123000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTruncIntegerValueNegativeGreaterThanPrecision() throws Exception {
        String sqlText =  "values truncate(123321, -7)";

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1 |\n" +
                "----\n" +
                " 0 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testTruncDecimalValueNonNumeric() throws Exception {
        String sqlText =  "values truncate(12345.6789, x)";

        try (ResultSet rs = spliceClassWatcher.executeQuery(sqlText)) {
            fail("Expected exception.");
        } catch (Exception e) {
            Assert.assertEquals("The truncate function expects a right-side argument of type INTEGER for an operand " +
                                    "of type DECIMAL but got: 'columnName: X\n" +
                                    "tableNumber: -1\n" +
                                    "columnNumber: 0\n" +
                                    "replacesAggregate: false\n" +
                                    "replacesWindowFunctionCall: false\n" +
                                    "tableName: null\n" +
                                    "nestingLevel: -1\n" +
                                    "sourceLevel: -1\n" +
                                    "dataTypeServices: null\n" +
                                    "'.",
                                e.getLocalizedMessage());
        }
    }

}
