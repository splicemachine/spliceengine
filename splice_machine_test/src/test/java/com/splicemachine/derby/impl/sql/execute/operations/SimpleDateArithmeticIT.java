package com.splicemachine.derby.impl.sql.execute.operations;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.fail;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;

/**
 * @author Jeff Cunningham
 *         Date: 2/19/15
 */
public class SimpleDateArithmeticIT {
    private static SpliceWatcher spliceClassWatcher = new SpliceWatcher();

    private static SpliceSchemaWatcher schemaWatcher =
        new SpliceSchemaWatcher(SimpleDateArithmeticIT.class.getSimpleName().toUpperCase());

    private static final String QUALIFIED_TABLE_NAME = schemaWatcher.schemaName + ".date_add_test";

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
                                            .around(schemaWatcher);

    @BeforeClass
    public static void createTable() throws Exception {
        new TableCreator(spliceClassWatcher.getOrCreateConnection())
            .withCreate(String.format("create table %s (s varchar(15), d date, t timestamp)", QUALIFIED_TABLE_NAME))
            .withInsert(String.format("insert into %s values(?,?,?)", QUALIFIED_TABLE_NAME))
            .withRows(rows(
                row("2012-05-23", new SimpleDateFormat("yyyy-MM-dd").parse("1988-12-26"), Timestamp.valueOf("2000-06-07 17:12:30"))))
            .create();
    }

    @Test
    public void testSelect() throws Exception {
        String sqlText =
            String.format("SELECT * from %s", QUALIFIED_TABLE_NAME);

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "S     |     D     |          T           |\n" +
                "-----------------------------------------------\n" +
                "2012-05-23 |1988-12-26 |2000-06-07 17:12:30.0 |";
        Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    //=========================================================================================================
    // Date column
    //=========================================================================================================

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
        String sqlText = "values  Date('2016-02-28') + 2 - 1";

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
        String sqlText = "values  timestamp('2016-03-28', '22:13:13') - 28";

        ResultSet rs = spliceClassWatcher.executeQuery(sqlText);

        String expected =
            "1           |\n" +
                "-----------------------\n" +
                "2016-02-29 22:13:13.0 |";
        Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }
}

