package com.splicemachine.derby.utils;


import static org.junit.Assert.assertEquals;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;

public class SpliceDateFunctionsIT {

    private static final String CLASS_NAME = SpliceDateFunctionsIT.class.getSimpleName().toUpperCase();
    private static SpliceWatcher classWatcher = new SpliceWatcher(CLASS_NAME);

    private static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    // Table for ADD_MONTHS testing.
    private static final SpliceTableWatcher tableWatcherA = new SpliceTableWatcher(
    	"A", schemaWatcher.schemaName, "(col1 date, col2 int, col3 date)");
    //Table for TO_DATE testing
    private static final SpliceTableWatcher tableWatcherB = new SpliceTableWatcher(
        "B", schemaWatcher.schemaName, "(col1 varchar(10), col2 varchar(10), col3 date)");
    //Table for last_day testing
    private static final SpliceTableWatcher tableWatcherC = new SpliceTableWatcher(
            "C", schemaWatcher.schemaName, "(col1 date, col2 date)");
    //Table for next_day testing
    private static final SpliceTableWatcher tableWatcherD = new SpliceTableWatcher(
            "D", schemaWatcher.schemaName, "(col1 date, col2 varchar(10), col3 date)");
    //Table for month_between testing
    private static final SpliceTableWatcher tableWatcherE = new SpliceTableWatcher(
            "E", schemaWatcher.schemaName, "(col1 date, col2 date, col3 double)");
    //Table for to_char testing
    private static final SpliceTableWatcher tableWatcherF = new SpliceTableWatcher(
            "F", schemaWatcher.schemaName, "(col1 date, col2 varchar(10), col3 varchar(10))");
    private static final SpliceTableWatcher tableWatcherG = new SpliceTableWatcher(
            "G", schemaWatcher.schemaName, "(col1 Timestamp, col2 varchar(10), col3 Timestamp)");
    private static final SpliceTableWatcher tableWatcherH = new SpliceTableWatcher(
            "H", schemaWatcher.schemaName, "(col1 Timestamp, col2 varchar(10), col3 varchar(10))");
    private static final SpliceTableWatcher tableWatcherI = new SpliceTableWatcher(
            "I", schemaWatcher.schemaName, "(ts Timestamp, pat varchar(30), datestr varchar(30))");
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher)
            .around(tableWatcherA)
            .around(tableWatcherB)
            .around(tableWatcherC)
            .around(tableWatcherD)
            .around(tableWatcherE)
            .around(tableWatcherF)
            .around(tableWatcherG)
            .around(tableWatcherH)
            .around(tableWatcherI)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        // Each of the following inserted rows represents an individual test,
                        // including expected result (column 'col3'), for less test code in the
                        // test methods
                        classWatcher.prepareStatement(
                            "insert into " + tableWatcherA + " (col1, col2, col3) values (date('2014-01-15'), 1, date" +
                                "('2014-02-15'))").execute();
                        classWatcher.prepareStatement(
                            "insert into " + tableWatcherA + " (col1, col2, col3) values (date('2014-01-16'), 0, date" +
                                "('2014-01-16'))").execute();
                        classWatcher.prepareStatement(
                            "insert into " + tableWatcherA + " (col1, col2, col3) values (date('2014-01-17'), -1, " +
                                "date('2013-12-17'))").execute();
                        classWatcher.prepareStatement(
                            "insert into " + tableWatcherB + " (col1, col2, col3) values ('01/27/2001', 'mm/dd/yyyy'," +
                                " date('2001-01-27'))").execute();
                        classWatcher.prepareStatement(
                            "insert into " + tableWatcherB + " (col1, col2, col3) values ('2002/02/26', 'yyyy/MM/dd'," +
                                " date('2002-02-26'))").execute();
                        classWatcher.prepareStatement(
                            "insert into " + tableWatcherC + " (col1, col2) values (date('2002-03-26'), date" +
                                "('2002-03-31'))").execute();
                        classWatcher.prepareStatement(
                            "insert into " + tableWatcherC + " (col1, col2) values (date('2012-06-02'), date" +
                                "('2012-06-30'))").execute();
                        classWatcher.prepareStatement(
                            "insert into " + tableWatcherD + " (col1, col2, col3) values (date('2002-03-26'), " +
                                "'friday', date('2002-03-29'))").execute();
                        classWatcher.prepareStatement(
                            "insert into " + tableWatcherD + " (col1, col2, col3) values (date('2008-11-11'), " +
                                "'thursday', date('2008-11-13'))").execute();
                        classWatcher.prepareStatement(
                            "insert into " + tableWatcherE + " (col1, col2, col3) values (date('1994-01-11'), date" +
                                "('1995-01-11'), 12.0)").execute();
                        classWatcher.prepareStatement(
                            "insert into " + tableWatcherE + " (col1, col2, col3) values (date('2014-05-29'), date" +
                                "('2014-04-29'), 1.0)").execute();
                        classWatcher.prepareStatement(
                            "insert into " + tableWatcherF + " (col1, col2, col3) values (date('2014-05-29'), " +
                                "'mm/dd/yyyy', '05/29/2014')").execute();
                        classWatcher.prepareStatement(
                            "insert into " + tableWatcherF + " (col1, col2, col3) values (date('2012-12-31'), " +
                                "'yyyy/mm/dd', '2012/12/31')").execute();
                        classWatcher.prepareStatement(
                            "insert into " + tableWatcherG + " (col1, col2, col3) values (Timestamp('2012-12-31 " +
                                "20:38:40'), 'hour', Timestamp('2012-12-31 20:00:00'))").execute();
                        classWatcher.prepareStatement(
                            "insert into " + tableWatcherG + " (col1, col2, col3) values (Timestamp('2012-12-31 " +
                                "20:38:40'), 'day', Timestamp('2012-12-31 00:00:00'))").execute();
                        classWatcher.prepareStatement(
                            "insert into " + tableWatcherG + " (col1, col2, col3) values (Timestamp('2012-12-31 " +
                                "20:38:40'), 'week', Timestamp('2012-12-30 00:00:00'))").execute();
                        classWatcher.prepareStatement(
                            "insert into " + tableWatcherG + " (col1, col2, col3) values (Timestamp('2012-12-31 " +
                                "20:38:40'), 'month', Timestamp('2012-12-01 00:00:00'))").execute();
                        classWatcher.prepareStatement(
                            "insert into " + tableWatcherG + " (col1, col2, col3) values (Timestamp('2012-12-31 " +
                                "20:38:40'), 'quarter', Timestamp('2012-10-01 00:00:00'))").execute();
                        classWatcher.prepareStatement(
                            "insert into " + tableWatcherG + " (col1, col2, col3) values (Timestamp('2012-12-31 " +
                                "20:38:40'), 'year', Timestamp('2012-01-01 00:00:00'))").execute();
                        classWatcher.prepareStatement(
                            "insert into " + tableWatcherG + " (col1, col2, col3) values (Timestamp('2012-01-29 " +
                                "20:38:40'), 'year', Timestamp('2012-01-29 00:00:00'))").execute();
                        classWatcher.prepareStatement(
                            "insert into " + tableWatcherG + " (col1, col2, col3) values (Timestamp('2012-01-29 " +
                                "20:38:40'), 'year', Timestamp('2011-01-29 00:00:00'))").execute();
                        classWatcher.prepareStatement(
                            "insert into " + tableWatcherH + " (col1, col2, col3) values (Timestamp('2012-12-31 " +
                                "20:38:40'), 'YYYY', '2012')").execute();
                        classWatcher.prepareStatement(
                            "insert into " + tableWatcherH + " (col1, col2, col3) values (Timestamp('2012-12-31 " +
                                "20:38:40'), 'MM', '12')").execute();
                        classWatcher.prepareStatement(
                            "insert into " + tableWatcherH + " (col1, col2, col3) values (Timestamp('2012-12-31 " +
                                "20:38:40'), 'DD', '31')").execute();
                        classWatcher.prepareStatement(
                            "insert into " + tableWatcherH + " (col1, col2, col3) values (Timestamp('2012-12-31 " +
                                "20:38:40'), 'YYYY-MM-DD', '2012-12-31')").execute();
                        classWatcher.prepareStatement(
                            "insert into " + tableWatcherI + " (ts, pat, datestr) values (Timestamp('2012-12-31 " +
                                "20:38:40'), 'YYYY-MM-DD HH:mm:ss', '2012-12-31 20:38:40')").execute();
                        classWatcher.prepareStatement(
                            "insert into " + tableWatcherI + " (ts, pat, datestr) values (Timestamp('2012-12-31 " +
                                "00:00:00.03'), 'YYYY-MM-DD HH:mm:ss.SSS', '2012-12-31 00:00:00.03')").execute();
                        // FIXME JC:
//                        classWatcher.prepareStatement(
//                            "insert into " + tableWatcherI + " (ts, pat, datestr) values (Timestamp('2011-09-17 " +
//                                "23:40:53'), 'yyyy-MM-dd''T''HH:mm:ssz', '2011-09-17T23:40:53GMT')").execute();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        classWatcher.closeAll();
                    }
                }
            });
   
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @Test @Ignore("DB-2937: database metadata not consistent")
    public void testDBMetadataGetFunctions() throws Exception {
        DatabaseMetaData dmd =  methodWatcher.getOrCreateConnection().getMetaData();
        TestUtils.printResult("getProcedures", dmd.getProcedures(null, "%", "%"), System.out);
        TestUtils.printResult("getFunctions", dmd.getFunctions(null, "%", "%"), System.out);

        System.out.println("getStringFunctions: " + dmd.getStringFunctions());
        System.out.println("getTimeDateFunctions: " + dmd.getTimeDateFunctions());

        for (String fn : dmd.getTimeDateFunctions().split(",")) {
            TestUtils.printResult(fn, dmd.getFunctions("%", "%", fn.trim()), System.out);
        }
    }

    @Test
    public void testToDateFunction() throws Exception{
        String sqlText = "SELECT col1 as datestr, col2 as pattern, TO_DATE(col1, col2) as todate, col3 as date from " + tableWatcherB + " order by col3";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {

            String expected =
                "DATESTR  |  PATTERN  |  TODATE   |   DATE    |\n" +
                    "------------------------------------------------\n" +
                    "01/27/2001 |mm/dd/yyyy |2001-01-27 |2001-01-27 |\n" +
                    "2002/02/26 |yyyy/MM/dd |2002-02-26 |2002-02-26 |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test @Ignore("Implemented in SpliceDateFunctions but not exposed in SpliceSystemProcedures due to timezone loss.")
    public void testToTimestampFunction() throws Exception{
        String sqlText = "SELECT ts, pat, datestr, TO_TIMESTAMP(datestr, pat) as totimestamp from " + tableWatcherI + " order by datestr";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {

            String expected =
                // FIXME JC: when we add the commnented row above, this test starts to fail because timezone
                // information is lost in the totimestamp output of the first row below. We wind up with an
                // answer that's 6 hours earlier (in STL - -6:00).  Not sure how to fix this.
//            "TS           |          PAT           |        DATESTR        |      TOTIMESTAMP      |\n" +
//                "-------------------------------------------------------------------------------------------------\n" +
//                " 2011-09-17 23:40:53.0 |yyyy-MM-dd'T'HH:mm:ssz  |2011-09-17T23:40:53GMT | 2011-09-17 23:40:53.0 |\n" +
//                "2012-12-31 00:00:00.03 |YYYY-MM-DD HH:mm:ss.SSS |2012-12-31 00:00:00.03 |2012-01-31 00:00:00.03 |\n" +
//                " 2012-12-31 20:38:40.0 |  YYYY-MM-DD HH:mm:ss   |  2012-12-31 20:38:40  | 2012-01-31 20:38:40.0 |";
            "TS           |          PAT           |        DATESTR        |      TOTIMESTAMP      |\n" +
                "-------------------------------------------------------------------------------------------------\n" +
                "2012-12-31 00:00:00.03 |YYYY-MM-DD HH:mm:ss.SSS |2012-12-31 00:00:00.03 |2012-01-31 00:00:00.03 |\n" +
                " 2012-12-31 20:38:40.0 |  YYYY-MM-DD HH:mm:ss   |  2012-12-31 20:38:40  | 2012-01-31 20:38:40.0 |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testMonthBetweenFunction() throws Exception{
        String sqlText = "SELECT MONTH_BETWEEN(col1, col2), col3 from " + tableWatcherE + " order by col3";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {

            String expected =
                "1  |COL3 |\n" +
                    "------------\n" +
                    "-1.0 | 1.0 |\n" +
                    "12.0 |12.0 |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }
    
    @Test
    public void testToCharFunction() throws Exception{
        String sqlText = "SELECT TO_CHAR(col1, col2), col3 from " + tableWatcherF + " order by col3";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {

            String expected =
                "1     |   COL3    |\n" +
                    "------------------------\n" +
                    "05/29/2014 |05/29/2014 |\n" +
                    "2012/12/31 |2012/12/31 |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }
    
    @Test
    public void testNextDayFunction() throws Exception{
        String sqlText = "SELECT NEXT_DAY(col1, col2), col3 from " + tableWatcherD + " order by col3";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {

            String expected =
                "1     |   COL3    |\n" +
                    "------------------------\n" +
                    "2002-03-29 |2002-03-29 |\n" +
                    "2008-11-13 |2008-11-13 |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }
   
    @Test
    public void testLastDayFunction() throws Exception{
        String sqlText = "SELECT LAST_DAY(col1), col2 from " + tableWatcherC + " order by col2";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {

            String expected =
                "1     |   COL2    |\n" +
                    "------------------------\n" +
                    "2002-03-31 |2002-03-31 |\n" +
                    "2012-06-30 |2012-06-30 |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }
   
    @Test
    public void testAddMonthsFunction() throws Exception {
        String sqlText = "SELECT ADD_MONTHS(col1, col2), col3 from " + tableWatcherA + " order by col3";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {

            String expected =
                "1     |   COL3    |\n" +
                    "------------------------\n" +
                    "2013-12-17 |2013-12-17 |\n" +
                    "2014-01-16 |2014-01-16 |\n" +
                    "2014-02-15 |2014-02-15 |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testTruncDateFunction() throws Exception {
        String sqlText = "SELECT TRUNC_DATE(col1, col2), col3 from " + tableWatcherG + " order by col3";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {

            String expected =
                "1           |        COL3          |\n" +
                    "----------------------------------------------\n" +
                    "2012-01-01 00:00:00.0 |2011-01-29 00:00:00.0 |\n" +
                    "2012-01-01 00:00:00.0 |2012-01-01 00:00:00.0 |\n" +
                    "2012-01-01 00:00:00.0 |2012-01-29 00:00:00.0 |\n" +
                    "2012-10-01 00:00:00.0 |2012-10-01 00:00:00.0 |\n" +
                    "2012-12-01 00:00:00.0 |2012-12-01 00:00:00.0 |\n" +
                    "2012-12-30 00:00:00.0 |2012-12-30 00:00:00.0 |\n" +
                    "2012-12-31 00:00:00.0 |2012-12-31 00:00:00.0 |\n" +
                    "2012-12-31 20:00:00.0 |2012-12-31 20:00:00.0 |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testTimeStampToChar() throws Exception {
        String sqlText = "SELECT TIMESTAMP_TO_CHAR(col1, col2), col3 from " + tableWatcherH + " order by col3";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {

            String expected =
                "1     |   COL3    |\n" +
                    "------------------------\n" +
                    "    12     |    12     |\n" +
                    "   2012    |   2012    |\n" +
                    "2012-12-31 |2012-12-31 |\n" +
                    "    31     |    31     |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testTimeStampAdd() throws Exception {
        String sqlText = "SELECT {fn TIMESTAMPADD(SQL_TSI_MONTH, 1, col3)}, col3 from " + tableWatcherG + " order by col3";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {

            String expected =
                "1           |        COL3          |\n" +
                    "----------------------------------------------\n" +
                    "2011-02-28 00:00:00.0 |2011-01-29 00:00:00.0 |\n" +
                    "2012-02-01 00:00:00.0 |2012-01-01 00:00:00.0 |\n" +
                    "2012-02-29 00:00:00.0 |2012-01-29 00:00:00.0 |\n" +
                    "2012-11-01 00:00:00.0 |2012-10-01 00:00:00.0 |\n" +
                    "2013-01-01 00:00:00.0 |2012-12-01 00:00:00.0 |\n" +
                    "2013-01-30 00:00:00.0 |2012-12-30 00:00:00.0 |\n" +
                    "2013-01-31 00:00:00.0 |2012-12-31 00:00:00.0 |\n" +
                    "2013-01-31 20:00:00.0 |2012-12-31 20:00:00.0 |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testTimeStampAddValues() throws Exception {
        String sqlText = "values {fn TIMESTAMPADD(SQL_TSI_MONTH, 1, current_timestamp)}";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
            }
            // just make sure we've got at least one row and no exceptions
            assertEquals("\n" + sqlText + "\n", 1, rowCount);
        }
    }

    @Test
    public void testSecondWithTimeStamp() throws Exception {
        String sqlText = "select second(col1), col1 from " + tableWatcherH + " order by col1";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected =
                "1  |        COL1          |\n" +
                    "-----------------------------\n" +
                    "40.0 |2012-12-31 20:38:40.0 |\n" +
                    "40.0 |2012-12-31 20:38:40.0 |\n" +
                    "40.0 |2012-12-31 20:38:40.0 |\n" +
                    "40.0 |2012-12-31 20:38:40.0 |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testTimeStampDiffWithTimestamp() throws Exception {
        String sqlText = "select {fn TIMESTAMPDIFF(SQL_TSI_DAY, date('2012-12-26'), col3)}, date(col3), date('2012-12-26') from " + tableWatcherG + " order by col3";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected =
                "1  |     2     |     3     |\n" +
                    "------------------------------\n" +
                    "-697 |2011-01-29 |2012-12-26 |\n" +
                    "-360 |2012-01-01 |2012-12-26 |\n" +
                    "-332 |2012-01-29 |2012-12-26 |\n" +
                    " -86 |2012-10-01 |2012-12-26 |\n" +
                    " -25 |2012-12-01 |2012-12-26 |\n" +
                    "  4  |2012-12-30 |2012-12-26 |\n" +
                    "  5  |2012-12-31 |2012-12-26 |\n" +
                    "  5  |2012-12-31 |2012-12-26 |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testTimeStampDiffWithDate() throws Exception {
        String sqlText = "select {fn TIMESTAMPDIFF(SQL_TSI_DAY, date('2003-03-29'), col3)}, col3, date('2012-12-26') from " + tableWatcherD + " order by col3";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected =
                "1  |   COL3    |     3     |\n" +
                    "------------------------------\n" +
                    "-365 |2002-03-29 |2012-12-26 |\n" +
                    "2056 |2008-11-13 |2012-12-26 |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }
}
