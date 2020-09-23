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

package com.splicemachine.derby.utils;


import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.Assert.*;

public class SpliceDateFunctionsIT {

    private static final String CLASS_NAME = SpliceDateFunctionsIT.class.getSimpleName().toUpperCase();
    private static SpliceWatcher classWatcher = new SpliceWatcher(CLASS_NAME);

    private static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    // Table for ADD_MONTHS/DAYS/YEARS testing.
    private static final SpliceTableWatcher tableWatcherA = new SpliceTableWatcher(
        "A", schemaWatcher.schemaName, "(col1 date, col2 int, col3 date, col4 varchar(10))");
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
            "I", schemaWatcher.schemaName, "(t Time, d Date, ts Timestamp, pat varchar(30), datestr varchar(30))");
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
                        "insert into " + tableWatcherA + " (col1, col2, col3, col4) values (date('2014-01-15'), 1, date" +
                            "('2014-02-15'), 'months')").execute();
                    classWatcher.prepareStatement(
                        "insert into " + tableWatcherA + " (col1, col2, col3, col4) values (date('2014-01-16'), 0, date" +
                            "('2014-01-16'), 'months')").execute();
                    classWatcher.prepareStatement(
                        "insert into " + tableWatcherA + " (col1, col2, col3, col4) values (date('2014-01-17'), -1, " +
                            "date('2013-12-17'), 'months')").execute();
                    classWatcher.prepareStatement(
                            "insert into " + tableWatcherA + " (col1, col2, col3, col4) values (date('2014-01-17'), null, " +
                                    "null, 'months')").execute();
                    classWatcher.prepareStatement(
                            "insert into " + tableWatcherA + " (col1, col2, col3, col4) values (null, 1, " +
                                    "null, 'months')").execute();
                    classWatcher.prepareStatement(
                            "insert into " + tableWatcherA + " (col1, col2, col3, col4) values (date('2014-01-15'), 1, date" +
                                    "('2014-01-16'), 'days')").execute();
                    classWatcher.prepareStatement(
                            "insert into " + tableWatcherA + " (col1, col2, col3, col4) values (date('2014-01-16'), 0, date" +
                                    "('2014-01-16'), 'days')").execute();
                    classWatcher.prepareStatement(
                            "insert into " + tableWatcherA + " (col1, col2, col3, col4) values (date('2014-01-01'), -1, " +
                                    "date('2013-12-31'), 'days')").execute();
                    classWatcher.prepareStatement(
                            "insert into " + tableWatcherA + " (col1, col2, col3, col4) values (date('2014-01-01'), null, " +
                                    "null, 'days')").execute();
                    classWatcher.prepareStatement(
                            "insert into " + tableWatcherA + " (col1, col2, col3, col4) values (null, 1, " +
                                    "null, 'days')").execute();
                    classWatcher.prepareStatement(
                            "insert into " + tableWatcherA + " (col1, col2, col3, col4) values (date('2014-01-15'), 1, date" +
                                    "('2015-01-15'), 'years')").execute();
                    classWatcher.prepareStatement(
                            "insert into " + tableWatcherA + " (col1, col2, col3, col4) values (date('2014-01-16'), 0, date" +
                                    "('2014-01-16'), 'years')").execute();
                    classWatcher.prepareStatement(
                            "insert into " + tableWatcherA + " (col1, col2, col3, col4) values (date('2014-01-17'), -1, " +
                                    "date('2013-01-17'), 'years')").execute();
                    classWatcher.prepareStatement(
                            "insert into " + tableWatcherA + " (col1, col2, col3, col4) values (date('2014-01-17'), null, " +
                                    "null, 'years')").execute();
                    classWatcher.prepareStatement(
                            "insert into " + tableWatcherA + " (col1, col2, col3, col4) values (null, 1, " +
                                    "null, 'years')").execute();
                    classWatcher.prepareStatement(
                        "insert into " + tableWatcherB + " (col1, col2, col3) values ('01/27/2001', 'MM/dd/yyyy'," +
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
                            "'MM/dd/yyyy', '05/29/2014')").execute();
                    classWatcher.prepareStatement(
                        "insert into " + tableWatcherF + " (col1, col2, col3) values (date('2012-12-31'), " +
                            "'yyyy/MM/dd', '2012/12/31')").execute();
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
                        "insert into " + tableWatcherI + " (t, d, ts, pat, datestr) values (Time('20:38:40'), " +
                            "Date('2012-12-31'), Timestamp('2012-12-31 20:38:40'), 'YYYY-MM-DD HH:mm:ss', '2012-12-31 20:38:40')").execute();
                    classWatcher.prepareStatement(
                        "insert into " + tableWatcherI + " (t, d, ts, pat, datestr) values (Time('23:59:59'), " +
                            "Date('2012-12-31'), Timestamp('2012-12-31 00:00:00.03'), 'YYYY-MM-DD HH:mm:ss.SSS', '2012-12-31 00:00:00.03')").execute();
                    classWatcher.prepareStatement(
                        "insert into " + tableWatcherI + " (t, d, ts, pat, datestr) values (Time('00:00:01'), " +
                            "Date('2009-07-02'), Timestamp('2009-07-02 11:22:33.04'), 'YYYY-MM-DD HH:mm:ss.SSS', '2009-07-02 11:22:33.04')").execute();
                    classWatcher.prepareStatement(
                        "insert into " + tableWatcherI + " (t, d, ts, pat, datestr) values (Time('18:44:28'), " +
                            "Date('2009-09-02'), Timestamp('2009-09-02 11:22:33.04'), 'YYYY-MM-DD HH:mm:ss.SSS', '2009-09-02 11:22:33.04')").execute();
                    classWatcher.prepareStatement(
                        "insert into " + tableWatcherI + " (t, d, ts, pat, datestr) values (Time('10:30:29'), " +
                            "Date('2009-01-02'), Timestamp('2009-01-02 11:22:33.04'), 'YYYY-MM-DD HH:mm:ss.SSS', '2009-01-02 11:22:33.04')").execute();
                    classWatcher.prepareStatement(
                        "insert into " + tableWatcherI + " (t, d, ts, pat, datestr) values (Time('05:22:33'), " +
                            "Date('2013-12-31'), Timestamp('2013-12-31 05:22:33.04'), 'YYYY-MM-DD HH:mm:ss.SSS', '2013-12-31 05:22:33.04')").execute();
                    // FIXME JC: See @Ingored test testToTimestampFunction below...
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

    @Test
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
                    "01/27/2001 |MM/dd/yyyy |2001-01-27 |2001-01-27 |\n" +
                    "2002/02/26 |yyyy/MM/dd |2002-02-26 |2002-02-26 |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
        sqlText = "Values TO_DATE('19000101','yyyyMMdd')";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {

            String expected =
                "1     |\n" +
                "------------\n" +
                "1900-01-01 |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
        sqlText = "Values TO_DATE('12190001','MMyyyydd')";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {

            String expected =
                "1     |\n" +
                "------------\n" +
                "1900-12-01 |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }

        sqlText = "VALUES TO_DATE('19000101 12:01:01','yyyyMMdd HH:mm:ss')";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {

            String expected =
                "1     |\n" +
                "------------\n" +
                "1900-01-01 |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }

        sqlText = "VALUES TO_DATE('1900365','yyyyDDD')";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {

            String expected =
                "1     |\n" +
                "------------\n" +
                "1900-12-31 |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test @Ignore("Implemented in SpliceDateFunctions but not exposed in SpliceSystemProcedures due to timezone loss.")
    public void testToTimestampFunction() throws Exception{
        String sqlText = "SELECT ts, pat, datestr, TIMESTAMP(datestr, pat) as totimestamp from " + tableWatcherI + " order by datestr";
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
        sqlText = "VALUES TO_TIMESTAMP('19000101 12:01:01','yyyyMMdd HH:mm:ss')";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {

            String expected =
                "1           |\n" +
                "-----------------------\n" +
                "1900-01-01 12:01:01.0 |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }

        sqlText = "VALUES TO_TIMESTAMP('1900365 12:01:01','yyyyDDD HH:mm:ss')";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {

            String expected =
                "1           |\n" +
                "-----------------------\n" +
                "1900-01-01 12:01:01.0 |";
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

    //test for DB-3439
    @Test
    public void testLastDayWithArithmetic() throws Exception {
        Date queryResult1 = methodWatcher.query("values last_day(date('2015-07-30') + 5)");
        assertEquals("2015-08-31", new SimpleDateFormat("YYYY-MM-dd").format(queryResult1));
    }

    //test for DB-3439
    @Test
    public void testLastDayWithoutArithmetic() throws Exception {
        Date queryResult1 = methodWatcher.query("values last_day(date('2015-07-30'))");
        assertEquals("2015-07-31", new SimpleDateFormat("YYYY-MM-dd").format(queryResult1));
    }

    //test for DB-3439
    @Test
    public void testLastDayWithoutArithmeticLeapYear() throws Exception {
        Date queryResult1 = methodWatcher.query("values last_day(date('2016-02-24') + 5)");
        assertEquals("2016-02-29", new SimpleDateFormat("YYYY-MM-dd").format(queryResult1));
    }
   
    @Test
    public void testAddMonthsFunction() throws Exception {
        String expected =
                "1     |   COL3    |\n" +
                        "------------------------\n" +
                        "2013-12-17 |2013-12-17 |\n" +
                        "2014-01-16 |2014-01-16 |\n" +
                        "2014-02-15 |2014-02-15 |\n" +
                        "   NULL    |   NULL    |\n" +
                        "   NULL    |   NULL    |";
        String[] sqlTexts = {
                "SELECT ADD_MONTHS(col1, col2), col3 from " + tableWatcherA + " where col4 = 'months' order by col3",
                "SELECT col1 + col2 MONTH, col3 from " + tableWatcherA + " where col4 = 'months' order by col3",
                "SELECT col1 + col2 MONTHS, col3 from " + tableWatcherA + " where col4 = 'months' order by col3",
        };
        for (String sqlText: sqlTexts) {
            try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {

                assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            }
        }
    }

    @Test
    public void testAddDaysFunction() throws Exception {
        String expected =
                "1     |   COL3    |\n" +
                        "------------------------\n" +
                        "2013-12-31 |2013-12-31 |\n" +
                        "2014-01-16 |2014-01-16 |\n" +
                        "2014-01-16 |2014-01-16 |\n" +
                        "   NULL    |   NULL    |\n" +
                        "   NULL    |   NULL    |";
        String[] sqlTexts = {
                "SELECT ADD_DAYS(col1, col2), col3 from " + tableWatcherA + " where col4='days' order by col3",
                "SELECT col1 + col2 DAY, col3 from " + tableWatcherA + " where col4='days' order by col3",
                "SELECT col1 + col2 DAYS, col3 from " + tableWatcherA + " where col4='days' order by col3",
        };
        for (String sqlText: sqlTexts) {
            try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {

                assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            }
        }
    }

    @Test
    public void testAddYearsFunction() throws Exception {
        String expected =
                "1     |   COL3    |\n" +
                        "------------------------\n" +
                        "2013-01-17 |2013-01-17 |\n" +
                        "2014-01-16 |2014-01-16 |\n" +
                        "2015-01-15 |2015-01-15 |\n" +
                        "   NULL    |   NULL    |\n" +
                        "   NULL    |   NULL    |";
        String[] sqlTexts = {
                "SELECT ADD_YEARS(col1, col2), col3 from " + tableWatcherA + " where col4='years' order by col3",
                "SELECT col1 + col2 YEAR, col3 from " + tableWatcherA + " where col4='years' order by col3",
                "SELECT col1 + col2 YEARS, col3 from " + tableWatcherA + " where col4='years' order by col3",
        };
        for (String sqlText: sqlTexts) {
            try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {

                assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            }
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
    public void testTimeStampAddUnescaped() throws Exception {
        // DB-2970
        String sqlText = "SELECT TIMESTAMPADD(SQL_TSI_MONTH, 1, col3), col3 from " + tableWatcherG + " order by col3";
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
    public void testTimeStampAddValuesUnescaped() throws Exception {
        String sqlText = "values TIMESTAMPADD(SQL_TSI_MONTH, 1, current_timestamp)";
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
    public void testTimeStampDiffWithTimestampUnescaped() throws Exception {
        // DB-2970
        String sqlText = "select TIMESTAMPDIFF(SQL_TSI_DAY, date('2012-12-26'), col3), date(col3), date('2012-12-26') from " + tableWatcherG + " order by col3";
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

    @Test
    public void testTimeStampDiffWithDateUnescaped() throws Exception {
        // DB-2970
        String sqlText = "select TIMESTAMPDIFF(SQL_TSI_DAY, date('2003-03-29'), col3), col3, date('2012-12-26') from " + tableWatcherD + " order by col3";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected =
                "1  |   COL3    |     3     |\n" +
                    "------------------------------\n" +
                    "-365 |2002-03-29 |2012-12-26 |\n" +
                    "2056 |2008-11-13 |2012-12-26 |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    //====================================================================================================
    // DB-2248, DB-2249: BI Tools and ODBC functions - Timestamp type
    //====================================================================================================

    @Test
    public void testExtractYearTimestamp() throws Exception {
        String sqlText = "select ts, EXTRACT(YEAR FROM ts) as \"YEAR\" from " + tableWatcherI + " order by ts";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected =
                "TS           |YEAR |\n" +
                    "------------------------------\n" +
                    "2009-01-02 11:22:33.04 |2009 |\n" +
                    "2009-07-02 11:22:33.04 |2009 |\n" +
                    "2009-09-02 11:22:33.04 |2009 |\n" +
                    "2012-12-31 00:00:00.03 |2012 |\n" +
                    " 2012-12-31 20:38:40.0 |2012 |\n" +
                    "2013-12-31 05:22:33.04 |2013 |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testExtractQuarterTimestamp() throws Exception {
        String sqlText = "select ts, EXTRACT(QUARTER FROM ts) as \"QUARTER\" from " + tableWatcherI + " order by ts";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected =
                "TS           | QUARTER |\n" +
                    "----------------------------------\n" +
                    "2009-01-02 11:22:33.04 |    1    |\n" +
                    "2009-07-02 11:22:33.04 |    3    |\n" +
                    "2009-09-02 11:22:33.04 |    3    |\n" +
                    "2012-12-31 00:00:00.03 |    4    |\n" +
                    " 2012-12-31 20:38:40.0 |    4    |\n" +
                    "2013-12-31 05:22:33.04 |    4    |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testQuarterTimestamp() throws Exception {
        String sqlText = "select ts, QUARTER(ts) as \"QUARTER\" from " + tableWatcherI + " order by ts";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected =
                "TS           | QUARTER |\n" +
                    "----------------------------------\n" +
                    "2009-01-02 11:22:33.04 |    1    |\n" +
                    "2009-07-02 11:22:33.04 |    3    |\n" +
                    "2009-09-02 11:22:33.04 |    3    |\n" +
                    "2012-12-31 00:00:00.03 |    4    |\n" +
                    " 2012-12-31 20:38:40.0 |    4    |\n" +
                    "2013-12-31 05:22:33.04 |    4    |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testExtractMonthTimestamp() throws Exception {
        String sqlText = "select ts, EXTRACT(MONTH FROM ts) as \"MONTH\" from " + tableWatcherI + " order by ts";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected =
                "TS           | MONTH |\n" +
                    "--------------------------------\n" +
                    "2009-01-02 11:22:33.04 |   1   |\n" +
                    "2009-07-02 11:22:33.04 |   7   |\n" +
                    "2009-09-02 11:22:33.04 |   9   |\n" +
                    "2012-12-31 00:00:00.03 |  12   |\n" +
                    " 2012-12-31 20:38:40.0 |  12   |\n" +
                    "2013-12-31 05:22:33.04 |  12   |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testExtractMonthNameTimestamp() throws Exception {
        String sqlText = "select ts, EXTRACT(MONTHNAME FROM ts) as \"MONTHNAME\" from " + tableWatcherI + " order by ts";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected =
                "TS           | MONTHNAME |\n" +
                    "------------------------------------\n" +
                    "2009-01-02 11:22:33.04 |  January  |\n" +
                    "2009-07-02 11:22:33.04 |   July    |\n" +
                    "2009-09-02 11:22:33.04 | September |\n" +
                    "2012-12-31 00:00:00.03 | December  |\n" +
                    " 2012-12-31 20:38:40.0 | December  |\n" +
                    "2013-12-31 05:22:33.04 | December  |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testMonthNameTimestamp() throws Exception {
        String sqlText = "select ts, MONTHNAME(ts) as \"MONTHNAME\" from " + tableWatcherI + " order by ts";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected =
                "TS           | MONTHNAME |\n" +
                    "------------------------------------\n" +
                    "2009-01-02 11:22:33.04 |  January  |\n" +
                    "2009-07-02 11:22:33.04 |   July    |\n" +
                    "2009-09-02 11:22:33.04 | September |\n" +
                    "2012-12-31 00:00:00.03 | December  |\n" +
                    " 2012-12-31 20:38:40.0 | December  |\n" +
                    "2013-12-31 05:22:33.04 | December  |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testExtractWeekTimestamp() throws Exception {
        String sqlText = "select ts, EXTRACT(WEEK FROM ts) as \"WEEK\" from " + tableWatcherI + " order by ts";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected =
                "TS           |WEEK |\n" +
                    "------------------------------\n" +
                    "2009-01-02 11:22:33.04 |  1  |\n" +
                    "2009-07-02 11:22:33.04 | 27  |\n" +
                    "2009-09-02 11:22:33.04 | 36  |\n" +
                    "2012-12-31 00:00:00.03 |  1  |\n" +
                    " 2012-12-31 20:38:40.0 |  1  |\n" +
                    "2013-12-31 05:22:33.04 |  1  |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testWeekTimestamp() throws Exception {
        // Note: Jodatime and Postgres get the same answer but SQL Sever gets week 53 for 12/31
        String sqlText = "select ts, WEEK(ts) as \"WEEK\" from " + tableWatcherI + " order by ts";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected =
                "TS           |WEEK |\n" +
                    "------------------------------\n" +
                    "2009-01-02 11:22:33.04 |  1  |\n" +
                    "2009-07-02 11:22:33.04 | 27  |\n" +
                    "2009-09-02 11:22:33.04 | 36  |\n" +
                    "2012-12-31 00:00:00.03 |  1  |\n" +
                    " 2012-12-31 20:38:40.0 |  1  |\n" +
                    "2013-12-31 05:22:33.04 |  1  |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testExtractWeekDayTimestamp() throws Exception {
        // Note: Jodatime and Postgres get the same answer but SQL Sever gets n+1
        String sqlText = "select ts, EXTRACT(WEEKDAY FROM ts) as \"WEEKDAY\" from " + tableWatcherI + " order by ts";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected =
                "TS           | WEEKDAY |\n" +
                    "----------------------------------\n" +
                    "2009-01-02 11:22:33.04 |    5    |\n" +
                    "2009-07-02 11:22:33.04 |    4    |\n" +
                    "2009-09-02 11:22:33.04 |    3    |\n" +
                    "2012-12-31 00:00:00.03 |    1    |\n" +
                    " 2012-12-31 20:38:40.0 |    1    |\n" +
                    "2013-12-31 05:22:33.04 |    2    |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testExtractWeekDayNameTimestamp() throws Exception {
        String sqlText = "select ts, EXTRACT(WEEKDAYNAME FROM ts) as \"WEEKDAYNAME\" from " + tableWatcherI + " order by ts";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected =
                "TS           | WEEKDAYNAME |\n" +
                    "--------------------------------------\n" +
                    "2009-01-02 11:22:33.04 |   Friday    |\n" +
                    "2009-07-02 11:22:33.04 |  Thursday   |\n" +
                    "2009-09-02 11:22:33.04 |  Wednesday  |\n" +
                    "2012-12-31 00:00:00.03 |   Monday    |\n" +
                    " 2012-12-31 20:38:40.0 |   Monday    |\n" +
                    "2013-12-31 05:22:33.04 |   Tuesday   |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testExtractDayOfYearTimestamp() throws Exception {
        String sqlText = "select ts, EXTRACT(DAYOFYEAR FROM ts) as \"DAYOFYEAR\" from " + tableWatcherI + " order by ts";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected =
                "TS           | DAYOFYEAR |\n" +
                    "------------------------------------\n" +
                    "2009-01-02 11:22:33.04 |     2     |\n" +
                    "2009-07-02 11:22:33.04 |    183    |\n" +
                    "2009-09-02 11:22:33.04 |    245    |\n" +
                    "2012-12-31 00:00:00.03 |    366    |\n" +
                    " 2012-12-31 20:38:40.0 |    366    |\n" +
                    "2013-12-31 05:22:33.04 |    365    |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testExtractDayTimestamp() throws Exception {
        String sqlText = "select ts, EXTRACT(DAY FROM ts) as \"DAY\" from " + tableWatcherI + " order by ts";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected =
                "TS           | DAY |\n" +
                    "------------------------------\n" +
                    "2009-01-02 11:22:33.04 |  2  |\n" +
                    "2009-07-02 11:22:33.04 |  2  |\n" +
                    "2009-09-02 11:22:33.04 |  2  |\n" +
                    "2012-12-31 00:00:00.03 | 31  |\n" +
                    " 2012-12-31 20:38:40.0 | 31  |\n" +
                    "2013-12-31 05:22:33.04 | 31  |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testExtractHourTimestamp() throws Exception {
        String sqlText = "select ts, EXTRACT(HOUR FROM ts) as \"HOUR\" from " + tableWatcherI + " order by ts";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected =
                "TS           |HOUR |\n" +
                    "------------------------------\n" +
                    "2009-01-02 11:22:33.04 | 11  |\n" +
                    "2009-07-02 11:22:33.04 | 11  |\n" +
                    "2009-09-02 11:22:33.04 | 11  |\n" +
                    "2012-12-31 00:00:00.03 |  0  |\n" +
                    " 2012-12-31 20:38:40.0 | 20  |\n" +
                    "2013-12-31 05:22:33.04 |  5  |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testExtractMinuteTimestamp() throws Exception {
        String sqlText = "select ts, EXTRACT(MINUTE FROM ts) as \"MINUTE\" from " + tableWatcherI + " order by ts";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected =
                "TS           |MINUTE |\n" +
                    "--------------------------------\n" +
                    "2009-01-02 11:22:33.04 |  22   |\n" +
                    "2009-07-02 11:22:33.04 |  22   |\n" +
                    "2009-09-02 11:22:33.04 |  22   |\n" +
                    "2012-12-31 00:00:00.03 |   0   |\n" +
                    " 2012-12-31 20:38:40.0 |  38   |\n" +
                    "2013-12-31 05:22:33.04 |  22   |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testExtractSecondTimestamp() throws Exception {
        // FIXME JC: should we have miliseconds in here?
        String sqlText = "select ts, EXTRACT(SECOND FROM ts) as \"SECOND\" from " + tableWatcherI + " order by ts";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected =
                "TS           |SECOND |\n" +
                    "--------------------------------\n" +
                    "2009-01-02 11:22:33.04 | 33.04 |\n" +
                    "2009-07-02 11:22:33.04 | 33.04 |\n" +
                    "2009-09-02 11:22:33.04 | 33.04 |\n" +
                    "2012-12-31 00:00:00.03 | 0.03  |\n" +
                    " 2012-12-31 20:38:40.0 | 40.0  |\n" +
                    "2013-12-31 05:22:33.04 | 33.04 |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    //====================================================================================================
    // DB-2248, DB-2249: BI Tools and ODBC functions - Date type
    //====================================================================================================

    @Test
    public void testExtractYearDate() throws Exception {
        String sqlText = "select d, EXTRACT(YEAR FROM d) as \"YEAR\" from " + tableWatcherI + " order by d";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected =
                "D     |YEAR |\n" +
                    "------------------\n" +
                    "2009-01-02 |2009 |\n" +
                    "2009-07-02 |2009 |\n" +
                    "2009-09-02 |2009 |\n" +
                    "2012-12-31 |2012 |\n" +
                    "2012-12-31 |2012 |\n" +
                    "2013-12-31 |2013 |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testExtractQuarterDate() throws Exception {
        String sqlText = "select d, EXTRACT(QUARTER FROM d) as \"QUARTER\" from " + tableWatcherI + " order by d";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected =
                "D     | QUARTER |\n" +
                    "----------------------\n" +
                    "2009-01-02 |    1    |\n" +
                    "2009-07-02 |    3    |\n" +
                    "2009-09-02 |    3    |\n" +
                    "2012-12-31 |    4    |\n" +
                    "2012-12-31 |    4    |\n" +
                    "2013-12-31 |    4    |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testQuarterDate() throws Exception {
        String sqlText = "select d, QUARTER(d) as \"QUARTER\" from " + tableWatcherI + " order by d";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected =
                "D     | QUARTER |\n" +
                    "----------------------\n" +
                    "2009-01-02 |    1    |\n" +
                    "2009-07-02 |    3    |\n" +
                    "2009-09-02 |    3    |\n" +
                    "2012-12-31 |    4    |\n" +
                    "2012-12-31 |    4    |\n" +
                    "2013-12-31 |    4    |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testExtractMonthDate() throws Exception {
        String sqlText = "select d, EXTRACT(MONTH FROM d) as \"MONTH\" from " + tableWatcherI + " order by d";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected =
                "D     | MONTH |\n" +
                    "--------------------\n" +
                    "2009-01-02 |   1   |\n" +
                    "2009-07-02 |   7   |\n" +
                    "2009-09-02 |   9   |\n" +
                    "2012-12-31 |  12   |\n" +
                    "2012-12-31 |  12   |\n" +
                    "2013-12-31 |  12   |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testExtractMonthNameDate() throws Exception {
        String sqlText = "select d, EXTRACT(MONTHNAME FROM d) as \"MONTHNAME\" from " + tableWatcherI + " order by d";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected =
                "D     | MONTHNAME |\n" +
                    "------------------------\n" +
                    "2009-01-02 |  January  |\n" +
                    "2009-07-02 |   July    |\n" +
                    "2009-09-02 | September |\n" +
                    "2012-12-31 | December  |\n" +
                    "2012-12-31 | December  |\n" +
                    "2013-12-31 | December  |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testMonthNameDate() throws Exception {
        String sqlText = "select d, MONTHNAME(d) as \"MONTHNAME\" from " + tableWatcherI + " order by d";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected =
                "D     | MONTHNAME |\n" +
                    "------------------------\n" +
                    "2009-01-02 |  January  |\n" +
                    "2009-07-02 |   July    |\n" +
                    "2009-09-02 | September |\n" +
                    "2012-12-31 | December  |\n" +
                    "2012-12-31 | December  |\n" +
                    "2013-12-31 | December  |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testExtractWeekDate() throws Exception {
        String sqlText = "select d, EXTRACT(WEEK FROM d) as \"WEEK\" from " + tableWatcherI + " order by d";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected =
                "D     |WEEK |\n" +
                    "------------------\n" +
                    "2009-01-02 |  1  |\n" +
                    "2009-07-02 | 27  |\n" +
                    "2009-09-02 | 36  |\n" +
                    "2012-12-31 |  1  |\n" +
                    "2012-12-31 |  1  |\n" +
                    "2013-12-31 |  1  |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testWeekDate() throws Exception {
        // Note: Jodatime and Postgres get the same answer but SQL Sever gets week 53 for 12/31
        String sqlText = "select d, WEEK(d) as \"WEEK\" from " + tableWatcherI + " order by d";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected =
                "D     |WEEK |\n" +
                    "------------------\n" +
                    "2009-01-02 |  1  |\n" +
                    "2009-07-02 | 27  |\n" +
                    "2009-09-02 | 36  |\n" +
                    "2012-12-31 |  1  |\n" +
                    "2012-12-31 |  1  |\n" +
                    "2013-12-31 |  1  |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testExtractWeekDayDate() throws Exception {
        // Note: Jodatime and Postgres get the same answer but SQL Sever gets n+1
        String sqlText = "select d, EXTRACT(WEEKDAY FROM d) as \"WEEKDAY\" from " + tableWatcherI + " order by d";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected =
                "D     | WEEKDAY |\n" +
                    "----------------------\n" +
                    "2009-01-02 |    5    |\n" +
                    "2009-07-02 |    4    |\n" +
                    "2009-09-02 |    3    |\n" +
                    "2012-12-31 |    1    |\n" +
                    "2012-12-31 |    1    |\n" +
                    "2013-12-31 |    2    |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testExtractWeekDayNameDate() throws Exception {
        // FIXME JC: validate
        String sqlText = "select d, EXTRACT(WEEKDAYNAME FROM d) as \"WEEKDAYNAME\" from " + tableWatcherI + " order by d";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected =
                "D     | WEEKDAYNAME |\n" +
                    "--------------------------\n" +
                    "2009-01-02 |   Friday    |\n" +
                    "2009-07-02 |  Thursday   |\n" +
                    "2009-09-02 |  Wednesday  |\n" +
                    "2012-12-31 |   Monday    |\n" +
                    "2012-12-31 |   Monday    |\n" +
                    "2013-12-31 |   Tuesday   |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testExtractDayOfYearDate() throws Exception {
        String sqlText = "select d, EXTRACT(DAYOFYEAR FROM d) as \"DAYOFYEAR\" from " + tableWatcherI + " order by d";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected =
                "D     | DAYOFYEAR |\n" +
                    "------------------------\n" +
                    "2009-01-02 |     2     |\n" +
                    "2009-07-02 |    183    |\n" +
                    "2009-09-02 |    245    |\n" +
                    "2012-12-31 |    366    |\n" +
                    "2012-12-31 |    366    |\n" +
                    "2013-12-31 |    365    |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testExtractDayDate() throws Exception {
        String sqlText = "select d, EXTRACT(DAY FROM d) as \"DAY\" from " + tableWatcherI + " order by d";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected =
                "D     | DAY |\n" +
                    "------------------\n" +
                    "2009-01-02 |  2  |\n" +
                    "2009-07-02 |  2  |\n" +
                    "2009-09-02 |  2  |\n" +
                    "2012-12-31 | 31  |\n" +
                    "2012-12-31 | 31  |\n" +
                    "2013-12-31 | 31  |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testExtractHourDate() throws Exception {
        String sqlText = "select d, EXTRACT(HOUR FROM d) as \"HOUR\" from " + tableWatcherI + " order by d";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            fail("Expected exception attempting to get HOUR from Date type.");
        } catch (SQLSyntaxErrorException e) {
            assertEquals("The 'EXTRACT HOUR' function is not allowed on the 'DATE' type.", e.getLocalizedMessage());
        }
    }

    @Test
    public void testExtractMinuteDate() throws Exception {
        String sqlText = "select d, EXTRACT(MINUTE FROM d) as \"MINUTE\" from " + tableWatcherI + " order by d";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            fail("Expected exception attempting to get MINUTE from Date type.");
        } catch (SQLSyntaxErrorException e) {
            assertEquals("The 'EXTRACT MINUTE' function is not allowed on the 'DATE' type.", e.getLocalizedMessage());
        }
    }

    @Test
    public void testExtractSecondDate() throws Exception {
        String sqlText = "select d, EXTRACT(SECOND FROM d) as \"SECOND\" from " + tableWatcherI + " order by d";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            fail("Expected exception attempting to get SECOND from Date type.");
        } catch (SQLSyntaxErrorException e) {
            assertEquals("The 'EXTRACT SECOND' function is not allowed on the 'DATE' type.", e.getLocalizedMessage());
        }
    }

    //====================================================================================================
    // DB-2248, DB-2249: BI Tools and ODBC functions - Time type
    //====================================================================================================

    @Test
    public void testExtractYearTime() throws Exception {
        String sqlText = "select t, EXTRACT(YEAR FROM t) as \"YEAR\" from " + tableWatcherI + " order by t";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            fail("Expected exception attempting to get YEAR from Time type.");
        } catch (SQLSyntaxErrorException e) {
            assertEquals("The 'EXTRACT YEAR' function is not allowed on the 'TIME' type.", e.getLocalizedMessage());
        }
    }

    @Test
    public void testExtractQuarterTime() throws Exception {
        String sqlText = "select t, EXTRACT(QUARTER FROM t) as \"QUARTER\" from " + tableWatcherI + " order by t";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            fail("Expected exception attempting to get QUARTER from Time type.");
        } catch (SQLSyntaxErrorException e) {
            assertEquals("The 'EXTRACT QUARTER' function is not allowed on the 'TIME' type.", e.getLocalizedMessage());
        }
    }

    @Test
    public void testQuarterTime() throws Exception {
        String sqlText = "select t, QUARTER(t) as \"QUARTER\" from " + tableWatcherI + " order by t";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            fail("Expected exception attempting to get QUARTER from Time type.");
        } catch (SQLSyntaxErrorException e) {
            assertEquals("The 'EXTRACT QUARTER' function is not allowed on the 'TIME' type.", e.getLocalizedMessage());
        }
    }

    @Test
    public void testExtractMonthTime() throws Exception {
        String sqlText = "select t, EXTRACT(MONTH FROM t) as \"MONTH\" from " + tableWatcherI + " order by t";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            fail("Expected exception attempting to get MONTH from Time type.");
        } catch (SQLSyntaxErrorException e) {
            assertEquals("The 'EXTRACT MONTH' function is not allowed on the 'TIME' type.", e.getLocalizedMessage());
        }
    }

    @Test
    public void testExtractMonthNameTime() throws Exception {
        String sqlText = "select t, EXTRACT(MONTHNAME FROM t) as \"MONTHNAME\" from " + tableWatcherI + " order by t";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            fail("Expected exception attempting to get MONTHNAME from Time type.");
        } catch (SQLSyntaxErrorException e) {
            assertEquals("The 'EXTRACT MONTHNAME' function is not allowed on the 'TIME' type.", e.getLocalizedMessage());
        }
    }

    @Test
    public void testMonthNameTime() throws Exception {
        String sqlText = "select t, MONTHNAME(t) as \"MONTHNAME\" from " + tableWatcherI + " order by t";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            fail("Expected exception attempting to get MONTHNAME from Time type.");
        } catch (SQLSyntaxErrorException e) {
            assertEquals("The 'EXTRACT MONTHNAME' function is not allowed on the 'TIME' type.", e.getLocalizedMessage());
        }
    }

    @Test
    public void testExtractWeekTime() throws Exception {
        String sqlText = "select t, EXTRACT(WEEK FROM t) as \"WEEK\" from " + tableWatcherI + " order by t";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            fail("Expected exception attempting to get WEEK from Time type.");
        } catch (SQLSyntaxErrorException e) {
            assertEquals("The 'EXTRACT WEEK' function is not allowed on the 'TIME' type.", e.getLocalizedMessage());
        }
    }

    @Test
    public void testWeekTime() throws Exception {
        String sqlText = "select t, WEEK(t) as \"WEEK\" from " + tableWatcherI + " order by t";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            fail("Expected exception attempting to get WEEK from Time type.");
        } catch (SQLSyntaxErrorException e) {
            assertEquals("The 'EXTRACT WEEK' function is not allowed on the 'TIME' type.", e.getLocalizedMessage());
        }
    }

    @Test
    public void testExtractWeekDayTime() throws Exception {
        String sqlText = "select t, EXTRACT(WEEKDAY FROM t) as \"WEEKDAY\" from " + tableWatcherI + " order by t";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            fail("Expected exception attempting to get WEEKDAY from Time type.");
        } catch (SQLSyntaxErrorException e) {
            assertEquals("The 'EXTRACT WEEKDAY' function is not allowed on the 'TIME' type.", e.getLocalizedMessage());
        }
    }

    @Test
    public void testExtractWeekDayNameTime() throws Exception {
        String sqlText = "select t, EXTRACT(WEEKDAYNAME FROM t) as \"WEEKDAYNAME\" from " + tableWatcherI + " order by t";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            fail("Expected exception attempting to get WEEKDAYNAME from Time type.");
        } catch (SQLSyntaxErrorException e) {
            assertEquals("The 'EXTRACT WEEKDAYNAME' function is not allowed on the 'TIME' type.", e.getLocalizedMessage());
        }
    }

    @Test
    public void testExtractDayOfYearTime() throws Exception {
        String sqlText = "select t, EXTRACT(DAYOFYEAR FROM t) as \"DAYOFYEAR\" from " + tableWatcherI + " order by t";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            fail("Expected exception attempting to get DAYOFYEAR from Time type.");
        } catch (SQLSyntaxErrorException e) {
            assertEquals("The 'EXTRACT DAYOFYEAR' function is not allowed on the 'TIME' type.", e.getLocalizedMessage());
        }
    }

    @Test
    public void testExtractDayTime() throws Exception {
        String sqlText = "select t, EXTRACT(DAY FROM t) as \"DAY\" from " + tableWatcherI + " order by t";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            fail("Expected exception attempting to get DAY from Time type.");
        } catch (SQLSyntaxErrorException e) {
            assertEquals("The 'EXTRACT DAY' function is not allowed on the 'TIME' type.", e.getLocalizedMessage());
        }
    }

    @Test
    public void testExtractHourTime() throws Exception {
        String sqlText = "select t, EXTRACT(HOUR FROM t) as \"HOUR\" from " + tableWatcherI + " order by t";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected =
                "T    |HOUR |\n" +
                    "----------------\n" +
                    "00:00:01 |  0  |\n" +
                    "05:22:33 |  5  |\n" +
                    "10:30:29 | 10  |\n" +
                    "18:44:28 | 18  |\n" +
                    "20:38:40 | 20  |\n" +
                    "23:59:59 | 23  |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testExtractMinuteTime() throws Exception {
        String sqlText = "select t, EXTRACT(MINUTE FROM t) as \"MINUTE\" from " + tableWatcherI + " order by t";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected =
                "T    |MINUTE |\n" +
                    "------------------\n" +
                    "00:00:01 |   0   |\n" +
                    "05:22:33 |  22   |\n" +
                    "10:30:29 |  30   |\n" +
                    "18:44:28 |  44   |\n" +
                    "20:38:40 |  38   |\n" +
                    "23:59:59 |  59   |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testExtractSecondTime() throws Exception {
        String sqlText = "select t, EXTRACT(SECOND FROM t) as \"SECOND\" from " + tableWatcherI + " order by t";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            String expected =
                "T    |SECOND |\n" +
                    "------------------\n" +
                    "00:00:01 |   1   |\n" +
                    "05:22:33 |  33   |\n" +
                    "10:30:29 |  29   |\n" +
                    "18:44:28 |  28   |\n" +
                    "20:38:40 |  40   |\n" +
                    "23:59:59 |  59   |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testNow() throws Exception {
        String sqlText = "select t, NOW() from " + tableWatcherI + " order by T";
        // NOW() is same as CURRENT_TIMESTAMP. Only way to test is to make sure no exception is thrown
        int rows = 0;
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            while (rs.next()) {
                rows++;
            }
        }
        assertTrue("Expected some NOW() rows.", rows > 0);
    }

    @Test
    public void testInsertTimestampDuringDaylightSavingGap() throws Exception {
        Connection conn = methodWatcher.getOrCreateConnection();
        conn.createStatement().executeUpdate("create table DST(a TIMESTAMP)");

        Statement s = conn.createStatement();

        s.executeUpdate("INSERT INTO DST VALUES('2019-03-10 02:24:11')");
        s.executeUpdate("INSERT INTO DST VALUES('2014-03-09 02:45:11')");
        s.executeUpdate("INSERT INTO DST VALUES('2006-04-02 02:12:33')");
        s.executeUpdate("INSERT INTO DST VALUES('1990-04-01 02:12:33')");
        s.executeUpdate("INSERT INTO DST VALUES('1984-04-29 02:12:33')");
        s.executeUpdate("INSERT INTO DST VALUES('1967-04-30 02:12:33')");
        s.executeUpdate("INSERT INTO DST VALUES('2061-03-13 02:03:43.715')");
        s.executeUpdate("INSERT INTO DST VALUES('2077-03-14 02:52:30.712')");
    }

    //====================================================================================================
    // DB-2248, DB-2249: BI Tools and ODBC functions - end
    //====================================================================================================

    //DB-6790
    @Test
    public void testToDateWithIncorrectFormatPattern() throws Exception {
        String sqlText = "values to_date('2018-12','MM-DDD')";
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            fail("Expected exception the syntax of the string representation of a datetime value is incorrect");
        } catch (SQLDataException e) {
            assertEquals("22007", e.getSQLState());
        }
    }
}
