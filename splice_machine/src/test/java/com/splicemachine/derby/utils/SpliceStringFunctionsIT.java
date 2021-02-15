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

import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static junit.framework.Assert.assertEquals;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.*;

/**
 * Tests associated with the String functions as defined in
 * {@link SpliceStringFunctions}.
 * 
 * @author Walt Koetke
 */
public class SpliceStringFunctionsIT extends SpliceUnitTest {
	
    private static final String CLASS_NAME = SpliceStringFunctionsIT.class.getSimpleName().toUpperCase();
    private static SpliceWatcher classWatcher = new SpliceWatcher(CLASS_NAME);

    private static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    // Table for repeat testing
    private static final SpliceTableWatcher tableWatcherA = new SpliceTableWatcher(
            "A", schemaWatcher.schemaName, "(a int, b char(6), c varchar(6))");

    // Table for INSTR testing.
    private static final SpliceTableWatcher tableWatcherB = new SpliceTableWatcher(
    	"B", schemaWatcher.schemaName, "(a int, b varchar(30), c varchar(30), d int)");

    // Table for INITCAP testing.
    private static final SpliceTableWatcher tableWatcherC = new SpliceTableWatcher(
    	"C", schemaWatcher.schemaName, "(a varchar(30), b varchar(30))");

    // Table for CONCAT testing.
    private static final SpliceTableWatcher tableWatcherD = new SpliceTableWatcher(
    	"D", schemaWatcher.schemaName, "(a varchar(30), b varchar(30), c varchar(30))");

    // Table for CHR testing.
    private static final SpliceTableWatcher tableWatcherE = new SpliceTableWatcher(
            "E", schemaWatcher.schemaName, "(a int, b char(1))");

    // Table for DIGITS testing.
    private static final SpliceTableWatcher tableWatcherF = new SpliceTableWatcher(
            "F", schemaWatcher.schemaName, "(a int, b char(4))");

    private static final SpliceTableWatcher tableWatcherG = new SpliceTableWatcher(
            "G", schemaWatcher.schemaName, "(a char(20), b varchar(20))");

    private static final SpliceTableWatcher tableWatcherH = new SpliceTableWatcher(
            "H", schemaWatcher.schemaName, "(a float(7), b real, c double)");

    // Table for STRING_AGG testing.
    private static final SpliceTableWatcher tableWatcherI = new SpliceTableWatcher(
            "I", schemaWatcher.schemaName, "(a int, b varchar(10))");

    // Table for long string concatenate testing.
    private static final SpliceTableWatcher tableWatcherJ = new SpliceTableWatcher(
            "J", schemaWatcher.schemaName, "(a double)");

    // Table for RTRIM testing.
    private static final SpliceTableWatcher tableWatcherK = new SpliceTableWatcher(
            "K", schemaWatcher.schemaName, "(a varchar(10), b varchar(20), c varchar(10))");

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
            .around(tableWatcherJ)
            .around(tableWatcherK)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try{
                        try (PreparedStatement ps = classWatcher.prepareStatement(
                                "insert into " + tableWatcherA + " (a, b, c) values (?, ?, ?)")) {
                            ps.setInt   (1, 2); // row 1
                            ps.setString(2, "aaa");
                            ps.setString(3, "aaa");
                            ps.execute();
                            ps.setInt   (1, 3);
                            ps.setObject(2, "bbb");
                            ps.setString(3, "bbb");
                            ps.execute();
                            ps.setInt   (1, 4);
                            ps.setObject(2, null);
                            ps.setString(3, null);
                            ps.execute();
                        }

                        // Each of the following inserted rows represents an individual test,
                        // including expected result (column 'd'), for less test code in the
                        // testInstr methods.

                        try (PreparedStatement ps = classWatcher.prepareStatement(
                            "insert into " + tableWatcherB + " (a, b, c, d) values (?, ?, ?, ?)")) {
                            ps.setInt(1, 1); // row 1
                            ps.setObject(2, "Fred Flintstone");
                            ps.setString(3, "Flint");
                            ps.setInt(4, 6);
                            ps.execute();

                            ps.setInt(1, 2);
                            ps.setObject(2, "Fred Flintstone");
                            ps.setString(3, "Fred");
                            ps.setInt(4, 1);
                            ps.execute();

                            ps.setInt(1, 3);
                            ps.setObject(2, "Fred Flintstone");
                            ps.setString(3, " F");
                            ps.setInt(4, 5);
                            ps.execute();

                            ps.setInt(1, 4);
                            ps.setObject(2, "Fred Flintstone");
                            ps.setString(3, "Flintstone");
                            ps.setInt(4, 6);
                            ps.execute();

                            ps.setInt(1, 5);
                            ps.setObject(2, "Fred Flintstone");
                            ps.setString(3, "stoner");
                            ps.setInt(4, 0);
                            ps.execute();

                            ps.setInt(1, 6);
                            ps.setObject(2, "Barney Rubble");
                            ps.setString(3, "Wilma");
                            ps.setInt(4, 0);
                            ps.execute();

                            ps.setInt(1, 7);
                            ps.setObject(2, "Bam Bam");
                            ps.setString(3, "Bam Bam Bam");
                            ps.setInt(4, 0);
                            ps.execute();

                            ps.setInt(1, 8);
                            ps.setObject(2, "Betty");
                            ps.setString(3, "");
                            ps.setInt(4, 0);
                            ps.execute();

                            ps.setInt(1, 9);
                            ps.setObject(2, null);
                            ps.setString(3, null);
                            ps.setInt(4, 0);
                            ps.execute();
                        }
                        try (PreparedStatement ps = classWatcher.prepareStatement(
                                "insert into " + tableWatcherC + " (a, b) values (?, ?)")) {
                            ps.setObject(1, "freDdy kruGeR");
                            ps.setString(2, "Freddy Kruger");
                            ps.execute();
                        }
                        try (PreparedStatement ps = classWatcher.prepareStatement(
                                "insert into " + tableWatcherD + " (a, b, c) values (?, ?, ?)")) {
                            ps.setString(1, "AAA");
                            ps.setString(2, "BBB");
                            ps.setString(3, "AAABBB");
                            ps.execute();

                            ps.setString(1, "");
                            ps.setString(2, "BBB");
                            ps.setString(3, "BBB");
                            ps.execute();

                            ps.setString(1, "AAA");
                            ps.setString(2, "");
                            ps.setString(3, "AAA");
                            ps.execute();

                            ps.setString(1, "");
                            ps.setString(2, "");
                            ps.setString(3, "");
                            ps.execute();

                            ps.setString(1, null);
                            ps.setString(2, "BBB");
                            ps.setString(3, null);
                            ps.execute();

                            ps.setString(1, "AAA");
                            ps.setString(2, null);
                            ps.setString(3, null);
                            ps.execute();
                        }
                        try (PreparedStatement ps = classWatcher.prepareStatement(
                                "insert into " + tableWatcherE + " (a, b) values (?, ?)")) {
                            ps.setInt(1, 0);
                            ps.setString(2, "\u0000");
                            ps.execute();

                            ps.setInt(1, 255);
                            ps.setString(2, "ÿ");
                            ps.execute();

                            ps.setInt(1, 256);
                            ps.setString(2, "\u0000");
                            ps.execute();

                            ps.setInt(1, 65);
                            ps.setString(2, "A");
                            ps.execute();

                            ps.setInt(1, 97);
                            ps.setString(2, "a");
                            ps.execute();

                            ps.setInt(1, 321);
                            ps.setString(2, "A");
                            ps.execute();
                        }
                        try (PreparedStatement ps = classWatcher.prepareStatement(
                                "insert into " + tableWatcherF+ " (a, b) values (?, ?)")) {
                            ps.setInt(1, 1111567890);
                            ps.setString(2, "1111");
                            ps.execute();

                            ps.setInt(1, 1234567890);
                            ps.setString(2, "1234");
                            ps.execute();

                            ps.setNull(1, 4);
                            ps.setNull(2, 1);
                            ps.execute();
                        }
                        try (PreparedStatement ps = classWatcher.prepareStatement(
                                "insert into " + tableWatcherG+ " (a, b) values (?, ?)")) {
                            ps.setString(1, "12345678901234567890");
                            ps.setString(2, "12345678901234567890");
                            ps.execute();
                            ps.setString(1, "21345678901234567890");
                            ps.setString(2, "21345678901234567890");
                            ps.execute();
                        }
                        try (PreparedStatement ps = classWatcher.prepareStatement(
                                "insert into " + tableWatcherH+ " (a, b, c) values (?, ?, ?)")) {
                            ps.setFloat(1, 0.123456789f);
                            ps.setFloat(2, 0.123456789f);
                            ps.setDouble(3, 1234567890.0123456789);
                            ps.execute();
                        }
                        try (PreparedStatement ps = classWatcher.prepareStatement(
                                "insert into " + tableWatcherI+ " (a, b) values (?, ?)")) {
                            ps.setInt(1, 1);
                            ps.setString(2, "a");
                            ps.execute();
                            ps.setInt(1, 1);
                            ps.setString(2, "b");
                            ps.execute();
                            ps.setInt(1, 2);
                            ps.setString(2, "c");
                            ps.execute();
                            ps.setInt(1, 2);
                            ps.setString(2, "d");
                            ps.execute();
                            ps.setInt(1, 2);
                            ps.setString(2, "e");
                            ps.execute();
                            ps.setInt(1, 2);
                            ps.setString(2, "c");
                            ps.execute();
                        }
                        try (PreparedStatement ps = classWatcher.prepareStatement(
                                "insert into " + tableWatcherJ+ " (a) values (?)")) {
                            ps.setDouble(1, 12.3);
                            ps.execute();
                        }

                        try (PreparedStatement ps = classWatcher.prepareStatement(
                                "insert into " + tableWatcherK + " (a, b, c) values (?, ?, ?)")) {
                            ps.setString(1, "aaa");
                            ps.setString(2, "aaa");
                            ps.setString(3, "");
                            ps.execute();
                            ps.setString(1, "Zicam");
                            ps.setString(2, "Mycam");
                            ps.setString(3, "Zi");
                            ps.execute();
                            ps.setString(1, "CVS");
                            ps.setString(2, "s");
                            ps.setString(3, "CVS");
                            ps.execute();
                            ps.setString(1, "aaa\t");
                            ps.setString(2, "aaa");
                            ps.setString(3, "aaa\t");
                            ps.execute();
                            ps.setString(1, "abc");
                            ps.setString(2, "abcabcabc");
                            ps.setString(3, "");
                            ps.execute();
                            ps.setString(1, "abc");
                            ps.setString(2, "ac");
                            ps.setString(3, "ab");
                            ps.execute();
                            ps.setString(1, "abc ");
                            ps.setString(2, "abc");
                            ps.setString(3, "abc ");
                            ps.execute();
                            ps.setString(1, " ab c");
                            ps.setString(2, " ab cabcabc");
                            ps.setString(3, "");
                            ps.execute();
                            ps.setString(1, "abcabc");
                            ps.setString(2, "abc");
                            ps.setString(3, "");
                            ps.execute();
                            ps.setString(1, "\t\t\t\t\t\t");
                            ps.setString(2, "\t\t");
                            ps.setString(3, "");
                            ps.execute();
                            ps.setString(1, "\t\t\t \t\t\t");
                            ps.setString(2, "\t\t");
                            ps.setString(3, "\t\t\t ");
                            ps.execute();
                            ps.setString(1, "abc\t\t\t  \t");
                            ps.setString(2, " \t");
                            ps.setString(3, "abc\t\t\t ");
                            ps.execute();
                            ps.setString(1, "abc g  ");
                            ps.setString(2, " ");
                            ps.setString(3, "abc g");
                            ps.execute();
                        }
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
    public void testInstrFunction() throws Exception {
        int count = 0;
	    String sCell1 = null;
	    String sCell2 = null;

	    try (ResultSet rs = methodWatcher.executeQuery("SELECT INSTR(b, c), d from " + tableWatcherB)) {
            count = 0;
            while (rs.next()) {
                sCell1 = rs.getString(1);
                sCell2 = rs.getString(2);
                Assert.assertEquals("Wrong result value", sCell1, sCell2);
                count++;
            }
            Assert.assertEquals("Incorrect row count", 9, count);
        }
    }

    private void runRTrimTests(boolean useSpark) throws Exception {
	    String sCell1 = null;
	    String sCell2 = null;
        int count = 0;

	    // Use a join so we can test native spark execution.
	    try (ResultSet rs = methodWatcher.executeQuery(
	            "SELECT RTRIM(a.a,a.b), a.c from " + tableWatcherK + " a --splice-properties joinStrategy=nestedloop,useSpark=" + useSpark +
                    "\n, " + tableWatcherK + " b where a.a = b.a and a.b = b.b")) {

            while (rs.next()) {
                sCell1 = rs.getString(1);
                sCell2 = rs.getString(2);
                Assert.assertEquals("Wrong result value", sCell2, sCell1);
                count++;
            }
        }
	    Assert.assertEquals("Incorrect row count", 13, count);
    }

    @Test
    public void testRtrimFunction() throws Exception {
        runRTrimTests(false);
        runRTrimTests(true);
    }

    private void runRTrimSysFunTests(boolean useSpark, String expected) throws Exception {
	    String sCell1 = null;
	    String sCell2 = null;
        int count = 0;

	    // Use a join so we can test native spark execution.
	    String sqlText = "SELECT SYSFUN.RTRIM(a.a) from " + tableWatcherK + " a --splice-properties useSpark=" + useSpark +
                    "\n, " + tableWatcherK + " b where a.a = b.a and a.b = b.b";

        testQuery(sqlText, expected, methodWatcher);
    }

    @Test
    public void testRtrimSysFun() throws Exception {
        String sqlText = "select '-'|| repeat(b, 3) || '-' from A order by 1";

        String expected =
                "1   |\n" +
                "--------\n" +
                "       |\n" +
                "       |\n" +
                "  CVS  |\n" +
                " Zicam |\n" +
                "  aaa  |\n" +
                "  aaa  |\n" +
                " ab c  |\n" +
                "  abc  |\n" +
                "  abc  |\n" +
                "  abc  |\n" +
                "  abc  |\n" +
                " abc g |\n" +
                "abcabc |";

        runRTrimSysFunTests(false, expected);
        runRTrimSysFunTests(true, expected);
    }

    @Test
    public void testInitcapFunction() throws Exception {
	    String sCell1 = null;
	    String sCell2 = null;

	    try (ResultSet rs = methodWatcher.executeQuery("SELECT INITCAP(a), b from " + tableWatcherC)) {
            while (rs.next()) {
                sCell1 = rs.getString(1);
                sCell2 = rs.getString(2);
                Assert.assertEquals("Wrong result value", sCell2, sCell1);
            }
        }
    }

    @Test
    public void testConcatFunction() throws Exception {
	    String sCell1 = null;
	    String sCell2 = null;

	    try (ResultSet rs = methodWatcher.executeQuery("SELECT CONCAT(a, b), c from " + tableWatcherD)) {
            while (rs.next()) {
                sCell1 = rs.getString(1);
                sCell2 = rs.getString(2);
                Assert.assertEquals("Wrong result value", sCell2, sCell1);
            }
        }
    }

    @Test
    public void testConcatAliasFunction() throws Exception {
	    String sCell1 = null;
	    String sCell2 = null;

	    try (ResultSet rs = methodWatcher.executeQuery("SELECT a CONCAT b, c from " + tableWatcherD)) {
            while (rs.next()) {
                sCell1 = rs.getString(1);
                sCell2 = rs.getString(2);
                Assert.assertEquals("Wrong result value", sCell2, sCell1);
            }
        }
    }

    @Test
    public void testConcatFunctionCastOnArgs() throws Exception {
        try (ResultSet rs = methodWatcher.executeQuery("select concat(year(date('2021-01-01')), '.') from sysibm.sysdummy1")) {
            String expected = "1   |\n" +
                    "-------\n" +
                    "2021. |";
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    @Test
    public void testReges() throws Exception {
        try (ResultSet rs = methodWatcher.executeQuery("select count(*) from d where REGEXP_LIKE(a, 'aa*')")) {
            rs.next();
            Assert.assertEquals(3, rs.getInt(1));
        }
    }

    @Test
    public void testRepeat() throws Exception {
        // Q1: repeat for fixed char type
        String sqlText = "select '-'|| repeat(b, 3) || '-' from A order by 1";
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "1          |\n" +
                        "----------------------\n" +
                        "-aaa   aaa   aaa   - |\n" +
                        "-bbb   bbb   bbb   - |\n" +
                        "        NULL         |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        // Q2: repeat for varchar type
        sqlText = "select '-'|| repeat(c, 3) || '-' from A order by 1";
        rs = methodWatcher.executeQuery(sqlText);
        expected =
                "1      |\n" +
                        "-------------\n" +
                        "-aaaaaaaaa- |\n" +
                        "-bbbbbbbbb- |\n" +
                        "   NULL     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        // Q3: repeat with column reference as the repeated time
        sqlText = "select '-'|| repeat(c, a) || '-' from A";
        rs = methodWatcher.executeQuery(sqlText);
        expected =
                "1      |\n" +
                        "-------------\n" +
                        " -aaaaaa-   |\n" +
                        "-bbbbbbbbb- |\n" +
                        "   NULL     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // Q4: repeat 0 times, empty string should b returned
        sqlText = "select '-'|| repeat(c, 0) || '-' from A";
        rs = methodWatcher.executeQuery(sqlText);
        expected =
                "1  |\n" +
                        "------\n" +
                        " --  |\n" +
                        " --  |\n" +
                        "NULL |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testLTrimNegative() throws Exception{
        try {
            String sqlText = "values LTRIM('XXXXKATEXXXXXX', 'X')";
            methodWatcher.executeQuery(sqlText);
            Assert.fail("Query is expected to fail with syntax error!");
        } catch (SQLSyntaxErrorException e) {
            Assert.assertEquals(SQLState.LANG_SYNTAX_ERROR, e.getSQLState());
        }
    }

    @Test
    public void testRepeatNegative() throws Exception {
        try {
            String sqlText = "select repeat(a, 3) from A order by 1";
            methodWatcher.executeQuery(sqlText);
            Assert.fail("Query is expected to fail with syntax error!");
        } catch (SQLSyntaxErrorException e) {
            Assert.assertEquals(e.getSQLState(), SQLState.LANG_INVALID_FUNCTION_ARG_TYPE);
        }

        try {
            String sqlText = "select repeat(b, -3) from A order by 1";
            methodWatcher.executeQuery(sqlText);
            Assert.fail("Query is expected to fail with syntax error!");
        } catch (SQLDataException e) {
            assertTrue("Unexpected error code: " + e.getSQLState(),  SQLState.LANG_INVALID_FUNCTION_ARGUMENT.startsWith(e.getSQLState()));
        }
    }

    @Test
    public void testRepeatWithNULLArguments() throws Exception {

        String sqlText = "select repeat(b, case when 1=0 then 1 end) from A order by 1";
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "1  |\n" +
                        "------\n" +
                        "NULL |\n" +
                        "NULL |\n" +
                        "NULL |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        sqlText = "select repeat(case when 1=0 then 'a' end, a) from A order by 1";
        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

    }
    @Test
    public void testCHR() throws Exception {
        String sCell1 = null;
        String sCell2 = null;
        ResultSet rs;

        rs = methodWatcher.executeQuery("SELECT chr(a), b from " + tableWatcherE);
        while (rs.next()) {
            sCell1 = rs.getString(1);
            sCell2 = rs.getString(2);
            Assert.assertEquals("Wrong result value", sCell2, sCell1);
        }
        rs.close();

        String sqlText = "values chr(67.0)";
        rs = methodWatcher.executeQuery(sqlText);
        rs.next();
        Assert.assertEquals("Wrong result value", "C", rs.getString(1) );

        sqlText = "values chr(null)";
        rs = methodWatcher.executeQuery(sqlText);
        rs.next();
        Assert.assertEquals("Wrong result value", null, rs.getString(1) );
    }

    @Test
    public void testCHRNegative() throws Exception {
        try {
            String sqlText = "values chr(1,2)";
            methodWatcher.executeQuery(sqlText);
            Assert.fail("Query is expected to fail with syntax error!");
        } catch (SQLSyntaxErrorException e) {
            Assert.assertEquals("42Y03" , e.getSQLState());
        }

        try {
            String sqlText = "values chr('A')";
            methodWatcher.executeQuery(sqlText);
            Assert.fail("Query is expected to fail with syntax error!");
        } catch (SQLDataException e) {
            Assert.assertEquals("22018", e.getSQLState());
        }
    }

    @Test
    public void testDIGITSWithNumberType() throws Exception {
        String sCell1;
        String sCell2;
        ResultSet rs;

        rs = methodWatcher.executeQuery("SELECT substr(digits(a),1,4), b from " + tableWatcherF);
        while (rs.next()) {
            sCell1 = rs.getString(1);
            sCell2 = rs.getString(2);
            Assert.assertEquals("Wrong result value", sCell2, sCell1);
        }
        rs.close();

        String sqlText = "values digits(cast(67 as smallint))";
        rs = methodWatcher.executeQuery(sqlText);
        rs.next();
        Assert.assertEquals("Wrong result value", "00067", rs.getString(1) );
        rs.close();

        sqlText = "values length (digits(cast(67 as smallint)))";
        rs = methodWatcher.executeQuery(sqlText);
        rs.next();
        Assert.assertEquals("Wrong result value", "5", rs.getString(1) );
        rs.close();

        sqlText = "values digits(cast(67 as int))";
        rs = methodWatcher.executeQuery(sqlText);
        rs.next();
        Assert.assertEquals("Wrong result value", "0000000067", rs.getString(1) );
        rs.close();

        sqlText = "values digits(cast(67 as bigint))";
        rs = methodWatcher.executeQuery(sqlText);
        rs.next();
        Assert.assertEquals("Wrong result value", "0000000000000000067", rs.getString(1) );
        rs.close();

        sqlText = "values digits(cast(67 as tinyint))";
        rs = methodWatcher.executeQuery(sqlText);
        rs.next();
        Assert.assertEquals("Wrong result value", "00067", rs.getString(1) );
        rs.close();


        sqlText = "values digits(cast(-6.28 as decimal(6,2)))";
        rs = methodWatcher.executeQuery(sqlText);
        rs.next();
        Assert.assertEquals("Wrong result value", "000628", rs.getString(1) );
        rs.close();

        /* test null */
        sqlText = "SELECT digits(a), digits(b) from " + tableWatcherF + " where a is null";
        rs = methodWatcher.executeQuery(sqlText);
        String expected = "1  |  2  |\n" +
                "------------\n" +
                "NULL |NULL |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* test float, real and double */
        sqlText = "SELECT digits(a), length(digits(a)), digits(b), length(digits(b)), digits(c), length(digits(c)) from " + tableWatcherH;
        rs = methodWatcher.executeQuery(sqlText);
        expected = "1            | 2 |           3            | 4 |                          5                          | 6 |\n" +
                "--------------------------------------------------------------------------------------------------------------------\n" +
                "00000000000000000123457 |23 |00000000000000000123457 |23 |0000000000000000000000000000000000001234567890012346 |52 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }


    @Test
    public void testDIGITSWithCharacterType() throws Exception {
        String sqlText = "SELECT digits(a), digits(b) from " + tableWatcherG;
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected = "1                |               2                |\n" +
                "------------------------------------------------------------------\n" +
                "0000012345678901234567890000000 |0000012345678901234567890000000 |\n" +
                "0000021345678901234567890000000 |0000021345678901234567890000000 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        sqlText = "SELECT digits(a), digits(b) from " + tableWatcherG + " --splice-properties useSpark=true";
        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // char type is converted to decimal (31,6), so it can only accommodate 25 digits before the decimal point
        sqlText = "values digits('1234567890123456789012345678901')";
        try {
        rs = methodWatcher.executeQuery(sqlText);
        } catch (SQLDataException e) {
            Assert.assertEquals("22003", e.getSQLState());
        } finally {
            if (rs != null)
                rs.close();
        }

        sqlText = "values digits('1234567890123456789012345.678901')";
        rs = methodWatcher.executeQuery(sqlText);
        expected = "1                |\n" +
                "---------------------------------\n" +
                "1234567890123456789012345678901 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        sqlText = "values digits('-1234567890123456789012345.678901')";
        rs = methodWatcher.executeQuery(sqlText);
        expected = "1                |\n" +
                "---------------------------------\n" +
                "1234567890123456789012345678901 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

    }

    @Test
    public void testDIGITSNegative() throws Exception {
        try {
            String sqlText = "values digits(1,2)";
            methodWatcher.executeQuery(sqlText);
            Assert.fail("Query is expected to fail with syntax error!");
        } catch (SQLSyntaxErrorException e) {
            Assert.assertEquals("42X01" , e.getSQLState());
        }

        try {
            String sqlText = "values digits('A')";
            methodWatcher.executeQuery(sqlText);
            Assert.fail("Query is expected to fail with syntax error!");
        } catch (SQLDataException e) {
            Assert.assertEquals("22018", e.getSQLState());
        }
    }

    @Test
    public void testDIGITSWithParameter() throws Exception {
        try (Connection conn = methodWatcher.getOrCreateConnection()) {
            /* test string as input parameter */
            String sqlText = format("select * from %s where digits(?) = (select col1 from (values digits('1234')) dt(col1))", tableWatcherF);
            String expected = "A     |  B  |\n" +
                    "------------------\n" +
                    "1111567890 |1111 |\n" +
                    "1234567890 |1234 |\n" +
                    "   NULL    |NULL |";
            try (PreparedStatement ps = conn.prepareStatement(sqlText)) {
                ps.setString(1, "1234");
                ResultSet rs = ps.executeQuery();
                assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
                rs.close();
            }

            /* test integer as input parameter, it will be treated as character also */
            try (PreparedStatement ps = conn.prepareStatement(sqlText)) {
                ps.setInt(1, 1234);
                ResultSet rs = ps.executeQuery();
                assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
                rs.close();
            }

            sqlText = format("select * from %s where digits(cast(? as int)) = (select col1 from (values digits(1234)) dt(col1))", tableWatcherF);
            try (PreparedStatement ps = conn.prepareStatement(sqlText)) {
                ps.setInt(1, 1234);
                ResultSet rs = ps.executeQuery();
                assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
                rs.close();
            }
        }
    }

    @Test
    public void testCastToFixedLengthCharType() throws Exception {
        String sqlText = "values '-' || cast('aaa' as CHAR(5)) || '-'";
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected = "1    |\n" +
                "---------\n" +
                "-aaa  - |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testHEX() throws Exception {

        // note that HEX is using getBytes("UTF-8") to get a fixed representation

        String tests[] = {
                "'B'",              "42",
                "'000020'",         "303030303230",
                "'000020'" ,        "303030303230",
                "'ß'",              "C39F",
                "'\u00E4'",         "C3A4",     // ä  https://www.compart.com/de/unicode/U+00E4
                "'\uD83D\uDE02'",   "F09F9882", // 😂 https://www.compart.com/de/unicode/U+1F602
                "'Hello, World!'",    "48656C6C6F2C20576F726C6421",

                "''",               "",
                "null",             null,
                "concat('a','b')",  "6162"
        };

        for(int i=0; i<tests.length; i+=2)
        {
            String sql = "values hex(" + tests[i] + ")", expected = tests[i+1];
            Assert.assertEquals( "when executing " + sql,
                    expected, methodWatcher.executeGetString(sql, 1) );
        }

        try {
            String sqlText = "values hex(1.2)";
            methodWatcher.executeQuery(sqlText);
            Assert.fail("Exception not thrown: Cannot convert types 'DECIMAL' to 'VARCHAR'");
        } catch (SQLSyntaxErrorException e) {
            Assert.assertEquals("42846", e.getSQLState());
        }
    }

    @Test(timeout=1000) //Time out after 1 second
    public void testLongConcatenateString() throws Exception {
        String sqlText = "SELECT CAST(CAST(a AS DECIMAL(3,1)) AS CHAR(4))||','"
                + "||CAST(CAST(a AS DECIMAL(3,1)) AS CHAR(4))||','"
                + "||CAST(CAST(a AS DECIMAL(3,1)) AS CHAR(4))||','"
                + "||CAST(CAST(a AS DECIMAL(3,1)) AS CHAR(4))||','"
                + "||CAST(CAST(a AS DECIMAL(3,1)) AS CHAR(4))||','"
                + "||CAST(CAST(a AS DECIMAL(3,1)) AS CHAR(4))||','"
                + "||CAST(CAST(a AS DECIMAL(3,1)) AS CHAR(4))||','"
                + "||CAST(CAST(a AS DECIMAL(3,1)) AS CHAR(4))||','"
                + "||CAST(CAST(a AS DECIMAL(3,1)) AS CHAR(4))||','"
                + "||CAST(CAST(a AS DECIMAL(3,1)) AS CHAR(4))||','"
                + "||CAST(CAST(a AS DECIMAL(3,1)) AS CHAR(4)) FROM " + tableWatcherJ ;
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        rs.next();

        assertEquals("Wrong result","12.3,12.3,12.3,12.3,12.3,12.3,12.3,12.3,12.3,12.3,12.3", rs.getString(1));
        rs.close();
    }


    @Test
    public void testSTRING_AGG() throws Exception {
        for (boolean useSpark : new boolean[]{ true, false }) {
            // Simple aggregate
            try (ResultSet rs = methodWatcher.executeQuery(String.format("select STRING_AGG(b, ', '), count(*) " +
                    "from I --splice-properties useSpark=%s", useSpark))) {
                assertTrue(rs.next());
                String result = rs.getString(1);
                assertThat(result, allOf(Stream.of("a", "b", "c", "d", "e").map(s -> containsString(s)).collect(Collectors.toList())));
                assertEquals(6, result.split(", ").length);
                int count = rs.getInt(2);
                assertEquals(6, count);
                assertFalse(rs.next());
            }

            // Group by aggregate
            try (ResultSet rs = methodWatcher.executeQuery(String.format("select a, STRING_AGG(b, '# ') " +
                    "from I --splice-properties useSpark=%s %n group by a", useSpark))) {
                for (int i = 0; i < 2; ++i) {
                    assertTrue(rs.next());
                    int group = rs.getInt(1);
                    String result = rs.getString(2);
                    Stream<String> values = null;
                    int count = 0;
                    switch (group) {
                        case 1:
                            values = Stream.of("a", "b");
                            count = 2;
                            break;
                        case 2:
                            values = Stream.of("c", "d", "e");
                            count = 4;
                            break;
                        default:
                            fail("Unexpected group: " + group);
                    }
                    assertThat(result, allOf(values.map(s -> containsString(s)).collect(Collectors.toList())));
                    assertEquals(count, result.split("# ").length);
                }
                assertFalse(rs.next());
            }

            // Window function aggregate
            try (ResultSet rs = methodWatcher.executeQuery(String.format(
                    "select a, string_agg(b, ' - ') over (partition by a order by b asc) as agg " +
                            "from I --splice-properties useSpark=%s", useSpark))) {
                String expected =
                        "A |     AGG      |\n" +
                        "-------------------\n" +
                        " 1 |      a       |\n" +
                        " 1 |    a - b     |\n" +
                        " 2 |    c - c     |\n" +
                        " 2 |    c - c     |\n" +
                        " 2 |  c - c - d   |\n" +
                        " 2 |c - c - d - e |";
                String result = TestUtils.FormattedResult.ResultFactory.toString(rs);
                assertEquals(expected, result);
            }

            // Simulate order by clause
            try (ResultSet rs = methodWatcher.executeQuery(String.format(
                    "select a, agg from (select a, " +
                            "lead(a) over (partition by a order by b asc) as l, " +
                            "string_agg(b, ' - ') over (partition by a order by b asc) as agg " +
                            "from I --splice-properties useSpark=%s %n) b where l is null", useSpark))) {
                String expected =
                        "A |     AGG      |\n" +
                        "-------------------\n" +
                        " 1 |    a - b     |\n" +
                        " 2 |c - c - d - e |";
                String result = TestUtils.FormattedResult.ResultFactory.toString(rs);
                assertEquals(expected, result);
            }
        }
    }

    @Test
    public void testSubstrResultType() throws Exception {
        try (TestConnection conn = methodWatcher.getOrCreateConnection()) {
            checkExpressionType("substr(varchar('abc', 40), 10)", "VARCHAR(40)", conn);
            checkExpressionType("substr(varchar('abc', 40), 10, 5)", "CHAR(5)", conn);
            checkExpressionType("substr(char('abc', 40), 10)", "CHAR(31)", conn);
            checkExpressionType("substr(char('abc', 40), 10, 5)", "CHAR(5)", conn);

            checkExpressionType("substr(cast('abc' as varchar(40) for bit data), 10)", "VARCHAR (40) FOR BIT DATA", conn);
            checkExpressionType("substr(cast('abc' as varchar(40) for bit data), 10, 5)", "CHAR (5) FOR BIT DATA", conn);
            checkExpressionType("substr(cast('abc' as char(40) for bit data), 10)", "CHAR (31) FOR BIT DATA", conn);
            checkExpressionType("substr(cast('abc' as char(40) for bit data), 10, 5)", "CHAR (5) FOR BIT DATA", conn);
        }
    }

    @Test
    public void testSubstr() throws Exception {
        methodWatcher.execute("drop table testSubstr if exists");
        methodWatcher.execute("create table testSubstr(a varchar(40), b varchar(40), c char(40), " +
                "d varchar(40) for bit data, e varchar(40) for bit data, f char(40) for bit data, dash char(1) for bit data)");
        methodWatcher.execute("insert into testSubstr values ('abc', 'abc ', 'abc'," +
                "cast('abc' as varchar(40) for bit data), cast('abc ' as varchar(40) for bit data), cast('abc' as char(40) for bit data), cast('-' as char(1) for bit data))");
        TestConnection[] conns = {
                methodWatcher.connectionBuilder().useOLAP(false).build(),
                methodWatcher.connectionBuilder().useOLAP(true).useNativeSpark(false).build(),
                methodWatcher.connectionBuilder().useOLAP(true).useNativeSpark(true).build()
        };
        for (TestConnection conn: conns) {
            checkStringExpression("'-' || substr(a, 2) || '-' from testSubstr", "-bc-", conn);
            checkStringExpression("'-' || substr(a, 2, 5) || '-' from testSubstr", "-bc   -", conn);
            checkStringExpression("'-' || substr(b, 2) || '-' from testSubstr", "-bc -", conn);
            checkStringExpression("'-' || substr(b, 2, 5) || '-' from testSubstr", "-bc   -", conn);
            checkStringExpression("'-' || substr(c, 2) || '-' from testSubstr", "-bc                                     -", conn);
            checkStringExpression("'-' || substr(c, 2, 5) || '-' from testSubstr", "-bc   -", conn);
            checkStringExpression("dash || substr(d, 2) || dash from testSubstr", "2d62632d", conn);
            checkStringExpression("dash || substr(d, 2, 5) || dash from testSubstr", "2d62632020202d", conn);
            checkStringExpression("dash || substr(e, 2) || dash from testSubstr", "2d6263202d", conn);
            checkStringExpression("dash || substr(e, 2, 5) || dash from testSubstr", "2d62632020202d", conn);
            checkStringExpression("dash || substr(f, 2) || dash from testSubstr", "2d6263202020202020202020202020202020202020202020202020202020202020202020202020202d", conn);
            checkStringExpression("dash || substr(f, 2, 5) || dash from testSubstr", "2d62632020202d", conn);
        }
    }
}
