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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 * Test decimal precision extension from 31 digits to 38 digits.
 */
@Category(value = {SerialTest.class})
@RunWith(Parameterized.class)
public class DecimalIT  extends SpliceUnitTest {
    
    private Boolean useSpark;
    
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{true});
        params.add(new Object[]{false});
        return params;
    }
    public static final String CLASS_NAME = DecimalIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);
    
    public DecimalIT(Boolean useSpark) {
        this.useSpark = useSpark;
    }
    
    public static void createData(Connection conn, String schemaName) throws Exception {


        new TableCreator(conn)
                .withCreate("create table ts_decimal (a dec(38,5), b int, primary key(a, b))")
                .withInsert("insert into ts_decimal values(?, ?)")
                .withRows(rows(
                        row(9999999999.7, 1),
                        row(9999999999.9, 2),
                        row(9999999999.9, 3)))
                .create();

        new TableCreator(conn)
                .withCreate("create table ts_decimal2 (b dec(38,0), c int, primary key(b, c))")
                .withInsert("insert into ts_decimal2 values(?, ?)")
                .withRows(rows(
                        row(new BigDecimal("10000000000000000000000000000000"), 1),
                        row(new BigDecimal("100000000000000000000000000000000"), 2),
                        row(new BigDecimal("1000000000000000000000000000000000"), 3),
                        row(new BigDecimal("10000000000000000000000000000000000"), 4),
                        row(new BigDecimal("100000000000000000000000000000000000"), 5),
                        row(new BigDecimal("1000000000000000000000000000000000000"), 6),
                        row(new BigDecimal("10000000000000000000000000000000000000"), 7),
                        row(new BigDecimal("10000000000000000000000000000000000000"), 8),
                        row(new BigDecimal("10000000000000000000000000000000000000"), 9),
                        row(new BigDecimal("10000000000000000000000000000000000000"), 10),
                        row(new BigDecimal("10000000000000000000000000000000000000"), 11),
                        row(new BigDecimal("10000000000000000000000000000000000000"), 12),
                        row(new BigDecimal("10000000000000000000000000000000000000"), 13),
                        row(new BigDecimal("10000000000000000000000000000000000000"), 14),
                        row(new BigDecimal("10000000000000000000000000000000000000"), 15)))
                .create();

        new TableCreator(conn)
                .withCreate("create table ts_decimal3 (b dec(38,0), c int)")
                .create();

        String
        sqlText = "insert into ts_decimal3 select * from ts_decimal2";
		spliceClassWatcher.executeUpdate(sqlText);
		spliceClassWatcher.executeUpdate(sqlText);

        new TableCreator(conn)
                .withCreate("create table ts_decimal4 (b dec(38,31), c int)")
                .withIndex("create index dec_ix4 on ts_decimal4(b)")
                .withInsert("insert into ts_decimal4 values(?, ?)")
                // The inserted rows will have digits truncated to match the column data type.
                .withRows(rows(
                        row(new BigDecimal("1234567.8901234567890123456789012345678"), 1),
                        row(new BigDecimal("123456.78901234567890123456789012345678"), 2),
                        row(new BigDecimal("12345.678901234567890123456789012345678"), 3),
                        row(new BigDecimal("1234.5678901234567890123456789012345678"), 4),
                        row(new BigDecimal("123.45678901234567890123456789012345678"), 5),
                        row(new BigDecimal("12.345678901234567890123456789012345678"), 6),
                        row(new BigDecimal("1.2345678901234567890123456789012345678"), 7),
                        row(new BigDecimal(".12345678901234567890123456789012345678"), 8),
                        row(new BigDecimal(".01234567890123456789012345678901234567"), 9),
                        row(new BigDecimal(".00123456789012345678901234567890123456"), 10),
                        row(new BigDecimal(".00012345678901234567890123456789012345"), 11),
                        row(new BigDecimal(".00001234567890123456789012345678901234"), 12),
                        row(new BigDecimal(".00000123456789012345678901234567890123"), 13),
                        row(new BigDecimal(".00000012345678901234567890123456789012"), 14),
                        row(new BigDecimal(".00000001234567890123456789012345678901"), 15)))
                .create();

        new TableCreator(conn)
                .withCreate("create table preparedDecimal (a dec(38,5))")
                .create();

        new TableCreator(conn)
                .withCreate("create table DB_9425_TBL (quantity decimal (22,4), units decimal (19,9), price decimal(19,9), factor decimal(19,9))")
                .withInsert("insert into DB_9425_TBL values (?, ?, ?, ?)")
                .withRows(rows(
                        row(1333333333.1111, 1, 150000.555555, 15.5555555)))
                .create();

        new TableCreator(conn)
                .withCreate("create table DB_9425_TBL_2 (a decimal (37,0), b decimal(22,11), c decimal(25,19))")
                .withInsert("insert into DB_9425_TBL_2 values (?, ?, ?)")
                .withRows(rows(
                        row(new BigDecimal("9999999999999999999999999999999999999"), 1333333333.1111, 150000.555555)))
                .create();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }

    @Test
    public void testIllegalCreateTable() throws Exception {

        String sqlText = format("create table dec1 (a dec(39,0)) --splice-properties useSpark=%s", useSpark);

        List<String> expectedErrors =
           Arrays.asList("Value '39' is not a valid precision for DECIMAL.");
        testUpdateFail(sqlText, expectedErrors, methodWatcher);

        sqlText = format("create table dec1 (a dec(38,39)) --splice-properties useSpark=%s", useSpark);
        expectedErrors =
           Arrays.asList("Scale '39' is not a valid scale for a DECIMAL.");
        testUpdateFail(sqlText, expectedErrors, methodWatcher);

        sqlText = format("create table dec1 (a dec(38,-1)) --splice-properties useSpark=%s", useSpark);
        expectedErrors =
           Arrays.asList("Syntax error: Encountered \"-\" at line 1, column 29.");
        testUpdateFail(sqlText, expectedErrors, methodWatcher);
    }

    @Test
    public void testAvg() throws Exception {
        // Native spark execution does not use an intermediate data type
        // for AVG which can avoid overflows when dealing with large numbers.
        // Since we have no control over native spark execution,
        // skip these tests on Spark for now...
        if (useSpark)
            return;
        String
        sqlText = format("select avg(a) from ts_decimal --splice-properties useSpark=%s", useSpark);

        String
        expected =
                "1        |\n" +
                "------------------\n" +
                "9999999999.83333 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select avg(a*a) from ts_decimal --splice-properties useSpark=%s", useSpark);

        expected =
                "1              |\n" +
                "-----------------------------\n" +
                "99999999996666666666.703333 |";

        testQuery(sqlText, expected, methodWatcher);


        sqlText = format("select avg(b) from ts_decimal2 --splice-properties useSpark=%s", useSpark);

        expected =
                "1                   |\n" +
                "---------------------------------------\n" +
                "6074074000000000000000000000000000000 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select avg(b) from ts_decimal3 --splice-properties useSpark=%s", useSpark);

        testQuery(sqlText, expected, methodWatcher);

        expected =
                "1                   |\n" +
                "---------------------------------------\n" +
                "91449.4733424781892181078189218107819 |";
        sqlText = format("select avg(b) from ts_decimal4 --splice-properties useSpark=%s", useSpark);

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select avg(a.a) from ts_decimal a --splice-properties useSpark=%s, joinStrategy=SORTMERGE\n" +
                         ", ts_decimal b where a.b = b.b", useSpark);

        expected =
                "1        |\n" +
                "------------------\n" +
                "9999999999.83333 |";

        testQuery(sqlText, expected, methodWatcher);
    }

    @Test
    public void testSum() throws Exception {

        String sqlText = format("select sum(a) from ts_decimal --splice-properties useSpark=%s", useSpark);

        String expected =
                "1         |\n" +
                "-------------------\n" +
                "29999999999.50000 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select sum(a*a) from ts_decimal --splice-properties useSpark=%s", useSpark);
        // DECIMAL(38,5) * DECIMAL(38,5), result scale is reduced to MIN_DECIMAL_MULTIPLICATION_SCALE from 10

        expected =
                "1              |\n" +
                "------------------------------\n" +
                "299999999990000000000.110000 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select sum(b) from ts_decimal2 --splice-properties useSpark=%s", useSpark);

        expected =
                "1                   |\n" +
                "----------------------------------------\n" +
                "91111110000000000000000000000000000000 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select sum(b) from ts_decimal3 --splice-properties useSpark=%s", useSpark);

        List<String> expectedErrors =
           Arrays.asList("Overflow occurred during numeric data type conversion of \"1.8222222E+38\".",
                         "The numeric literal \"182222220000000000000000000000000000000\" is not valid because its value is out of range.");
        //testFail(sqlText, expectedErrors, methodWatcher);

        expected =
                "1                    |\n" +
                "-----------------------------------------\n" +
                "1371742.1001371728382716172838271617284 |";

        sqlText = format("select sum(b) from ts_decimal4 --splice-properties useSpark=%s", useSpark);
        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select sum(a.a) from ts_decimal a --splice-properties useSpark=%s, joinStrategy=SORTMERGE\n" +
                         ", ts_decimal b where a.b = b.b", useSpark);

        expected =
                "1         |\n" +
                "-------------------\n" +
                "29999999999.50000 |";

        testQuery(sqlText, expected, methodWatcher);
    }

    @Test
    public void testArithmetic() throws Exception {
        String sqlText = format("select a+a+a+a+a+a+a+a+a+a+a+a+a+a+a+a from ts_decimal --splice-properties useSpark=%s", useSpark);

        String expected =
                "1         |\n" +
                "--------------------\n" +
                "159999999995.20000 |\n" +
                "159999999998.40000 |\n" +
                "159999999998.40000 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select a+a+a+a+a+a+a+a+a+a+a+a+a+a+a+a/3 from ts_decimal --splice-properties useSpark=%s", useSpark);

        expected =
                "1         |\n" +
                "--------------------\n" +
                "153333333328.73333 |\n" +
                "153333333331.80000 |\n" +
                "153333333331.80000 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select b+b+b+b+b+b+b+b from ts_decimal4 --splice-properties useSpark=%s", useSpark);

        expected =
                "1                    |\n" +
                "-----------------------------------------\n" +
                "   0.0000098765431209876543120987656    |\n" +
                "   0.0000987654312098765431209876544    |\n" +
                "   0.0009876543120987654312098765432    |\n" +
                "   0.0098765431209876543120987654312    |\n" +
                "   0.0987654312098765431209876543120    |\n" +
                "   0.9876543120987654312098765431208    |\n" +
                "   9.8765431209876543120987654312096    |\n" +
                "     9.876543120987654312098768E-7      |\n" +
                "     9.87654312098765431209880E-8       |\n" +
                "  98.7654312098765431209876543120984    |\n" +
                "  987.6543120987654312098765431209880   |\n" +
                " 9876.5431209876543120987654312098768   |\n" +
                " 98765.4312098765431209876543120987656  |\n" +
                "987654.3120987654312098765431209876544  |\n" +
                "9876543.1209876543120987654312098765424 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select (b+b+b+b+b+b+b+b)/3 from ts_decimal4 --splice-properties useSpark=%s", useSpark);

        expected =
                "1                    |\n" +
                "-----------------------------------------\n" +
                "   0.0000032921810403292181040329219    |\n" +
                "   0.0000329218104032921810403292181    |\n" +
                "   0.0003292181040329218104032921811    |\n" +
                "   0.0032921810403292181040329218104    |\n" +
                "   0.0329218104032921810403292181040    |\n" +
                "   0.3292181040329218104032921810403    |\n" +
                "   3.2921810403292181040329218104032    |\n" +
                "     3.292181040329218104032923E-7      |\n" +
                "     3.29218104032921810403293E-8       |\n" +
                "  32.9218104032921810403292181040328    |\n" +
                "  329.2181040329218104032921810403293   |\n" +
                " 3292.1810403292181040329218104032923   |\n" +
                " 32921.8104032921810403292181040329219  |\n" +
                "329218.1040329218104032921810403292181  |\n" +
                "3292181.0403292181040329218104032921808 |";

        testQuery(sqlText, expected, methodWatcher);

    }

    @Test
    public void testLiterals() throws Exception {

        String sqlText = format("select a, 12345678901234567890123456789012345678 from ts_decimal --splice-properties useSpark=%s\n order by 1,2", useSpark);

        String expected =
        "A        |                   2                   |\n" +
        "----------------------------------------------------------\n" +
        "9999999999.70000 |12345678901234567890123456789012345678 |\n" +
        "9999999999.90000 |12345678901234567890123456789012345678 |\n" +
        "9999999999.90000 |12345678901234567890123456789012345678 |";

        testQueryUnsorted(sqlText, expected, methodWatcher);

        sqlText = format("select a, -0.12345678901234567890123456789012345678 from ts_decimal --splice-properties useSpark=%s\n order by 1,2", useSpark);
        expected =
        "A        |                    2                     |\n" +
        "-------------------------------------------------------------\n" +
        "9999999999.70000 |-0.12345678901234567890123456789012345678 |\n" +
        "9999999999.90000 |-0.12345678901234567890123456789012345678 |\n" +
        "9999999999.90000 |-0.12345678901234567890123456789012345678 |";

        testQueryUnsorted(sqlText, expected, methodWatcher);

        sqlText = format("values -0.12345678901234567890123456789012345678, -.00000000000000000000000000000000000001", useSpark);
        expected =
        "1                     |\n" +
        "-------------------------------------------\n" +
        "-0.12345678901234567890123456789012345678 |\n" +
        "                 -1E-38                   |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("values 12345678901234567890123456789012345678, -0.12345678901234567890123456789012345678, -.00000000000000000000000000000000000001", useSpark);

        List<String> expectedErrors =
           Arrays.asList("The resulting value is outside the range for the data type DECIMAL/NUMERIC(38,38).");
        testFail(sqlText, expectedErrors, methodWatcher);
    }


    @Test
    public void testOrderBy() throws Exception {

        String sqlText = format("select a from ts_decimal --splice-properties useSpark=%s\n order by a", useSpark);

        String expected =
                "A        |\n" +
                "------------------\n" +
                "9999999999.70000 |\n" +
                "9999999999.90000 |\n" +
                "9999999999.90000 |";

        testQueryUnsorted(sqlText, expected, methodWatcher);

        sqlText = format("select b from ts_decimal2 --splice-properties useSpark=%s\n order by b", useSpark);

        expected =
                "B                   |\n" +
                "----------------------------------------\n" +
                "   10000000000000000000000000000000    |\n" +
                "   100000000000000000000000000000000   |\n" +
                "  1000000000000000000000000000000000   |\n" +
                "  10000000000000000000000000000000000  |\n" +
                " 100000000000000000000000000000000000  |\n" +
                " 1000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |";

        testQueryUnsorted(sqlText, expected, methodWatcher);

        sqlText = format("select b from ts_decimal4 --splice-properties useSpark=%s, index=dec_ix4\n order by b", useSpark);

        expected =
                "B                    |\n" +
                "-----------------------------------------\n" +
                "     1.23456789012345678901235E-8       |\n" +
                "     1.234567890123456789012346E-7      |\n" +
                "   0.0000012345678901234567890123457    |\n" +
                "   0.0000123456789012345678901234568    |\n" +
                "   0.0001234567890123456789012345679    |\n" +
                "   0.0012345678901234567890123456789    |\n" +
                "   0.0123456789012345678901234567890    |\n" +
                "   0.1234567890123456789012345678901    |\n" +
                "   1.2345678901234567890123456789012    |\n" +
                "  12.3456789012345678901234567890123    |\n" +
                "  123.4567890123456789012345678901235   |\n" +
                " 1234.5678901234567890123456789012346   |\n" +
                " 12345.6789012345678901234567890123457  |\n" +
                "123456.7890123456789012345678901234568  |\n" +
                "1234567.8901234567890123456789012345678 |";

        testQueryUnsorted(sqlText, expected, methodWatcher);
    }

    @Test
    public void testJoin() throws Exception {

        String sqlText = format("select t1.b from ts_decimal2 t1, ts_decimal2 t2 --splice-properties useSpark=%s\n where t1.b = t2.b", useSpark);

        String expected =
                "B                   |\n" +
                "----------------------------------------\n" +
                "   10000000000000000000000000000000    |\n" +
                "   100000000000000000000000000000000   |\n" +
                "  1000000000000000000000000000000000   |\n" +
                "  10000000000000000000000000000000000  |\n" +
                " 100000000000000000000000000000000000  |\n" +
                " 1000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |\n" +
                "10000000000000000000000000000000000000 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select t1.b from ts_decimal4 t1, ts_decimal4 t2 --splice-properties useSpark=%s\n where t1.b = t2.b", useSpark);

        expected =
                "B                    |\n" +
                "-----------------------------------------\n" +
                "   0.0000012345678901234567890123457    |\n" +
                "   0.0000123456789012345678901234568    |\n" +
                "   0.0001234567890123456789012345679    |\n" +
                "   0.0012345678901234567890123456789    |\n" +
                "   0.0123456789012345678901234567890    |\n" +
                "   0.1234567890123456789012345678901    |\n" +
                "   1.2345678901234567890123456789012    |\n" +
                "     1.234567890123456789012346E-7      |\n" +
                "     1.23456789012345678901235E-8       |\n" +
                "  12.3456789012345678901234567890123    |\n" +
                "  123.4567890123456789012345678901235   |\n" +
                " 1234.5678901234567890123456789012346   |\n" +
                " 12345.6789012345678901234567890123457  |\n" +
                "123456.7890123456789012345678901234568  |\n" +
                "1234567.8901234567890123456789012345678 |";

        testQuery(sqlText, expected, methodWatcher);
    }

    @Test
    public void testMultiProbe() throws Exception {

        String sqlText = format("select * from ts_decimal4 --splice-properties useSpark=%s, index=dec_ix4\n" +
                                "where b in (1234567.8901234567890123456789012345678,\n" +
                                "123456.7890123456789012345678901234568,\n" +
                                "12345.6789012345678901234567890123457,\n" +
                                "1234.5678901234567890123456789012346,\n" +
                                "123.4567890123456789012345678901235,\n" +
                                "12.3456789012345678901234567890123,\n" +
                                "1.2345678901234567890123456789012,\n" +
                                ".1234567890123456789012345678901,\n" +
                                ".0123456789012345678901234567890,\n" +
                                ".0012345678901234567890123456789,\n" +
                                ".0001234567890123456789012345679,\n" +
                                ".0000123456789012345678901234568,\n" +
                                ".0000012345678901234567890123457,\n" +
                                ".0000001234567890123456789012346,\n" +
                                ".0000000123456789012345678901235) order by b", useSpark);

        String expected =
                "B                    | C |\n" +
                "---------------------------------------------\n" +
                "     1.23456789012345678901235E-8       |15 |\n" +
                "     1.234567890123456789012346E-7      |14 |\n" +
                "   0.0000012345678901234567890123457    |13 |\n" +
                "   0.0000123456789012345678901234568    |12 |\n" +
                "   0.0001234567890123456789012345679    |11 |\n" +
                "   0.0012345678901234567890123456789    |10 |\n" +
                "   0.0123456789012345678901234567890    | 9 |\n" +
                "   0.1234567890123456789012345678901    | 8 |\n" +
                "   1.2345678901234567890123456789012    | 7 |\n" +
                "  12.3456789012345678901234567890123    | 6 |\n" +
                "  123.4567890123456789012345678901235   | 5 |\n" +
                " 1234.5678901234567890123456789012346   | 4 |\n" +
                " 12345.6789012345678901234567890123457  | 3 |\n" +
                "123456.7890123456789012345678901234568  | 2 |\n" +
                "1234567.8901234567890123456789012345678 | 1 |";

        testQueryUnsorted(sqlText, expected, methodWatcher);
    }

    @Test
    public void testPreparedStatementRetainsFractionalDigits() throws Exception {

        PreparedStatement statement = spliceClassWatcher.prepareStatement(String.format("delete from %s.%s", CLASS_NAME, "preparedDecimal"));
        statement.executeUpdate();
	statement.close();

        statement = spliceClassWatcher.prepareStatement(String.format("insert into %s.%s values (?)", CLASS_NAME, "preparedDecimal"));
        BigDecimal bd = new BigDecimal("14.51");
        statement.setObject(1, bd, Types.DECIMAL);
        statement.executeUpdate();
        statement.close();

        String
        sqlText = format("select a from preparedDecimal --splice-properties useSpark=%s", useSpark);

        String
        expected =
                "A    |\n" +
                "----------\n" +
                "14.51000 |";

        testQuery(sqlText, expected, methodWatcher);
        statement = spliceClassWatcher.prepareStatement(String.format("delete from %s.%s", CLASS_NAME, "preparedDecimal"));
        statement.executeUpdate();
	statement.close();


        // The following should input "14" because a Double doesn't
        // have a set scale, so an implicit scale of 0 is used
        // inside setObject.
        statement = spliceClassWatcher.prepareStatement(String.format("insert into %s.%s values (?)", CLASS_NAME, "preparedDecimal"));
        Double db = new Double("14.51");
        statement.setObject(1, db, Types.DECIMAL);
        statement.executeUpdate();
        statement.close();

        expected =
                "A    |\n" +
                "----------\n" +
                "14.00000 |";
        testQuery(sqlText, expected, methodWatcher);

        statement = spliceClassWatcher.prepareStatement(String.format("delete from %s.%s", CLASS_NAME, "preparedDecimal"));
        statement.executeUpdate();
	statement.close();

        // Using the version of setObject which allows scale to be passed in
        // allows the decimal digits to be retained.
        statement = spliceClassWatcher.prepareStatement(String.format("insert into %s.%s values (?)", CLASS_NAME, "preparedDecimal"));
        db = new Double("14.51");
        statement.setObject(1, db, Types.DECIMAL, 2);
        statement.executeUpdate();
        statement.close();

        expected =
                "A    |\n" +
                "----------\n" +
                "14.51000 |";
        testQuery(sqlText, expected, methodWatcher);
    }

    @Test
    public void testReduceFractionalDigitsForIntegralPartSum() throws Exception {
        /* We don't have enough digits for the whole result but only integral part.
         * Returning an integral value should be better than throws an exception.
         */
        String sqlText = String.format("select a + b\n" +
                "from DB_9425_TBL_2 --splice-properties useSpark=%s", useSpark);

        String expected = "1                   |\n" +
                "----------------------------------------\n" +
                "10000000000000000000000000001333333332 |";

        testQuery(sqlText, expected, methodWatcher);
    }

    @Test
    public void testReduceFractionalDigitsForIntegralPartTimes() throws Exception {
        /* DECIMAL(22,11) * DECIMAL(25,19), initial result type is DECIMAL(38,30).
         * Integral part needs at maximum 22 - 11 + 25 - 19 + 1 = 18 digits. Result type has 8.
         */
        String sqlText = String.format("select b * c\n" +
                "from DB_9425_TBL_2 --splice-properties useSpark=%s", useSpark);

        String expected = "1                  |\n" +
                "--------------------------------------\n" +
                "200000740706664.87653716050000000000 |";

        testQuery(sqlText, expected, methodWatcher);
    }

    @Test
    public void testReduceFractionalDigitsForIntegralPartDivide() throws Exception {
        /* DECIMAL(22,4) / DECIMAL(19,9), initial result type is DECIMAL(38,15).
         * Integral part needs at maximum 22 - 4 + 9 = 27 digits. Result type has 23.
         */
        String sqlText = String.format("select quantity / units\n" +
                "from DB_9425_TBL --splice-properties useSpark=%s", useSpark);

        String expected = "1           |\n" +
                "------------------------\n" +
                "1333333333.11110000000 |";

        testQuery(sqlText, expected, methodWatcher);
    }

    @Test
    public void testReduceFractionalDigitsForIntegralPartCustomerCase() throws Exception {
        String sqlText = String.format("select cast(COALESCE(((quantity/units) * ( price / units)* cast(factor AS decimal(10,2)) ),0 ) AS NUMERIC(31,10))\n" +
                "from DB_9425_TBL --splice-properties useSpark=%s", useSpark);

        String expected = "1              |\n" +
                "-----------------------------\n" +
                "3112011525395705.4789160000 |";

        testQuery(sqlText, expected, methodWatcher);
    }
}
