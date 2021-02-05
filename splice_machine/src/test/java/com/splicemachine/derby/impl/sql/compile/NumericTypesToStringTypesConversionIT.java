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
package com.splicemachine.derby.impl.sql.compile;

import com.splicemachine.db.iapi.types.FloatingPointDataType;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

@Category(SerialTest.class) // Not run in parallel since it is changing global DB configs
public class NumericTypesToStringTypesConversionIT extends SpliceUnitTest {
    private static final String SCHEMA = NumericTypesToStringTypesConversionIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);
    protected  static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(schemaWatcher);

    @BeforeClass
    public static void createdSharedTables() throws Exception{
        TestConnection conn = spliceClassWatcher.getOrCreateConnection();
        new TableCreator(conn)
                .withCreate("create table test (t tinyint, s smallint, i int, b bigint, f float, r real, d double, dc decimal(4,2), df decfloat)")
                .withInsert("insert into test values(?,?,?,?,?,?,?,?,?)")
                .withRows(rows(
                        row(127,0,+3,99887766,2.5,-1.5E+3,.2E2,-11.35,3.141592654),
                        row(-128,-1,987,-99887766,-.35,2E-2,-200,9.2,-0.12345678),
                        row(null,null,null,null,null,null,null,null,null)))
                .create();
    }

    private void withNotation(int notation) throws Exception {
        String notationStr = "";
        switch (notation) {
            case FloatingPointDataType.PLAIN:
                notationStr = "plain";
                break;
            case FloatingPointDataType.NORMALIZED:
                notationStr = "normalized";
                break;
            default:
                assert false;
        }
        methodWatcher.executeUpdate(String.format("call SYSCS_UTIL.SYSCS_SET_GLOBAL_DATABASE_PROPERTY( 'splice.function.floatingPointNotation', '%s' )", notationStr));
    }

    private static final String queryTemplate = "select cast(%s as %s(%d)), cast(%s as %s(%d)), %s(%s), %s(%s) from test";
    private static final String c = "char";
    private static final String vc = "varchar";

    private void testHelper(String expected, String columnName, String constLiteral, int length) throws Exception {
        // to char
        try(ResultSet rs = methodWatcher.executeQuery(format(queryTemplate,
                columnName, c, length, constLiteral, c, length, c, columnName, c, constLiteral))) {
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        try {
            methodWatcher.executeQuery(format(queryTemplate,
                    columnName, c, length - 1, constLiteral, c, length - 1, c, columnName, c, constLiteral));
        } catch (SQLException e) {
            assertEquals("22001", e.getSQLState());  // truncation error
        }

        // to varchar
        try(ResultSet rs = methodWatcher.executeQuery(format(queryTemplate,
                columnName, vc, length, constLiteral, vc, length, vc, columnName, vc, constLiteral))) {
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        try {
            methodWatcher.executeQuery(format(queryTemplate,
                    columnName, vc, length - 1, constLiteral, vc, length - 1, vc, columnName, vc, constLiteral));
        } catch (SQLException e) {
            assertEquals("22001", e.getSQLState());  // truncation error
        }

        // to long varchar
        try {
            methodWatcher.executeQuery(format("select cast(%s as long varchar) from test", columnName));
        } catch (SQLException e) {
            assertEquals("42846", e.getSQLState());  // unsupported cast
        }
    }

    @Test
    public void testTinyIntToCharacterString() throws Exception {
        String expected = "1  | 2  |  3  | 4  |\n" +
                "----------------------\n" +
                "-128 |127 |-128 |127 |\n" +
                " 127 |127 | 127 |127 |\n" +
                "NULL |127 |NULL |127 |";

        testHelper(expected, "t", "127", 4);
    }

    @Test
    public void testSmallIntToCharacterString() throws Exception {
        String expected = "1  | 2 |  3  | 4 |\n" +
                "--------------------\n" +
                " -1  |11 | -1  |11 |\n" +
                "  0  |11 |  0  |11 |\n" +
                "NULL |11 |NULL |11 |";

        testHelper(expected, "s", "11", 2);
    }

    @Test
    public void testIntegerToCharacterString() throws Exception {
        String expected = "1  | 2  |  3  | 4  |\n" +
                "----------------------\n" +
                "  3  |-55 |  3  |-55 |\n" +
                " 987 |-55 | 987 |-55 |\n" +
                "NULL |-55 |NULL |-55 |";

        testHelper(expected, "i", "-55", 3);
    }

    @Test
    public void testBigIntToCharacterString() throws Exception {
        String expected = "1     | 2  |    3     | 4  |\n" +
                "--------------------------------\n" +
                "-99887766 |919 |-99887766 |919 |\n" +
                "99887766  |919 |99887766  |919 |\n" +
                "  NULL    |919 |  NULL    |919 |";

        testHelper(expected, "b", "919", 9);
    }

    @Test
    public void testFloatToCharacterString() throws Exception {
        String expected = "1   |  2  |  3   |  4  |\n" +
                "--------------------------\n" +
                "-0.35 |12.7 |-0.35 |12.7 |\n" +
                " 2.5  |12.7 | 2.5  |12.7 |\n" +
                "NULL  |12.7 |NULL  |12.7 |";

        testHelper(expected, "f", "1.27E1", 5);
    }

    @Test
    public void testRealToCharacterString() throws Exception {
        String expected = "1    |  2  |   3    |  4  |\n" +
                "------------------------------\n" +
                "-1500.0 |-0.7 |-1500.0 |-0.7 |\n" +
                " 0.02   |-0.7 | 0.02   |-0.7 |\n" +
                " NULL   |-0.7 | NULL   |-0.7 |";

        testHelper(expected, "r", "-7E-1", 7);

        withNotation(FloatingPointDataType.NORMALIZED);
        expected = "1   |   2    |   3   |   4    |\n" +
                "----------------------------------\n" +
                "-1.5E3 |-7.0E-1 |-1.5E3 |-7.0E-1 |\n" +
                "2.0E-2 |-7.0E-1 |2.0E-2 |-7.0E-1 |\n" +
                " NULL  |-7.0E-1 | NULL  |-7.0E-1 |";

        testHelper(expected, "r", "-7E-1", 7);
        withNotation(FloatingPointDataType.PLAIN);
    }

    @Test
    public void testDoubleToCharacterString() throws Exception {
        String expected = "1   |     2      |   3   |     4      |\n" +
                "------------------------------------------\n" +
                "-200.0 |1.79769E308 |-200.0 |1.79769E308 |\n" +
                " 20.0  |1.79769E308 | 20.0  |1.79769E308 |\n" +
                " NULL  |1.79769E308 | NULL  |1.79769E308 |";

        testHelper(expected, "d", "1.79769E+308", 12);
    }

    @Test
    public void testDecimalToCharacterString() throws Exception {
        String expected = "1   |  2  |   3   |  4  |\n" +
                "----------------------------\n" +
                "-11.35 |12.0 |-11.35 |12.0 |\n" +
                "  9.2  |12.0 |  9.2  |12.0 |\n" +
                " NULL  |12.0 | NULL  |12.0 |";

        testHelper(expected, "dc", "12.0", 6);
    }

    @Test
    public void testDecfloatToCharacterString() throws Exception {
        String expected = "1      |    2    |     3      |    4    |\n" +
                "----------------------------------------------\n" +
                "-0.12345678 |0.000001 |-0.12345678 |0.000001 |\n" +
                "3.141592654 |0.000001 |3.141592654 |0.000001 |\n" +
                "   NULL     |0.000001 |   NULL     |0.000001 |";

        testHelper(expected, "df", "0.000001", 11);
    }

    @Test
    public void testImplicitCastOnFunctionArgumentFails() throws Exception {
        try {
            methodWatcher.executeQuery("select hex(dc) from test");
        } catch (SQLException e) {
            assertEquals("42846", e.getSQLState());  // unsupported cast
        }

        String expected = "1      |\n" +
                "--------------\n" +
                "2D31312E3335 |\n" +
                "   392E32    |\n" +
                "    NULL     |";
        try(ResultSet rs = methodWatcher.executeQuery("select hex(cast(dc as varchar(12))) from test")) {
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    @Test
    public void testImplicitCastInGenerationClauseValid() throws Exception {
        methodWatcher.executeUpdate("CREATE TABLE if not exists char_alter ( VAL1 CHAR(10), VAL2 CHAR(10) GENERATED ALWAYS AS (VAL1+1))");
        methodWatcher.executeUpdate("insert into char_alter(VAL1) select * from (values('111'),('222'),('333')) char_alter order by 1");

        String expected = "VAL1 |VAL2 |\n" +
                "------------\n" +
                " 111 | 112 |";
        try(ResultSet rs = methodWatcher.executeQuery("select * from char_alter order by val1 {limit 1}")) {
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
    }

    @Test
    public void testFloatNumbersZeroNormalizedNotation() throws Exception {
        // We output at least one decimal digit for all floating point numbers except zero
        TestConnection conn = methodWatcher.getOrCreateConnection();
        withNotation(FloatingPointDataType.NORMALIZED);
        checkStringExpression("varchar(double(0.0))", "0E0", conn);
        checkStringExpression("char(double(0.0))", "0E0                                                  ", conn);
        checkStringExpression("varchar(cast(0.0 as real))", "0E0", conn);
        checkStringExpression("char(cast(0.0 as real))", "0E0                     ", conn);
        withNotation(FloatingPointDataType.PLAIN);
        checkStringExpression("varchar(double(0.0))", "0.0", conn);
        checkStringExpression("char(double(0.0))", "0.0                                                  ", conn);
        checkStringExpression("varchar(cast(0.0 as real))", "0.0", conn);
        checkStringExpression("char(cast(0.0 as real))", "0.0                     ", conn);
    }

    @Test
    public void testFloatNumbersRounding() throws Exception {
        // In converting floating-point values to string, rounding policy in DB2 is half-even.
        withNotation(FloatingPointDataType.NORMALIZED);
        try(ResultSet rs = methodWatcher.executeQuery("select varchar(double(123456789012344.5)) from sysibm.sysdummy1")) {
            String expected = "1          |\n" +
                    "---------------------\n" +
                    "1.23456789012344E14 |";
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        try(ResultSet rs = methodWatcher.executeQuery("select varchar(double(123456789012344.6)) from sysibm.sysdummy1")) {
            String expected = "1          |\n" +
                    "---------------------\n" +
                    "1.23456789012345E14 |";
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        try(ResultSet rs = methodWatcher.executeQuery("select varchar(double(123456789012345.5)) from sysibm.sysdummy1")) {
            String expected = "1          |\n" +
                    "---------------------\n" +
                    "1.23456789012346E14 |";
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        try(ResultSet rs = methodWatcher.executeQuery("select varchar(double(123456789012345.4)) from sysibm.sysdummy1")) {
            String expected = "1          |\n" +
                    "---------------------\n" +
                    "1.23456789012345E14 |";
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
        withNotation(FloatingPointDataType.PLAIN);
    }

    @Test
    public void testFloatNumbersTruncate() throws Exception {
        // Here, the value is an integer but in double type. In output format, there is not enough decimal
        // digits to hold the value. In DB2, it's the same behavior as rounding (half-even). But Java seems
        // to have half-up. The rounding option settings has no effect in this situation.
        withNotation(FloatingPointDataType.NORMALIZED);
        try(ResultSet rs = methodWatcher.executeQuery("select varchar(double(1234567890123445)) from sysibm.sysdummy1")) {
            String expected = "1          |\n" +
                    "---------------------\n" +
                    "1.23456789012345E15 |";   // 1.23456789012344E15 in DB2
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        try(ResultSet rs = methodWatcher.executeQuery("select varchar(double(1234567890123446)) from sysibm.sysdummy1")) {
            String expected = "1          |\n" +
                    "---------------------\n" +
                    "1.23456789012345E15 |";
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        try(ResultSet rs = methodWatcher.executeQuery("select varchar(double(1234567890123455)) from sysibm.sysdummy1")) {
            String expected = "1          |\n" +
                    "---------------------\n" +
                    "1.23456789012346E15 |";
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }

        try(ResultSet rs = methodWatcher.executeQuery("select varchar(double(1234567890123454)) from sysibm.sysdummy1")) {
            String expected = "1          |\n" +
                    "---------------------\n" +
                    "1.23456789012345E15 |";
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        }
        withNotation(FloatingPointDataType.PLAIN);
    }

}




