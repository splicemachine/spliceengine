/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import com.splicemachine.derby.test.framework.*;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormatSymbols;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.splicemachine.db.shared.common.reference.SQLState.*;

@RunWith(Parameterized.class)
public class DecimalFunctionIT extends SpliceUnitTest {
    public static final    String        CLASS_NAME         = DecimalFunctionIT.class.getSimpleName().toUpperCase();
    protected final static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final    String        TABLE1_NAME        = "A";

    protected final static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static String tableDef = "(I INT)";
    protected final static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE1_NAME, CLASS_NAME, tableDef);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher1).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    PreparedStatement ps;
                    try {
                        ps = spliceClassWatcher.prepareStatement(
                                String.format("insert into %s (i) values (?)", spliceTableWatcher1));
                        for (int i = 1; i <= 100; i++) {
                            ps.setInt(1, i);
                            ps.execute();
                        }
                    }  catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
    private final String functionName;

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    private static BigDecimal toBigDecimal(String input) {
        if (input == null) return null;
        boolean isNegative = false;
        if (input.charAt(0) == '-') {
            isNegative = true;
            input = input.substring(1);
        } else if (input.charAt(0) == '+') {
            input = input.substring(1);
        }
        Pattern p = Pattern.compile("[^\\d]");
        Matcher m = p.matcher(input);
        int commaIndex = -1;
        if (m.find()) {
            commaIndex = m.start();
        }
        int stringScale = 0;
        if (commaIndex != -1) {
            stringScale = input.length() - commaIndex - 1;
        }
        input = input.replaceAll("[^\\d]", ""); // remove comma if exists
        if (isNegative) {
            input = "-" + input;
        }
        return new BigDecimal(new BigInteger(input), stringScale);
    }

    @Parameterized.Parameters(name = "with function {0}")
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{"decimal"});
        params.add(new Object[]{"dec"});
        return params;
    }

    public DecimalFunctionIT(String functionName) {
        this.functionName = functionName;
    }

    public void check(String what, String expectedNumber) throws SQLException {
        checkQuery(String.format("values %s %s", functionName, what), expectedNumber);
    }

    public void checkQuery(String what, String expectedNumber) throws SQLException {
        try(ResultSet rs = methodWatcher.executeQuery(what)) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(toBigDecimal(expectedNumber), rs.getBigDecimal(1));
            Assert.assertFalse(rs.next());
        }
    }

    enum ErrorCode { DECIMAL_CHARACTER, CONVERSION, ARGUMENT, INVALID_STRING, INVALID_TYPE };

    public void shouldFail(String what, ErrorCode errorCode) {
        try {
            methodWatcher.executeQuery(String.format("values %s %s", functionName, what));
            Assert.fail("should have failed with error");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof SQLException);
            SQLException sqlException = (SQLException)e;
            switch (errorCode) {
                case DECIMAL_CHARACTER:
                    Assert.assertEquals(LANG_INVALID_DECIMAL_CHARACTER, sqlException.getSQLState());
                    break;
                case CONVERSION:
                    Assert.assertEquals(LANG_INVALID_DECIMAL_CONVERSION, sqlException.getSQLState());
                    break;
                case ARGUMENT:
                    Assert.assertEquals(LANG_INVALID_DECIMAL_ARGUMENT, sqlException.getSQLState());
                    break;
                case INVALID_STRING:
                    Assert.assertEquals(LANG_INVALID_DECIMAL_STRING, sqlException.getSQLState());
                    break;
                case INVALID_TYPE:
                    Assert.assertEquals(LANG_INVALID_DECIMAL_TYPE, sqlException.getSQLState());
                    break;
            }
        }
    }

    @Test
    public void createFromInteger() throws Exception {
        check("(10)", "10");
        check("(10, 6)", "10");
        check("(1000, 6, 2)", "1000.00");
        check("(1000, 7, 3)", "1000.000");
        check("(1000, 8, 4)", "1000.0000");
        check("(1000, 9, 4)", "1000.0000");
        check("(1000+1, 9, 4)", "1001.0000");
        check("(32767 + 5, 9, 4)", "32772.0000");
        check("(cast('2147483648' as bigint) + 5)", "2147483653");
        check("(cast('2147483648' as bigint) + 5, 12)", "2147483653");
        check("(cast('2147483648' as bigint) + 5, 14, 4)", "2147483653.0000");
        check("(-1000, 6, 2)", "-1000.00");
        check("(-1000, 7, 3)", "-1000.000");
        check("(-1000, 8, 4)", "-1000.0000");
        check("(-1000, 9, 4)", "-1000.0000");
        check("(-1000-1, 9, 4)", "-1001.0000");
        check("(-32767-5, 9, 4)", "-32772.0000");
        check("(cast('2147483648' as bigint) * -1 - 5, 14, 4)", "-2147483653.0000");
        check("(cast('2147483648' as bigint) * -1 - 5)", "-2147483653");
        check("(cast('2147483648' as bigint) * -1 - 5, 12)", "-2147483653");
        shouldFail("(100000, 1,1)", ErrorCode.CONVERSION);
    }

    @Test
    public void createFromDecimal() throws Exception {
        check("(cast ('1234.5678' as decimal(8,4)))", "1234");
        check("(cast ('1234.5678' as decimal(8,4)), 5)", "1234");
        check("(cast ('1234.5678' as decimal(8,4)), 8, 4)", "1234.5678");
        check("(cast ('1234.5678' as decimal(8,4)), 8, 2)", "1234.56");
        check("(cast ('1234.5678' as decimal(8,4)), 9, 5)", "1234.56780");
        shouldFail("(cast ('1234.5678' as decimal(8,4)), 6, 5)", ErrorCode.CONVERSION);
        shouldFail("(cast ('1234.5678' as decimal(8,4)), 6, 5)", ErrorCode.CONVERSION);
    }

    @Test
    public void createFromDecfloat() throws Exception {
        check("(cast ('1234.5678' as decfloat))", "1234");
        check("(cast ('1234.5678' as decfloat), 5)", "1234");
        check("(cast ('1234.5678' as decfloat), 8, 4)", "1234.5678");
        check("(cast ('1234.5678' as decfloat), 8, 2)", "1234.56");
        check("(cast ('1234.5678' as decfloat), 9, 5)", "1234.56780");
        shouldFail("(cast ('1234.5678' as decfloat), 6, 5)", ErrorCode.CONVERSION);
        shouldFail("(cast ('1234.5678' as decfloat), 6, 5)", ErrorCode.CONVERSION);
    }

    @Test
    public void createFromDouble() throws Exception {
        check("(cast ('1234.56789' as double))", "1234");
        check("(cast ('1234.56789' as double), 5)", "1234");
        check("(cast ('1234.56789' as double), 8, 4)", "1234.5678");
        check("(cast ('1234.56789' as double), 8, 2)", "1234.56");
        shouldFail("(cast ('1234.56789' as double), 6, 5)", ErrorCode.CONVERSION);
        shouldFail("(cast ('1234.56789' as double), 6, 5)", ErrorCode.CONVERSION);
    }

    @Test
    public void createFromFloat() throws Exception {
        check("(cast ('1234.56789' as float))", "1234");
        check("(cast ('1234.56789' as float), 5)", "1234");
        check("(cast ('1234.56789' as float), 8, 4)", "1234.5678");
        check("(cast ('1234.56789' as float), 8, 2)", "1234.56");
        shouldFail("(cast ('1234.56789' as float), 6, 5)", ErrorCode.CONVERSION);
        shouldFail("(cast ('1234.56789' as float), 6, 5)", ErrorCode.CONVERSION);
    }

    // localize the decimal separator (if any) in the string according to the JVM's default locale settings
    private static String l(String in) {
        return in.replaceAll("\\.", String.valueOf(new DecimalFormatSymbols().getDecimalSeparator()));
    }

    @Test
    public void createFromVarchar() throws Exception {
        check(l("(cast ('1234.56789' as varchar(15)))"), "1234");
        check(l("(cast ('1234.56789' as varchar(15)), 5)"), "1234");
        check(l("(cast ('1234.56789' as varchar(15)), 8, 4)"), "1234.5678");
        check(l("(cast ('1234.56789' as varchar(15)), 8, 2)"), "1234.56");
        check(l("(cast ('1000000.23' as varchar(20)), 10, 2, '.')"), "1000000.23");
        shouldFail(l("(cast ('1234.56789' as varchar(15)), 6, 5)"), ErrorCode.CONVERSION);
        shouldFail(l("(cast ('1234.56789' as varchar(15)), 6, 5)"), ErrorCode.CONVERSION);
        shouldFail(l("(cast ('10.00000.23' as varchar(20)), 10, 2)"), ErrorCode.INVALID_STRING);
        shouldFail(l("(cast ('10000ABC00.23' as varchar(20)), 10, 2)"), ErrorCode.INVALID_STRING);
        shouldFail(l("(cast ('' as varchar(20)), 10, 2)"), ErrorCode.INVALID_STRING);
        shouldFail(l("(cast ('1000000.23' as varchar(20)), 10, 2, 'HELLO')"), ErrorCode.DECIMAL_CHARACTER);
    }

    @Test
    public void createFromChar() throws Exception {
        check(l("(cast ('1234.56789' as char(15)))"), "1234");
        check(l("(cast ('1234.56789' as char(15)), 5)"), "1234");
        check(l("(cast ('1234.56789' as char(15)), 8, 4)"), "1234.5678");
        check(l("(cast ('1234.56789' as char(15)), 8, 2)"), "1234.56");
        check(l("(cast ('1000000.23' as char(20)), 10, 2, '.')"), "1000000.23");
        shouldFail(l("(cast ('1234.56789' as char(15)), 6, 5)"), ErrorCode.CONVERSION);
        shouldFail(l("(cast ('1234.56789' as char(15)), 6, 5)"), ErrorCode.CONVERSION);
        shouldFail(l("(cast ('10.00000.23' as char(20)), 10, 2)"), ErrorCode.INVALID_STRING);
        shouldFail(l("(cast ('10000ABC00.23' as char(20)), 10, 2)"), ErrorCode.INVALID_STRING);
        shouldFail(l("(cast ('' as char(20)), 10, 2)"), ErrorCode.INVALID_STRING);
        shouldFail(l("(cast ('1000000.23' as char(20)), 10, 2, 'HELLO')"), ErrorCode.DECIMAL_CHARACTER);
    }

    @Test
    public void createFromDate() throws Exception {
        check("(cast ('2020-11-11' as date))", "20201111");
        check("(cast ('2020-11-11' as date), 10)", "20201111");
        check("(cast ('2020-11-11' as date), 14, 4)", "20201111.0000");
        shouldFail("(cast ('2020-11-11' as date), 6)", ErrorCode.CONVERSION);
        shouldFail("(cast ('2020-11-11' as date), 6, 5)", ErrorCode.CONVERSION);
    }

    @Test
    public void createFromTime() throws Exception {
        check("(cast ('11:11:11' as time))", "111111");
        check("(cast ('11:11:11' as time), 10)", "111111");
        check("(cast ('11:11:11' as time), 14, 4)", "111111.0000");
        shouldFail("(cast ('11:11:11' as time), 5)", ErrorCode.CONVERSION);
        shouldFail("(cast ('11:11:11' as time), 5, 2)", ErrorCode.CONVERSION);
    }

    @Test
    public void createFromTimestamp() throws Exception {
        check("(cast ('2020-11-11 11:11:11.1234' as timestamp))", "20201111111111.123400000");
        check("(cast ('2020-11-11 11:11:11.123456' as timestamp), 14)", "20201111111111");
        check("(cast ('2020-11-11 11:11:11.123456' as timestamp), 18, 4)", "20201111111111.1234");
        check("(cast ('2020-11-11 11:11:11.19999' as timestamp), 15, 1)", "20201111111111.1");
        shouldFail("(cast ('2020-11-11 11:11:11.123456' as timestamp), 5)", ErrorCode.CONVERSION);
        shouldFail("(cast ('2020-11-11 11:11:11.123456' as timestamp), 5, 2)", ErrorCode.CONVERSION);
    }

    @Test
    public void invalidTypes() throws Exception {
        shouldFail("(cast ('HELLO' AS CLOB(10)), 10, 2)", ErrorCode.INVALID_TYPE);
        shouldFail("(cast ('HELLO' AS BLOB(10)), 10, 2)", ErrorCode.INVALID_TYPE);
    }

    @Test
    public void decimalFunctionWorksWithAggregates() throws Exception {
        checkQuery(String.format("select %s(max(I)) from %s", functionName, spliceTableWatcher1), "100");
        checkQuery(String.format("select %s(sum(I), 10, 2) from %s", functionName, spliceTableWatcher1), "5050.00");
    }
}
