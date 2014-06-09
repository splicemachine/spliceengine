package com.splicemachine.derby.impl.sql.execute.tester;

import com.splicemachine.derby.test.framework.DefaultedSpliceWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class NumericConstantsIT {

    private static final String CLASS_NAME = NumericConstantsIT.class.getSimpleName().toUpperCase();
    private static final DefaultedSpliceWatcher spliceClassWatcher = new DefaultedSpliceWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(new SpliceSchemaWatcher(CLASS_NAME))
            .around(TestUtils.createFileDataWatcher(spliceClassWatcher, "test_data/NumericConstantsIT.sql", CLASS_NAME));

    @Rule
    public SpliceWatcher methodWatcher = new DefaultedSpliceWatcher(CLASS_NAME);

    // - - - - - - - - - - - - - - - - - - - - - -
    //
    // smallint
    //
    // - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void smallInt_min() throws Exception {
        assertCount(1, "table_smallint", "a", "=", Short.MIN_VALUE);
        assertCount(4, "table_smallint", "a", "!=", Short.MIN_VALUE);
        assertCount(0, "table_smallint", "a", "<", Short.MIN_VALUE);
        assertCount(1, "table_smallint", "a", "<=", Short.MIN_VALUE);
        assertCount(4, "table_smallint", "a", ">", Short.MIN_VALUE);
        assertCount(5, "table_smallint", "a", ">=", Short.MIN_VALUE);
    }

    @Test
    public void smallInt_max() throws Exception {
        assertCount(1, "table_smallint", "a", "=", Short.MAX_VALUE);
        assertCount(4, "table_smallint", "a", "!=", Short.MAX_VALUE);
        assertCount(4, "table_smallint", "a", "<", Short.MAX_VALUE);
        assertCount(5, "table_smallint", "a", "<=", Short.MAX_VALUE);
        assertCount(0, "table_smallint", "a", ">", Short.MAX_VALUE);
        assertCount(1, "table_smallint", "a", ">=", Short.MAX_VALUE);
    }

    @Test
    public void smallInt_max_plusOne() throws Exception {
        String SHORT_MAX_PLUS_1 = new BigInteger(String.valueOf(Short.MAX_VALUE)).add(BigInteger.ONE).toString();

        assertCount(0, "table_smallint", "a", "=", SHORT_MAX_PLUS_1);
        assertCount(5, "table_smallint", "a", "!=", SHORT_MAX_PLUS_1);
        assertCount(5, "table_smallint", "a", "<", SHORT_MAX_PLUS_1);
        assertCount(5, "table_smallint", "a", "<=", SHORT_MAX_PLUS_1);
        assertCount(0, "table_smallint", "a", ">", SHORT_MAX_PLUS_1);
        assertCount(0, "table_smallint", "a", ">=", SHORT_MAX_PLUS_1);
    }

    @Test
    public void smallInt_min_minusOne() throws Exception {
        String SHORT_MIN_MINUS_1 = new BigInteger(String.valueOf(Short.MIN_VALUE)).subtract(BigInteger.ONE).toString();

        assertCount(0, "table_smallint", "a", "=", SHORT_MIN_MINUS_1);
        assertCount(5, "table_smallint", "a", "!=", SHORT_MIN_MINUS_1);
        assertCount(0, "table_smallint", "a", "<", SHORT_MIN_MINUS_1);
        assertCount(0, "table_smallint", "a", "<=", SHORT_MIN_MINUS_1);
        assertCount(5, "table_smallint", "a", ">", SHORT_MIN_MINUS_1);
        assertCount(5, "table_smallint", "a", ">=", SHORT_MIN_MINUS_1);
    }

    // - - - - - - - - - - - - - - - - - - - - - -
    //
    // integer
    //
    // - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void integer_min() throws Exception {
        assertCount(1, "table_integer", "a", "=", Integer.MIN_VALUE);
        assertCount(4, "table_integer", "a", "!=", Integer.MIN_VALUE);
        assertCount(0, "table_integer", "a", "<", Integer.MIN_VALUE);
        assertCount(1, "table_integer", "a", "<=", Integer.MIN_VALUE);
        assertCount(4, "table_integer", "a", ">", Integer.MIN_VALUE);
        assertCount(5, "table_integer", "a", ">=", Integer.MIN_VALUE);
    }

    @Test
    public void integer_max() throws Exception {
        assertCount(1, "table_integer", "a", "=", Integer.MAX_VALUE);
        assertCount(4, "table_integer", "a", "!=", Integer.MAX_VALUE);
        assertCount(4, "table_integer", "a", "<", Integer.MAX_VALUE);
        assertCount(5, "table_integer", "a", "<=", Integer.MAX_VALUE);
        assertCount(0, "table_integer", "a", ">", Integer.MAX_VALUE);
        assertCount(1, "table_integer", "a", ">=", Integer.MAX_VALUE);
    }

    @Test
    public void integer_max_plusOne() throws Exception {
        String INT_MAX_PLUS_1 = new BigInteger(String.valueOf(Integer.MAX_VALUE)).add(BigInteger.ONE).toString();

        assertCount(0, "table_integer", "a", "=", INT_MAX_PLUS_1);
        assertCount(5, "table_integer", "a", "!=", INT_MAX_PLUS_1);
        assertCount(5, "table_integer", "a", "<", INT_MAX_PLUS_1);
        assertCount(5, "table_integer", "a", "<=", INT_MAX_PLUS_1);
        assertCount(0, "table_integer", "a", ">", INT_MAX_PLUS_1);
        assertCount(0, "table_integer", "a", ">=", INT_MAX_PLUS_1);
    }

    @Test
    public void integer_min_minusOne() throws Exception {
        String INT_MIN_MINUS_1 = new BigInteger(String.valueOf(Integer.MIN_VALUE)).subtract(BigInteger.ONE).toString();

        assertCount(0, "table_integer", "a", "=", INT_MIN_MINUS_1);
        assertCount(5, "table_integer", "a", "!=", INT_MIN_MINUS_1);
        assertCount(0, "table_integer", "a", "<", INT_MIN_MINUS_1);
        assertCount(0, "table_integer", "a", "<=", INT_MIN_MINUS_1);
        assertCount(5, "table_integer", "a", ">", INT_MIN_MINUS_1);
        assertCount(5, "table_integer", "a", ">=", INT_MIN_MINUS_1);
    }

    // - - - - - - - - - - - - - - - - - - - - - -
    //
    // bigint
    //
    // - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void bigint_min() throws Exception {
        assertCount(1, "table_bigint", "a", "=", Long.MIN_VALUE);
        assertCount(4, "table_bigint", "a", "!=", Long.MIN_VALUE);
        assertCount(0, "table_bigint", "a", "<", Long.MIN_VALUE);
        assertCount(1, "table_bigint", "a", "<=", Long.MIN_VALUE);
        assertCount(4, "table_bigint", "a", ">", Long.MIN_VALUE);
        assertCount(5, "table_bigint", "a", ">=", Long.MIN_VALUE);
    }

    @Test
    public void bigint_max() throws Exception {
        assertCount(1, "table_bigint", "a", "=", Long.MAX_VALUE);
        assertCount(4, "table_bigint", "a", "!=", Long.MAX_VALUE);
        assertCount(4, "table_bigint", "a", "<", Long.MAX_VALUE);
        assertCount(5, "table_bigint", "a", "<=", Long.MAX_VALUE);
        assertCount(0, "table_bigint", "a", ">", Long.MAX_VALUE);
        assertCount(1, "table_bigint", "a", ">=", Long.MAX_VALUE);
    }

    @Test
    public void bigint_max_plusOne() throws Exception {
        String LONG_MAX_PLUS_1 = new BigInteger(String.valueOf(Long.MAX_VALUE)).add(BigInteger.ONE).toString();

        assertCount(0, "table_bigint", "a", "=", LONG_MAX_PLUS_1);
        assertCount(5, "table_bigint", "a", "!=", LONG_MAX_PLUS_1);
        assertCount(5, "table_bigint", "a", "<", LONG_MAX_PLUS_1);
        assertCount(5, "table_bigint", "a", "<=", LONG_MAX_PLUS_1);
        assertCount(0, "table_bigint", "a", ">", LONG_MAX_PLUS_1);
        assertCount(0, "table_bigint", "a", ">=", LONG_MAX_PLUS_1);
    }

    @Test
    public void bigint_min_minusOne() throws Exception {
        String LONG_MIN_MINUS_1 = new BigInteger(String.valueOf(Long.MIN_VALUE)).subtract(BigInteger.ONE).toString();

        assertCount(0, "table_bigint", "a", "=", LONG_MIN_MINUS_1);
        assertCount(5, "table_bigint", "a", "!=", LONG_MIN_MINUS_1);
        assertCount(0, "table_bigint", "a", "<", LONG_MIN_MINUS_1);
        assertCount(0, "table_bigint", "a", "<=", LONG_MIN_MINUS_1);
        assertCount(5, "table_bigint", "a", ">", LONG_MIN_MINUS_1);
        assertCount(5, "table_bigint", "a", ">=", LONG_MIN_MINUS_1);
    }

    // - - - - - - - - - - - - - - - - - - - - - -
    //
    // real
    //
    // - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void real_min() throws Exception {
        String REAL_MIN = "-3.402E+38";

        assertCount(1, "table_real", "a", "=", REAL_MIN);
        assertCount(4, "table_real", "a", "!=", REAL_MIN);
        assertCount(0, "table_real", "a", "<", REAL_MIN);
        assertCount(1, "table_real", "a", "<=", REAL_MIN);
        assertCount(4, "table_real", "a", ">", REAL_MIN);
        assertCount(5, "table_real", "a", ">=", REAL_MIN);
    }

    @Test
    public void real_max() throws Exception {
        String REAL_MAX = "3.402E+38";

        assertCount(1, "table_real", "a", "=", REAL_MAX);
        assertCount(4, "table_real", "a", "!=", REAL_MAX);
        assertCount(4, "table_real", "a", "<", REAL_MAX);
        assertCount(5, "table_real", "a", "<=", REAL_MAX);
        assertCount(0, "table_real", "a", ">", REAL_MAX);
        assertCount(1, "table_real", "a", ">=", REAL_MAX);
    }

    @Test
    public void real_minTimesTen() throws Exception {
        String REAL_MIN_TIMES_10 = "-3.402E+39";

        assertCount(0, "table_real", "a", "=", REAL_MIN_TIMES_10);
        assertCount(5, "table_real", "a", "!=", REAL_MIN_TIMES_10);
        assertCount(0, "table_real", "a", "<", REAL_MIN_TIMES_10);
        assertCount(0, "table_real", "a", "<=", REAL_MIN_TIMES_10);
        assertCount(5, "table_real", "a", ">", REAL_MIN_TIMES_10);
        assertCount(5, "table_real", "a", ">=", REAL_MIN_TIMES_10);
    }

    @Test
    public void real_maxTimesTen() throws Exception {
        String REAL_MAX_TIMES_10 = "3.402E+39";

        assertCount(0, "table_real", "a", "=", REAL_MAX_TIMES_10);
        assertCount(5, "table_real", "a", "!=", REAL_MAX_TIMES_10);
        assertCount(5, "table_real", "a", "<", REAL_MAX_TIMES_10);
        assertCount(5, "table_real", "a", "<=", REAL_MAX_TIMES_10);
        assertCount(0, "table_real", "a", ">", REAL_MAX_TIMES_10);
        assertCount(0, "table_real", "a", ">=", REAL_MAX_TIMES_10);
    }

    // - - - - - - - - - - - - - - - - - - - - - -
    //
    // double
    //
    // - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void double_min() throws Exception {
        String DOUBLE_MIN = "-1.79769E+308";

        assertCount(1, "table_double", "a", "=", DOUBLE_MIN);
        assertCount(4, "table_double", "a", "!=", DOUBLE_MIN);
        assertCount(0, "table_double", "a", "<", DOUBLE_MIN);
        assertCount(1, "table_double", "a", "<=", DOUBLE_MIN);
        assertCount(4, "table_double", "a", ">", DOUBLE_MIN);
        assertCount(5, "table_double", "a", ">=", DOUBLE_MIN);
    }

    @Test
    public void double_max() throws Exception {
        String DOUBLE_MAX = "1.79769E+308";

        assertCount(1, "table_double", "a", "=", DOUBLE_MAX);
        assertCount(4, "table_double", "a", "!=", DOUBLE_MAX);
        assertCount(4, "table_double", "a", "<", DOUBLE_MAX);
        assertCount(5, "table_double", "a", "<=", DOUBLE_MAX);
        assertCount(0, "table_double", "a", ">", DOUBLE_MAX);
        assertCount(1, "table_double", "a", ">=", DOUBLE_MAX);
    }

    /* Assert that we have the same behavior as derby when using numeric constant greater than double */
    @Test
    public void double_minTimesTen() throws Exception {
        String DOUBLE_MIN_TIMES_10 = "-1.79769E+309";
        assertException("select * from table_double where a = " + DOUBLE_MIN_TIMES_10, SQLDataException.class,
                "The resulting value is outside the range for the data type DOUBLE.");
    }

    /* Assert that we have the same behavior as derby when using numeric constant greater than double */
    @Test
    public void double_maxTimesTen() throws Exception {
        String DOUBLE_MAX_TIMES_10 = "1.79769E+309";
        assertException("select * from table_double where a = " + DOUBLE_MAX_TIMES_10, SQLDataException.class,
                "The resulting value is outside the range for the data type DOUBLE.");
    }

    /* Assert that we have the same behavior as derby when using numeric constant greater than double */
    @Test
    public void double_maxTimesTen_LessThan() throws Exception {
        String DOUBLE_MAX_TIMES_10 = "1.79769E+309";
        assertException("select * from table_double where a < " + DOUBLE_MAX_TIMES_10, SQLDataException.class,
                "The resulting value is outside the range for the data type DOUBLE.");
    }

    /* Assert that we have the same behavior as derby when using numeric constant greater than double */
    @Test
    public void double_maxTimesTen_GreaterThan() throws Exception {
        String DOUBLE_MAX_TIMES_10 = "1.79769E+309";
        assertException("select * from table_double where a > " + DOUBLE_MAX_TIMES_10, SQLDataException.class,
                "The resulting value is outside the range for the data type DOUBLE.");
    }

    /* Assert that we have the same behavior as derby when using numeric constant greater than double */
    @Test
    public void double_maxTimesTen_GreaterThanConstantOnLeft() throws Exception {
        assertException("select * from table_double where 1.79769E+309 < a", SQLDataException.class,
                "The resulting value is outside the range for the data type DOUBLE.");
    }

    /**
     * EXECUTES:
     *
     * SELECT * FROM [table] WHERE [operandOne] [operator] [operandTwo]
     * AND
     * SELECT * FROM [table] WHERE [operandTwo] [operator] [operandOnE]
     */
    private void assertCount(int expectedCount, String table, String operandOne, String operator, Object operandTwo) throws Exception {
        String SQL_TEMPLATE = "select * from %s where %s %s %s";
        assertCount(expectedCount, format(SQL_TEMPLATE, table, operandOne, operator, operandTwo));
        String operatorTwo = newOperator(operator);
        assertCount(expectedCount, format(SQL_TEMPLATE, table, operandTwo, operatorTwo, operandOne));
    }

    private void assertCount(int expectedCount, String sql) throws Exception {
        ResultSet rs = methodWatcher.executeQuery(sql);
        assertEquals(format("count mismatch for sql='%s'", sql), expectedCount, count(rs));
    }

    private void assertException(String sql, Class expectedException, String expectedMessage) throws Exception {
        try {
            methodWatcher.executeQuery(sql);
            fail();
        } catch (Exception e) {
            assertEquals(expectedException, e.getClass());
            assertEquals(expectedMessage, e.getMessage());
        }
    }

    private static int count(ResultSet rs) throws SQLException {
        int count = 0;
        while (rs.next()) {
            count++;
        }
        return count;
    }

    private static String newOperator(String operator) {
        if ("<".equals(operator.trim())) {
            return ">";
        }
        if (">".equals(operator.trim())) {
            return "<";
        }
        if ("<=".equals(operator.trim())) {
            return ">=";
        }
        if (">=".equals(operator.trim())) {
            return "<=";
        }
        return operator;
    }

}
