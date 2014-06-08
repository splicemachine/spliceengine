package com.splicemachine.derby.impl.sql.execute.tester;

import com.splicemachine.derby.test.framework.DefaultedSpliceWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

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
        assertCount(1, format("select * from table_smallint where a = %s", Short.MIN_VALUE));
        assertCount(4, format("select * from table_smallint where a != %s", Short.MIN_VALUE));
        assertCount(0, format("select * from table_smallint where a < %s", Short.MIN_VALUE));
        assertCount(1, format("select * from table_smallint where a <= %s", Short.MIN_VALUE));
        assertCount(4, format("select * from table_smallint where a > %s", Short.MIN_VALUE));
        assertCount(5, format("select * from table_smallint where a >= %s", Short.MIN_VALUE));
    }

    @Test
    public void smallInt_max() throws Exception {
        assertCount(1, format("select * from table_smallint where a = %s", Short.MAX_VALUE));
        assertCount(4, format("select * from table_smallint where a != %s", Short.MAX_VALUE));
        assertCount(4, format("select * from table_smallint where a < %s", Short.MAX_VALUE));
        assertCount(5, format("select * from table_smallint where a <= %s", Short.MAX_VALUE));
        assertCount(0, format("select * from table_smallint where a > %s", Short.MAX_VALUE));
        assertCount(1, format("select * from table_smallint where a >= %s", Short.MAX_VALUE));
    }

    @Ignore("fails, DB-1362")
    @Test
    public void smallInt_max_plusOne() throws Exception {
        String SHORT_MAX_PLUS_1 = new BigInteger(String.valueOf(Short.MAX_VALUE)).add(BigInteger.ONE).toString();

        assertCount(0, format("select * from table_smallint where a = %s", SHORT_MAX_PLUS_1));
        assertCount(5, format("select * from table_smallint where a != %s", SHORT_MAX_PLUS_1));
        assertCount(5, format("select * from table_smallint where a < %s", SHORT_MAX_PLUS_1));
        assertCount(5, format("select * from table_smallint where a <= %s", SHORT_MAX_PLUS_1));
        assertCount(0, format("select * from table_smallint where a > %s", SHORT_MAX_PLUS_1));
        assertCount(0, format("select * from table_smallint where a >= %s", SHORT_MAX_PLUS_1));
    }

    @Ignore("fails, DB-1362")
    @Test
    public void smallInt_min_minusOne() throws Exception {
        String SHORT_MIN_MINUS_1 = new BigInteger(String.valueOf(Short.MIN_VALUE)).subtract(BigInteger.ONE).toString();

        assertCount(0, format("select * from table_smallint where a = %s", SHORT_MIN_MINUS_1));
        assertCount(5, format("select * from table_smallint where a != %s", SHORT_MIN_MINUS_1));
        assertCount(0, format("select * from table_smallint where a < %s", SHORT_MIN_MINUS_1));
        assertCount(0, format("select * from table_smallint where a <= %s", SHORT_MIN_MINUS_1));
        assertCount(5, format("select * from table_smallint where a > %s", SHORT_MIN_MINUS_1));
        assertCount(5, format("select * from table_smallint where a >= %s", SHORT_MIN_MINUS_1));
    }

    // - - - - - - - - - - - - - - - - - - - - - -
    //
    // integer
    //
    // - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void integer_min() throws Exception {
        assertCount(1, format("select * from table_integer where a = %s", Integer.MIN_VALUE));
        assertCount(4, format("select * from table_integer where a != %s", Integer.MIN_VALUE));
        assertCount(0, format("select * from table_integer where a < %s", Integer.MIN_VALUE));
        assertCount(1, format("select * from table_integer where a <= %s", Integer.MIN_VALUE));
        assertCount(4, format("select * from table_integer where a > %s", Integer.MIN_VALUE));
        assertCount(5, format("select * from table_integer where a >= %s", Integer.MIN_VALUE));
    }

    @Test
    public void integer_max() throws Exception {
        assertCount(1, format("select * from table_integer where a = %s", Integer.MAX_VALUE));
        assertCount(4, format("select * from table_integer where a != %s", Integer.MAX_VALUE));
        assertCount(4, format("select * from table_integer where a < %s", Integer.MAX_VALUE));
        assertCount(5, format("select * from table_integer where a <= %s", Integer.MAX_VALUE));
        assertCount(0, format("select * from table_integer where a > %s", Integer.MAX_VALUE));
        assertCount(1, format("select * from table_integer where a >= %s", Integer.MAX_VALUE));
    }

    @Ignore("fails, DB-1362")
    @Test
    public void integer_max_plusOne() throws Exception {
        String INT_MAX_PLUS_1 = new BigInteger(String.valueOf(Integer.MAX_VALUE)).add(BigInteger.ONE).toString();

        assertCount(0, format("select * from table_integer where a = %s", INT_MAX_PLUS_1));
        assertCount(5, format("select * from table_integer where a != %s", INT_MAX_PLUS_1));
        assertCount(5, format("select * from table_integer where a < %s", INT_MAX_PLUS_1));
        assertCount(5, format("select * from table_integer where a <= %s", INT_MAX_PLUS_1));
        assertCount(0, format("select * from table_integer where a > %s", INT_MAX_PLUS_1));
        assertCount(0, format("select * from table_integer where a >= %s", INT_MAX_PLUS_1));
    }

    @Ignore("fails, DB-1362")
    @Test
    public void integer_min_minusOne() throws Exception {
        String INT_MIN_MINUS_1 = new BigInteger(String.valueOf(Integer.MIN_VALUE)).subtract(BigInteger.ONE).toString();

        assertCount(0, format("select * from table_integer where a = %s", INT_MIN_MINUS_1));
        assertCount(5, format("select * from table_integer where a != %s", INT_MIN_MINUS_1));
        assertCount(0, format("select * from table_integer where a < %s", INT_MIN_MINUS_1));
        assertCount(0, format("select * from table_integer where a <= %s", INT_MIN_MINUS_1));
        assertCount(5, format("select * from table_integer where a > %s", INT_MIN_MINUS_1));
        assertCount(5, format("select * from table_integer where a >= %s", INT_MIN_MINUS_1));
    }

    // - - - - - - - - - - - - - - - - - - - - - -
    //
    // bigint
    //
    // - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void bigint_min() throws Exception {
        assertCount(1, format("select * from table_bigint where a = %s", Long.MIN_VALUE));
        assertCount(4, format("select * from table_bigint where a != %s", Long.MIN_VALUE));
        assertCount(0, format("select * from table_bigint where a < %s", Long.MIN_VALUE));
        assertCount(1, format("select * from table_bigint where a <= %s", Long.MIN_VALUE));
        assertCount(4, format("select * from table_bigint where a > %s", Long.MIN_VALUE));
        assertCount(5, format("select * from table_bigint where a >= %s", Long.MIN_VALUE));
    }

    @Test
    public void bigint_max() throws Exception {
        assertCount(1, format("select * from table_bigint where a = %s", Long.MAX_VALUE));
        assertCount(4, format("select * from table_bigint where a != %s", Long.MAX_VALUE));
        assertCount(4, format("select * from table_bigint where a < %s", Long.MAX_VALUE));
        assertCount(5, format("select * from table_bigint where a <= %s", Long.MAX_VALUE));
        assertCount(0, format("select * from table_bigint where a > %s", Long.MAX_VALUE));
        assertCount(1, format("select * from table_bigint where a >= %s", Long.MAX_VALUE));
    }

    @Ignore("fails, DB-1362")
    @Test
    public void bigint_max_plusOne() throws Exception {
        String LONG_MAX_PLUS_1 = new BigInteger(String.valueOf(Long.MAX_VALUE)).add(BigInteger.ONE).toString();

        assertCount(0, format("select * from table_bigint where a = %s", LONG_MAX_PLUS_1));
        assertCount(5, format("select * from table_bigint where a != %s", LONG_MAX_PLUS_1));
        assertCount(5, format("select * from table_bigint where a < %s", LONG_MAX_PLUS_1));
        assertCount(5, format("select * from table_bigint where a <= %s", LONG_MAX_PLUS_1));
        assertCount(0, format("select * from table_bigint where a > %s", LONG_MAX_PLUS_1));
        assertCount(0, format("select * from table_bigint where a >= %s", LONG_MAX_PLUS_1));
    }

    @Ignore("fails, DB-1362")
    @Test
    public void bigint_min_minusOne() throws Exception {
        String LONG_MIN_MINUS_1 = new BigInteger(String.valueOf(Long.MIN_VALUE)).subtract(BigInteger.ONE).toString();

        assertCount(0, format("select * from table_bigint where a = %s", LONG_MIN_MINUS_1));
        assertCount(5, format("select * from table_bigint where a != %s", LONG_MIN_MINUS_1));
        assertCount(0, format("select * from table_bigint where a < %s", LONG_MIN_MINUS_1));
        assertCount(0, format("select * from table_bigint where a <= %s", LONG_MIN_MINUS_1));
        assertCount(5, format("select * from table_bigint where a > %s", LONG_MIN_MINUS_1));
        assertCount(5, format("select * from table_bigint where a >= %s", LONG_MIN_MINUS_1));
    }


    private void assertCount(int expectedCount, String sql) throws Exception {
        ResultSet rs = methodWatcher.executeQuery(sql);
        assertEquals(format("count mismatch for sql='%s'", sql), expectedCount, count(rs));
    }

    private static int count(ResultSet rs) throws SQLException {
        int count = 0;
        while (rs.next()) {
            count++;
        }
        return count;
    }

}
