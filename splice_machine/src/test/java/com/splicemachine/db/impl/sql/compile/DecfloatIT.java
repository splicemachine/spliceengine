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
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test.SerialTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.*;
import java.util.Collection;

import static com.splicemachine.test_tools.Rows.rows;

@Category(value = {SerialTest.class})
@RunWith(Parameterized.class)
public class DecfloatIT extends SpliceUnitTest {

    private Boolean useSpark;
    private TestConnection conn;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{true});
        params.add(new Object[]{false});
        return params;
    }
    public static final String CLASS_NAME = DecfloatIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @Before
    public void setUp() throws Exception{
        SpliceWatcher.ConnectionBuilder connBuilder = methodWatcher.connectionBuilder();
        if (useSpark != null){
            connBuilder.useOLAP(useSpark);
        }
        conn = connBuilder.build();
        conn.setAutoCommit(false);
    }

    public DecfloatIT(Boolean useSpark) {
        this.useSpark = useSpark;
    }

    @Test
    public void castToDecfloat() throws Exception {
        String sqlText = "select cast('1.012345678901234567890123456789' as decfloat)";

        try (ResultSet rs = conn.query(sqlText)) {
            rs.next();
            Assert.assertEquals(new BigDecimal("1.012345678901234567890123456789"), rs.getBigDecimal(1));
        }
    }

    @Test
    public void decfloatMath() throws Exception {
        String lhs = "1.1234";
        String rhs = "1.1";
        String sqlText = format("select " +
                "cast('%s' as decfloat) / cast('%s' as decfloat), " +
                "cast('%s' as decfloat) + cast('%s' as decfloat), " +
                "cast('%s' as decfloat) - cast('%s' as decfloat), " +
                "cast('%s' as decfloat) * cast('%s' as decfloat) ",
                lhs, rhs, lhs, rhs, lhs, rhs, lhs, rhs);

        try(ResultSet rs = conn.query(sqlText)) {
            rs.next();
            BigDecimal lhsBd = new BigDecimal(lhs, MathContext.DECIMAL128);
            BigDecimal rhsBd = new BigDecimal(rhs, MathContext.DECIMAL128);
            Assert.assertEquals(lhsBd.divide(rhsBd, MathContext.DECIMAL128), rs.getBigDecimal(1));
            Assert.assertEquals(lhsBd.add(rhsBd), rs.getBigDecimal(2));
            Assert.assertEquals(lhsBd.subtract(rhsBd), rs.getBigDecimal(3));
            Assert.assertEquals(lhsBd.multiply(rhsBd), rs.getBigDecimal(4));
        }
    }

    @Test
    public void typeFunction() throws Exception {
        String sqlText = "select cast('1.00000000001' as decfloat), decfloat(1.00000000001)";
        try (ResultSet rs = conn.query(sqlText)) {
            rs.next();
            Assert.assertEquals(rs.getBigDecimal(1), rs.getBigDecimal(2));
        }
    }

    @Test
    public void insertIntoTable() throws Exception {
        conn.execute("drop table insertIntoTable if exists");
        conn.execute("create table insertIntoTable (a int, b decfloat, c decfloat, d decfloat not null with default, e decfloat not null with default 5.4)");
        conn.execute("insert into insertIntoTable (a, b) values (1, 1.2)");
        try (ResultSet rs = conn.query("select * from insertIntoTable")) {
            rs.next();
            Assert.assertEquals(1, rs.getInt(1));
            Assert.assertEquals(new BigDecimal("1.2"), rs.getBigDecimal(2));
            Assert.assertNull(rs.getBigDecimal(3));
            Assert.assertEquals(new BigDecimal(0), rs.getBigDecimal(4));
            Assert.assertEquals(new BigDecimal("5.4"), rs.getBigDecimal(5));
        }
    }

    @Test
    public void testJoin() throws Exception {
        conn.execute("drop table testJoin_A if exists");
        conn.execute("drop table testJoin_B if exists");
        conn.execute("create table testJoin_A(a1 decfloat, a2 int)");
        conn.execute("create table testJoin_B(b1 decfloat, b2 int)");
        conn.execute("insert into testJoin_A values ('3',3), ('3.0000000000000000000000000000001', 4)");
        conn.execute("insert into testJoin_B values ('3',3), ('3.0000000000000000000000000000001', 4)");
        String sql = "select * from testJoin_A, testJoin_B --splice-properties joinStrategy=broadcast\n" +
                "where a1 = b1";
        String expected = "A1                 |A2 |               B1                 |B2 |\n" +
                "------------------------------------------------------------------------------\n" +
                "                3                 | 3 |                3                 | 3 |\n" +
                "3.0000000000000000000000000000001 | 4 |3.0000000000000000000000000000001 | 4 |";
        testQuery(sql, expected, conn);
    }

    @Test
    public void testRoundDefaultHalfEven() throws Exception {
        // exactly 34 digits
        checkDecfloatExpression("decfloat('0.1234567890123456789012345678901234')", "0.1234567890123456789012345678901234", conn);
        // 35 digits round down
        checkDecfloatExpression("decfloat('0.12345678901234567890123456789012341')", "0.1234567890123456789012345678901234", conn);
        // 35 digits round up
        checkDecfloatExpression("decfloat('0.12345678901234567890123456789012348')", "0.1234567890123456789012345678901235", conn);
        // 35 digits half round down
        checkDecfloatExpression("decfloat('0.12345678901234567890123456789012345')", "0.1234567890123456789012345678901234", conn);
        // 35 digits half round up
        checkDecfloatExpression("decfloat('0.12345678901234567890123456789012335')", "0.1234567890123456789012345678901234", conn);
    }

    @Test
    public void testCompare() throws Exception {
        checkBooleanExpression("decfloat('3.0') = decfloat('3.0')", true, conn);
        checkBooleanExpression("decfloat('3.00') = decfloat('3.0')", true, conn);
        checkBooleanExpression("decfloat('3.0') = decfloat('3.00')", true, conn);
        checkBooleanExpression("decfloat('3.01') = decfloat('3.0')", false, conn);
        checkBooleanExpression("decfloat('3.01') > decfloat('3.0')", true, conn);
        checkBooleanExpression("decfloat('-0') = decfloat('0')", true, conn);
    }

    @Test
    public void testBorders() throws Exception {
        checkDecfloatExpression("decfloat('-9.999999999999999999999999999999999E6144')", "-9.999999999999999999999999999999999E+6144", conn);
        checkDecfloatExpression("decfloat('-1E-6143')", "-1E-6143", conn);
        checkDecfloatExpression("decfloat('1E-6143')", "1E-6143", conn);

        // FIXME DB-10864
        //checkDecfloatExpression("decfloat('9.999999999999999999999999999999999E6144')", "9.999999999999999999999999999999999E+6144", conn);
    }

    @Test
    public void testRoundFunction() throws Exception {
        checkDecfloatExpression("round(decfloat('0.123459'), 5)", "0.12346", conn);
        checkDecfloatExpression("round(decfloat('0.123459'), 5)", "0.12346", conn);
        checkDecfloatExpression("round(decfloat('0.123451'), 5)", "0.12345", conn);
        checkDecfloatExpression("round(decfloat('0.123455'), 5)", "0.12346", conn);
        checkDecfloatExpression("round(decfloat('0.123445'), 5)", "0.12345", conn);
    }

    @Test
    public void testCastFromOtherTypes() throws Exception {
        checkDecfloatExpression("cast( cast(1 as int) as decfloat)", "1", conn);
        checkDecfloatExpression("cast( cast(1 as bigint) as decfloat)", "1", conn);

        // FIXME DB-10866 (different toString representation between spark and control)
        // checkDecfloatExpression uses .compareTo instead of .equals to make this right for now
        checkDecfloatExpression("cast( cast(1 as double) as decfloat)", "1", conn);
        checkDecfloatExpression("cast( char('1.0') as decfloat)", "1", conn);
        checkDecfloatExpression("cast( cast(1.0 as float) as decfloat)", "1", conn);
        checkDecfloatExpression("cast( cast (1.0 as real) as decfloat)", "1", conn);

        checkDecfloatExpression("cast( smallint(1) as decfloat)", "1", conn);
        checkDecfloatExpression("cast( tinyint(1) as decfloat)", "1", conn);

        try {
            checkDecfloatExpression("cast( true as decfloat)", "1", conn);
            Assert.fail("bool to decfloat cast should fail");
        } catch (SQLException e) {
            Assert.assertEquals("42846", e.getSQLState());
        }
    }

    @Test
    @Ignore("DB-10868")
    public void testOverflow() throws Exception {
        checkDecfloatExpression("decfloat('-9.999999999999999999999999999999999E6144') * 10", "", conn);
        checkDecfloatExpression("decfloat('-1E-6143') / 10", "", conn);
        checkDecfloatExpression("decfloat('1E-6143') / 10", "", conn);
        checkDecfloatExpression("decfloat('9.999999999999999999999999999999999E6144') * 10", "", conn);
    }
}
