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
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

@Category(value = {SerialTest.class})
@RunWith(Parameterized.class)
public class DecfloatIT extends SpliceUnitTest {

    private Boolean useSpark;

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

    public DecfloatIT(Boolean useSpark) {
        this.useSpark = useSpark;
    }

    @Test
    public void castToDecfloat() throws Exception {
        String sqlText = format("select cast('1.012345678901234567890123456789' as decfloat) from sysibm.sysdummy1 --spliceproperties useSpark=%s\n", useSpark);

        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
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
                "cast('%s' as decfloat) * cast('%s' as decfloat) " +
                "from sysibm.sysdummy1 --spliceproperties useSpark=%s\n", lhs, rhs, lhs, rhs, lhs, rhs, lhs, rhs, useSpark);

        try(ResultSet rs = methodWatcher.executeQuery(sqlText)) {
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
        String sqlText = format("select cast('1.00000000001' as decfloat), decfloat(1.00000000001) " +
                "from sysibm.sysdummy1 --spliceproperties useSpark=%s\n", useSpark);
        try (ResultSet rs = methodWatcher.executeQuery(sqlText)) {
            rs.next();
            Assert.assertEquals(rs.getBigDecimal(1), rs.getBigDecimal(2));
        }
    }

    @Test
    public void insertIntoTable() throws Exception {
        methodWatcher.executeUpdate("drop table insertIntoTable if exists");
        methodWatcher.executeUpdate("create table insertIntoTable (a int, b decfloat, c decfloat, d decfloat not null with default, e decfloat not null with default 5.4)");
        methodWatcher.executeUpdate("insert into insertIntoTable (a, b) values (1, 1.2)");
        try (ResultSet rs = methodWatcher.executeQuery("select * from insertIntoTable")) {
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
        methodWatcher.executeUpdate("drop table testJoin_A if exists");
        methodWatcher.executeUpdate("drop table testJoin_B if exists");
        methodWatcher.executeUpdate("create table testJoin_A(a1 decfloat, a2 int)");
        methodWatcher.executeUpdate("create table testJoin_B(b1 decfloat, b2 int)");
        methodWatcher.executeUpdate("insert into testJoin_A values ('3',3), ('3.0000000000000000000000000000001', 4)");
        methodWatcher.executeUpdate("insert into testJoin_B values ('3',3), ('3.0000000000000000000000000000001', 4)");
        String sql = format("select * from testJoin_A, testJoin_B --splice-properties useSpark=%s, joinStrategy=broadcast\n" +
                "where a1 = b1", useSpark);
        String expected = "A1                 |A2 |               B1                 |B2 |\n" +
                "------------------------------------------------------------------------------\n" +
                "                3                 | 3 |                3                 | 3 |\n" +
                "3.0000000000000000000000000000001 | 4 |3.0000000000000000000000000000001 | 4 |";
        testQuery(sql, expected, methodWatcher);
    }
}
