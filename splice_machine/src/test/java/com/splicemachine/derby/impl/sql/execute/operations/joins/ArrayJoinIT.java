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
 *
 */

package com.splicemachine.derby.impl.sql.execute.operations.joins;

import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLArray;
import com.splicemachine.db.iapi.types.SQLDecimal;
import com.splicemachine.db.iapi.types.SQLDouble;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.iapi.types.SQLReal;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.SlowTest;
import com.splicemachine.utils.Pair;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;
import splice.com.google.common.collect.Maps;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.splicemachine.homeless.TestUtils.o;
import static org.junit.Assert.assertEquals;


@RunWith(Parameterized.class)
public class ArrayJoinIT extends SpliceUnitTest {

    private static Logger LOG = Logger.getLogger(ArrayJoinIT.class);

    public static final String SCHEMA = ArrayJoinIT.class.getSimpleName().toUpperCase();
    public static final String TABLE_NAME_1 = "A";


    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);
    protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_NAME_1, SCHEMA,
            "(ia int array, va varchar(40) array,ra real array,fa float array, da decimal array, i int, v varchar(20))");
    private TestConnection conn;

    private static String CONTROL = "jdbc:splice://localhost:1527/splicedb;create=true;user=splice;password=admin;useSpark=false";
    private static String SPARK = "jdbc:splice://localhost:1527/splicedb;create=true;user=splice;password=admin;useSpark=true";

    private static String NLJ = "NESTEDLOOP\n";
    private static String MSJ = "SORTMERGE\n";
    private static String BCJ = "BROADCAST\n";

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {CONTROL, NLJ},
                {CONTROL, MSJ},
                {CONTROL, BCJ},
                {SPARK, NLJ},
                {SPARK, MSJ},
                {SPARK, BCJ},
        });
    }

    private final String connectionString;
    private final String joinStrategy;

    public ArrayJoinIT(String connecitonString, String joinStrategy) {
        this.connectionString = connecitonString;
        this.joinStrategy = joinStrategy;
    }

    /* Each test starts with same table state */
    @Before
    public void initTable() throws Exception {
        conn = new TestConnection(DriverManager.getConnection(connectionString, new Properties()));
        conn.setAutoCommit(false);
        conn.setSchema(SCHEMA.toUpperCase());
    }

    @After
    public void rollback() throws Exception {
        conn.rollback();
        conn.reset();
    }

    public static Array from(DataValueDescriptor... dvds) {
        return new SQLArray(dvds);
    }

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher1)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        PreparedStatement ps = spliceClassWatcher.prepareStatement(format("insert into %s (ia, va, ra, fa, da, i, v) values (?,?,?,?,?,?,?)", TABLE_NAME_1));
                        for (int i = 0; i < 10; i++) {
                            ps.setArray(1, from(new SQLInteger(i), new SQLInteger(i + 1)));
                            ps.setArray(2, from(new SQLVarchar("" + i), new SQLVarchar("" + (i + 1))));
                            ps.setArray(3, from(new SQLReal(i)));
                            ps.setArray(4, from(new SQLDouble(i)));
                            ps.setArray(5, from(new SQLDecimal("" + i)));
                            ps.setInt(6, i);
                            ps.setString(7, "" + i);
                            ps.executeUpdate();
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        spliceClassWatcher.closeAll();
                    }
                }

            });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @Test
    public void testInnerJoin() throws Exception {
        ResultSet rs = conn.createStatement().executeQuery("select a.ia[0], b.ia[0] from a --splice-properties joinStrategy=" + joinStrategy +
                "inner join a b on a.ia[0] = b.ia[0]");
        String s = TestUtils.FormattedResult.ResultFactory.toString(rs);
        rs.close();
        String expected =
                "1 | 2 |\n" +
                        "--------\n" +
                        " 0 | 0 |\n" +
                        " 1 | 1 |\n" +
                        " 2 | 2 |\n" +
                        " 3 | 3 |\n" +
                        " 4 | 4 |\n" +
                        " 5 | 5 |\n" +
                        " 6 | 6 |\n" +
                        " 7 | 7 |\n" +
                        " 8 | 8 |\n" +
                        " 9 | 9 |";
        assertEquals(s, expected, s);
    }

    @Test
    public void testInnerJoin2() throws Exception {
        ResultSet rs = conn.createStatement().executeQuery("select a.ia[0], b.ia[1] from a --splice-properties joinStrategy=" + joinStrategy +
                "inner join a b on a.ia[0] = b.ia[1]");
        String s = TestUtils.FormattedResult.ResultFactory.toString(rs);
        rs.close();
        String expected =
                "1 | 2 |\n" +
                        "--------\n" +
                        " 1 | 1 |\n" +
                        " 2 | 2 |\n" +
                        " 3 | 3 |\n" +
                        " 4 | 4 |\n" +
                        " 5 | 5 |\n" +
                        " 6 | 6 |\n" +
                        " 7 | 7 |\n" +
                        " 8 | 8 |\n" +
                        " 9 | 9 |";
        assertEquals(s, expected, s);
    }

    @Test
    public void testInnerJoinVarchar() throws Exception {
        ResultSet rs = conn.createStatement().executeQuery("select a.va[0], b.va[0] from a --splice-properties joinStrategy=" + joinStrategy +
                "inner join a b on a.va[0] = b.va[0]");
        String s = TestUtils.FormattedResult.ResultFactory.toString(rs);
        rs.close();
        String expected =
                "1 | 2 |\n" +
                        "--------\n" +
                        " 0 | 0 |\n" +
                        " 1 | 1 |\n" +
                        " 2 | 2 |\n" +
                        " 3 | 3 |\n" +
                        " 4 | 4 |\n" +
                        " 5 | 5 |\n" +
                        " 6 | 6 |\n" +
                        " 7 | 7 |\n" +
                        " 8 | 8 |\n" +
                        " 9 | 9 |";
        assertEquals(s, expected, s);
    }

    @Test
    public void testInnerJoinReal() throws Exception {
        ResultSet rs = conn.createStatement().executeQuery("select a.ra[0], b.ra[0] from a --splice-properties joinStrategy=" + joinStrategy +
                "inner join a b on a.ra[0] = b.ra[0]");
        String s = TestUtils.FormattedResult.ResultFactory.toString(rs);
        rs.close();
        String expected =
                "1  | 2  |\n" +
                        "----------\n" +
                        "0.0 |0.0 |\n" +
                        "1.0 |1.0 |\n" +
                        "2.0 |2.0 |\n" +
                        "3.0 |3.0 |\n" +
                        "4.0 |4.0 |\n" +
                        "5.0 |5.0 |\n" +
                        "6.0 |6.0 |\n" +
                        "7.0 |7.0 |\n" +
                        "8.0 |8.0 |\n" +
                        "9.0 |9.0 |";
        assertEquals(s, expected, s);
    }

    @Test
    public void testInnerJoinFloat() throws Exception {
        ResultSet rs = conn.createStatement().executeQuery("select a.fa[0], b.fa[0] from a --splice-properties joinStrategy=" + joinStrategy +
                "inner join a b on a.fa[0] = b.fa[0]");
        String s = TestUtils.FormattedResult.ResultFactory.toString(rs);
        rs.close();
        String expected =
                "1  | 2  |\n" +
                        "----------\n" +
                        "0.0 |0.0 |\n" +
                        "1.0 |1.0 |\n" +
                        "2.0 |2.0 |\n" +
                        "3.0 |3.0 |\n" +
                        "4.0 |4.0 |\n" +
                        "5.0 |5.0 |\n" +
                        "6.0 |6.0 |\n" +
                        "7.0 |7.0 |\n" +
                        "8.0 |8.0 |\n" +
                        "9.0 |9.0 |";
        assertEquals(s, expected, s);
    }

    @Test
    @Ignore("SPLICE-1553")
    public void testInnerJoinDecimal() throws Exception {
        ResultSet rs = conn.createStatement().executeQuery("select a.da[0], b.da[0] from a --splice-properties joinStrategy=" + joinStrategy +
                "inner join a b on a.da[0] = b.da[0]");
        String s = TestUtils.FormattedResult.ResultFactory.toString(rs);
        rs.close();
        String expected =
                "1 | 2 |\n" +
                        "--------\n" +
                        " 0 | 0 |\n" +
                        " 1 | 1 |\n" +
                        " 2 | 2 |\n" +
                        " 3 | 3 |\n" +
                        " 4 | 4 |\n" +
                        " 5 | 5 |\n" +
                        " 6 | 6 |\n" +
                        " 7 | 7 |\n" +
                        " 8 | 8 |\n" +
                        " 9 | 9 |";
        assertEquals(s, expected, s);
    }
    @Test
    public void testJoinArrayElementWithColumn() throws Exception {
        ResultSet rs = conn.createStatement().executeQuery("select a.ia[0], b.i from a --splice-properties joinStrategy=" + joinStrategy +
                "inner join a b on a.ia[0] = b.i");
        String s = TestUtils.FormattedResult.ResultFactory.toString(rs);
        rs.close();
        String expected =
                "1 | I |\n" +
                        "--------\n" +
                        " 0 | 0 |\n" +
                        " 1 | 1 |\n" +
                        " 2 | 2 |\n" +
                        " 3 | 3 |\n" +
                        " 4 | 4 |\n" +
                        " 5 | 5 |\n" +
                        " 6 | 6 |\n" +
                        " 7 | 7 |\n" +
                        " 8 | 8 |\n" +
                        " 9 | 9 |";
        assertEquals(s, expected, s);
    }
}
