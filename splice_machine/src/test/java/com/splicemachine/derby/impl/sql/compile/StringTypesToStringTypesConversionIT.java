/*
 * Copyright (c) 2012 - 2021 Splice Machine, Inc.
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

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

public class StringTypesToStringTypesConversionIT extends SpliceUnitTest {
    private static final String SCHEMA = StringTypesToStringTypesConversionIT.class.getSimpleName().toUpperCase();
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
                .withCreate("create table test (c char(4), vc varchar(4), lvc long varchar, lob clob)")
                .withInsert("insert into test values(?,?,?,?)")
                .withRows(rows(row("abc", "abc", "abc", "abc")))
                .create();
    }

    private static final String queryTemplate = "select cast(%3$s as %1$s(%2$d)), cast('%4$s' as %1$s(%2$d)), %1$s(%3$s, %2$d), %1$s('%4$s', %2$d) from test";
    private static final String c = "char";
    private static final String vc = "varchar";

    private void testToChar(int length, String columnName, String constLiteral,
                            String columnExpected, String literalExpected) throws Exception {
        // to char
        try (ResultSet rs = methodWatcher.executeQuery(format(queryTemplate, c, length, columnName, constLiteral))) {
            rs.next();
            assertEquals(columnExpected, rs.getString(1));
            assertEquals(columnExpected, rs.getString(3));
            assertEquals(literalExpected, rs.getString(2));
            assertEquals(literalExpected, rs.getString(4));
        }
    }

    private void testToVarchar(int length, String columnName, String constLiteral,
                               String columnExpected, String literalExpected) throws Exception {
        // to varchar
        try (ResultSet rs = methodWatcher.executeQuery(format(queryTemplate, vc, length, columnName, constLiteral))) {
            rs.next();
            assertEquals(columnExpected, rs.getString(1));
            assertEquals(columnExpected, rs.getString(3));
            assertEquals(literalExpected, rs.getString(2));
            assertEquals(literalExpected, rs.getString(4));
        }
    }

    private void testToLongVarchar(String columnName, String constLiteral,
                                   String columnExpected, String literalExpected) throws Exception {
        // to long varchar
        try (ResultSet rs = methodWatcher.executeQuery(
                // duplicate columns for easier test construction, no need to have a separate expect string
                format("select cast(%2$s as %1$s), cast('%3$s' as %1$s), cast(%2$s as %1$s), cast('%3$s' as %1$s) from test",
                        "long varchar", columnName, constLiteral))) {
            rs.next();
            assertEquals(columnExpected, rs.getString(1));
            assertEquals(columnExpected, rs.getString(3));
            assertEquals(literalExpected, rs.getString(2));
            assertEquals(literalExpected, rs.getString(4));
        }
    }

    @Test
    public void testEqualWidthChar() throws Exception {
        // in char column, values stored are padded
        testToChar(4, "c", "xyz", "abc ", "xyz ");
        testToVarchar(4, "c", "xyz", "abc ", "xyz");
    }

    @Test
    public void testEqualWidthVarchar() throws Exception {
        // in varchar column, values stored are not padded
        testToChar(4, "vc", "xyz", "abc ", "xyz ");
        testToVarchar(4, "vc", "xyz", "abc", "xyz");
    }

    @Test
    public void testEqualWidthLongVarchar() throws Exception {
        testToLongVarchar("lvc", "xyz", "abc", "xyz");
    }

    @Test
    public void testWiderTargetTypeChar() throws Exception {
        testToChar(8, "c", "ghi", "abc     ", "ghi     ");
        testToVarchar(8, "c", "ghi", "abc ", "ghi");
        testToLongVarchar("c", "ghi", "abc ", "ghi");
    }

    @Test
    public void testWiderTargetTypeVarchar() throws Exception {
        testToChar(8, "vc", "ghi", "abc     ", "ghi     ");
        testToVarchar(8, "vc", "ghi", "abc", "ghi");
        testToLongVarchar("vc", "ghi", "abc", "ghi");
    }

    @Test
    public void testNarrowerTargetTypeChar() throws Exception {
        testToChar(2, "c", "ghi", "ab", "gh");
        testToVarchar(2, "c", "ghi", "ab", "gh");
    }

    @Test
    public void testNarrowerTargetTypeVarchar() throws Exception {
        testToChar(2, "vc", "xyz", "ab", "xy");
        testToVarchar(2, "vc", "xyz", "ab", "xy");
    }

    @Test
    public void testNarrowerTargetTypeLongVarchar() throws Exception {
        try {
            testToChar(2, "lvc", "xyz", "ab", "xy");
        } catch (SQLException e) {
            assertEquals("42846", e.getSQLState());
            Assert.assertTrue(e.getMessage().contains("Cannot set a length for a cast from 'LONG VARCHAR' to 'CHAR'"));
        }

        try {
            testToVarchar(2, "lvc", "xyz", "ab", "xy");
        } catch (SQLException e) {
            assertEquals("42846", e.getSQLState());
            Assert.assertTrue(e.getMessage().contains("Cannot set a length for a cast from 'LONG VARCHAR' to 'VARCHAR'"));
        }
    }
}




