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

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import java.sql.Connection;
import static org.junit.Assert.assertEquals;

/**
 *
 * These IT's support string type conversion into numeric types
 *
 *
 */
public class StringTypesToNumericTypeConversionIT extends SpliceUnitTest {
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    private static final String CLASS_NAME = StringTypesToNumericTypeConversionIT.class.getSimpleName().toUpperCase();

    protected  static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    private static SpliceTableWatcher T1 = new SpliceTableWatcher("T1", schemaWatcher.schemaName,"(col1 int)");
    private static SpliceTableWatcher T2 = new SpliceTableWatcher("T2", schemaWatcher.schemaName,"(col1 bigint)");
    private static SpliceTableWatcher T3 = new SpliceTableWatcher("T3", schemaWatcher.schemaName,"(col1 decimal(18,2))");
    private static SpliceTableWatcher T4 = new SpliceTableWatcher("T4", schemaWatcher.schemaName,"(col1 float)");
    private static SpliceTableWatcher T5 = new SpliceTableWatcher("T5", schemaWatcher.schemaName,"(col1 double)");
    private static String SEL = "select * from %s order by col1";

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(schemaWatcher.schemaName);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(schemaWatcher)
            .around(T1)
            .around(T2)
            .around(T3)
            .around(T4)
            .around(T5);

    private Connection conn;

    @Before
    public void setUpTest() throws Exception{
        conn=methodWatcher.createConnection();
        conn.setAutoCommit(false);
    }

    @After
    public void tearDownTest() throws Exception{
        try {
            conn.rollback();
        } catch (Exception e) {}
    }

    @Test
    public void testCharToInt() throws Exception {
        conn.prepareStatement(String.format("insert into %s values ( cast('1' as char)), (cast(null as char))",T1)).execute();
        assertEquals("COL1 |\n" +
                "------\n" +
                "  1  |\n" +
                "NULL |", TestUtils.FormattedResult.ResultFactory.toString(conn.prepareStatement(String.format(SEL,T1)).executeQuery()));
    }

    @Test
    public void testClobToInt() throws Exception {
        conn.prepareStatement(String.format("insert into %s values ( cast('1' as clob)), ( cast(null as clob))",T1)).execute();
        assertEquals("COL1 |\n" +
                "------\n" +
                "  1  |\n" +
                "NULL |", TestUtils.FormattedResult.ResultFactory.toString(conn.prepareStatement(String.format(SEL,T1)).executeQuery()));
    }

    @Test
    public void testVarcharToInt() throws Exception {
        conn.prepareStatement(String.format("insert into %s values ( cast('1' as varchar(56))), ( cast(null as varchar(56)))",T1)).execute();
        assertEquals("COL1 |\n" +
                "------\n" +
                "  1  |\n" +
                "NULL |", TestUtils.FormattedResult.ResultFactory.toString(conn.prepareStatement(String.format(SEL,T1)).executeQuery()));
    }

    @Test
    public void testCharToBigint() throws Exception {
        conn.prepareStatement(String.format("insert into %s values ( cast('1' as char)), ( cast(null as char))",T2)).execute();
        assertEquals("COL1 |\n" +
                "------\n" +
                "  1  |\n" +
                "NULL |", TestUtils.FormattedResult.ResultFactory.toString(conn.prepareStatement(String.format(SEL,T2)).executeQuery()));
    }

    @Test
    public void testClobToBigint() throws Exception {
        conn.prepareStatement(String.format("insert into %s values ( cast('1' as clob)), ( cast(null as clob))",T2)).execute();
        assertEquals("COL1 |\n" +
                "------\n" +
                "  1  |\n" +
                "NULL |", TestUtils.FormattedResult.ResultFactory.toString(conn.prepareStatement(String.format(SEL,T2)).executeQuery()));
    }

    @Test
    public void testVarcharToBigint() throws Exception {
        conn.prepareStatement(String.format("insert into %s values ( cast('1' as varchar(56))), ( cast(null as varchar(56)))",T2)).execute();
        assertEquals("COL1 |\n" +
                "------\n" +
                "  1  |\n" +
                "NULL |", TestUtils.FormattedResult.ResultFactory.toString(conn.prepareStatement(String.format(SEL,T2)).executeQuery()));
    }

    @Test
    public void testCharToDecimal() throws Exception {
        conn.prepareStatement(String.format("insert into %s values ( cast('1.00' as char)), ( cast(null as char))",T3)).execute();
        assertEquals("COL1 |\n" +
                "------\n" +
                "1.00 |\n" +
                "NULL |", TestUtils.FormattedResult.ResultFactory.toString(conn.prepareStatement(String.format(SEL,T3)).executeQuery()));
    }

    @Test
    public void testClobToDecimal() throws Exception {
        conn.prepareStatement(String.format("insert into %s values ( cast('1' as clob)), ( cast(null as clob))",T3)).execute();
        assertEquals("COL1 |\n" +
                "------\n" +
                "1.00 |\n" +
                "NULL |", TestUtils.FormattedResult.ResultFactory.toString(conn.prepareStatement(String.format(SEL,T3)).executeQuery()));
    }

    @Test
    public void testVarcharToDecimal() throws Exception {
        conn.prepareStatement(String.format("insert into %s values ( cast('1' as varchar(56))), ( cast(null as varchar(56)))",T3)).execute();
        assertEquals("COL1 |\n" +
                "------\n" +
                "1.00 |\n" +
                "NULL |", TestUtils.FormattedResult.ResultFactory.toString(conn.prepareStatement(String.format(SEL,T3)).executeQuery()));
    }

    @Test
    public void testCharToFloat() throws Exception {
        conn.prepareStatement(String.format("insert into %s values ( cast('1.00' as char)), ( cast(null as char))",T4)).execute();
        assertEquals("COL1 |\n" +
                "------\n" +
                " 1.0 |\n" +
                "NULL |", TestUtils.FormattedResult.ResultFactory.toString(conn.prepareStatement(String.format(SEL,T4)).executeQuery()));
    }

    @Test
    public void testClobToFloat() throws Exception {
        conn.prepareStatement(String.format("insert into %s values ( cast('1' as clob)), ( cast(null as clob))",T4)).execute();
        assertEquals("COL1 |\n" +
                "------\n" +
                " 1.0 |\n" +
                "NULL |", TestUtils.FormattedResult.ResultFactory.toString(conn.prepareStatement(String.format(SEL,T4)).executeQuery()));
    }

    @Test
    public void testVarcharToFloat() throws Exception {
        conn.prepareStatement(String.format("insert into %s values ( cast('1' as varchar(56))), ( cast(null as varchar(56)))",T4)).execute();
        assertEquals("COL1 |\n" +
                "------\n" +
                " 1.0 |\n" +
                "NULL |", TestUtils.FormattedResult.ResultFactory.toString(conn.prepareStatement(String.format(SEL,T4)).executeQuery()));
    }

    @Test
    public void testCharToDouble() throws Exception {
        conn.prepareStatement(String.format("insert into %s values ( cast('1.00' as char)), ( cast(null as char))",T5)).execute();
        assertEquals("COL1 |\n" +
                "------\n" +
                " 1.0 |\n" +
                "NULL |", TestUtils.FormattedResult.ResultFactory.toString(conn.prepareStatement(String.format(SEL,T5)).executeQuery()));
    }

    @Test
    public void testClobToDouble() throws Exception {
        conn.prepareStatement(String.format("insert into %s values ( cast('1' as clob)), ( cast(null as clob))",T5)).execute();
        assertEquals("COL1 |\n" +
                "------\n" +
                " 1.0 |\n" +
                "NULL |", TestUtils.FormattedResult.ResultFactory.toString(conn.prepareStatement(String.format(SEL,T5)).executeQuery()));
    }

    @Test
    public void testVarcharToDouble() throws Exception {
        conn.prepareStatement(String.format("insert into %s values ( cast('1' as varchar(56))), ( cast(null as varchar(56)))",T5)).execute();
        assertEquals("COL1 |\n" +
                "------\n" +
                " 1.0 |\n" +
                "NULL |", TestUtils.FormattedResult.ResultFactory.toString(conn.prepareStatement(String.format(SEL,T5)).executeQuery()));
    }


}




