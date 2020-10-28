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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.ResultSet;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

/**
 * Created by yxia on 10/21/20.
 */
public class ComparisonForBitDataIT extends SpliceUnitTest {
    private static Logger LOG = Logger.getLogger(TernaryFunctionForBitDataIT.class);
    public static final String CLASS_NAME = ComparisonForBitDataIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);


    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table t1(id int, a1 char(5) for bit data, b1 char(5) for bit data, primary key (a1))")
                .withInsert("insert into t1 values (?, ?, ?) ")
                .withRows(rows(
                        row(1, "abc".getBytes(), "abc".getBytes()),
                        row(2, "ab".getBytes(), "ab".getBytes()),
                        row(3, "".getBytes(), "".getBytes())
                ))
                .create();


        new TableCreator(conn)
                .withCreate("create table t2(id int, a2 varchar(5) for bit data, b2 varchar(5) for bit data, primary key (a2))")
                .withInsert("insert into t2 values (?, ?, ?) ")
                .withRows(rows(
                        row(1, "abc".getBytes(), "abc".getBytes()),
                        row(2, "ab".getBytes(), "ab".getBytes()),
                        row(3, "".getBytes(), "".getBytes())
                ))
                .create();

        new TableCreator(conn)
                .withCreate("create table t3(id int, a3 char(5), b3 varchar(5))")
                .withInsert("insert into t3 values (?, ?, ?) ")
                .withRows(rows(
                        row(1, "abc", "abc"),
                        row(2, "ab", "ab")
                ))
                .create();

        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }

    /**************************
     * test fixed bit data type
     **************************/
    @Test
    public void testFixedBitDataPkColumnEqualsToConstantBitDataValueNeedPadding() throws Exception {
        String sqlText = "select id from t1 where a1=X'616263'";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected_result = "ID |\n" +
                "----\n" +
                " 1 |";
        assertEquals(expected_result, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testFixedBitDataPkColumnEqualsToConstantStringDataValueNeedPadding() throws Exception {
        String sqlText = "select id from t1 where a1='abc'";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected_result = "ID |\n" +
                "----\n" +
                " 1 |";
        assertEquals(expected_result, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testFixedBitDataNonPkColumnEqualsToConstantBitDataValue() throws Exception {
        String sqlText = "select id from t1 where b1=X'616263'";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected_result = "ID |\n" +
                "----\n" +
                " 1 |";
        assertEquals(expected_result, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testFixedBitDataNonPkColumnEqualsToConstantStringDataValue() throws Exception {
        String sqlText = "select id from t1 where b1='abc'";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected_result = "ID |\n" +
                "----\n" +
                " 1 |";
        assertEquals(expected_result, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testJoinOnColumnWithFixedBitDataType() throws Exception {
        String[] sqlText = new String[] {"select t1.id, t3.id from t1, t3 where a1=a3",
                                       "select t1.id, t3.id from t1, t3 where a1=b3",
                                       "select t1.id, t3.id from t1, t3 where b1=a3",
                                       "select t1.id, t3.id from t1, t3 where b1=b3"};

        for (int i=0; i<sqlText.length; i++) {
            ResultSet rs = methodWatcher.executeQuery(sqlText[i]);
            String expected_result = "ID |ID |\n" +
                    "--------\n" +
                    " 1 | 1 |\n" +
                    " 2 | 2 |";
            assertEquals(expected_result, TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();
        }
    }

    /**************************
     * test varbit data type
     **************************/
    @Test
    public void testVarbitDataPkColumnEqualsToConstantBitDataValueNeedPadding() throws Exception {
        String sqlText = "select id from t2 where a2=X'616263'";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected_result = "ID |\n" +
                "----\n" +
                " 1 |";
        assertEquals(expected_result, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testVarbitDataPkColumnEqualsToConstantStringDataValueNeedPadding() throws Exception {
        String sqlText = "select id from t2 where a2='abc'";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected_result = "ID |\n" +
                "----\n" +
                " 1 |";
        assertEquals(expected_result, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testVarBbitDataNonPkColumnEqualsToConstantBitDataValue() throws Exception {
        String sqlText = "select id from t2 where b2=X'616263'";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected_result = "ID |\n" +
                "----\n" +
                " 1 |";
        assertEquals(expected_result, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testVarbitDataNonPkColumnEqualsToConstantStringDataValue() throws Exception {
        String sqlText = "select id from t2 where b2='abc'";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected_result = "ID |\n" +
                "----\n" +
                " 1 |";
        assertEquals(expected_result, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testJoinOnColumnWithVarbitDataType() throws Exception {
        String[] sqlText = new String[] {"select t2.id, t3.id from t2, t3 where a2=a3",
                "select t2.id, t3.id from t2, t3 where a2=b3",
                "select t2.id, t3.id from t2, t3 where b2=a3",
                "select t2.id, t3.id from t2, t3 where b2=b3"};

        for (int i=0; i<sqlText.length; i++) {
            ResultSet rs = methodWatcher.executeQuery(sqlText[i]);
            String expected_result = "ID |ID |\n" +
                    "--------\n" +
                    " 1 | 1 |\n" +
                    " 2 | 2 |";
            assertEquals(expected_result, TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();
        }
    }

    /*****************
     * misc test
     ****************/
    @Test
    public void testExpressionOnVarbitDataColumn() throws Exception {
        String sqlText = "select id from t2 where substr(b2,2,2) ='bc'";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected_result = "ID |\n" +
                "----\n" +
                " 1 |";
        assertEquals(expected_result, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }
}
