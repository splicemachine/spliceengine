/*
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
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Created by yxia on 6/12/18.
 */
public class SessionPropertyIT extends SpliceUnitTest {
    private static Logger LOG = Logger.getLogger(SessionPropertyIT.class);
    public static final String CLASS_NAME = SessionPropertyIT.class.getSimpleName().toUpperCase();
    protected final static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected final static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table t1(a1 int, b1 int, c1 int)")
                .withInsert("insert into t1 values(?,?,?)")
                .withRows(rows(
                        row(1,1,1),
                        row(2,2,2),
                        row(3,3,3),
                        row(4,4,4),
                        row(5,5,5),
                        row(6,6,6),
                        row(7,7,7),
                        row(8,8,8),
                        row(9,9,9),
                        row(10,10,10)))
                .create();

        // t2: 20 rows
        new TableCreator(conn)
                .withCreate("create table t2(a2 int, b2 int, c2 int)")
                .create();

        for (int i = 0; i < 2; i++) {
            spliceClassWatcher.executeUpdate("insert into t2 select * from t1");
        }

        // t3: 10 rows
        new TableCreator(conn)
                .withCreate("create table t3(a3 int, b3 int, c3 int)")
                .create();

        spliceClassWatcher.executeUpdate("insert into t3 select * from t1");

        // t1: 40 rows
        for (int i = 0; i < 2; i++) {
            spliceClassWatcher.executeUpdate("insert into t1 select * from t1");
        }

        spliceClassWatcher.execute(format("analyze schema %s", CLASS_NAME));
        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }

    @Test
    public void testSkipStatsSessionProperty() throws Exception {
        TestConnection conn = methodWatcher.createConnection();
        conn.execute("set session_property skipStats=true");

        String sqlText = "values current session_property";
        ResultSet rs = conn.query(sqlText);
        String expected = "1        |\n" +
                "-----------------\n" +
                "SKIPSTATS=true; |";
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sqlText = "explain select * from t1 where a1=1";
        rowContainsQuery(2, sqlText, "outputRows=18", conn);

        // reset property
        conn.execute("set session_property skipStats=null");
        sqlText = "values current session_property";
        rs = conn.query(sqlText);
        expected = "1 |\n" +
                "----\n" +
                "   |";
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sqlText = "explain select * from t1 where a1=1";
        rowContainsQuery(2, sqlText, "outputRows=4", conn);

        conn.close();
    }

    @Test
    public void testUseSparkSessionProperty() throws Exception {
        TestConnection conn = methodWatcher.createConnection();
        conn.execute("set session_property useSpark=true");

        String sqlText = "values current session_property";
        ResultSet rs = conn.query(sqlText);
        String expected = "1              |\n" +
                "------------------------------\n" +
                "USESPARK=true; USEOLAP=true; |";
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sqlText = "explain select * from t1 where a1=1";
        rowContainsQuery(1, sqlText, "engine=OLAP", conn);

        // set property to false
        conn.execute("set session_property useSpark=false");
        sqlText = "values current session_property";
        rs = conn.query(sqlText);
        expected = "1               |\n" +
                "--------------------------------\n" +
                "USESPARK=false; USEOLAP=false; |";
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sqlText = "explain select * from t1 where a1=1";
        rowContainsQuery(1, sqlText, "engine=OLTP", conn);

        //reset property
        conn.execute("set session_property useSpark=null");
        sqlText = "values current session_property";
        rs = conn.query(sqlText);
        expected = "1 |\n" +
                "----\n" +
                "   |";
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        conn.close();
    }

    @Test
    public void testDefaultSelectivityFactorSessionProperty() throws Exception {
        TestConnection conn = methodWatcher.createConnection();
        conn.execute("set session_property defaultSelectivityFactor=1e-4");

        String sqlText = "values current session_property";
        ResultSet rs = conn.query(sqlText);
        String expected = "1                |\n" +
                "----------------------------------\n" +
                "DEFAULTSELECTIVITYFACTOR=1.0E-4; |";
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sqlText = "explain select * from t1 where a1=1";
        rowContainsQuery(3, sqlText, "outputRows=1", conn);

        // set property to false
        conn.execute("set session_property defaultSelectivityFactor=null");
        sqlText = "values current session_property";
        rs = conn.query(sqlText);
        expected = "1 |\n" +
                "----\n" +
                "   |";
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sqlText = "explain select * from t1 where a1=1";
        rowContainsQuery(3, sqlText, "outputRows=4", conn);

        conn.close();

    }

    @Test
    public void TestTableLimitForExhaustiveSearchSessionProperty() throws Exception {
        TestConnection conn = methodWatcher.createConnection();
        String testQuery = "explain select * from %s\n t1 inner join t2 on a1=a2 inner join t3 on a2=a3";

        conn.execute("set session_property tableLimitForExhaustiveSearch=1");
        String sqlText = "values current session_property";
        try (ResultSet rs = conn.query(sqlText)) {
            String expected = "1                |\n" +
                    "----------------------------------\n" +
                    "TABLELIMITFOREXHAUSTIVESEARCH=1; |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
        // number of tables (3) > tableLimitForExhaustiveSearch (1), use heuristic instead of enumerating,
        // intermediate result size is less satisfying
        // DB-10054
        // Disable the following test for now because the number of join orders enumerated under jump mode
        // before timeout exceeded is quite nondeterministic.
        //rowContainsQuery(5, String.format(testQuery, ""), "outputRows=80", conn);

        // overwrite session property by using a query hint
        // intermediate result size should be better
        rowContainsQuery(6, String.format(testQuery, "--SPLICE-PROPERTIES tableLimitForExhaustiveSearch=3"), "outputRows=40", conn);

        // set session property to null
        conn.execute("set session_property tableLimitForExhaustiveSearch=null");
        sqlText = "values current session_property";
        try (ResultSet rs = conn.query(sqlText)) {
            String expected = "1 |\n" +
                    "----\n" +
                    "   |";
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }

        // default value is 6, we should achieve the same good plan without using the hint now
        rowContainsQuery(6, String.format(testQuery, ""), "outputRows=40", conn);

        conn.close();
    }

    @Test
    public void testIllegalSessionPropertyNameAndValue() throws Exception  {
        TestConnection conn = methodWatcher.createConnection();
        try {
            conn.execute("set session_property newpropertyname=true");
            fail("statement should not pass");
        } catch (SQLException e) {
            assertEquals(format("Expect ErrorState %s, actual is %s", SQLState.LANG_INVALID_SESSION_PROPERTY, e.getSQLState()), SQLState.LANG_INVALID_SESSION_PROPERTY, e.getSQLState());
        }

        try {
            conn.execute("set session_property defaultSelectivityFactor='xxxx'");
            fail("statement should not pass");
        } catch (SQLException e) {
            assertEquals(format("Expect ErrorState %s, actual is %s", SQLState.LANG_INVALID_SESSION_PROPERTY_VALUE, e.getSQLState()), SQLState.LANG_INVALID_SESSION_PROPERTY_VALUE, e.getSQLState());
        }
        conn.close();
    }

    @Test
    public void testCurrentTimeSyntax() throws Exception {
        String sql = "select count(*) from (values current time) dt";
        ResultSet rs = methodWatcher.executeQuery(sql);
        String expected = "1 |\n" +
                "----\n" +
                " 1 |";
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sql = "select count(*) from (values current_time) dt";
        rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sql = "select count(*) from (values current date) dt";
        rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sql = "select count(*) from (values current_date) dt";
        rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    private void rowContainsQuery(int level, String query, String contains, TestConnection con) throws Exception {
        try(ResultSet resultSet = con.query(query)){
            for(int i=0;i<level;i++){
                resultSet.next();
            }
            String actualString=resultSet.getString(1);
            String failMessage=String.format("expected result of query '%s' to contain '%s' at row %,d but did not, actual result was '%s'",
                    query,contains,level,actualString);
            Assert.assertTrue(failMessage,actualString.contains(contains));
        }
    }

    @Test
    public void TestMinPlanTimeout() throws Exception {
        TestConnection conn = methodWatcher.createConnection();
        String testQuery = "explain select * from t1 a, t1 b, t1 c, t1 d, t1 e, t1 f, t1 g, t1 h, t1 i, t1 j " +
            "where a.a1 = b.a1 and b.a1 = c.a1 and c.a1 = d.a1 and d.a1 = e.a1 and e.a1 = f.a1 and f.a1 = g.a1 and g.a1 = h.a1 and h.a1 = i.a1 and i.a1 = j.a1";

        long startTime = System.currentTimeMillis();
        conn.execute("set session_property minPlanTimeout=1");
        try (ResultSet rs = conn.query(testQuery)) { }
        long endTime = System.currentTimeMillis();
        Assert.assertTrue("Expected query planning to take less than 5 seconds", (endTime - startTime) < 5000);
        conn.execute("set session_property minPlanTimeout=5000");
        startTime = System.currentTimeMillis();
        try (ResultSet rs = conn.query(testQuery)) { }
        endTime = System.currentTimeMillis();
        Assert.assertTrue("Expected query planning to take more than 5 seconds", (endTime - startTime) > 5000);
        conn.close();

        // "jdbc:splice://localhost:1527/splicedb;create=true;user=splice;password=admin;minPlanTimeout=3000";
        conn = methodWatcher.connectionBuilder().minPlanTimeout(3000).build();
        startTime = System.currentTimeMillis();
        try (ResultSet rs = conn.query(testQuery)) { }
        endTime = System.currentTimeMillis();
        Assert.assertTrue("Expected query planning to take more than 3 seconds", (endTime - startTime) > 3000);
        conn.execute("set session_property minPlanTimeout=1");
        startTime = System.currentTimeMillis();
        try (ResultSet rs = conn.query(testQuery)) { }
        endTime = System.currentTimeMillis();
        Assert.assertTrue("Expected query planning to take less than 3 seconds", (endTime - startTime) < 3000);

        List<String> expectedErrors =
           Arrays.asList("Invalid session property value '-1' specified, 'value should be a positive long' is expected. ");
        testUpdateFail("set session_property minPlanTimeout=-1", expectedErrors, methodWatcher);
    }
}
