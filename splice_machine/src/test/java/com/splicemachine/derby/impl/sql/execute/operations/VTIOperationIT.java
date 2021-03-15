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

import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Created by jyuan on 10/12/15.
 */
public class VTIOperationIT extends SpliceUnitTest {
    public static final String CLASS_NAME = VTIOperationIT.class.getSimpleName().toUpperCase();
    private static final String TABLE_NAME="EMPLOYEE";

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);

    @BeforeClass
    public static void setup() throws Exception {
        setup(spliceClassWatcher);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        spliceClassWatcher.execute("drop function JDBCTableVTI ");
        spliceClassWatcher.execute("drop function JDBCSQLVTI ");
        spliceClassWatcher.execute("call sqlj.remove_jar('VTIOperationITFlatMapFunctionTest',0) ");
        spliceClassWatcher.execute("drop trigger sourceToDest");
    }

    private static void setup(SpliceWatcher spliceClassWatcher) throws Exception {
        Connection conn = spliceClassWatcher.getOrCreateConnection();
        new TableCreator(conn)
                .withCreate("create table employee (name varchar(56),id bigint,salary numeric(9,2),ranking int)")
                .withInsert("insert into employee values(?,?,?,?)")
                .withRows(rows(
                        row("Andy", 1, 100000, 1),
                        row("Billy", 2, 100000, 2))).create();

        new TableCreator(conn)
                .withCreate("create table sourceTable(a int, b int, d varchar(250) primary key, e varchar(250))")
                .create();

        new TableCreator(conn)
                .withCreate("create table destTable(a int, b int, d varchar(250), e varchar(250))")
                .create();

        String sql = "create function JDBCTableVTI(conn varchar(32672), s varchar(1024), t varchar(1024))\n" +
                "returns table\n" +
                "(\n" +
                "   name varchar(56),\n" +
                "   id bigint,\n" +
                "   salary numeric(9,2),\n" +
                "   ranking int\n" +
                ")\n" +
                "language java\n" +
                "parameter style SPLICE_JDBC_RESULT_SET\n" +
                "no sql\n" +
                "external name 'com.splicemachine.derby.vti.SpliceJDBCVTI.getJDBCTableVTI'";
        spliceClassWatcher.execute(sql);

        sql = "create function JDBCSQLVTI(conn varchar(32672), s varchar(32672))\n" +
                "returns table\n" +
                "(\n" +
                "   name varchar(56),\n" +
                "   id bigint,\n" +
                "   salary numeric(9,2),\n" +
                "   ranking int\n" +
                ")\n" +
                "language java\n" +
                "parameter style SPLICE_JDBC_RESULT_SET\n" +
                "no sql\n" +
                "external name 'com.splicemachine.derby.vti.SpliceJDBCVTI.getJDBCSQLVTI'";
        spliceClassWatcher.execute(sql);
    }

    @Test
    public void testJDBCSQLVTI() throws Exception {
        String sql = String.format("select * from table (JDBCSQLVTI('jdbc:splice://localhost:1527/splicedb;create=true;" +
                "user=splice;password=admin', " +
                "'select * from %s.%s'))a", CLASS_NAME, TABLE_NAME);
        ResultSet rs = spliceClassWatcher.executeQuery(sql);
        int count = 0;
        while (rs.next()) {
            count++;
        }
        Assert.assertEquals(2, count);
    }

    @Test
    public void testJDBCTableVTI() throws Exception {
        String sql = String.format("select * from table (JDBCTableVTI('jdbc:splice://localhost:1527/splicedb;create=true;user=splice;password=admin', '%s', '%s'))a", CLASS_NAME, TABLE_NAME);
        ResultSet rs = spliceClassWatcher.executeQuery(sql);
        int count = 0;
        while (rs.next()) {
            count++;
        }
        Assert.assertEquals(2, count);
    }

    @Test
    public void testJDBCTableVTIWithJoin() throws Exception {
        String sql = String.format("select * from table (JDBCTableVTI('jdbc:splice://localhost:1527/splicedb;create=true;user=splice;password=admin', '%s', '%s'))a" +
                ", employee where employee.id = a.id", CLASS_NAME, TABLE_NAME);
        ResultSet rs = spliceClassWatcher.executeQuery(sql);
        int count = 0;
        while (rs.next()) {
            count++;
        }
        Assert.assertEquals(2, count);
    }

    @Test
    public void testFileVTI() throws Exception {
        String location = getResourceDirectory()+"importTest.in";
        String sql = String.format("select * from new com.splicemachine.derby.vti.SpliceFileVTI('%s','',',') as b (c1 varchar(128), c2 varchar(128), c3 int)", location);
        ResultSet rs = spliceClassWatcher.executeQuery(sql);
        int count = 0;
        while (rs.next()) {
            count++;
        }
        Assert.assertEquals(5, count);
    }


    @Test
    public void testFileVTIWithJoin() throws Exception {
        String location = getResourceDirectory()+"importTest.in";
        String sql = String.format("select * from new com.splicemachine.derby.vti.SpliceFileVTI('%s','',',') as b (c1 varchar(128), c2 varchar(128), c3 int)" +
                ", employee where employee.id + 25 = b.c3", location);
        ResultSet rs = spliceClassWatcher.executeQuery(sql);
        int count = 0;
        while (rs.next()) {
            count++;
        }
        Assert.assertEquals(1, count);
    }

    @Test
    @Ignore("DB-4641: failing when in Jenkins when run under the mem DB profile")
    public void testFileVTIExpectError() throws Exception {
        String location = getResourceDirectory()+"importTest.in";
        String sql = String.format("select * from new com.splicemachine.derby.vti.SpliceFileVTI('%s','',',') as b" +
                                       " (name varchar(10), title varchar(30), age int, something varchar(12), " +
                                       "date_hired timestamp, clock time)\n" +
                                       " where age < 40 and date_hired > TIMESTAMP('2015-08-21', '08:09:08') order" +
                                       " by name", location);
        try {
            ResultSet rs = spliceClassWatcher.executeQuery(sql);
            fail("Expected: java.sql.SQLException: Number of columns in column definition, 6, differ from those found in import file 3.");
        } catch (SQLException e) {
           // expected: "Number of columns in column definition, 6, differ from those found in import file 3. "
            assertEquals("XIE0A", e.getSQLState());
            return;
        }
        fail("Expected: java.sql.SQLException: Number of columns in column definition, 6, differ from those found in import file 3.");
    }

    @Test
    public void testFileVTITypes() throws Exception {
        String location = getResourceDirectory()+"vtiConversion.in";
        String sql = String.format("select '-' || name || '-', '-' || title || '-', age, something, date_hired, clock from new com.splicemachine.derby.vti.SpliceFileVTI('%s','',',') as b" +
                                       " (name varchar(10), title varchar(30), age int, something varchar(12), " +
                                       "date_hired timestamp, clock time) --splice-properties useSpark=false\n" +
                                       " where age < 40 and date_hired > TIMESTAMP('2015-08-21', '08:09:08') order" +
                                       " by name", location);
        ResultSet rs = spliceClassWatcher.executeQuery(sql);
        String expected =
            "1    |             2             | AGE | SOMETHING  |     DATE_HIRED       |  CLOCK  |\n" +
                    "------------------------------------------------------------------------------------------\n" +
                    "-jzhang- | -How The West Won Texas-  | 34  |08-23X-2015 |2015-08-22 08:12:08.0 |11:08:08 |\n" +
                    "-sfines- |-Senior Software Engineer- | 27  |08X-27-2015 |2015-08-27 08:08:08.0 |06:08:08 |";
        assertEquals("\n"+sql+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        //test spark path
        sql = String.format("select '-' || name || '-', '-' || title || '-', age, something, date_hired, clock from new com.splicemachine.derby.vti.SpliceFileVTI('%s','',',') as b" +
                " (name varchar(10), title varchar(30), age int, something varchar(12), " +
                "date_hired timestamp, clock time) --splice-properties useSpark=true\n" +
                " where age < 40 and date_hired > TIMESTAMP('2015-08-21', '08:09:08') order" +
                " by name", location);
        rs = spliceClassWatcher.executeQuery(sql);
        assertEquals("\n"+sql+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }


    @Test
    public void testFileVTIFixedLengthCharType() throws Exception {
        String location = getResourceDirectory()+"vtiConversion.in";
        String sql = String.format("select '-' || name || '-', '-' || title || '-', age, something, date_hired, clock from new com.splicemachine.derby.vti.SpliceFileVTI('%s','',',') as b" +
                " (name char(20), title char(30), age int, something varchar(12), " +
                "date_hired timestamp, clock time) --splice-properties useSpark=false\n" +
                " where name='jzhang' order" +
                " by name", location);
        ResultSet rs = spliceClassWatcher.executeQuery(sql);
        String expected =
                "1           |                2                | AGE | SOMETHING  |     DATE_HIRED       |  CLOCK  |\n" +
                        "--------------------------------------------------------------------------------------------------------------\n" +
                        "-jzhang              - |-How The West Won Texas        - | 34  |08-23X-2015 |2015-08-22 08:12:08.0 |11:08:08 |";
        assertEquals("\n"+sql+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        //test spark path
        sql = String.format("select '-' || name || '-', '-' || title || '-', age, something, date_hired, clock from new com.splicemachine.derby.vti.SpliceFileVTI('%s','',',') as b" +
                " (name char(20), title char(30), age int, something varchar(12), " +
                "date_hired timestamp, clock time) --splice-properties useSpark=true\n" +
                " where name='jzhang' order" +
                " by name", location);
        rs = spliceClassWatcher.executeQuery(sql);
        assertEquals("\n"+sql+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testFileVTIFixedLengthCharTypeErrorCase() throws Exception {
        /* title has value with more than 10 characters, specifying it of fixed char(10) should make it error out */
        String location = getResourceDirectory()+"vtiConversion.in";
        String sql = String.format("select '-' || name || '-', '-' || title || '-', age, something, date_hired, clock from new com.splicemachine.derby.vti.SpliceFileVTI('%s','',',') as b" +
                " (name char(20), title char(10), age int, something varchar(12), " +
                "date_hired timestamp, clock time)\n" +
                " where name='jzhang' order" +
                " by name", location);
        try {
            spliceClassWatcher.executeQuery(sql);
            Assert.fail("Query is expected to fail with error ERROR 22001: A truncation error was encountered trying to shrink CHAR ...");
        } catch (SQLException se) {
            Assert.assertEquals(se.getSQLState(), SQLState.LANG_STRING_TRUNCATION);
        }
    }

    @Test
    public void testFileVTIVarCharTypeErrorCase() throws Exception {
        /* title has value with more than 10 characters, specifying it of varchar(10) should make it error out */
        String location = getResourceDirectory()+"vtiConversion.in";
        String sql = String.format("select '-' || name || '-', '-' || title || '-', age, something, date_hired, clock from new com.splicemachine.derby.vti.SpliceFileVTI('%s','',',') as b" +
                " (name char(20), title varchar(10), age int, something varchar(12), " +
                "date_hired timestamp, clock time)\n" +
                " where name='jzhang' order" +
                " by name", location);
        try {
            spliceClassWatcher.executeQuery(sql);
            Assert.fail("Query is expected to fail with error ERROR 22001: A truncation error was encountered trying to shrink CHAR ...");
        } catch (SQLException se) {
            Assert.assertEquals(se.getSQLState(), SQLState.LANG_STRING_TRUNCATION);
        }
    }

    @Test
    public void testVTIConversion() throws Exception {
        String location = getResourceDirectory()+"vtiConversion.in";
        String sql = String.format("select * from new com.splicemachine.derby.vti.SpliceFileVTI('%s','',',') " +
                "as b (c1 varchar(128), c2 varchar(128), c3 varchar(128), c4 varchar(128), c5 varchar(128), " +
                "c6 varchar(128))", location);
        ResultSet rs = spliceClassWatcher.executeQuery(sql);
        int count = 0;
        while (rs.next()) {
            count++;
        }
        Assert.assertEquals(5, count);
    }

    @Test
    // SPLICE-957
    public void testVTIEncoding() throws Exception {
        String location = getResourceDirectory()+"vtiConversion.in";
        try {
            ResultSet rs = spliceClassWatcher.executeQuery(
                    String.format("select * from new com.splicemachine.derby.vti.SpliceFileVTI('%s', null, null, null, null, null, null, null, 'utf-50') as t (bi_col BIGINT)",location));
            fail("Expected: Unsupported Encoding");
        } catch (SQLException e) {
            // expected: "Number of columns in column definition, 6, differ from those found in import file 3. "
            assertEquals("EXT20", e.getSQLState());
            return;
        }
    }

    @Test
    public void testFlatMapFunctionVTI() throws Exception {
        String location = getResourceDirectory()+"VTIOperationITFlatMapFunctionTest-1.0.8-SNAPSHOT.jar";
        String VTIName = CLASS_NAME + ".VTIOperationITFlatMapFunctionTest";
        String sqlText = format("call sqlj.install_jar('%s', '%s',0)", location, VTIName);
        spliceClassWatcher.execute(sqlText);
        sqlText = format("CALL SYSCS_UTIL.SYSCS_SET_GLOBAL_DATABASE_PROPERTY('derby.database.classpath', '%s')",VTIName);
        spliceClassWatcher.execute(sqlText);

        sqlText = format("create trigger sourceToDest\n" +
                    "after insert\n" +
                    "on sourceTable\n" +
                    "referencing new table as NT\n" +
                    "for each statement\n" +
                    "insert into destTable\n" +
                    "        select b.a,b.b,b.d,b.e\n" +
                    "        from new com.splicemachine.VTIOperationITFlatMapFunctionTester.VTIOperationITFlatMapFunctionTest(\n" +
                    "            '%s','SOURCETABLE', new com.splicemachine.derby.catalog.TriggerNewTransitionRows()\n" +
                    "        ) as b (a INT, b INT, d VARCHAR(250), e VARCHAR(250))\n", CLASS_NAME);
        spliceClassWatcher.execute(sqlText);


        sqlText = "insert into sourceTable (a,b,d, e)  --splice-properties useSpark=false\n values(2,5,'test1', 'some info')";
        spliceClassWatcher.execute(sqlText);
        sqlText = "delete from sourceTable";
        spliceClassWatcher.execute(sqlText);
        sqlText = "insert into sourceTable (a,b,d, e)  --splice-properties useSpark=true\n values(2,5,'test1', 'some info')";
        spliceClassWatcher.execute(sqlText);
        sqlText = "select * from destTable";
        String expected = "A | B |  D   |    E    |\n" +
                        "-------------------------\n" +
                        " 2 | 5 |test1 |SOME PIG |\n" +
                        " 2 | 5 |test1 |SOME PIG |";
        testQuery(sqlText, expected, spliceClassWatcher);

        // Test nested loop join in a VTI that accesses trigger rows.
        sqlText = "CREATE TABLE SKLEARN_MODEL (\n" +
                  "                CUR_USER VARCHAR(50) DEFAULT CURRENT_USER,\n" +
                  "                EVAL_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n" +
                  "                RUN_ID VARCHAR(50) DEFAULT '631e274ec9fd',\n" +
                  "                EXPECTED_WEEKLY_TRANS_CNT FLOAT, WEEKLY_TRANS_CNT FLOAT, RSI_TRANS_AMNT FLOAT," +
                  " AMOUNT DECIMAL, CLASS_RESULT BIGINT,MOMENT_KEY INT,PREDICTION INT,PRIMARY KEY(MOMENT_KEY))";
        spliceClassWatcher.execute(sqlText);

        sqlText = format("CREATE TRIGGER runModel_SKLEARN_MODEL_631e274ec9fd\n" +
                  "                        AFTER INSERT ON SKLEARN_MODEL REFERENCING NEW TABLE AS NT FOR EACH STATEMENT\n" +
                  "                        UPDATE SKLEARN_MODEL --splice-properties useSpark=False, joinStrategy=nestedloop\n" +
                  "SET (PREDICTION) = (SELECT b.PREDICTION FROM new com.splicemachine.VTIOperationITFlatMapFunctionTester.VTIOperationITFlatMapFunctionTest2(\n" +
                  "        '%s','SOURCETABLE', new com.splicemachine.derby.catalog.TriggerNewTransitionRows())" +
                  "         as b (cur_user VARCHAR(50),eval_time TIMESTAMP,run_id VARCHAR(50),expected_weekly_trans_cnt FLOAT,weekly_trans_cnt FLOAT,rsi_trans_amnt FLOAT,amount DECIMAL(5, 0),class_result BIGINT,moment_key INTEGER,prediction INTEGER)  --splice-properties joinStrategy=nestedloop\n" +
                  "WHERE SKLEARN_MODEL.MOMENT_KEY = b.MOMENT_KEY)", CLASS_NAME);
        spliceClassWatcher.execute(sqlText);
        sqlText = "CREATE TABLE feeder (\n" +
                  "                EXPECTED_WEEKLY_TRANS_CNT FLOAT, WEEKLY_TRANS_CNT FLOAT, RSI_TRANS_AMNT FLOAT, AMOUNT DECIMAL, CLASS_RESULT BIGINT,MOMENT_KEY INT,PRIMARY KEY(MOMENT_KEY))";
        spliceClassWatcher.execute(sqlText);
        sqlText = "insert into feeder(EXPECTED_WEEKLY_TRANS_CNT,WEEKLY_TRANS_CNT,RSI_TRANS_AMNT,AMOUNT, MOMENT_KEY) values (2.5, 3.5, 23.5, 2.5, 1)";
        spliceClassWatcher.execute(sqlText);
        String templateSQL = "insert into feeder(EXPECTED_WEEKLY_TRANS_CNT,WEEKLY_TRANS_CNT,RSI_TRANS_AMNT,AMOUNT, MOMENT_KEY) select EXPECTED_WEEKLY_TRANS_CNT,WEEKLY_TRANS_CNT,RSI_TRANS_AMNT,AMOUNT, MOMENT_KEY+%d from feeder";
        for (int i=1; i <= 32768; i*=2) {
            sqlText = format(templateSQL, i);
            spliceClassWatcher.execute(sqlText);
        }

        // Run the SQL that fires the trigger
        templateSQL = "insert into sklearn_model (expected_weekly_trans_cnt, weekly_trans_cnt, rsi_trans_amnt, amount, moment_key)     --splice-properties useSpark=False\n" +
                      "select top 10000 expected_weekly_trans_cnt, weekly_trans_cnt, rsi_trans_amnt, amount, moment_key+%d  from feeder --splice-properties useSpark=False";
        for (int i=10000; i <= 40000; i*=2) {
            sqlText = format(templateSQL, i);
            spliceClassWatcher.execute(sqlText);
        }
    }

    @Test
    public void testQueryingVTIWithoutExplicitSchema() throws Exception {
        ResultSet rs = spliceClassWatcher.executeQuery("select * From new com.splicemachine.derby.vti.SpliceAllRolesVTI() x");
        int count = 0;
        while (rs.next()) {
            count++;
        }
        Assert.assertTrue(count > 0);
    }

    @Test
    public void testQueryingVTIWithExplicitSchema() throws Exception {
        ResultSet rs = spliceClassWatcher.executeQuery("select * From new com.splicemachine.derby.vti.SpliceAllRolesVTI() x(y varchar(12))");
        int count = 0;
        while (rs.next()) {
            count++;
        }
        Assert.assertTrue(count > 0);
    }

}
