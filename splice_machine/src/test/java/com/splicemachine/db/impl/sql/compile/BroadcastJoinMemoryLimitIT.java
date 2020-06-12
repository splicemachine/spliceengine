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
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.ResultSet;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 * Created by yxia on 5/12/17.
 */
public class BroadcastJoinMemoryLimitIT extends SpliceUnitTest {
    public static final String CLASS_NAME = BroadcastJoinMemoryLimitIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table t2 (a2 int, b2 int, c2 int, d2 varchar(10000))")
                .withInsert("insert into t2 values(?,?,?,?)")
                .withRows(rows(
                        row(1,1,1,"a"),
                        row(2,2,2,"b"),
                        row(3,3,3,"c"),
                        row(4,4,4,"d"),
                        row(5,5,5,"e"),
                        row(6,6,6,"f"),
                        row(7,7,7,"g"),
                        row(8,8,8,"h"),
                        row(9,9,9,"i"),
                        row(10,10,10,"j")))
                .create();

        new TableCreator(conn)
                .withCreate("create table t1 (a1 int, b1 int, c1 int, d1 varchar(10000))")
                .create();

        conn.createStatement().executeQuery(format(
                "CALL SYSCS_UTIL.FAKE_TABLE_STATISTICS('%s','T1', 100000000, 10050, 1)",
                schemaName));

        conn.createStatement().executeQuery(format(
                "CALL SYSCS_UTIL.FAKE_TABLE_STATISTICS('%s','T2', 5000, 10050, 1)",
                schemaName));

        conn.createStatement().executeQuery(format(
                "CALL SYSCS_UTIL.FAKE_COLUMN_STATISTICS('%s','T2', 'A2', 0, 500)",
                schemaName));



        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }

    @Test
    public void testMemoryLimitForConsecutiveBroadcastJoin() throws Exception {
        StringBuilder fromClause = new StringBuilder("from --splice-properties joinOrder=fixed\n" +
                "t1 \n");
        StringBuilder whereClause = new StringBuilder("where\n");

        int numT2 = 100;

        for (int i=1; i<=numT2; i++) {
            fromClause.append(format(", t2 as X%d\n", i));
            if (i>1)
                whereClause.append(format("and d1=X%d.d2\n", i));
            else
                whereClause.append(format("d1=X%d.d2\n", i));
        }
        String sqlText = "explain select a1, X1.a2\n" + fromClause + whereClause;


        ResultSet rs = methodWatcher.executeQuery(sqlText);
        int i=0;
        while(rs.next()) {
            String resultString = rs.getString(1);
            if (resultString.contains("BroadcastJoin"))
                i++;
        }

        Assert.assertTrue("Query should not pick broadcast join for all the joins.", i < 100);

    }

    @Test
    public void testMemoryLimitForConsecutiveBroadcastLeftJoins() throws Exception {
        StringBuilder fromClause = new StringBuilder("from --splice-properties joinOrder=fixed\n" +
                "t1\n");

        int numT2 = 100;

        for (int i=1; i<=numT2; i++) {
            fromClause.append(format("left join t2 as X%d on a1=X%d.a2 \n", i, i));
        }
        String sqlText = "explain select a1, X1.a2\n" + fromClause;

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        int i=0;
        while(rs.next()) {
            String resultString = rs.getString(1);
            if (resultString.contains("BroadcastJoin"))
                i++;
        }
        Assert.assertTrue("Query should not pick broadcast join for all the joins.", i < 100);
    }

    @Test
    public void testMemoryLimitForConsecutiveBroadcastLeftJoinsWithDT() throws Exception {
        StringBuilder fromClause = new StringBuilder("from --splice-properties joinOrder=fixed\n" +
                "(select * from t1) as DT\n");

        int numT2 = 100;

        for (int i=1; i<=numT2; i++) {
            fromClause.append(format("left join (select * from t2) as DT%d on DT.a1=DT%d.a2 \n", i, i));
        }
        String sqlText = "explain select DT.a1, DT1.a2\n" + fromClause;

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        int i=0;
        while(rs.next()) {
            String resultString = rs.getString(1);
            if (resultString.contains("BroadcastJoin"))
                i++;
        }
        Assert.assertTrue("Query should not pick broadcast join for all the joins.", i < 100);
    }
}
