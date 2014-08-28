package com.splicemachine.derby.impl.sql.execute.operations.joins;

import com.splicemachine.derby.test.framework.DefaultedSpliceWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

public class HashNestedLoopJoinIT {

    public static final String CLASS_NAME = HashNestedLoopJoinIT.class.getSimpleName().toUpperCase();

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @Rule
    public DefaultedSpliceWatcher watcher = new DefaultedSpliceWatcher(CLASS_NAME);

    /* DB-1715: rows were dropped from the expected result set */
    @Test
    public void expectedRowsPresent() throws Exception {

        Connection conn = watcher.getOrCreateConnection();

        TableCreator tableCreator = new TableCreator(watcher.getOrCreateConnection())
                .withCreate("create table %s (c1 int, c2 int, primary key(c1))")
                .withInsert("insert into %s values(?,?)")
                .withRows(rows(row(1, 10), row(2, 20), row(3, 30), row(4, 40)));

        tableCreator.withTableName("t1").create();
        tableCreator.withTableName("t2").create();

        String JOIN_SQL = "select * from --SPLICE-PROPERTIES joinOrder=fixed\n" +
                "t1 inner join t2 --SPLICE-PROPERTIES joinStrategy=HASH\n" +
                "on t1.c1 = t2.c1";

        ResultSet rs = conn.createStatement().executeQuery(JOIN_SQL);

        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert("", rs);

        String EXPECTED = "" +
                "C1 |C2 |C1 |C2 |\n" +
                "----------------\n" +
                " 1 |10 | 1 |10 |\n" +
                " 2 |20 | 2 |20 |\n" +
                " 3 |30 | 3 |30 |\n" +
                " 4 |40 | 4 |40 |\n";

        assertEquals(EXPECTED.trim(), fr.toString().trim());
    }

    /* DB-1684: predicates were not applied to the right table scan */
    @Test
    public void rightSidePredicatesApplied() throws Exception {
        Connection conn = watcher.getOrCreateConnection();

        new TableCreator(watcher.getOrCreateConnection())
                .withCreate("create table ta (c1 int, alpha int, primary key(c1))")
                .withInsert("insert into ta values(?,?)")
                .withRows(rows(row(1, 10), row(2, 20), row(3, 30), row(4, 40)))
                .create();

        new TableCreator(watcher.getOrCreateConnection())
                .withCreate("create table tb (c1 int, beta int, primary key(c1))")
                .withInsert("insert into tb values(?,?)")
                .withRows(rows(row(1, 10), row(2, 20), row(3, 30), row(4, 40)))
                .create();

        String JOIN_SQL = "select * from --SPLICE-PROPERTIES joinOrder=fixed\n" +
                "ta inner join tb --SPLICE-PROPERTIES joinStrategy=HASH\n" +
                "on ta.c1 = tb.c1 where beta=30";

        ResultSet rs = conn.createStatement().executeQuery(JOIN_SQL);

        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert("", rs);

        String EXPECTED = "" +
                "C1 |ALPHA |C1 |BETA |\n" +
                "---------------------\n" +
                " 3 |30    | 3 |30   |\n";

        assertEquals(EXPECTED.trim(), fr.toString().trim());
    }

}