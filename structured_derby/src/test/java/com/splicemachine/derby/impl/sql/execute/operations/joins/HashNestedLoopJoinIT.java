package com.splicemachine.derby.impl.sql.execute.operations.joins;

import com.splicemachine.derby.test.framework.DefaultedSpliceWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.test_dao.*;
import com.splicemachine.test_tools.IntegerRows;
import com.splicemachine.test_tools.TableCreator;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.isEmpty;
import static com.splicemachine.derby.metrics.OperationMetric.*;
import static com.splicemachine.homeless.TestUtils.FormattedResult.ResultFactory;
import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class HashNestedLoopJoinIT {

    private static final String CLASS_NAME = HashNestedLoopJoinIT.class.getSimpleName().toUpperCase();

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @Rule
    public DefaultedSpliceWatcher watcher = new DefaultedSpliceWatcher(CLASS_NAME);

    /* DB-1715: rows were dropped from the expected result set */
    @Test
    public void expectedRowsPresent() throws Exception {

        Connection conn = watcher.getOrCreateConnection();

        TableCreator tableCreator = new TableCreator(conn)
                .withCreate("create table %s (c1 int, c2 int, primary key(c1))")
                .withInsert("insert into %s values(?,?)")
                .withRows(rows(row(1, 10), row(2, 20), row(3, 30), row(4, 40)));

        tableCreator.withTableName("t1").create();
        tableCreator.withTableName("t2").create();

        String JOIN_SQL = "select * from --SPLICE-PROPERTIES joinOrder=fixed\n" +
                "t1 inner join t2 --SPLICE-PROPERTIES joinStrategy=HASH\n" +
                "on t1.c1 = t2.c1";

        ResultSet rs = conn.createStatement().executeQuery(JOIN_SQL);

        String EXPECTED = "" +
                "C1 |C2 |C1 |C2 |\n" +
                "----------------\n" +
                " 1 |10 | 1 |10 |\n" +
                " 2 |20 | 2 |20 |\n" +
                " 3 |30 | 3 |30 |\n" +
                " 4 |40 | 4 |40 |";

        assertEquals(EXPECTED, ResultFactory.toString(rs));
    }

    /* DB-1715: rows were dropped from the expected result set */
    @Test
    public void expectedRowsPresentBigTableLotsOfPredicates() throws Exception {

        Connection conn = watcher.getOrCreateConnection();

        TableCreator tableCreator = new TableCreator(conn)
                .withCreate("create table %s (c1 int, c2 int, c3 int, c4 int, c5 int, primary key(c1))")
                .withInsert("insert into %s values(?,?,?,?,?)")
                .withRows(new IntegerRows(1000, 5));

        tableCreator.withTableName("A").create();
        tableCreator.withTableName("B").create();

        String JOIN_SQL = "select * from --SPLICE-PROPERTIES joinOrder=fixed\n" +
                "A inner join B --SPLICE-PROPERTIES joinStrategy=HASH\n" +
                "on A.c1 = B.c1" +
                " where A.c2 > 1 AND" +
                " (" +
                " B.c5=3379 or " +
                " B.c4=98 or" +
                " B.c2=99999999 or " +
                " (B.c2=106 and B.c3=107 and B.c4=108 and B.c5=109)" +
                ")";

        ResultSet rs = conn.createStatement().executeQuery(JOIN_SQL);

        String EXPECTED = "" +
                "C1  | C2  | C3  | C4  | C5  | C1  | C2  | C3  | C4  | C5  |\n" +
                "------------------------------------------------------------\n" +
                " 105 | 106 | 107 | 108 | 109 | 105 | 106 | 107 | 108 | 109 |\n" +
                "3375 |3376 |3377 |3378 |3379 |3375 |3376 |3377 |3378 |3379 |\n" +
                " 95  | 96  | 97  | 98  | 99  | 95  | 96  | 97  | 98  | 99  |";

        assertEquals(EXPECTED, ResultFactory.toString(rs));
    }

    /* DB-1684: predicates were not applied to the right table scan */
    @Test
    public void rightSidePredicatesApplied() throws Exception {
        Connection conn = watcher.getOrCreateConnection();

        new TableCreator(conn)
                .withCreate("create table ta (c1 int, alpha int, primary key(c1))")
                .withInsert("insert into ta values(?,?)")
                .withRows(rows(row(1, 10), row(2, 20), row(3, 30), row(4, 40), row(5, 50), row(6, 60), row(7, 70)))
                .create();

        new TableCreator(conn)
                .withCreate("create table tb (c1 int, beta int, primary key(c1))")
                .withInsert("insert into tb values(?,?)")
                .withRows(rows(row(1, 10), row(2, 20), row(3, 30), row(4, 40), row(5, 50), row(6, 60), row(7, 70)))
                .create();

        String JOIN_SQL = "select * from --SPLICE-PROPERTIES joinOrder=fixed\n" +
                "ta inner join tb --SPLICE-PROPERTIES joinStrategy=HASH\n" +
                "on ta.c1 = tb.c1 where beta=30";

        ResultSet rs = conn.createStatement().executeQuery(JOIN_SQL);

        String EXPECTED = "" +
                "C1 | ALPHA |C1 |BETA |\n" +
                "----------------------\n" +
                " 3 |  30   | 3 | 30  |";

        assertEquals(EXPECTED, ResultFactory.toString(rs));
    }

    /* DB-1684: predicates were not applied to the right table scan */
    @Test
    public void rightSidePredicatesAppliedComplex() throws Exception {
        Connection conn = watcher.getOrCreateConnection();

        new TableCreator(conn)
                .withCreate("create table schemas (schemaid char(36), schemaname varchar(128))")
                .withIndex("create unique index schemas_i1 on schemas (schemaname)")
                .withIndex("create unique index schemas_i2 on schemas (schemaid)")
                .withInsert("insert into schemas values(?,?)")
                .withRows(rows(
                                row("s1", "schema_01"),
                                row("s2", "schema_02"),
                                row("s3", "schema_03"))
                )
                .create();

        new TableCreator(conn)
                .withCreate("create table tables (tableid char(36), tablename varchar(128),schemaid char(36), version varchar(128))")
                .withIndex("create unique index tables_i1 on tables (tablename, schemaid)")
                .withIndex("create unique index tables_i2 on tables (tableid)")
                .withInsert("insert into tables values(?,?,?, 'version_x')")
                .withRows(
                        rows(
                                row("100", "table_01", "s1"), row("101", "table_02", "s1"),
                                row("500", "table_06", "s2"), row("501", "table_07", "s2"),
                                row("900", "table_11", "s3"), row("901", "table_12", "s3")
                        )
                )
                .create();

        String JOIN_SQL = "select schemaname,tablename,version from --SPLICE-PROPERTIES joinOrder=fixed\n" +
                "schemas s join tables t --SPLICE-PROPERTIES joinStrategy=HASH\n" +
                "on s.schemaid = t.schemaid where t.tablename='table_07' and s.schemaname='schema_02'";

        ResultSet rs = conn.createStatement().executeQuery(JOIN_SQL);

        String EXPECTED = "" +
                "SCHEMANAME | TABLENAME | VERSION  |\n" +
                "-----------------------------------\n" +
                " schema_02 | table_07  |version_x |";

        assertEquals(EXPECTED, ResultFactory.toString(rs));
    }

    @Test
    public void joinWithStatistics() throws Exception {

        Connection conn = watcher.getOrCreateConnection();
        StatementHistoryDAO statementHistoryDAO = new StatementHistoryDAO(conn);
        TaskHistoryDAO taskHistoryDAO = new TaskHistoryDAO(conn);

        final String RANDOM_LONG = RandomStringUtils.randomNumeric(9);

        watcher.prepareCall("CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)").execute();
        watcher.prepareCall("CALL SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(1)").execute();

        // primary keys are 0,1,2...99
        new TableCreator(conn).withCreate("create table tableA (c1 int, primary key(c1))")
                .withInsert("insert into tableA values(?)")
                .withRows(new IntegerRows(100, 1)).create();

        // primary keys are 75,76,77...300
        new TableCreator(conn).withCreate("create table tableB (c1 int, primary key(c1))")
                .withInsert("insert into tableB values(?)")
                .withRows(new IntegerRows(300, 1, 75)).create();

        String JOIN_SQL = "select count(*) as \"Row Count\" from --SPLICE-PROPERTIES joinOrder=fixed\n" +
                "tableA a inner join tableB b --SPLICE-PROPERTIES joinStrategy=HASH\n" +
                "on a.c1 = b.c1 and " + RANDOM_LONG + "=" + RANDOM_LONG;

        ResultSet rs = conn.createStatement().executeQuery(JOIN_SQL);

        String EXPECTED = "" +
                "Row Count |\n" +
                "------------\n" +
                "    25     |";

        assertEquals(EXPECTED, ResultFactory.toString(rs));

        // Assert that there are task history rows with stats: REMOTE_SCAN_ROWS=26, LOCAL_SCAN_ROWS=100, OUTPUT_ROWS=26
        // Stats are inserted into SYS.SYSTASKHISTORY asynchronously.
        StatementHistory statement = statementHistoryDAO.findStatement(RANDOM_LONG, 20, TimeUnit.SECONDS);
        List<TaskHistory> taskHistoryList = taskHistoryDAO.getByStatementId(statement.getStatementId(), 20, TimeUnit.SECONDS);
        assertFalse(isEmpty(filter(taskHistoryList, new TaskHistoryOperationMetricPredicate(REMOTE_SCAN_ROWS, 25L))));
        assertFalse(isEmpty(filter(taskHistoryList, new TaskHistoryOperationMetricPredicate(LOCAL_SCAN_ROWS, 100L))));
        assertFalse(isEmpty(filter(taskHistoryList, new TaskHistoryOperationMetricPredicate(OUTPUT_ROWS, 25L))));
    }

}

