/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.execute.operations.joins;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.util.List;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

/**
 * Tests various permutations of join strategy and inclusion of RTRIM function
 * in predicate.
 */
public class JoinWithTrimIT {

    // These tests were derived from some Wells Fargo POC defects,
    // for example DB-4921, DB-4922, DB-4883.

    private static final String SCHEMA = JoinWithTrimIT.class.getSimpleName().toUpperCase();

    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);
    private static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    private static final String NESTED_LOOP = "--SPLICE-PROPERTIES joinStrategy=NESTEDLOOP";
    private static final String BROADCAST = "--SPLICE-PROPERTIES joinStrategy=BROADCAST";
    private static final String MERGE_SORT = "--SPLICE-PROPERTIES joinStrategy=SORTMERGE";

    private static final String RTRIM = "RTRIM";
    private static final String TRIM = "TRIM";

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
        .around(spliceSchemaWatcher);

    @Rule
    public final SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);


    @BeforeClass
    public static void createSharedTables() throws Exception {

        Connection conn = spliceClassWatcher.getOrCreateConnection();

        // There is a method to the madness with the following column widths.
        // They correspond to what Wells Fargo has in their production schema.
        // Having the id be varchar(6) in one table and varchar(16) in the other
        // is also good in that exposes potential edge cases to test.

        // t1 - pad/nopad/nopad/pad
        // padded to 6 chars within varchar(6)
        TableCreator tableCreator = new TableCreator(conn)
            .withCreate("CREATE TABLE %s (id varchar(6), info varchar(32))")
            .withInsert("insert into %s values (?,?)")
            .withRows(rows(
                row("1     ", "small info 1"),
                row("2",      "small info 2"),
                row("3",      "small info e"),
                row("4     ", "small info 4")));
        tableCreator.withTableName("t1").create();

        // t2 -  pad/nopad/pad/nopad
        // padded to 12 chars (not 6) within varchar(16) (not 6, not 12)
        TableCreator tableCreator2 = new TableCreator(conn)
            .withCreate("CREATE TABLE %s (id varchar(16), info varchar(32))")
            .withInsert("insert into %s values (?,?)")
            .withRows(rows(
                row("1           ", "info 1"),
                row("2",            "info 2"),
                row("3           ", "info 3"),
                row("4",            "info 4")));
        tableCreator2.withTableName("t2").create();
    }

    String ROWS_MSG = "Incorrect number of rows returned";

    String SQL_NO_TRIM =
        "select t1.id, t2.id from --SPLICE-PROPERTIES joinOrder=fixed\n" +
        "t1 inner join t2 %s\n" +
        "on t1.id = t2.id order by t1.id";

    String SQL_TRIM_LEFT_OP =
        "select t1.id, t2.id from --SPLICE-PROPERTIES joinOrder=fixed\n" +
        "t1 inner join t2 %s\n" +
        "on %s(t1.id) = t2.id order by t1.id";

    String SQL_TRIM_RIGHT_OP =
        "select t1.id, t2.id from --SPLICE-PROPERTIES joinOrder=fixed\n" +
        "t1 inner join t2 %s\n" +
        "on t1.id = %s(t2.id) order by t1.id";

    String SQL_TRIM_BOTH =
        "select t1.id, t2.id from --SPLICE-PROPERTIES joinOrder=fixed\n" +
        "t1 inner join t2 %s\n" +
        "on %s(t1.id) = %s(t2.id) order by t1.id";

    String SQL_TRIM_BOTH_REVERSE_TABLE_ORDER =
        "select t1.id, t2.id from --SPLICE-PROPERTIES joinOrder=fixed\n" +
        "t2 inner join t1 %s\n" +
        "on %s(t1.id) = %s(t2.id) order by t1.id";

    @Test
    public void testNoTrimNoHint() throws Exception {
        // Needed because occasionally an explicit join hint will cause infeasible plan error,
        // but if we remove hint, optimizer picks that join strategy anyway.
        assertNoTrimResult(methodWatcher.queryListMulti(String.format(SQL_NO_TRIM, ""), 2));
    }

    @Test
    public void testNoTrimNestedLoop() throws Exception {
        assertNoTrimResult(methodWatcher.queryListMulti(String.format(SQL_NO_TRIM, NESTED_LOOP), 2));
    }

    @Test
    public void testNoTrimBroadcast() throws Exception {
        assertNoTrimResult(methodWatcher.queryListMulti(String.format(SQL_NO_TRIM, BROADCAST), 2));
    }

    @Test
    public void testNoTrimMergeSort() throws Exception {
        assertNoTrimResult(methodWatcher.queryListMulti(String.format(SQL_NO_TRIM, MERGE_SORT), 2));
    }

    @Test
    public void testLeftOpTrimNoHint() throws Exception {
        // Needed because occasionally an explicit join hint will cause infeasible plan error,
        // but if we remove hint, optimizer picks that join strategy anyway.
        assertLeftOpTrimResult(methodWatcher.queryListMulti(String.format(SQL_TRIM_LEFT_OP, "", RTRIM), 2));
        assertLeftOpTrimResult(methodWatcher.queryListMulti(String.format(SQL_TRIM_LEFT_OP, "", TRIM), 2));
    }

    @Test
    public void testLeftOpTrimNestedLoop() throws Exception {
        assertLeftOpTrimResult(methodWatcher.queryListMulti(String.format(SQL_TRIM_LEFT_OP, NESTED_LOOP, RTRIM), 2));
        assertLeftOpTrimResult(methodWatcher.queryListMulti(String.format(SQL_TRIM_LEFT_OP, NESTED_LOOP, TRIM), 2));
    }

    @Test
    public void testLeftOpTrimBroadcast() throws Exception {
        assertLeftOpTrimResult(methodWatcher.queryListMulti(String.format(SQL_TRIM_LEFT_OP, BROADCAST, RTRIM), 2));
        assertLeftOpTrimResult(methodWatcher.queryListMulti(String.format(SQL_TRIM_LEFT_OP, BROADCAST, TRIM), 2));
    }

    @Test
    public void testLeftOpTrimMergeSort() throws Exception {
        assertLeftOpTrimResult(methodWatcher.queryListMulti(String.format(SQL_TRIM_LEFT_OP, MERGE_SORT, RTRIM), 2));
        assertLeftOpTrimResult(methodWatcher.queryListMulti(String.format(SQL_TRIM_LEFT_OP, MERGE_SORT, TRIM), 2));
    }

    @Test
    public void testRightOpTrimNoHint() throws Exception {
        // Needed because occasionally an explicit join hint will cause infeasible plan error,
        // but if we remove hint, optimizer picks that join strategy anyway.
        assertRightOpTrimResult(methodWatcher.queryListMulti(String.format(SQL_TRIM_RIGHT_OP, "", RTRIM), 2));
        assertRightOpTrimResult(methodWatcher.queryListMulti(String.format(SQL_TRIM_RIGHT_OP, "", TRIM), 2));
    }

    @Test
    public void testRightOpTrimNestedLoop() throws Exception {
        assertRightOpTrimResult(methodWatcher.queryListMulti(String.format(SQL_TRIM_RIGHT_OP, NESTED_LOOP, RTRIM), 2));
        assertRightOpTrimResult(methodWatcher.queryListMulti(String.format(SQL_TRIM_RIGHT_OP, NESTED_LOOP, TRIM), 2));
    }

    @Test
    public void testRightOpTrimBroadcast() throws Exception {
        assertRightOpTrimResult(methodWatcher.queryListMulti(String.format(SQL_TRIM_RIGHT_OP, BROADCAST, RTRIM), 2));
    }

    @Test
    public void testRightOpTrimSortMerge() throws Exception {
        assertRightOpTrimResult(methodWatcher.queryListMulti(String.format(SQL_TRIM_RIGHT_OP, MERGE_SORT, RTRIM), 2));
    }

    @Test
    public void testBothTrimNoHint() throws Exception {
        // Needed because occasionally an explicit join hint will cause infeasible plan error,
        // but if we remove hint, optimizer picks that join strategy anyway.
        assertBothTrimResult(methodWatcher.queryListMulti(String.format(SQL_TRIM_BOTH, "", RTRIM, RTRIM), 2));
        assertBothTrimResult(methodWatcher.queryListMulti(String.format(SQL_TRIM_BOTH, "", TRIM, RTRIM), 2));
        assertBothTrimResult(methodWatcher.queryListMulti(String.format(SQL_TRIM_BOTH, "", RTRIM, TRIM), 2));
        assertBothTrimResult(methodWatcher.queryListMulti(String.format(SQL_TRIM_BOTH, "", TRIM, TRIM), 2));
    }

    @Test
    public void testBothTrimNestedLoop() throws Exception {
        assertBothTrimResult(methodWatcher.queryListMulti(String.format(SQL_TRIM_BOTH, NESTED_LOOP, RTRIM, RTRIM), 2));
        assertBothTrimResult(methodWatcher.queryListMulti(String.format(SQL_TRIM_BOTH, NESTED_LOOP, RTRIM, TRIM), 2));
        assertBothTrimResult(methodWatcher.queryListMulti(String.format(SQL_TRIM_BOTH, NESTED_LOOP, TRIM, RTRIM), 2));
        assertBothTrimResult(methodWatcher.queryListMulti(String.format(SQL_TRIM_BOTH, NESTED_LOOP, TRIM, TRIM), 2));
    }

    @Test
    public void testBothTrimBroadcast() throws Exception {
        assertBothTrimResult(methodWatcher.queryListMulti(String.format(SQL_TRIM_BOTH, BROADCAST, RTRIM, RTRIM), 2));
    }

    @Test
    public void testBothTrimMergeSort() throws Exception {
        assertBothTrimResult(methodWatcher.queryListMulti(String.format(SQL_TRIM_BOTH, MERGE_SORT, RTRIM, RTRIM), 2));
    }

    @Test
    public void testBothTrimNoHintReverse() throws Exception {
        assertBothTrimResult(methodWatcher.queryListMulti(String.format(SQL_TRIM_BOTH_REVERSE_TABLE_ORDER, "", RTRIM, RTRIM), 2));
        assertBothTrimResult(methodWatcher.queryListMulti(String.format(SQL_TRIM_BOTH_REVERSE_TABLE_ORDER, "", RTRIM, TRIM), 2));
        assertBothTrimResult(methodWatcher.queryListMulti(String.format(SQL_TRIM_BOTH_REVERSE_TABLE_ORDER, "", TRIM, RTRIM), 2));
        assertBothTrimResult(methodWatcher.queryListMulti(String.format(SQL_TRIM_BOTH_REVERSE_TABLE_ORDER, "", TRIM, TRIM), 2));
    }

    @Test
    public void testBothTrimNestedLoopReverse() throws Exception {
        assertBothTrimResult(methodWatcher.queryListMulti(String.format(SQL_TRIM_BOTH_REVERSE_TABLE_ORDER, NESTED_LOOP, RTRIM, RTRIM), 2));
        assertBothTrimResult(methodWatcher.queryListMulti(String.format(SQL_TRIM_BOTH_REVERSE_TABLE_ORDER, NESTED_LOOP, RTRIM, TRIM), 2));
        assertBothTrimResult(methodWatcher.queryListMulti(String.format(SQL_TRIM_BOTH_REVERSE_TABLE_ORDER, NESTED_LOOP, TRIM, RTRIM), 2));
        assertBothTrimResult(methodWatcher.queryListMulti(String.format(SQL_TRIM_BOTH_REVERSE_TABLE_ORDER, NESTED_LOOP, TRIM, TRIM), 2));
    }

    @Test
    public void testBothTrimBroadcastReverse() throws Exception {
        assertBothTrimResult(methodWatcher.queryListMulti(String.format(SQL_TRIM_BOTH_REVERSE_TABLE_ORDER, BROADCAST, RTRIM, RTRIM), 2));
    }

    @Test
    public void testBothTrimMergeSortReverse() throws Exception {
        assertBothTrimResult(methodWatcher.queryListMulti(String.format(SQL_TRIM_BOTH_REVERSE_TABLE_ORDER, MERGE_SORT, RTRIM, RTRIM), 2));
    }

    private void assertNoTrimResult(List<Object[]> result) throws Exception {
        assertEquals(ROWS_MSG, 1, result.size());
        assertRow(1, result, "2", "2");
    }

    private void assertLeftOpTrimResult(List<Object[]> result) throws Exception {
        assertEquals(ROWS_MSG, 2, result.size());
        assertRow(1, result, "2",      "2");
        assertRow(2, result, "4     ", "4");
    }

    private void assertRightOpTrimResult(List<Object[]> result) throws Exception {
        assertEquals(ROWS_MSG, 2, result.size());
        assertRow(1, result, "2", "2");
        assertRow(2, result, "3", "3           ");
    }

    private void assertBothTrimResult(List<Object[]> result) throws Exception {
        assertEquals(ROWS_MSG, 4, result.size());
        assertRow(1, result, "1     ", "1           ");
        assertRow(2, result, "2",      "2");
        assertRow(3, result, "3",      "3           ");
        assertRow(4, result, "4     ", "4");
    }

    private void assertRow(int rowNum, List<Object[]> result, String v1, String v2) {
        assertEquals(String.format("Wrong result row %d, col 1", rowNum), v1, result.get(rowNum - 1)[0].toString());
        assertEquals(String.format("Wrong result row %d, col 2", rowNum), v2, result.get(rowNum - 1)[1].toString());
    }
}

