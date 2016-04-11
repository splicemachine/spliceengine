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

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
        .around(spliceSchemaWatcher);

    @Rule
    public final SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);


    @BeforeClass
    public static void createSharedTables() throws Exception {

        Connection conn = spliceClassWatcher.getOrCreateConnection();

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
        // padded to 12 chars within varchar(16)
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
        "t1 inner join t2 --SPLICE-PROPERTIES joinStrategy=%s\n" +
        "on t1.id = t2.id";

    String SQL_TRIM_LEFT_OP =
        "select t1.id, t2.id from --SPLICE-PROPERTIES joinOrder=fixed\n" +
        "t1 inner join t2 --SPLICE-PROPERTIES joinStrategy=%s\n" +
        "on RTRIM(t1.id) = t2.id";

    String SQL_TRIM_RIGHT_OP =
        "select t1.id, t2.id from --SPLICE-PROPERTIES joinOrder=fixed\n" +
        "t1 inner join t2 --SPLICE-PROPERTIES joinStrategy=%s\n" +
        "on t1.id = RTRIM(t2.id)";

    String SQL_TRIM_BOTH =
        "select t1.id, t2.id from --SPLICE-PROPERTIES joinOrder=fixed\n" +
        "t1 inner join t2 --SPLICE-PROPERTIES joinStrategy=%s\n" +
        "on RTRIM(t1.id) = RTRIM(t2.id)";

    // These 3 tests should return the same results, because the only difference
    // between them is the join strategy:
    //
    // testNoTrimNestedLoop, testNoTrimBroadcast, testNoTrimMergeSort

    @Test
    public void testNoTrimNestedLoop() throws Exception {
        List<Object[]> result = methodWatcher.queryListMulti(String.format(SQL_NO_TRIM, "NESTEDLOOP"), 2);
        assertEquals(ROWS_MSG, 1, result.size());
        assertRow(1, result, "2", "2");
    }

    @Test
    public void testNoTrimBroadcast() throws Exception {
        List<Object[]> result = methodWatcher.queryListMulti(String.format(SQL_NO_TRIM, "BROADCAST"), 2);
        assertEquals(ROWS_MSG, 1, result.size());
        assertRow(1, result, "2", "2");
    }

    @Test
    public void testNoTrimMergeSort() throws Exception {
        List<Object[]> result = methodWatcher.queryListMulti(String.format(SQL_NO_TRIM, "SORTMERGE"), 2);
        assertEquals(ROWS_MSG, 1, result.size());
        assertRow(1, result, "2", "2");
    }


    // These 3 tests should return the same results, because the only difference
    // between them is the join strategy:
    //
    // testLeftOpTrimNestedLoop, testLeftOpTrimBroadcast, testLeftOpMergeSort

    @Test
    public void testLeftOpTrimNestedLoop() throws Exception {
        List<Object[]> result = methodWatcher.queryListMulti(String.format(SQL_TRIM_LEFT_OP, "NESTEDLOOP"), 2);
        assertEquals(ROWS_MSG, 2, result.size());
        assertRow(1, result, "2",      "2");
        assertRow(2, result, "4     ", "4");
    }

    @Ignore("Will pass when wells fargo issue DB-4924 is fixed")
    public void testLeftOpTrimBroadcast() throws Exception {
        List<Object[]> result = methodWatcher.queryListMulti(String.format(SQL_TRIM_LEFT_OP, "BROADCAST"), 2);
        assertEquals(ROWS_MSG, 2, result.size());
        assertRow(1, result, "2"     , "2");
        assertRow(2, result, "4     ", "4");
    }

    @Ignore("Will pass when wells fargo issue DB-4924 is fixed")
    public void testLeftOpMergeSort() throws Exception {
        List<Object[]> result = methodWatcher.queryListMulti(String.format(SQL_TRIM_LEFT_OP, "SORTMERGE"), 2);
        assertEquals(ROWS_MSG, 2, result.size());
        assertRow(1, result, "2",      "2");
        assertRow(2, result, "4     ", "4");
    }
    
    @Test
    public void testRightOpTrimNestedLoop() throws Exception {
        List<Object[]> result = methodWatcher.queryListMulti(String.format(SQL_TRIM_RIGHT_OP, "NESTEDLOOP"), 2);
        assertEquals(ROWS_MSG, 2, result.size());
        assertRow(1, result, "2", "2");
        assertRow(2, result, "3", "3           ");
    }

    /* Returns "No valid execution plan", but keep around for reference
    public void testRightOpTrimBroadcast() throws Exception {
        List<Object[]> result = methodWatcher.queryListMulti(String.format(SQL_TRIM_RIGHT_OP, "BROADCAST"), 2);
        assertEquals(ROWS_MSG, 2, result.size());
        assertRow(1, result, "2", "2");
        assertRow(2, result, "3", "3           ");
    }
    */

    /* Returns "No valid execution plan", but keep around for reference
    public void testRightOpTrimSortMerge() throws Exception {
        List<Object[]> result = methodWatcher.queryListMulti(String.format(SQL_TRIM_RIGHT_OP, "SORTMERGE"), 2);
        assertEquals(ROWS_MSG, 2, result.size());
        assertRow(1, result, "2", "2");
        assertRow(2, result, "3", "3           ");
    }
    */

    @Test
    public void testBothTrimNestedLoop() throws Exception {
        List<Object[]> result = methodWatcher.queryListMulti(String.format(SQL_TRIM_BOTH, "NESTEDLOOP"), 2);
        assertEquals(ROWS_MSG, 4, result.size());
        assertRow(1, result, "1     ", "1           ");
        assertRow(2, result, "2",      "2");
        assertRow(3, result, "3",      "3           ");
        assertRow(4, result, "4     ", "4");
    }

    /* Returns "No valid execution plan", but keep around for reference
    public void testBothTrimBroadcast() throws Exception {
        List<Object[]> result = methodWatcher.queryListMulti(String.format(SQL_TRIM_BOTH, "BROADCAST"), 2);
        assertEquals(ROWS_MSG, 4, result.size());
        assertRow(1, result, "1     ", "1           ");
        assertRow(2, result, "2",      "2");
        assertRow(3, result, "3",      "3           ");
        assertRow(4, result, "4     ", "4");
    }
    */

    /* Returns "No valid execution plan", but keep around for reference
    public void testBothTrimMergeSort() throws Exception {
        List<Object[]> result = methodWatcher.queryListMulti(String.format(SQL_TRIM_BOTH, "SORTMERGE"), 2);
        assertEquals(ROWS_MSG, 4, result.size());
        assertRow(1, result, "1     ", "1           ");
        assertRow(2, result, "2",      "2");
        assertRow(3, result, "3",      "3           ");
        assertRow(4, result, "4     ", "4");
    }
    */

    private void assertRow(int rowNum, List<Object[]> result, String v1, String v2) {
        assertEquals(String.format("Wrong result row %d, col 1", rowNum), v1, result.get(rowNum - 1)[0].toString());
        assertEquals(String.format("Wrong result row %d, col 2", rowNum), v2, result.get(rowNum - 1)[1].toString());
    }
}

