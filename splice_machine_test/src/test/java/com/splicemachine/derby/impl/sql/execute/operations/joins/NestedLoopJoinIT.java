package com.splicemachine.derby.impl.sql.execute.operations.joins;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.SerialTest;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

/**
 * Integration tests for NestedLoopJoinOperation.
 */
@Category(SerialTest.class) //in Serial category because of the NestedLoopIteratorClosesStatements test
public class NestedLoopJoinIT {

    private static final String SCHEMA_NAME = NestedLoopJoinIT.class.getSimpleName().toUpperCase();
    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA_NAME);
    @ClassRule
    public static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA_NAME);

    @BeforeClass
    public static void createTables() throws Exception {
        // B
        classWatcher.executeUpdate("create table B(c1 int, c2 int, c3 char(1), c4 int, c5 int, c6 int)");
        // B2
        classWatcher.executeUpdate("create table B2 (c1 int, c2 int, c3 char(1), c4 int, c5 int,c6 int)");
        classWatcher.executeUpdate("insert into B2 (c5,c1,c3,c4,c6) values (3,4, 'F',43,23)");
        // B3
        classWatcher.executeUpdate("create table B3(c8 int, c9 int, c5 int, c6 int)");
        classWatcher.executeUpdate("insert into B3 (c5,c8,c9,c6) values (2,3,19,28)");
        // B4
        classWatcher.executeUpdate("create table B4(c7 int, c4 int, c6 int)");
        classWatcher.executeUpdate("insert into B4 (c7,c4,c6) values (4, 42, 31)");
        // VIEW
        classWatcher.executeUpdate("create view bvw (c5,c1,c2,c3,c4) as select c5,c1,c2,c3,c4 from B2 union select c5,c1,c2,c3,c4 from B");
    }

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA_NAME);

    /* Regression test for DB-1027 */
    @Test
    public void testCanJoinTwoTablesWithViewAndQualifiedSinkOperation() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select B3.* from B3 join BVW on (B3.c8 = BVW.c5) join B4 on (BVW.c1 = B4.c7) where B4.c4 = 42");
        assertEquals("" +
                "C8 |C9 |C5 |C6 |\n" +
                "----------------\n" +
                " 3 |19 | 2 |28 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    /* Regression test for DB-1129 */
    @Test
    public void testNestedLoopIteratorCloseStatements() throws Exception {
        int statementCountBefore = getSysStatementCount();
        methodWatcher.executeQuery("select * from B2 a, B2 b --SPLICE-PROPERTIES joinStrategy=NESTEDLOOP");
        int statementCountAfter = getSysStatementCount();
        assertEquals(statementCountBefore, statementCountAfter);
    }

    private int getSysStatementCount() throws Exception {
        Statement s = methodWatcher.getStatement();
        ResultSet rs = s.executeQuery("call SYSCS_UTIL.SYSCS_GET_STATEMENT_SUMMARY()");
        return SpliceUnitTest.resultSetSize(rs);
    }

}
