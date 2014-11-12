package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Sets;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.ResultSet;
import java.util.List;

import static com.splicemachine.derby.test.framework.SpliceUnitTest.resultSetSize;
import static org.junit.Assert.assertEquals;

public class AnyOperationIT {

    private static final String CLASS_NAME = AnyOperationIT.class.getSimpleName();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    private static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(schemaWatcher)
            .around(TestUtils.createFileDataWatcher(spliceClassWatcher, "test_data/AnyOperationIT.sql", CLASS_NAME));

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @Test
    public void testSelectValid() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("SELECT CITY FROM STAFF WHERE EMPNUM = 'E8'");
        assertEquals("Expecting 1 rows from STAFF table.", 1, resultSetSize(rs));
    }

    @Test
    public void testSelectInvalid() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("SELECT CITY FROM STAFF WHERE EMPNUM = 'E9'");
        assertEquals("Expecting 0 rows from STAFF table for invalid criteria.", 0, resultSetSize(rs));
    }

    @Test
    public void testAllQueryValidVal() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("SELECT CITY FROM PROJ WHERE CITY = ALL (SELECT CITY FROM STAFF WHERE EMPNUM = 'E8')");
        assertEquals("Expecting 1 row from PROJ table.", 1, resultSetSize(rs));
    }

    @Test
    public void testAllQueryInvalidValAll() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("SELECT CITY FROM PROJ WHERE CITY = ALL (SELECT CITY FROM STAFF WHERE EMPNUM = 'E9')");
        assertEquals("Expecting all rows from PROJ table.", 7, resultSetSize(rs));
    }

    @Test
    public void testAllQueryInvalidVal() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("SELECT CITY FROM PROJ WHERE CITY = (SELECT CITY FROM STAFF WHERE EMPNUM = 'E9')");
        assertEquals("Expecting no rows from PROJ table.", 0, resultSetSize(rs));
    }

    @Test
    public void anyOperatorInSelectClause() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select EMPNAME, empnum = any (select empnum from staff where grade > 12) from staff order by empname");

        String EXPECTED = "" +
                "EMPNAME |  2   |\n" +
                "-----------------\n" +
                "  Alice  |false |\n" +
                "  Betty  |false |\n" +
                " Carmen  |true  |\n" +
                "   Don   |false |\n" +
                "   Ed    |true  |\n" +
                "  Fred   |true  |\n" +
                "  Jane   |true  |\n" +
                "   Joe   |true  |";

        assertEquals(EXPECTED, TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void anyOperatorInSelectClause_allTrue() throws Exception {
        List<Long> resultBooleanList = methodWatcher.queryList("select empnum = any (select empnum from staff) from staff");
        assertEquals(8, resultBooleanList.size());
        assertEquals(Sets.newHashSet(true), Sets.newHashSet(resultBooleanList));
    }

    @Test
    public void anyOperatorInSelectClause_allFalse() throws Exception {
        List<Long> resultBooleanList = methodWatcher.queryList("select empnum = any (select EMPNAME from staff) from staff");
        assertEquals(8, resultBooleanList.size());
        assertEquals(Sets.newHashSet(false), Sets.newHashSet(resultBooleanList));
    }

}
