package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.DefaultedSpliceWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.apache.derby.client.am.SqlException;
import org.apache.log4j.Logger;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.ResultSet;
import java.util.List;

/**
 * User: pjt
 * Date: 6/10/13
 */
public class BroadcastJoinOperationTest extends SpliceUnitTest {

    private static Logger LOG = Logger.getLogger(BroadcastJoinOperationTest.class);

    public static final String CLASS_NAME = BroadcastJoinOperationTest.class.getSimpleName().toUpperCase();

    protected static DefaultedSpliceWatcher spliceClassWatcher = new DefaultedSpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(TestUtils.createFileDataWatcher(spliceClassWatcher, "test_data/employee.sql", CLASS_NAME));

    @Rule
    public SpliceWatcher methodWatcher = new DefaultedSpliceWatcher(CLASS_NAME);

    @Test
    public void testSimpleInnerEquijoin() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select staff.empnum from staff inner join works --DERBY-PROPERTIES joinStrategy=broadcast \n" +
                "on staff.empnum = works.empnum");
        List results = TestUtils.resultSetToMaps(rs);
        Assert.assertEquals(12, results.size());
    }

    @Test
    public void testSimpleLeftEquijoin() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select staff.empnum from staff left join works --DERBY-PROPERTIES joinStrategy=broadcast \n" +
                "on staff.empnum = works.empnum");
        List results = TestUtils.resultSetToMaps(rs);
        Assert.assertEquals(13, results.size());
    }

    @Test
    public void testFailsOnNonEquijoin() throws Exception {
        Exception caught = null;
        try {
            methodWatcher.executeQuery("select staff.empnum from staff inner join works --DERBY-PROPERTIES joinStrategy=broadcast \n" +
                    "on staff.empnum != works.empnum");
        } catch (Exception e) {
            caught = e;
        }

        if (caught != null) {
            Assert.assertSame(caught.getCause().getClass(), SqlException.class);
            Assert.assertThat("SqlException message different than expected", caught.getCause().getMessage(),
                    new BaseMatcher<String>() {
                        public boolean matches(Object msg) {
                            return ((String) msg).contains("No valid execution plan was found");
                        }

                        public void describeTo(Description d) {
                        }
                    });
        } else {
            Assert.fail("This query should have raised an error.");
        }
    }

    @Test
    @Ignore("Semijoin not yet implemented")
    public void testInWithCorrelatedSubQueryOrSemijoin() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select empnum from staff --DERBY-PROPERTIES joinStrategy=broadcast \n" +
                "where empnum in " +
                "(select works.empnum from works where staff.empnum = works.empnum)");
        List results = TestUtils.resultSetToMaps(rs);
        Assert.assertEquals(4, results.size());
    }

    @Test
    @Ignore("Antijoin not yet implemented")
    public void testNotInWithCorrelatedSubQueryOrAntijoin() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select empnum from staff --DERBY-PROPERTIES joinStrategy=broadcast \n" +
                "where empnum not in " +
                "(select works.empnum from works where staff.empnum = works.empnum)");
        List results = TestUtils.resultSetToMaps(rs);
        Assert.assertEquals(1, results.size());
    }
}
