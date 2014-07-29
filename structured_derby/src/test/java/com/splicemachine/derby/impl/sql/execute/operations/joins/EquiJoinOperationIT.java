package com.splicemachine.derby.impl.sql.execute.operations.joins;

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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.util.*;

/**
 * User: pjt
 * Date: 6/10/13
 */
@RunWith(Parameterized.class)
public class EquiJoinOperationIT extends SpliceUnitTest {

    private static Logger LOG = Logger.getLogger(EquiJoinOperationIT.class);

    public static final String CLASS_NAME = EquiJoinOperationIT.class.getSimpleName().toUpperCase();

    protected static DefaultedSpliceWatcher spliceClassWatcher = new DefaultedSpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(TestUtils.createFileDataWatcher(spliceClassWatcher, "test_data/employee.sql", CLASS_NAME))
            .around(TestUtils.createFileDataWatcher(spliceClassWatcher, "test_data/hits.sql", CLASS_NAME));

    @Rule public SpliceWatcher methodWatcher = new DefaultedSpliceWatcher(CLASS_NAME);

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.<Object[]>asList(
                new Object[]{"broadcast",false,true},
                new Object[]{"nestedloop",true,true},
                new Object[]{"hash",false,false}, //TODO -sf- replace this with a test at some point
                new Object[]{"sortmerge",false,true}
        );
    }

    private String joinStrategy;
    private boolean allowsNonEqui;
    private boolean allowRhsJoin;

    public EquiJoinOperationIT(String joinStrategy, boolean allowsNonEqui,boolean allowRhsJoin) {
        this.joinStrategy = joinStrategy;
        this.allowsNonEqui = allowsNonEqui;
        this.allowRhsJoin = allowRhsJoin;
    }

    @Test
    public void testSimpleInnerEquijoin() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select works.empnum from staff inner join works --SPLICE-PROPERTIES joinStrategy=" + joinStrategy + " \n" +
                "on staff.empnum = works.empnum");
        List<Map> results = TestUtils.resultSetToMaps(rs);
        Assert.assertEquals(12, results.size());
        Set empnums = new HashSet();
        for (Map row: results){
            empnums.add(row.get("EMPNUM"));
        }
        Assert.assertEquals(4, empnums.size());
    }

    @Test
    public void testSimpleLeftEquijoin() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select staff.empnum from staff left join works --SPLICE-PROPERTIES joinStrategy="+joinStrategy+" \n" +
                "on staff.empnum = works.empnum");
        List<Map> results = TestUtils.resultSetToMaps(rs);
        Assert.assertEquals(13, results.size());
        Set empnums = new HashSet();
        for (Map row: results){
            empnums.add(row.get("EMPNUM"));
        }
        Assert.assertEquals(5, empnums.size());
    }

    @Test
    public void testFailsOnNonEquijoin() throws Exception {
        //don't test for strategies which allow non-equi-joins
        Assume.assumeFalse("Ignoring because "+ joinStrategy+" allows non-equi-joins",allowsNonEqui);
        Exception caught = null;
        try {
            methodWatcher.executeQuery("select staff.empnum from staff inner join works --SPLICE-PROPERTIES joinStrategy="+joinStrategy+" \n" +
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
    public void testRHSAggregateUnderSink() throws Exception {
        Assume.assumeTrue("Ignoring because join strategy "+ joinStrategy+" does not allow right hand side sinks",allowRhsJoin);
        int count = 48;
        ResultSet rs = methodWatcher.executeQuery("SELECT m1.month, m1.ip_address, m1.hits\n" +
                                                      "FROM monthly_hits m1 \n" +
                                                      "LEFT OUTER JOIN monthly_hits m2 " +
                                                      "     --splice-properties joinStrategy="+joinStrategy+"\n" +
                                                      "  ON (m1.month = m2.month AND m1.hits < m2.hits) \n" +
                                                      "order by m1.month, m1.ip_address, m1.hits");
        Assert.assertEquals("Unexpected result set cardinality", count, TestUtils.resultSetToArrays(rs).size());
    }

    @Test
    @Ignore("Hinting not working for semijoin")
    public void testInWithCorrelatedSubQueryOrSemijoin() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select empnum from staff --SPLICE-PROPERTIES joinStrategy="+joinStrategy+" \n" +
                "where empnum in " +
                "(select works.empnum from works where staff.empnum = works.empnum)");
        List results = TestUtils.resultSetToMaps(rs);
        Assert.assertEquals(4, results.size());
    }

    @Test
    @Ignore("Hinting not working for antijoin")
    public void testNotInWithCorrelatedSubQueryOrAntijoin() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select empnum from staff --SPLICE-PROPERTIES joinStrategy="+joinStrategy+" \n" +
                "where empnum not in " +
                "(select works.empnum from works where staff.empnum = works.empnum)");
        List results = TestUtils.resultSetToMaps(rs);
        Assert.assertEquals(1, results.size());
    }
}
