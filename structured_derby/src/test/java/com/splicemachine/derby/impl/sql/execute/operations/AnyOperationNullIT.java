package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.DefaultedSpliceWatcher;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

/**
 * Bug 910 - NPE when inner select uses value for which there is no corresponding
 * row in the table.
 *
 * @author Jeff Cunningham
 *         Date: 11/15/13
 */
public class AnyOperationNullIT extends SpliceUnitTest {

    private static List<String> PROJ_VALS = Arrays.asList(
            "INSERT INTO PROJ VALUES  ('P1','MXSS','Design',10000,'Deale')",
            "INSERT INTO PROJ VALUES  ('P2','CALM','Code',30000,'Vienna')",
            "INSERT INTO PROJ VALUES  ('P3','SDP','Test',30000,'Tampa')",
            "INSERT INTO PROJ VALUES  ('P4','SDP','Design',20000,'Deale')",
            "INSERT INTO PROJ VALUES  ('P5','IRM','Test',10000,'Vienna')",
            "INSERT INTO PROJ VALUES  ('P6','PAYR','Design',50000,'Deale')",
            "INSERT INTO PROJ VALUES  ('P7','KMA','Design',50000,'Akron')");

    private static List<String> STAFF_VALS = Arrays.asList(
            "INSERT INTO STAFF VALUES ('E1','Alice',12,'Deale')",
            "INSERT INTO STAFF VALUES ('E2','Betty',10,'Vienna')",
            "INSERT INTO STAFF VALUES ('E3','Carmen',13,'Vienna')",
            "INSERT INTO STAFF VALUES ('E4','Don',12,'Deale')",
            "INSERT INTO STAFF VALUES ('E5','Ed',13,'Akron')",
            "INSERT INTO STAFF VALUES ('E6','Joe',13,'Deale')",
            "INSERT INTO STAFF VALUES ('E7','Fred',13,'Vienna')",
            "INSERT INTO STAFF VALUES ('E8','Jane',13,'Akron')");

    public static final String CLASS_NAME = AnyOperationNullIT.class.getSimpleName();

    protected static SpliceWatcher spliceClassWatcher = new DefaultedSpliceWatcher(CLASS_NAME);

    protected static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    protected static SpliceTableWatcher PROJ_TABLE = new SpliceTableWatcher("PROJ",schemaWatcher.schemaName,
            "(PNUM VARCHAR(3) NOT NULL, "+
            "PNAME  VARCHAR(20), "+
            "PTYPE    CHAR(6), "+
            "BUDGET   DECIMAL(9), "+
            "CITY     VARCHAR(15))");
    protected static SpliceTableWatcher STAFF_TABLE = new SpliceTableWatcher("STAFF",schemaWatcher.schemaName,
            "(EMPNUM   VARCHAR(3) NOT NULL, "+
            "EMPNAME  VARCHAR(20), "+
            "GRADE    DECIMAL(4), "+
            "CITY     VARCHAR(15))");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(schemaWatcher)
            .around(PROJ_TABLE)
            .around(STAFF_TABLE)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        for (String rowVal : PROJ_VALS) {
                            spliceClassWatcher.getStatement().executeUpdate(rowVal);
                        }

                        for (String rowVal : STAFF_VALS) {
                            spliceClassWatcher.getStatement().executeUpdate(rowVal);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        spliceClassWatcher.closeAll();
                    }
                }
            });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testSelectValid() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("SELECT CITY FROM %s.STAFF WHERE EMPNUM = 'E8'", CLASS_NAME, CLASS_NAME));
        Assert.assertEquals("Expecting 1 rows from STAFF table.", 1, resultSetSize(rs));
    }

    @Test
    public void testSelectInvalid() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("SELECT CITY FROM %s.STAFF WHERE EMPNUM = 'E9'", CLASS_NAME, CLASS_NAME));
        Assert.assertEquals("Expecting 0 rows from STAFF table for invalid criteria.", 0, resultSetSize(rs));
    }

    @Test
    public void testAllQueryValidVal() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("SELECT CITY FROM %s.PROJ WHERE CITY = ALL (SELECT CITY FROM %s.STAFF WHERE EMPNUM = 'E8')", CLASS_NAME, CLASS_NAME));
        Assert.assertEquals("Expecting 1 row from PROJ table.", 1, resultSetSize(rs));
    }

    @Test
    public void testAllQueryInvalidValAll() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("SELECT CITY FROM %s.PROJ WHERE CITY = ALL (SELECT CITY FROM %s.STAFF WHERE EMPNUM = 'E9')", CLASS_NAME, CLASS_NAME));
        Assert.assertEquals("Expecting all rows from PROJ table.", 7, resultSetSize(rs));
    }

    @Test
    public void testAllQueryInvalidVal() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("SELECT CITY FROM %s.PROJ WHERE CITY = (SELECT CITY FROM %s.STAFF WHERE EMPNUM = 'E9')", CLASS_NAME, CLASS_NAME));
        Assert.assertEquals("Expecting no rows from PROJ table.", 0, resultSetSize(rs));
    }
}
