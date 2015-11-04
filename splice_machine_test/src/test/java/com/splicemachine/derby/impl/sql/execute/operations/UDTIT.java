package com.splicemachine.derby.impl.sql.execute.operations;

/**
 * Created by jyuan on 11/3/15.
 */
import com.customer.*;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_tools.TableCreator;
import org.apache.log4j.Logger;
import org.junit.*;

import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.ResultSet;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

public class UDTIT extends SpliceUnitTest {

    private static Logger LOG = Logger.getLogger(UDTIT.class);
    public static final String CLASS_NAME = UDTIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static final String CALL_INSTALL_JAR_FORMAT_STRING = "CALL SQLJ.INSTALL_JAR('%s', '%s', 0)";
    private static final String CALL_REMOVE_JAR_FORMAT_STRING = "CALL SQLJ.REMOVE_JAR('%s', 0)";
    private static final String STORED_PROCS_JAR_FILE = getResourceDirectory() + "/UDT.jar";
    private static final String JAR_FILE_SQL_NAME = CLASS_NAME + ".UDT_JAR";

    private static final String CALL_SET_CLASSPATH_FORMAT_STRING = "CALL SYSCS_UTIL.SYSCS_SET_GLOBAL_DATABASE_PROPERTY('derby.database.classpath', '%s')";


    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @ClassRule
    public static SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @BeforeClass
    public static void setup() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection());
    }

    //@AfterClass
    public static void tearDown() throws Exception {
        methodWatcher.execute(String.format(CALL_REMOVE_JAR_FORMAT_STRING, JAR_FILE_SQL_NAME));
        methodWatcher.execute("DROP DERBY AGGREGATE Median RESTRICT");
        methodWatcher.execute("DROP FUNCTION makePrice");
        methodWatcher.execute("DROP TYPE price restrict");
    }

    public static void createData(Connection conn) throws Exception {
        methodWatcher.execute(String.format(CALL_INSTALL_JAR_FORMAT_STRING, STORED_PROCS_JAR_FILE, JAR_FILE_SQL_NAME));
        methodWatcher.execute(String.format(CALL_SET_CLASSPATH_FORMAT_STRING, JAR_FILE_SQL_NAME));
        methodWatcher.execute("create derby aggregate median for int external name 'com.customer.Median'");

        new TableCreator(conn)
                .withCreate("create table t(i int)")
                .withInsert("insert into t values(?)")
                .withRows(rows(
                        row(1),
                        row(2),
                        row(3),
                        row(4),
                        row(5)))
                .create();

        methodWatcher.execute("CREATE TYPE price EXTERNAL NAME 'com.customer.Price' language Java");
        methodWatcher.execute("CREATE FUNCTION makePrice(varchar(30), double)\n" +
                "RETURNS Price\n" +
                "LANGUAGE JAVA\n" +
                "PARAMETER STYLE JAVA\n" +
                "NO SQL EXTERNAL NAME 'com.customer.CreatePrice.createPriceObject'");

        methodWatcher.execute("create table orders(orderID INT,customerID INT,totalPrice price)");
        methodWatcher.execute("insert into orders values (12345, 12, makePrice('USD', 12))");
    }

    @Test
    public void testUDA() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select median(i) from t");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(3, rs.getInt(1));
    }

    @Test
    public void testUDT() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select orderID, customerID, totalPrice from orders");
        Assert.assertTrue(rs.next());
        int oid = rs.getInt(1);
        int cid = rs.getInt(2);
        Price price = (Price)rs.getObject(3);
        Assert.assertEquals(12345, oid);
        Assert.assertEquals(12, cid);
        Assert.assertEquals(12, price.amount, 0.01);
        Assert.assertTrue(price.currencyCode.compareTo("USD") == 0);
    }
}
