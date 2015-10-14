package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.ResultSet;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 * Created by jyuan on 10/12/15.
 */
public class VTIOperationIT extends SpliceUnitTest {
    public static final String CLASS_NAME = VTIOperationIT.class.getSimpleName().toUpperCase();
    private static final String TABLE_NAME="EMPLOYEE";

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);

    @BeforeClass
    public static void setup() throws Exception {
        setup(spliceClassWatcher);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        spliceClassWatcher.execute("drop function JDBCTableVTI ");
        spliceClassWatcher.execute("drop function JDBCSQLVTI ");
    }

    private static void setup(SpliceWatcher spliceClassWatcher) throws Exception {
        Connection conn = spliceClassWatcher.getOrCreateConnection();
        new TableCreator(conn)
                .withCreate("create table employee (name varchar(56),id bigint,salary numeric(9,2),ranking int)")
                .withInsert("insert into employee values(?,?,?,?)")
                .withRows(rows(
                        row("Andy", 1, 100000, 1),
                        row("Billy", 2, 100000, 2))).create();

        String sql = "create function JDBCTableVTI(conn varchar(32672), s varchar(1024), t varchar(1024))\n" +
                "returns table\n" +
                "(\n" +
                "   name varchar(56),\n" +
                "   id bigint,\n" +
                "   salary numeric(9,2),\n" +
                "   ranking int\n" +
                ")\n" +
                "language java\n" +
                "parameter style SPLICE_JDBC_RESULT_SET\n" +
                "no sql\n" +
                "external name 'com.splicemachine.derby.vti.SpliceJDBCVTI.getJDBCTableVTI'";
        spliceClassWatcher.execute(sql);

        sql = "create function JDBCSQLVTI(conn varchar(32672), s varchar(32672))\n" +
                "returns table\n" +
                "(\n" +
                "   name varchar(56),\n" +
                "   id bigint,\n" +
                "   salary numeric(9,2),\n" +
                "   ranking int\n" +
                ")\n" +
                "language java\n" +
                "parameter style SPLICE_JDBC_RESULT_SET\n" +
                "no sql\n" +
                "external name 'com.splicemachine.derby.vti.SpliceJDBCVTI.getJDBCSQLVTI'";
        spliceClassWatcher.execute(sql);
    }

    @Test
    public void testJDBCSQLVTI() throws Exception {
        String sql = String.format("select * from table (JDBCSQLVTI('jdbc:splice://localhost:1527/splicedb;create=true;user=splice;password=admin', 'select * from %s.%s'))a", CLASS_NAME, TABLE_NAME);
        ResultSet rs = spliceClassWatcher.executeQuery(sql);
        int count = 0;
        while (rs.next()) {
            count++;
        }
        Assert.assertEquals(2, count);
    }

    @Test
    public void testJDBCTableVTI() throws Exception {
        String sql = String.format("select * from table (JDBCTableVTI('jdbc:splice://localhost:1527/splicedb;create=true;user=splice;password=admin', '%s', '%s'))a", CLASS_NAME, TABLE_NAME);
        ResultSet rs = spliceClassWatcher.executeQuery(sql);
        int count = 0;
        while (rs.next()) {
            count++;
        }
        Assert.assertEquals(2, count);
    }

    @Test
    public void testFileVTI() throws Exception {
        String location = getResourceDirectory()+"importTest.in";
        String sql = String.format("select * from new com.splicemachine.derby.vti.SpliceFileVTI('%s','',',') as b (c1 varchar(128), c2 varchar(128), c3 int)", location);
        ResultSet rs = spliceClassWatcher.executeQuery(sql);
        int count = 0;
        while (rs.next()) {
            count++;
        }
        Assert.assertEquals(5, count);
    }
}
