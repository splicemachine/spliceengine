package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.derby.test.framework.SpliceNetConnection.ConnectionBuilder;
import com.splicemachine.test.HBaseTest;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

@Category(HBaseTest.class)
public class CreateAliasIT extends SpliceUnitTest {

    private static final String SCHEMA_NAME = CreateAliasIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA_NAME);
    private static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA_NAME);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);


    @BeforeClass
    public static void createDataSet() throws Exception {
        Connection conn = spliceClassWatcher.getOrCreateConnection();
        new TableCreator(conn)
                .withCreate("create table t1 (c1 int)")
                .withInsert("insert into t1 values(?)")
                .withRows(rows(row(1)))
                .create();
        conn.commit();
    }


    @Test
    public void testCreateSynonym() throws  Exception {
        methodWatcher.executeUpdate("create synonym t2 for t1");
        ResultSet rs = methodWatcher.executeQuery("select c1 from t2");
        rs.next();
        assertEquals(1, rs.getInt("C1"));
        rs.close();
        methodWatcher.executeUpdate("drop synonym t2");

        // create a synonym with the same name again, and it should be successful
        // to confirm that dictionary cache has cleared the entry for previous t2.
        methodWatcher.executeUpdate("create synonym t2 for t1");
        rs = methodWatcher.executeQuery("select c1 from t2");
        rs.next();
        assertEquals(1, rs.getInt("C1"));
        rs.close();
        methodWatcher.executeUpdate("drop synonym t2");
    }

    @Test
    public void testCreateAlias() throws  Exception {
        methodWatcher.executeUpdate("create alias t3 for t1");
        ResultSet rs = methodWatcher.executeQuery("select c1 from t3");
        rs.next();
        assertEquals(1, rs.getInt("C1"));
        rs.close();
        methodWatcher.executeUpdate("drop alias t3");

        // create a synonym with the same name again, and it should be successful
        // to confirm that dictionary cache has cleared the entry for previous t2.
        methodWatcher.executeUpdate("create alias t3 for t1");
        rs = methodWatcher.executeQuery("select c1 from t3");
        rs.next();
        assertEquals(1, rs.getInt("C1"));
        rs.close();
        methodWatcher.executeUpdate("drop alias t3");

        // SHOW ALIASES is also working but cannot test through query since it is an ij command
    }

    @Test
    public void testDropAliasInvalidateDistributedCache() throws Exception {
        // step 1: create alias on RS0
        methodWatcher.executeUpdate("create alias a4 for t1");
        // step 2: select on RS1 to populate the cache
        TestConnection rs1Conn = spliceClassWatcher.connectionBuilder().user("splice").password("admin").port(1528).create(true).build();
        rs1Conn.query("select c1 from a4");
        rs1Conn.commit();

        // step 3: drop the alias on RS0
        methodWatcher.executeUpdate("drop alias a4");

        // step 4: check that we can re-use the same alias name on RS1
        try {
            rs1Conn.execute("create alias a4 for t1");
        } catch (SQLException e) {
            Assert.fail("fail to re-create alias a4!");
        }

        // step 5: check that we can use the re-created alias on RS0
        ResultSet rs = methodWatcher.executeQuery("select c1 from a4");
        rs.next();
        assertEquals(1, rs.getInt("C1"));
        rs.close();

        rs1Conn.close();
        methodWatcher.executeUpdate("drop alias a4");
    }
}
