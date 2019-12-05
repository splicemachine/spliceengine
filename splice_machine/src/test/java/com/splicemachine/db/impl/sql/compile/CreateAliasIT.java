package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_tools.TableCreator;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.ResultSet;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

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
}
