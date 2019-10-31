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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

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
                .create();
        conn.commit();
    }


    @Test
    public void testCreateSynonym() throws  Exception {
        methodWatcher.executeUpdate("create synonym t2 for t1");
        methodWatcher.executeUpdate("drop synonym t2");
    }

    @Test
    public void testCreateAlias() throws  Exception {
        methodWatcher.executeUpdate("create alias t2 for t1");
        methodWatcher.executeUpdate("drop alias t2");

        // SHOW ALIASES is also working but cannot test through query since it is an ij command
    }
}
