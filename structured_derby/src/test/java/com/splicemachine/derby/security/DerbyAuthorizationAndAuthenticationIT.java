package com.splicemachine.derby.security;

import org.apache.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import com.splicemachine.derby.impl.sql.actions.index.CustomerTable;
import com.splicemachine.derby.impl.sql.actions.index.OrderLineTable;
import com.splicemachine.derby.impl.sql.actions.index.OrderTable;
import com.splicemachine.derby.test.framework.SpliceIndexWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
@Ignore
public class DerbyAuthorizationAndAuthenticationIT extends SpliceUnitTest { 
    private static final Logger LOG = Logger.getLogger(DerbyAuthorizationAndAuthenticationIT.class);

    private static final String SCHEMA_NAME = DerbyAuthorizationAndAuthenticationIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();
    
    @Test
    public void createIndexesInOrderNonUnique() throws Exception {
        try {
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, CustomerTable.TABLE_NAME, CustomerTable.INDEX_NAME, CustomerTable.INDEX_DEF, false);
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, OrderTable.TABLE_NAME, OrderTable.INDEX_NAME, OrderTable.INDEX_DEF, false);
            SpliceIndexWatcher.createIndex(methodWatcher.createConnection(), SCHEMA_NAME, OrderLineTable.TABLE_NAME, OrderLineTable.INDEX_NAME, OrderLineTable.INDEX_DEF, false);
        } finally {
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,CustomerTable.INDEX_NAME);
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,OrderTable.INDEX_NAME);
            SpliceIndexWatcher.executeDrop(SCHEMA_NAME,OrderLineTable.INDEX_NAME);
        }
    }
   
}
