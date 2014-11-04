package org.apache.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Tests around creating schemas
 */
public class CreateSchemaIT {
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    private static final Logger LOG = Logger.getLogger(CreateSchemaIT.class);

    protected static SpliceSchemaWatcher sullivan1SchemaWatcher = new SpliceSchemaWatcher("SULLIVAN1");
    protected static SpliceSchemaWatcher sullivanSchemaWatcher = new SpliceSchemaWatcher("SULLIVAN");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(sullivan1SchemaWatcher)
            .around(sullivanSchemaWatcher);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testCreateSchemasWitSimilarName() throws Exception {
        PreparedStatement ps = methodWatcher.prepareStatement("select * from sys.sysschemas where schemaName like 'SULLIVAN%'");
        ResultSet rs = ps.executeQuery();
        int count = 0;
        while (rs.next()) {
            count++;
        }
        Assert.assertEquals("Incorrect row count", 2, count);
    }
}
