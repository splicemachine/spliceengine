package com.splicemachine.derby.impl.sql.execute.operations;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

import org.apache.commons.dbutils.DbUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

/**
 * @author Scott Fines
 *         Created on: 3/8/13
 */
public class EmbeddedCallStatementOperationIT extends SpliceUnitTest {
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    private static final Logger LOG = Logger.getLogger(EmbeddedCallStatementOperationIT.class);
	@ClassRule public static TestRule chain = RuleChain.outerRule(spliceClassWatcher);
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testCallGetIndexInfo() throws Exception{
        DatabaseMetaData dmd = methodWatcher.getOrCreateConnection().getMetaData();
        ResultSet rs = dmd.getIndexInfo(null, "SYS", "SYSSCHEMAS", false, true);
        int count = 0;
        while(rs.next()){
        	count++;
            LOG.trace(rs.getString(1));
        }
        Assert.assertTrue(count > 0);
        DbUtils.closeQuietly(rs);
    }

}
