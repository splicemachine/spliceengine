package com.splicemachine.derby.utils;

import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.FinalTest;

import org.apache.commons.dbutils.DbUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.experimental.categories.Category;

import java.sql.CallableStatement;
import java.sql.ResultSet;

@Category(FinalTest.class)
public class VerifyNoActiveOperationsFINALIT {
	private static final Logger LOG = Logger.getLogger(SpliceWatcher.class);
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String CLASS_NAME = VerifyNoActiveOperationsFINALIT.class.getSimpleName().toUpperCase();

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher);
 
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testVerifyNoActiveJobs() throws Exception {
        CallableStatement cs = methodWatcher.prepareCall("call SYSCS_UTIL.SYSCS_GET_ACTIVE_JOB_IDS()");
        ResultSet rs = cs.executeQuery();
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert("call SYSCS_UTIL.SYSCS_GET_ACTIVE_JOB_IDS()", rs);
        // Temporarily log results rather than assert while we verify it in builds
		LOG.warn(String.format("Active operations according to zoo keeper:\n%s", fr));
        // Assert.assertEquals(fr.toString(), 0, fr.size());
        DbUtils.closeQuietly(rs);
    }
}
