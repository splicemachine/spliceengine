package com.splicemachine.derby.impl.sql.execute.sequence;

import com.splicemachine.derby.test.framework.SpliceSequenceWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.Assert;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Created by jyuan on 11/12/14.
 */
public class SpliceSequenceIT extends SpliceUnitTest {
    public static final String CLASS_NAME = SpliceSequenceIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String SEQUENCE_NAME = "SMALLSEQ";

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    private static String sequenceDef = "AS smallint ";
    protected static SpliceSequenceWatcher spliceSequenceWatcher = new SpliceSequenceWatcher(SEQUENCE_NAME,CLASS_NAME, sequenceDef);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSequenceWatcher);

    @Test
    public void test() throws Exception {

        PreparedStatement ps = spliceClassWatcher.prepareStatement(
                String.format("values (next value for %s.%s)", CLASS_NAME, SEQUENCE_NAME));
        ResultSet rs = ps.executeQuery();
        Assert.assertTrue(rs.next());
        int m = rs.getInt(1);
        rs.close();

        rs = ps.executeQuery();
        Assert.assertTrue(rs.next());
        int n = rs.getInt(1);

        Assert.assertTrue(n == m + 1);
    }
}
