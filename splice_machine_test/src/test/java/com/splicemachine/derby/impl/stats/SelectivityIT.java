package com.splicemachine.derby.impl.stats;

import com.splicemachine.derby.test.framework.*;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.sql.Types;

import static java.lang.String.format;

/**
 * Created by jyuan on 7/23/15.
 */
public class SelectivityIT extends SpliceUnitTest {
    private static final String SCHEMA_NAME =SelectivityIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA_NAME);
    private static final String TABLE_NAME = "FOO";
    private static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA_NAME);
    private static final SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher(TABLE_NAME, SCHEMA_NAME, "(col1 int, col2 char, col3 timestamp)");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    PreparedStatement ps;
                    try {
                        ps = spliceClassWatcher.prepareStatement(format("insert into %s values (?, ?, ?)", TABLE_NAME));
                        for (int i = 0; i < 5; ++i) {
                            ps.setInt(1, i);
                            ps.setString(2, "" + i);
                            ps.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
                            ps.execute();
                        }

                        for (int i = 0; i < 3; ++i) {
                            ps.setNull(1, Types.INTEGER);
                            ps.setNull(2, Types.CHAR);
                            ps.setNull(3, Types.TIMESTAMP);
                            ps.execute();
                        }

                        ps = spliceClassWatcher.prepareStatement(format("call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS('%s',false)", SCHEMA_NAME));
                        ps.execute();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA_NAME);

    @Test
    public void testNullSelectivity() throws Exception {
        PreparedStatement ps = spliceClassWatcher.prepareStatement(format("explain select * from %s where col1 is null", getTableReference(TABLE_NAME)));
        ResultSet rs = ps.executeQuery();
        Assert.assertTrue(rs.next());
        String explain = rs.getString(1);
        Assert.assertTrue(explain.contains("outputRows=3"));

        ps = spliceClassWatcher.prepareStatement(format("explain select * from %s where col1 is not null", getTableReference(TABLE_NAME)));
        rs = ps.executeQuery();
        Assert.assertTrue(rs.next());
        explain = rs.getString(1);
        Assert.assertTrue(explain.contains("outputRows=5"));
    }
}
