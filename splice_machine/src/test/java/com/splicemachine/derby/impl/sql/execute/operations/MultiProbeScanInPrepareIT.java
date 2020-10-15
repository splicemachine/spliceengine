/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */
package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

/**
 * Created by yxia on 3/5/18.
 */
@RunWith(Parameterized.class)
public class MultiProbeScanInPrepareIT extends SpliceUnitTest {
    public static final String CLASS_NAME = MultiProbeScanInPrepareIT.class.getSimpleName();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{"true"});
        params.add(new Object[]{"false"});
        return params;
    }
    private String useSparkString;

    public MultiProbeScanInPrepareIT(String useSparkString) {
        this.useSparkString = useSparkString;
    }

    protected static SpliceTableWatcher t1Watcher = new SpliceTableWatcher("user_groups",schemaWatcher.schemaName,"(user_id BIGINT NOT NULL,segment_id INT NOT NULL,unixtime BIGINT, primary key(segment_id, user_id))");
    protected static SpliceTableWatcher t8Watcher = new SpliceTableWatcher("t8",schemaWatcher.schemaName,"(a8 int, b8 int, c8 int)");
    protected static SpliceIndexWatcher i8Watcher = new SpliceIndexWatcher("t8",schemaWatcher.schemaName,"ix_t8",schemaWatcher.schemaName,"(b8, c8)");

    private static int size = 10;

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(schemaWatcher)
            .around(t1Watcher)
            .around(t8Watcher)
            .around(i8Watcher)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        PreparedStatement ps = spliceClassWatcher.prepareStatement("insert into " + t1Watcher.toString() + " values (?,?,?)");
                        for (int i = 0; i < size; i++) {
                            ps.setInt(1, i);
                            ps.setInt(2, i);
                            ps.setLong(3, 1l);
                            ps.execute();
                        }

                        for (int i = 0; i < size; i++) {
                            if ((i == 4) || (i == 6)) {
                                ps.setInt(1, size + i);
                                ps.setInt(2, i);
                                ps.setLong(3, 1l);
                                ps.execute();
                            }
                        }

                        ps = spliceClassWatcher.prepareStatement("insert into " + t8Watcher.toString() + " values (?,?,?)");
                        for (int i = 0; i < size*2; i++) {
                            ps.setInt(1, i);
                            ps.setInt(2, i/4);
                            ps.setInt(3, i);
                            ps.addBatch();
                        }
                        ps.executeBatch();

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        spliceClassWatcher.closeAll();
                    }
                }

            });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @Test
    public void testMultiProbeTableScanPrepareStatemntMultipleExecution() throws Exception {
		/* case 1 all parameters in an inlist */
        PreparedStatement ps = methodWatcher.prepareStatement(format("select * from "+t1Watcher+
                " --splice-properties useSpark=%s \n" +
                " where segment_id in (?,?,?) and unixtime = ?", this.useSparkString));
        ps.setInt(1,1);
        ps.setInt(2,5);
        ps.setInt(3,8);
        ps.setLong(4,1);
        ResultSet rs = ps.executeQuery();

        String expected = "USER_ID |SEGMENT_ID |UNIXTIME |\n" +
                "--------------------------------\n" +
                "    1    |     1     |    1    |\n" +
                "    5    |     5     |    1    |\n" +
                "    8    |     8     |    1    |";
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // execute with a different set of parameters
        ps.setInt(1,4);
        ps.setInt(2,5);
        ps.setInt(3,6);
        ps.setLong(4,1);
        rs = ps.executeQuery();

        expected = "USER_ID |SEGMENT_ID |UNIXTIME |\n" +
                "--------------------------------\n" +
                "   14    |     4     |    1    |\n" +
                "   16    |     6     |    1    |\n" +
                "    4    |     4     |    1    |\n" +
                "    5    |     5     |    1    |\n" +
                "    6    |     6     |    1    |";
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

		/* case 2: inlist is a mixture of constants and parameters */
        ps = methodWatcher.prepareStatement(format("select * from "+t1Watcher+
                " --splice-properties useSpark=%s \n" +
                " where segment_id in (1,?,5, ?) and unixtime = ?", this.useSparkString));
        ps.setInt(1,5);
        ps.setInt(2,8);
        ps.setLong(3,1);
        rs = ps.executeQuery();

        expected = expected = "USER_ID |SEGMENT_ID |UNIXTIME |\n" +
                "--------------------------------\n" +
                "    1    |     1     |    1    |\n" +
                "    5    |     5     |    1    |\n" +
                "    8    |     8     |    1    |";;
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // execute with a different set of parameters
        ps.setInt(1,4);
        ps.setInt(2,5);
        ps.setLong(3,1);
        rs = ps.executeQuery();

        expected = "USER_ID |SEGMENT_ID |UNIXTIME |\n" +
                "--------------------------------\n" +
                "    1    |     1     |    1    |\n" +
                "   14    |     4     |    1    |\n" +
                "    4    |     4     |    1    |\n" +
                "    5    |     5     |    1    |";
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testMultiProbeIndexScanPrepareStatemntMultipleExecution() throws Exception {
		/* case 1: inlist on first column */
        PreparedStatement ps = methodWatcher.prepareStatement(format("select b8, c8 from "+t8Watcher+
                " --splice-properties index=ix_t8, useSpark=%s\n" +
                "where b8 in (?,?)", this.useSparkString));
        ps.setInt(1,1);
        ps.setInt(2,5);
        ResultSet rs = ps.executeQuery();

        String expected = "B8 |C8 |\n" +
                "--------\n" +
                " 1 | 4 |\n" +
                " 1 | 5 |\n" +
                " 1 | 6 |\n" +
                " 1 | 7 |";
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // execute with a different set of parameters
        ps.setInt(1,2);
        ps.setInt(2,3);
        rs = ps.executeQuery();

        expected = "B8 |C8 |\n" +
                "--------\n" +
                " 2 |10 |\n" +
                " 2 |11 |\n" +
                " 2 | 8 |\n" +
                " 2 | 9 |\n" +
                " 3 |12 |\n" +
                " 3 |13 |\n" +
                " 3 |14 |\n" +
                " 3 |15 |";
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

		/* case 2: inlist on second index column */
        ps = methodWatcher.prepareStatement(format("select b8, c8 from "+t8Watcher+
                " --splice-properties index=ix_t8, useSpark=%s\n" +
                "where b8=1 and c8 in (?,?)", this.useSparkString));
        ps.setInt(1,4);
        ps.setInt(2,5);
        rs = ps.executeQuery();

        expected = "B8 |C8 |\n" +
                "--------\n" +
                " 1 | 4 |\n" +
                " 1 | 5 |";
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // execute with a different set of parameters
        ps.setInt(1,4);
        ps.setInt(2,6);
        rs = ps.executeQuery();

        expected = "B8 |C8 |\n" +
                "--------\n" +
                " 1 | 4 |\n" +
                " 1 | 6 |";
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

    }

    @Test
    public void testMultiProbeIndexScanPrepareStatemntWithLookupMultipleExecution() throws Exception {
		/* case 1: inlist on first column */
        PreparedStatement ps = methodWatcher.prepareStatement(format("select * from "+t8Watcher+
                " --splice-properties index=ix_t8, useSpark=%s\n" +
                "where b8 in (?,?)", this.useSparkString));
        ps.setInt(1,1);
        ps.setInt(2,5);
        ResultSet rs = ps.executeQuery();

        String expected = "A8 |B8 |C8 |\n" +
                "------------\n" +
                " 4 | 1 | 4 |\n" +
                " 5 | 1 | 5 |\n" +
                " 6 | 1 | 6 |\n" +
                " 7 | 1 | 7 |";
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // execute with a different set of parameters
        ps.setInt(1,2);
        ps.setInt(2,3);
        rs = ps.executeQuery();

        expected = "A8 |B8 |C8 |\n" +
                "------------\n" +
                "10 | 2 |10 |\n" +
                "11 | 2 |11 |\n" +
                "12 | 3 |12 |\n" +
                "13 | 3 |13 |\n" +
                "14 | 3 |14 |\n" +
                "15 | 3 |15 |\n" +
                " 8 | 2 | 8 |\n" +
                " 9 | 2 | 9 |";
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

		/* case 2: inlist on second index column */
        ps = methodWatcher.prepareStatement(format("select * from "+t8Watcher+
                " --splice-properties index=ix_t8, useSpark=%s\n" +
                "where b8=1 and c8 in (?,?)", this.useSparkString));
        ps.setInt(1,4);
        ps.setInt(2,5);
        rs = ps.executeQuery();

        expected = "A8 |B8 |C8 |\n" +
                "------------\n" +
                " 4 | 1 | 4 |\n" +
                " 5 | 1 | 5 |";
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // execute with a different set of parameters
        ps.setInt(1,4);
        ps.setInt(2,6);
        rs = ps.executeQuery();

        expected = "A8 |B8 |C8 |\n" +
                "------------\n" +
                " 4 | 1 | 4 |\n" +
                " 6 | 1 | 6 |";
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testMultiProbeIndexScanPrepareStatemntWithJoinMultipleExecution() throws Exception {
        PreparedStatement ps = methodWatcher.prepareStatement(format("select * from "+t8Watcher+
                " as X --splice-properties index=ix_t8, useSpark=%s\n" +
                ", " + t8Watcher + " as Y --splice-properties index=ix_t8\n" +
                "where X.b8 in (?,?) and X.c8=Y.c8 and Y.b8 in (?,?)", this.useSparkString));
        ps.setInt(1,1);
        ps.setInt(2,5);
        ps.setInt(3,1);
        ps.setInt(4,5);
        ResultSet rs = ps.executeQuery();

        String expected = "A8 |B8 |C8 |A8 |B8 |C8 |\n" +
                "------------------------\n" +
                " 4 | 1 | 4 | 4 | 1 | 4 |\n" +
                " 5 | 1 | 5 | 5 | 1 | 5 |\n" +
                " 6 | 1 | 6 | 6 | 1 | 6 |\n" +
                " 7 | 1 | 7 | 7 | 1 | 7 |";
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // execute with a different set of parameters
        ps.setInt(1,2);
        ps.setInt(2,3);
        ps.setInt(3,2);
        ps.setInt(4,3);
        rs = ps.executeQuery();

        expected = "A8 |B8 |C8 |A8 |B8 |C8 |\n" +
                "------------------------\n" +
                "10 | 2 |10 |10 | 2 |10 |\n" +
                "11 | 2 |11 |11 | 2 |11 |\n" +
                "12 | 3 |12 |12 | 3 |12 |\n" +
                "13 | 3 |13 |13 | 3 |13 |\n" +
                "14 | 3 |14 |14 | 3 |14 |\n" +
                "15 | 3 |15 |15 | 3 |15 |\n" +
                " 8 | 2 | 8 | 8 | 2 | 8 |\n" +
                " 9 | 2 | 9 | 9 | 2 | 9 |";
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

}
