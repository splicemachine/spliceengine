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

package com.splicemachine.mrio.api.core;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.derby.stream.spark.fake.FakeOutputFormat;
import com.splicemachine.derby.test.framework.*;
import com.splicemachine.mrio.MRConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import static org.junit.Assert.assertTrue;

/**
 * @author Mark Sirek
 *         Date: 4/30/19
 */
public class RegionSplitsIT extends SpliceUnitTest {
    private static final Logger LOG = Logger.getLogger(RegionSplitsIT.class);
    private static final String SCHEMA_NAME = RegionSplitsIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA_NAME);
    private static final String TABLE1_NAME = "TAB1";
    private static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA_NAME);
    private static final SpliceTableWatcher spliceTableWatcher1 =
            new SpliceTableWatcher(TABLE1_NAME, SCHEMA_NAME, "(I INT, C1 CHAR(254), C2 CHAR(254), C3 CHAR(254), C4 CHAR(254), C5 CHAR(254))");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher1).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    PreparedStatement ps;
                    try {
                        long conglomId = spliceClassWatcher.getConglomId(TABLE1_NAME, SCHEMA_NAME);
                        ps = spliceClassWatcher.prepareStatement(format("insert into %s (i, c1, c2, c3, c4, c5) values (?, ?, ?, ?, ?, ?)", TABLE1_NAME));
                        for (int j = 0; j < 100; ++j) {
                            for (int i = 0; i < 10; i++) {
                                ps.setInt(1, i);
                                ps.setString(2, "filler");
                                ps.setString(3, "filler");
                                ps.setString(4, "filler");
                                ps.setString(5, "filler");
                                ps.setString(6, "filler");
                                ps.execute();
                            }
                        }
                        //spliceClassWatcher.commit();
                        RegionUtils.splitTable(conglomId);

                        for (int j = 0; j < 100; ++j) {
                            for (int i = 0; i < 10; i++) {
                                ps.setInt(1, i);
                                ps.setString(2, "filler");
                                ps.setString(3, "filler");
                                ps.setString(4, "filler");
                                ps.setString(5, "filler");
                                ps.setString(6, "filler");
                                ps.execute();
                            }
                        }
                       // spliceClassWatcher.commit();
                        String compactionStmt = "call SYSCS_UTIL.SYSCS_PERFORM_MAJOR_COMPACTION_ON_TABLE(?, ?)";
                        ps = spliceClassWatcher.prepareStatement(compactionStmt);
                        RegionUtils.splitTable(conglomId);
                        compactionStmt = "call SYSCS_UTIL.SYSCS_PERFORM_MAJOR_COMPACTION_ON_TABLE(?, ?)";
                        ps = spliceClassWatcher.prepareStatement(compactionStmt);
                        ps.setString(1, SCHEMA_NAME);
                        ps.setString(2, TABLE1_NAME);
                        ps.execute();
                       // spliceClassWatcher.commit();

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA_NAME);

    @Test
    public void testGetSplits() throws Exception{

        SMInputFormat smInputFormat = new SMInputFormat();
        final Configuration conf=new Configuration(HConfiguration.unwrapDelegate());
        conf.setClass(JobContext.OUTPUT_FORMAT_CLASS_ATTR, FakeOutputFormat.class,FakeOutputFormat.class);
        conf.setInt(MRConstants.SPLICE_SPLITS_PER_TABLE, 8);
        // Get splits for the SYSCOLUMNS table.
        String tableName = format("%s.%s", SCHEMA_NAME, TABLE1_NAME);
        conf.set(MRConstants.SPLICE_INPUT_TABLE_NAME, tableName);
        long conglomId = spliceClassWatcher.getConglomId(TABLE1_NAME, SCHEMA_NAME);
        String conglomAsString = format("%d", conglomId);
        conf.set(MRConstants.SPLICE_INPUT_CONGLOMERATE, conglomAsString);
        String jdbcString = "jdbc:splice://localhost:1527/splicedb;user=splice;password=admin";
        conf.set(MRConstants.SPLICE_JDBC_STR, jdbcString);

        SMSQLUtil util = SMSQLUtil.getInstance(jdbcString);
        List<String> columns = new ArrayList<>();
        columns.add("I");
        conf.set(MRConstants.SPLICE_SCAN_INFO, util.getTableScannerBuilder(tableName, columns).base64Encode());
        smInputFormat.setConf(conf);
        JobContext ctx = new JobContextImpl(conf,new JobID("test",1));
        List<InputSplit> splits = smInputFormat.getSplits(ctx);

        LOG.info("Got "+splits.size() + " splits");
        assertTrue(format("Expected between 6 and 10 splits, got %d.", splits.size()),
                splits.size() >= 6 && splits.size() <= 10);

    }

}
