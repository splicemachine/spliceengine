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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.test.SerialTest;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import scala.Tuple2;
import com.splicemachine.derby.test.framework.SpliceSparkWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.mrio.MRConstants;

@Category(value = {SerialTest.class})
public class SMInputFormatIT extends BaseMRIOTest {
    private static final String CLASS_NAME = SMInputFormatIT.class.getSimpleName().toUpperCase();
    private static SpliceWatcher classWatcher = new SpliceWatcher(CLASS_NAME);
    private static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    private static final SpliceTableWatcher tableWatcherA = new SpliceTableWatcher(
            "A", schemaWatcher.schemaName, "(col1 varchar(100) primary key, col2 varchar(100))");
    private static final SpliceTableWatcher tableWatcherB = new SpliceTableWatcher(
            "B", schemaWatcher.schemaName, "(col1 bigint primary key, col2 varchar(100))");
    private static final SpliceTableWatcher tableWatcherC = new SpliceTableWatcher(
            "C", schemaWatcher.schemaName, "(col1 bigint primary key, col2 varchar(100))");
    private static final SpliceSparkWatcher sparkWatcher = new SpliceSparkWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher)
            .around(tableWatcherA)
            .around(tableWatcherB)
            .around(tableWatcherC)
            .around(sparkWatcher)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try{
                        PreparedStatement ps1;
                        PreparedStatement ps2;
                        PreparedStatement ps3;
                        ps1 = classWatcher.prepareStatement(
                                "insert into " + tableWatcherA + " (col1, col2) values (?,?)");
                        ps1.setString(1, "John");
                        ps1.setString(2, "Leach");
                        ps1.executeUpdate();
                        ps1.setString(1, "Jenny");
                        ps1.setString(2, "Swift");
                        ps1.executeUpdate();

                        ps2 = classWatcher.prepareStatement(
                                "insert into " + tableWatcherB + " (col1, col2) values (?,?)");
                        for (int i =0; i<10000;i++) {
                            ps2.setInt(1, i);
                            ps2.setString(2, i+"sdfasdgffdgdfgdfgdfgdfgdfgdfgd");

                            ps2.addBatch();

                            if (i==500) {
                                ps2.executeBatch();
                                flushTable(tableWatcherB.toString());
                            }

                            if (i==1000) {
                                ps2.executeBatch();
                                flushTable(tableWatcherB.toString());
                            }

                            if (i==2000) {
                                ps2.executeBatch();
                                flushTable(tableWatcherB.toString());
                                splitTable(tableWatcherB.toString());
                            }
                        }
                        ps2.executeBatch();

                        ps3 = classWatcher.prepareStatement(
                                "insert into " + tableWatcherC + " (col1, col2) values (?,?)");
                        for (int i =0; i<10000;i++) {
                            ps3.setInt(1, i);
                            ps3.setString(2, i + "sdfasdgffdgdfgdfgdfgdfgdfgdfgd");
                            ps3.addBatch();
                        }

                        ps3.executeBatch();

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } catch (Throwable t) {
                        throw new RuntimeException(t);
                    } finally {
                        classWatcher.closeAll();
                    }
                }
            });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @Test
    public void testSparkIntegrationWithInputFormat() throws Exception {
        config.set(MRConstants.SPLICE_TABLE_NAME, tableWatcherA.toString());
        config.set(MRConstants.SPLICE_INPUT_TABLE_NAME, tableWatcherA.toString());
        config.set(MRConstants.SPLICE_INPUT_CONGLOMERATE, String.valueOf(classWatcher.getConglomId(tableWatcherA.tableName, tableWatcherA.getSchema())));
        Job job = Job.getInstance(config, "Test Scan");
        JavaPairRDD<RowLocation, ExecRow> table = sparkWatcher.jsc.newAPIHadoopRDD(job.getConfiguration(), SMInputFormat.class, RowLocation.class, ExecRow.class);
        List<Tuple2<RowLocation, ExecRow>> data = table.collect();
        int i = 0;
        for (Tuple2<RowLocation, ExecRow> tuple: data) {
            i++;
            Assert.assertNotNull(tuple._1());
            Assert.assertNotNull(tuple._2());
        }
        Assert.assertEquals("Incorrect Results Returned", 2,i);
    }

    @Test
    public void testCountOverMultipleRegionsInSpark() throws Exception {
        config.set(MRConstants.SPLICE_TABLE_NAME, tableWatcherB.toString());
        config.set(MRConstants.SPLICE_INPUT_TABLE_NAME, tableWatcherB.toString());
        config.set(MRConstants.SPLICE_INPUT_CONGLOMERATE, String.valueOf(classWatcher.getConglomId(tableWatcherB.tableName, tableWatcherB.getSchema())));
        Job job = Job.getInstance(config, "Test Scan");
        JavaPairRDD<RowLocation, ExecRow> table = sparkWatcher.jsc.newAPIHadoopRDD(job.getConfiguration(), SMInputFormat.class, RowLocation.class, ExecRow.class);
        List<Tuple2<RowLocation, ExecRow>> data = table.collect();
        int i = 0;
        for (Tuple2<RowLocation, ExecRow> tuple: data) {
            i++;
            Assert.assertNotNull(tuple._1());
            Assert.assertNotNull(tuple._2());
        }
        Assert.assertEquals("Incorrect Results Returned", 10000,i);

    }

    private void executeQueryAndAssert(String query, int expected) throws SQLException {
        try (PreparedStatement ps = methodWatcher.prepareStatement(query)) {
            try (ResultSet rs = ps.executeQuery()) {
                Assert.assertTrue("No rows returned from count query!", rs.next());
                Assert.assertEquals("Incorrect query count!", expected, rs.getLong(1));
            }
        }
    }

    private void testQueryInSpark(SpliceTableWatcher tableWatcher) throws SQLException {
        executeQueryAndAssert("select count(*) from " + tableWatcher.toString() + " where col1 in (1,2,4) --splice-properties useSpark=true", 3);
        executeQueryAndAssert("select count(*) from " + tableWatcher.toString() +
                        " where col2 in ('1sdfasdgffdgdfgdfgdfgdfgdfgdfgd') --splice-properties useSpark=true",
                1);
        executeQueryAndAssert("select count(*) from " + tableWatcher.toString() +
                        " where col1 in (1,2,5,6,7,8,9,10) and col2 in ('1sdfasdgffdgdfgdfgdfgdfgdfgdfgd', '7sdfasdgffdgdfgdfgdfgdfgdfgdfgd') --splice-properties useSpark=true",
                2);
    }

    @Test
    public void testQueryOverMultipleRegionsInSpark() throws Exception {
        Assert.assertTrue("Should be more than one region for this table", getRegionCount(tableWatcherB.toString()) > 1);
        testQueryInSpark(tableWatcherB);
    }

    @Test
    public void testQueryOverSingleRegionInSpark() throws Exception {
        Assert.assertEquals("Should be one region for this table", 1, getRegionCount(tableWatcherC.toString()));
        testQueryInSpark(tableWatcherC);
    }

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testSplitNeedsRefreshMaxRetries() throws Exception {
        expectedEx.expect(RuntimeException.class);
        expectedEx.expectMessage("MAX_RETRIES exceeded during getSplits");

        config.set(MRConstants.SPLICE_TABLE_NAME, tableWatcherB.toString());
        config.set(MRConstants.SPLICE_INPUT_TABLE_NAME, tableWatcherB.toString());
        config.set(MRConstants.SPLICE_INPUT_CONGLOMERATE, String.valueOf(classWatcher.getConglomId(tableWatcherB.tableName, tableWatcherB.getSchema())));
        Job job = Job.getInstance(config, "Test Scan");
        JavaPairRDD<RowLocation, ExecRow> table = sparkWatcher.jsc.newAPIHadoopRDD(job.getConfiguration(), SMInputFormatFail.class, RowLocation.class, ExecRow.class);
        List<Tuple2<RowLocation, ExecRow>> data = table.collect();
    }

}
