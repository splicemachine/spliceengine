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

package com.splicemachine.derby.stream.utils;

import com.clearspring.analytics.util.Lists;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.services.property.PropertyUtil;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.stats.ColumnStatisticsImpl;
import com.splicemachine.db.iapi.stats.ColumnStatisticsMerge;
import com.splicemachine.db.iapi.types.SQLBlob;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import com.yahoo.sketches.quantiles.ItemsSketch;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jyuan on 10/9/18.
 */
public class BulkLoadUtils {
    protected static final Logger LOG=Logger.getLogger(BulkLoadUtils.class);
    /**
     * Calculate cut points according to statistics. Number of cut points is decided by max region size.
     * @param statistics
     * @return
     * @throws StandardException
     */
    public static List<Tuple2<Long, byte[][]>> getCutPoints(double sampleFraction,
                                                        List<Tuple2<Long, Tuple2<Double, ColumnStatisticsImpl>>> statistics) throws StandardException{
        Map<Long, Tuple2<Double, ColumnStatisticsImpl>> mergedStatistics = mergeResults(statistics);
        List<Tuple2<Long, byte[][]>> result = Lists.newArrayList();

        SConfiguration sConfiguration = HConfiguration.getConfiguration();
        long maxRegionSize = sConfiguration.getRegionMaxFileSize()/2;

        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "maxRegionSize = %d", maxRegionSize);

        // determine how many regions the table/index should be split into
        Map<Long, Integer> numPartitions = new HashMap<>();
        for (Long conglomId : mergedStatistics.keySet()) {
            Tuple2<Double, ColumnStatisticsImpl> stats = mergedStatistics.get(conglomId);
            double size = stats._1;
            int numPartition = (int)(size/sampleFraction/(1.0*maxRegionSize)) + 1;

            if (LOG.isDebugEnabled()) {
                SpliceLogUtils.debug(LOG, "total size of the table is %f", size);
            }
            if (numPartition > 1) {
                numPartitions.put(conglomId, numPartition);
            }
        }

        // calculate cut points for each table/index using histogram
        for (Long conglomId : numPartitions.keySet()) {
            int numPartition = numPartitions.get(conglomId);
            Map<String, byte[]> cutPointMap = new HashMap<>();

            ColumnStatisticsImpl columnStatistics = mergedStatistics.get(conglomId)._2;
            ItemsSketch itemsSketch = columnStatistics.getQuantilesSketch();
            for (int i = 1; i < numPartition; ++i) {
                SQLBlob blob = (SQLBlob) itemsSketch.getQuantile(i*1.0d/(double)numPartition);
                byte[] bytes = blob.getBytes();
                String s = Bytes.toHex(bytes);
                // remove duplicate cut points
                if(cutPointMap.get(s) == null) {
                    cutPointMap.put(s, bytes);
                }
            }
            byte[][] cutPoints = new byte[cutPointMap.size()][];
            cutPoints = cutPointMap.values().toArray(cutPoints);
            Tuple2<Long, byte[][]> tuple = new Tuple2<>(conglomId, cutPoints);
            result.add(tuple);
        }
        return result;
    }

    /**
     * Merge statistics from each RDD partition
     * @param tuples
     * @return
     * @throws StandardException
     */
    private static Map<Long, Tuple2<Double, ColumnStatisticsImpl>> mergeResults(
            List<Tuple2<Long, Tuple2<Double, ColumnStatisticsImpl>>> tuples) throws StandardException{

        Map<Long, ColumnStatisticsMerge> sm = new HashMap<>();
        Map<Long, Double> sizeMap = new HashMap<>();

        for (Tuple2<Long,Tuple2<Double, ColumnStatisticsImpl>> t : tuples) {
            Long conglomId = t._1;
            Double size = t._2._1;
            if (LOG.isDebugEnabled()) {
                SpliceLogUtils.debug(LOG, "conglomerate=%d, size=%f", conglomId, size);
            }
            // Merge statistics for keys
            ColumnStatisticsImpl cs = t._2._2;
            ColumnStatisticsMerge columnStatisticsMerge = sm.get(conglomId);
            if (columnStatisticsMerge == null) {
                columnStatisticsMerge = new ColumnStatisticsMerge();
                sm.put(conglomId, columnStatisticsMerge);
            }
            columnStatisticsMerge.accumulate(cs);

            // merge key/value size from all partition
            Double totalSize = sizeMap.get(conglomId);
            if (totalSize == null)
                totalSize = new Double(0);
            totalSize += size;
            if (LOG.isDebugEnabled()) {
                SpliceLogUtils.debug(LOG, "totalSize=%f", totalSize);
            }
            sizeMap.put(conglomId, totalSize);
        }

        Map<Long, Tuple2<Double, ColumnStatisticsImpl>> statisticsMap = new HashMap<>();
        for (Long conglomId : sm.keySet()) {
            Double totalSize = sizeMap.get(conglomId);
            ColumnStatisticsImpl columnStatistics = sm.get(conglomId).terminate();
            statisticsMap.put(conglomId, new Tuple2(totalSize, columnStatistics));
        }

        return statisticsMap;
    }

    /**
     * Split a table using cut points
     * @param cutPointsList
     * @throws StandardException
     */
    public static void splitTables(List<Tuple2<Long, byte[][]>> cutPointsList) throws StandardException {
        SIDriver driver=SIDriver.driver();
        try(PartitionAdmin pa = driver.getTableFactory().getAdmin()){
            for (Tuple2<Long, byte[][]> tuple : cutPointsList) {
                String table = tuple._1.toString();
                byte[][] cutpoints = tuple._2;
                if (LOG.isDebugEnabled()) {
                    SpliceLogUtils.debug(LOG, "split keys for table %s", table);
                    for(byte[] cutpoint : cutpoints) {
                        SpliceLogUtils.debug(LOG, "%s", Bytes.toHex(cutpoint));
                    }
                }
                pa.splitTable(table, cutpoints);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
            throw StandardException.plainWrapException(e);
        }
    }

    public static double getSampleFraction(LanguageConnectionContext lcc) throws StandardException {

        double sampleFraction;
        String sampleFractionString = PropertyUtil.getCachedDatabaseProperty(lcc,
                Property.BULK_IMPORT_SAMPLE_FRACTION);
        if (sampleFractionString != null) {
            try {
                sampleFraction = Double.parseDouble(sampleFractionString);
            }
            catch (NumberFormatException e) {
                SConfiguration sConfiguration = HConfiguration.getConfiguration();
                sampleFraction = sConfiguration.getBulkImportSampleFraction();
            }
        }
        else {
            SConfiguration sConfiguration = HConfiguration.getConfiguration();
            sampleFraction = sConfiguration.getBulkImportSampleFraction();
        }

        return sampleFraction;
    }

}
