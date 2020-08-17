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
 *
 */

package com.splicemachine.derby.stream.function;

import splice.com.google.common.collect.Iterators;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.stats.ColumnStatisticsImpl;
import com.splicemachine.db.iapi.stats.ColumnStatisticsMerge;
import com.splicemachine.db.iapi.stats.ItemStatistics;
import com.splicemachine.db.iapi.types.SQLBoolean;
import com.splicemachine.db.iapi.types.SQLDouble;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.iapi.types.SQLTimestamp;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.sql.catalog.SYSCOLUMNSTATISTICSRowFactory;
import com.splicemachine.db.impl.sql.catalog.SYSTABLESTATISTICSRowFactory;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import splice.com.google.common.base.Function;
import splice.com.google.common.collect.FluentIterable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nullable;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by dgomezferro on 18/06/2017.
 */
public class MergeStatisticsHolder implements Externalizable {
    private Map<Integer, ColumnStatisticsMerge> columnStatisticsMergeHashMap;
    private ExecRow tableMergedStatistics;

    public MergeStatisticsHolder() {
        this.columnStatisticsMergeHashMap = new HashMap<>();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(columnStatisticsMergeHashMap);
        out.writeObject(tableMergedStatistics);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        columnStatisticsMergeHashMap = (Map<Integer, ColumnStatisticsMerge>) in.readObject();
        tableMergedStatistics = (ExecRow) in.readObject();
    }

    @SuppressFBWarnings(value = "DM_NUMBER_CTOR", justification = "DB-9844")
    public void merge(ExecRow nextRow) throws StandardException {
        if (nextRow.nColumns() == SYSCOLUMNSTATISTICSRowFactory.SYSCOLUMNSTATISTICS_COLUMN_COUNT) {
            // process columnstats row
            ItemStatistics itemStatistics = (ItemStatistics) nextRow.getColumn(SYSCOLUMNSTATISTICSRowFactory.DATA).getObject();
            Integer columnId = new Integer(nextRow.getColumn(SYSCOLUMNSTATISTICSRowFactory.COLUMNID).getInt());
            ColumnStatisticsMerge builder = columnStatisticsMergeHashMap.get(columnId);
            if (builder == null) {
                builder = ColumnStatisticsMerge.instance();
                columnStatisticsMergeHashMap.put(columnId, builder);
            }
            builder.accumulate((ColumnStatisticsImpl) itemStatistics);
        } else {
            if (tableMergedStatistics == null) {
                tableMergedStatistics = nextRow.getClone();
                long partitionRowCount = nextRow.getColumn(SYSTABLESTATISTICSRowFactory.ROWCOUNT).getLong();
                long avgRowWidth = nextRow.getColumn(SYSTABLESTATISTICSRowFactory.MEANROWWIDTH).getInt() * partitionRowCount;
                tableMergedStatistics.setColumn(SYSTABLESTATISTICSRowFactory.MEANROWWIDTH, new SQLLongint(avgRowWidth));
                return;
            }
            // process tablestats row
            // in case no columns stats is collected, we need to set it conglomId here
            long conglomId = nextRow.getColumn(SYSCOLUMNSTATISTICSRowFactory.CONGLOMID).getLong();
            long partitionRowCount = nextRow.getColumn(SYSTABLESTATISTICSRowFactory.ROWCOUNT).getLong();
            long rowCount = tableMergedStatistics.getColumn(SYSTABLESTATISTICSRowFactory.ROWCOUNT).getLong() + partitionRowCount;
            long totalSize = tableMergedStatistics.getColumn(SYSTABLESTATISTICSRowFactory.PARTITION_SIZE).getLong() + nextRow.getColumn(SYSTABLESTATISTICSRowFactory.PARTITION_SIZE).getLong();
            long avgRowWidth = tableMergedStatistics.getColumn(SYSTABLESTATISTICSRowFactory.MEANROWWIDTH).getLong() + nextRow.getColumn(SYSTABLESTATISTICSRowFactory.MEANROWWIDTH).getInt() * partitionRowCount;
            long numberOfPartitions = tableMergedStatistics.getColumn(SYSTABLESTATISTICSRowFactory.NUMBEROFPARTITIONS).getLong() + 1;
            int statsType = nextRow.getColumn(SYSTABLESTATISTICSRowFactory.STATSTYPE).getInt();
            double sampleFraction = nextRow.getColumn(SYSTABLESTATISTICSRowFactory.SAMPLEFRACTION).getDouble();
            tableMergedStatistics = generateTemporaryRowFromStats(conglomId, "-All-", rowCount, totalSize, avgRowWidth, numberOfPartitions, statsType, sampleFraction);
        }
    }

    public static ExecRow generateTemporaryRowFromStats(long conglomId,
                                               String partitionId,
                                               long rowCount,
                                               long partitionSize,
                                               long meanRowWidth,
                                               long numberOfPartitions,
                                               int statsType,
                                               double sampleFraction) throws StandardException {
        ExecRow row = new ValueRow(SYSTABLESTATISTICSRowFactory.SYSTABLESTATISTICS_COLUMN_COUNT);
        row.setColumn(SYSTABLESTATISTICSRowFactory.CONGLOMID,new SQLLongint(conglomId));
        row.setColumn(SYSTABLESTATISTICSRowFactory.PARTITIONID,new SQLVarchar(partitionId));
        row.setColumn(SYSTABLESTATISTICSRowFactory.TIMESTAMP,new SQLTimestamp(new Timestamp(System.currentTimeMillis())));
        row.setColumn(SYSTABLESTATISTICSRowFactory.STALENESS,new SQLBoolean(false));
        row.setColumn(SYSTABLESTATISTICSRowFactory.INPROGRESS,new SQLBoolean(false));
        row.setColumn(SYSTABLESTATISTICSRowFactory.ROWCOUNT,new SQLLongint(rowCount));
        row.setColumn(SYSTABLESTATISTICSRowFactory.PARTITION_SIZE,new SQLLongint(partitionSize));
        row.setColumn(SYSTABLESTATISTICSRowFactory.MEANROWWIDTH,new SQLLongint(meanRowWidth));
        row.setColumn(SYSTABLESTATISTICSRowFactory.NUMBEROFPARTITIONS,new SQLLongint(numberOfPartitions));
        row.setColumn(SYSTABLESTATISTICSRowFactory.STATSTYPE,new SQLInteger(statsType));
        row.setColumn(SYSTABLESTATISTICSRowFactory.SAMPLEFRACTION, new SQLDouble(sampleFraction));
        return row;
    }

    public void merge(MergeStatisticsHolder second) throws StandardException {
        // process tablestats row
        ExecRow nextRow = second.tableMergedStatistics;
        long conglomId = nextRow.getColumn(SYSCOLUMNSTATISTICSRowFactory.CONGLOMID).getLong();
        long partitionRowCount = nextRow.getColumn(SYSTABLESTATISTICSRowFactory.ROWCOUNT).getLong();
        long rowCount = tableMergedStatistics.getColumn(SYSTABLESTATISTICSRowFactory.ROWCOUNT).getLong() + partitionRowCount;
        long totalSize = tableMergedStatistics.getColumn(SYSTABLESTATISTICSRowFactory.PARTITION_SIZE).getLong() + nextRow.getColumn(SYSTABLESTATISTICSRowFactory.PARTITION_SIZE).getLong();
        long avgRowWidth = tableMergedStatistics.getColumn(SYSTABLESTATISTICSRowFactory.MEANROWWIDTH).getLong() + nextRow.getColumn(SYSTABLESTATISTICSRowFactory.MEANROWWIDTH).getLong();
        long numberOfPartitions = tableMergedStatistics.getColumn(SYSTABLESTATISTICSRowFactory.NUMBEROFPARTITIONS).getLong() +
                nextRow.getColumn(SYSTABLESTATISTICSRowFactory.NUMBEROFPARTITIONS).getLong();
        int statsType = nextRow.getColumn(SYSTABLESTATISTICSRowFactory.STATSTYPE).getInt();
        double sampleFraction = nextRow.getColumn(SYSTABLESTATISTICSRowFactory.SAMPLEFRACTION).getDouble();
        tableMergedStatistics = generateTemporaryRowFromStats(conglomId, "-All-", rowCount, totalSize, avgRowWidth, numberOfPartitions, statsType, sampleFraction);

        // process columns
        for (Map.Entry<Integer, ColumnStatisticsMerge> entry : columnStatisticsMergeHashMap.entrySet()) {
            // process columnstats row
            ColumnStatisticsMerge itemStatistics = second.columnStatisticsMergeHashMap.get(entry.getKey());
            if (itemStatistics != null) {
                ColumnStatisticsMerge builder = entry.getValue();
                builder.merge(itemStatistics);
            }
        }

        for (Map.Entry<Integer, ColumnStatisticsMerge> entry : second.columnStatisticsMergeHashMap.entrySet()) {
            if (!columnStatisticsMergeHashMap.containsKey(entry.getKey())) {
                columnStatisticsMergeHashMap.put(entry.getKey(), entry.getValue());
            }
        }

    }

    public Map<Integer, ColumnStatisticsMerge> getColumnStatisticsMergeHashMap() {
        return columnStatisticsMergeHashMap;
    }

    public ExecRow getTableMergedStatistics() {
        return tableMergedStatistics;
    }

    public Iterator<ExecRow> toList() throws StandardException {
        long rowCount = tableMergedStatistics.getColumn(SYSTABLESTATISTICSRowFactory.ROWCOUNT).getLong();
        long avgRowWidth = tableMergedStatistics.getColumn(SYSTABLESTATISTICSRowFactory.MEANROWWIDTH).getLong();
        tableMergedStatistics.setColumn(SYSTABLESTATISTICSRowFactory.MEANROWWIDTH, new SQLInteger( (int) ((double) avgRowWidth / rowCount)));
        return Iterators.concat(Arrays.asList(tableMergedStatistics).iterator(),
                FluentIterable.from(columnStatisticsMergeHashMap.entrySet()).transform(new Function<Map.Entry<Integer, ColumnStatisticsMerge>, ExecRow>() {
                    @Nullable
                    @Override
                    @SuppressFBWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE", justification = "DB-9844")
                    public ExecRow apply(@Nullable Map.Entry<Integer, ColumnStatisticsMerge> entry) {
                        return  entry.getValue().toExecRow(entry.getKey());
                    }
                }).iterator());
    }
}
