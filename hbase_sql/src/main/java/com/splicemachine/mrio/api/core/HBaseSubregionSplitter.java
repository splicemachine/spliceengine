/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

import com.google.protobuf.ByteString;
import com.google.protobuf.ZeroCopyLiteralByteString;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.hbase.HBaseTableInfoFactory;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.storage.Partition;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Scott Fines
 *         Date: 1/6/16
 */
public class HBaseSubregionSplitter implements SubregionSplitter{
    private static final Logger LOG = Logger.getLogger(HBaseSubregionSplitter.class);
    @Override
    public List<InputSplit> getSubSplits(Table table, List<Partition> splits, final byte[] scanStartRow, final byte[] scanStopRow, int requestedSplits, long tableSize) {
        int splitsPerPartition;
        if (splits.size() > 3) {
            // First and last partitions could be very small, don't take them into account
            splitsPerPartition = requestedSplits / (splits.size() - 2);
        } else {
            splitsPerPartition = requestedSplits;
        }
        try {
            // Results has to be synchronized because we are adding to it from multiple threads concurrently
            final List<InputSplit> results = Collections.synchronizedList(new ArrayList<>());
            final AtomicBoolean failure = new AtomicBoolean(false);
            SpliceMessage.SpliceSplitServiceRequest.Builder builder = SpliceMessage.SpliceSplitServiceRequest.newBuilder()
                    .setBeginKey(ZeroCopyLiteralByteString.wrap(scanStartRow))
                    .setEndKey(ZeroCopyLiteralByteString.wrap(scanStopRow));
            long bytesPerSplit = 0;
            if (splitsPerPartition > 0) {
                builder.setRequestedSplits(splitsPerPartition);
                if (tableSize > 0) {
                    bytesPerSplit = (tableSize + (tableSize % requestedSplits)) / requestedSplits;
                    builder.setBytesPerSplit(bytesPerSplit);
                }

            }
            SpliceMessage.SpliceSplitServiceRequest message = builder.build();

            table.batchCoprocessorService(com.splicemachine.coprocessor.SpliceMessage.
                    SpliceDerbyCoprocessorService.getDescriptor().findMethodByName("computeSplits"),message,
                    scanStartRow,scanStopRow,SpliceMessage.SpliceSplitServiceResponse.getDefaultInstance(),
                    new Batch.Callback<SpliceMessage.SpliceSplitServiceResponse>() {
                        @Override
                        public void update(byte[] region, byte[] row, SpliceMessage.SpliceSplitServiceResponse result) {
                            try {
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("Update for region " + Bytes.toStringBinary(region) + " and row " + Bytes.toStringBinary(row) + " results " + result.getCutPointList());
                                }
                                Iterator<ByteString> it = result.getCutPointList().iterator();
                                byte[] first = it.next().toByteArray();
                                while (it.hasNext()) {
                                    byte[] end = it.next().toByteArray();
                                    if (Arrays.equals(first, end) && first != null && first.length > 0) {
                                        // We have two cutpoints that are the same, that's only OK when the whole table fits into
                                        // a single partition: ([], [])
                                        LOG.warn(String.format("Two cutpoints are equal: %s", Bytes.toStringBinary(first)));
                                        continue; // skip this cutpoint
                                    }
                                    results.add(new SMSplit(
                                            new TableSplit(
                                                    table.getName(),
                                                    first,
                                                    end,
                                                    result.getHostName())));

                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug(String.format("New split [%s,%s]", CellUtils.toHex(first), CellUtils.toHex(end)));
                                    }
                                    first = end;
                                }
                            } catch (Throwable t) {
                                LOG.error("Unexpected exception, will fallback to whole region partitions", t);
                                failure.set(true);
                                throw new RuntimeException(t);
                            }
                        }
                    });
            if (!failure.get())
                return results;
        } catch (Throwable throwable) {
            LOG.error("Error while computing cutpoints, falling back to whole region partitions", throwable);
        }
        // There was a failure somewhere, just return one split per region
        final List<InputSplit> results = new ArrayList<>();
        try {
            final HBaseTableInfoFactory infoFactory = HBaseTableInfoFactory.getInstance(HConfiguration.getConfiguration());
            for (final Partition split : splits) {
                results.add(new SMSplit(
                        new TableSplit(
                                infoFactory.getTableInfo(split.getTableName()),
                                split.getStartKey(),
                                split.getEndKey(),
                                split.owningServer().getHostname())));
            }
            return results;
        } catch (IOException e) {
            // Failed to add split, bail out
            throw new RuntimeException(e);
        }
    }
}
