/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
import com.splicemachine.si.impl.HMissedSplitException;
import com.splicemachine.storage.Partition;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.*;

/**
 * @author Scott Fines
 *         Date: 1/6/16
 */
public class HBaseSubregionSplitter implements SubregionSplitter{
    private static final Logger LOG = Logger.getLogger(HBaseSubregionSplitter.class);
    @Override
    public List<InputSplit> getSubSplits(Table table, List<Partition> splits, final byte[] scanStartRow, final byte[] scanStopRow) throws HMissedSplitException {
        // Synchronize the map since it will be acted on by multiple threads...
        final SortedMap<byte[],String> map = Collections.synchronizedSortedMap(new TreeMap<>(Bytes.BYTES_COMPARATOR));
        final List<InputSplit> results = new ArrayList<>();
        try {
            SpliceMessage.SpliceSplitServiceRequest message = SpliceMessage.SpliceSplitServiceRequest.newBuilder()
                    .setBeginKey(ZeroCopyLiteralByteString.wrap(scanStartRow))
                    .setEndKey(ZeroCopyLiteralByteString.wrap(scanStopRow)).build();

            table.batchCoprocessorService(com.splicemachine.coprocessor.SpliceMessage.
                    SpliceDerbyCoprocessorService.getDescriptor().findMethodByName("computeSplits"),message,
                    scanStartRow,scanStopRow,SpliceMessage.SpliceSplitServiceResponse.getDefaultInstance(),
                    new Batch.Callback<SpliceMessage.SpliceSplitServiceResponse>() {
                        @Override
                        public void update(byte[] region, byte[] row, SpliceMessage.SpliceSplitServiceResponse result) {
                            Iterator<ByteString> it = result.getCutPointList().iterator();
                            while (it.hasNext()) {
                                map.put(it.next().toByteArray(),result.getHostName());
                            }
                        }
                    });
            Iterator<Map.Entry<byte[],String>> foo = map.entrySet().iterator();
            if (!foo.hasNext()) {
                throw new IOException("split points not available");
            }
            byte[] firstKey = foo.next().getKey();
            while (foo.hasNext()) {
                Map.Entry<byte[],String> entry = foo.next();
                results.add(new SMSplit(
                        new TableSplit(
                                table.getName(),
                                firstKey,
                                entry.getKey(),
                                entry.getValue())));
               firstKey = entry.getKey();
            }
    } catch (Throwable throwable) {
                LOG.error("Error while computing cutpoints, falling back to whole region partition", throwable);

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
                } catch (IOException e) {
                    // Failed to add split, bail out
                    throw new RuntimeException(e);
                }
            }
        return results;
    }

}
