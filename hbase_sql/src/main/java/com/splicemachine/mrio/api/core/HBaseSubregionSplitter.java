package com.splicemachine.mrio.api.core;

import com.google.protobuf.ByteString;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.hbase.HBaseTableInfoFactory;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.si.impl.HMissedSplitException;
import com.splicemachine.storage.Partition;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 1/6/16
 */
public class HBaseSubregionSplitter implements SubregionSplitter{
    private static final Logger LOG = Logger.getLogger(HBaseSubregionSplitter.class);
    @Override
    public List<InputSplit> getSubSplits(Table table, List<Partition> splits) throws HMissedSplitException {
        List<InputSplit> results = new ArrayList<>();
        for (final Partition split : splits) {
            try {
                byte[] probe;
                byte[] start = split.getStartKey();
                if (start.length == 0) {
                    // first region, pick smallest rowkey possible
                    probe = new byte[]{0};
                } else {
                    // any other region, pick start row
                    probe = start;
                }

                Map<byte[], List<InputSplit>> splitResults = table.coprocessorService(SpliceMessage.SpliceDerbyCoprocessorService.class, probe, probe,
                        new Batch.Call<SpliceMessage.SpliceDerbyCoprocessorService, List<InputSplit>>() {
                            @Override
                            public List<InputSplit> call(SpliceMessage.SpliceDerbyCoprocessorService instance) throws IOException {
                                ServerRpcController controller = new ServerRpcController();
                                byte[] startKey = split.getStartKey();
                                byte[] stopKey = split.getEndKey();

                                if (LOG.isDebugEnabled()) {
                                    LOG.debug(String.format("Original split [%s,%s]", CellUtils.toHex(startKey), CellUtils.toHex(stopKey)));
                                }

                                SpliceMessage.SpliceSplitServiceRequest message = SpliceMessage.SpliceSplitServiceRequest.newBuilder()
                                        .setBeginKey(ByteString.copyFrom(startKey)).setEndKey(ByteString.copyFrom(stopKey)).build();

                                BlockingRpcCallback<SpliceMessage.SpliceSplitServiceResponse> rpcCallback = new BlockingRpcCallback<>();
                                instance.computeSplits(controller, message, rpcCallback);
                                SpliceMessage.SpliceSplitServiceResponse response = rpcCallback.get();
                                if (controller.failed()) {
                                    throw controller.getFailedOn();
                                }
                                List<InputSplit> result = new ArrayList<>();
                                HBaseTableInfoFactory infoFactory = HBaseTableInfoFactory.getInstance(HConfiguration.getConfiguration());
                                Iterator<ByteString> it = response.getCutPointList().iterator();
                                byte[] first = it.next().toByteArray();
                                while (it.hasNext()) {
                                    byte[] end = it.next().toByteArray();
                                    result.add(new SMSplit(
                                            new TableSplit(
                                                    infoFactory.getTableInfo(split.getTableName()),
                                                    first,
                                                    end,
                                                    split.owningServer().getHostname())));

                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug(String.format("New split [%s,%s]", CellUtils.toHex(first), CellUtils.toHex(end)));
                                    }
                                    first = end;
                                }
                                return result;
                            }
                        });
                for (List<InputSplit> value : splitResults.values()) {
                    results.addAll(value);
                }
            } catch (HMissedSplitException ms) {
                throw ms;
            } catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }
        }
        return results;
    }
}
