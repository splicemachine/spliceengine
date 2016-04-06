package com.splicemachine.mrio.api.core;

import com.google.protobuf.ByteString;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.hbase.SpliceRpcController;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.mapreduce.InputSplit;

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
    @Override
    public List<InputSplit> getSubSplits(Table table,List<InputSplit> splits){
        List<InputSplit> results = new ArrayList<>();
        for (InputSplit split : splits) {
            final TableSplit tableSplit = (TableSplit) split;
            try {
                byte[] probe;
                byte[] start = tableSplit.getStartRow();
                if (start.length == 0) {
                    // first region, pick smallest rowkey possible
                    probe = new byte[] { 0 };
                } else  {
                    // any other region, pick start row
                    probe = start;
                }

                Map<byte[], List<InputSplit>> splitResults = table.coprocessorService(SpliceMessage.SpliceDerbyCoprocessorService.class, probe, probe,
                        new Batch.Call<SpliceMessage.SpliceDerbyCoprocessorService, List<InputSplit>>() {
                            @Override
                            public List<InputSplit> call(SpliceMessage.SpliceDerbyCoprocessorService instance) throws IOException{
                                SpliceRpcController controller = new SpliceRpcController();
                                byte[] startKey = tableSplit.getStartRow();
                                byte[] stopKey = tableSplit.getEndRow();

                                SpliceMessage.SpliceSplitServiceRequest message = SpliceMessage.SpliceSplitServiceRequest.newBuilder().setBeginKey(ByteString.copyFrom(startKey)).setEndKey(ByteString.copyFrom(stopKey)).build();

                                BlockingRpcCallback<SpliceMessage.SpliceSplitServiceResponse> rpcCallback = new BlockingRpcCallback<>();
                                instance.computeSplits(controller, message, rpcCallback);
                                SpliceMessage.SpliceSplitServiceResponse response = rpcCallback.get();
                                List<InputSplit> result =new ArrayList<>();
                                Iterator<ByteString> it = response.getCutPointList().iterator();
                                byte[] first = it.next().toByteArray();
                                while (it.hasNext()) {
                                    byte[] end = it.next().toByteArray();
                                    result.add(new SMSplit(new TableSplit(tableSplit.getTable(), first, end, tableSplit.getRegionLocation())));
                                    first = end;
                                }
                                return result;
                            }
                        });
                for (List<InputSplit> value : splitResults.values()) {
                    results.addAll(value);
                }
            } catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }
        }
        return results;
    }
}
