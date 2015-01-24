package com.splicemachine.pipeline.coprocessor;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import com.splicemachine.pipeline.Pipeline;

/**
 * @author Scott Fines
 *         Date: 1/14/15
 */
public class MultiRowEndpoint extends Pipeline.MultiRowService implements CoprocessorService,Coprocessor {
    private static final Logger LOG = Logger.getLogger(MultiRowEndpoint.class);
    private HRegion region;

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        this.region = ((RegionCoprocessorEnvironment)env).getRegion();
    }

    @Override public void stop(CoprocessorEnvironment env) throws IOException {  }

    @Override
    public void bulkWrite(RpcController controller,
                          Pipeline.MultiRowRequest request,
                          RpcCallback<Pipeline.MultiRowResponse> done) {
        try {
            done.run(bulkWrite(request.getTimestamp(),request.getKvsList()));
        } catch (IOException e) {
            ResponseConverter.setControllerException(controller,e);
        }
    }

    @Override
    public Service getService() {
        return this;
    }

    /************************************************************************************************/
    /*private helper methods*/
    private Pipeline.MultiRowResponse bulkWrite(final long timestamp,Collection<Pipeline.KV> kvsList) throws IOException{
        kvsList = Collections2.filter(kvsList, new Predicate<Pipeline.KV>() {
            @Override
            public boolean apply(Pipeline.KV kv) {
                return HRegion.rowIsInRange(region.getRegionInfo(),kv.getRow().toByteArray());
            }
        });
        SpliceLogUtils.trace(LOG,"Writing %d records to HBase",kvsList.size());
        Collection<Put> puts = Collections2.transform(kvsList, new Function<Pipeline.KV, Put>() {
            @Override
            public Put apply(Pipeline.KV kv) {
                Put put = new Put(kv.getRow().toByteArray());
                put.add(SpliceConstants.DEFAULT_FAMILY_BYTES,
                        SpliceConstants.PACKED_COLUMN_BYTES,
                        timestamp,
                        kv.getRow().toByteArray());
                return put;
            }
        });
        Mutation[] mutations = new Mutation[puts.size()];
        int i=0;
        for(Put put:puts){
            mutations[i] = put;
            i++;
        }
        OperationStatus[] operationStatuses = region.batchMutate(mutations);
        Pipeline.MultiRowResponse.Builder response = Pipeline.MultiRowResponse.newBuilder();
        for(int j=0;j<operationStatuses.length;j++){
            response.addSuccessFlags(operationStatuses[j].getOperationStatusCode() == HConstants.OperationStatusCode.SUCCESS);
        }
        return response.build();
    }
}
