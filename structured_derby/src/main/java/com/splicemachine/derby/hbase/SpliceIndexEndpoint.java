package com.splicemachine.derby.hbase;

import com.google.common.collect.Lists;
import com.splicemachine.derby.impl.sql.execute.LocalWriteContextFactory;
import com.splicemachine.derby.impl.sql.execute.index.IndexSet;
import com.splicemachine.derby.impl.sql.execute.index.WriteContextFactoryPool;
import com.splicemachine.derby.utils.Mutations;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.BatchProtocol;
import com.splicemachine.hbase.MutationRequest;
import com.splicemachine.hbase.MutationResponse;
import com.splicemachine.hbase.MutationResult;
import com.splicemachine.hbase.batch.WriteContext;
import com.splicemachine.hbase.batch.WriteContextFactory;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Endpoint to allow special batch operations that the HBase API doesn't explicitly enable
 * by default (such as bulk-processed mutations)
 *
 * @author Scott Fines
 * Created on: 3/11/13
 */
public class SpliceIndexEndpoint extends BaseEndpointCoprocessor implements BatchProtocol{
    private static final Logger LOG = Logger.getLogger(SpliceIndexEndpoint.class);

    private volatile WriteContextFactory<RegionCoprocessorEnvironment> writeContextFactory;
    //TODO -sf- instantiate!

    @Override
    public void start(CoprocessorEnvironment env) {
        String tableName = ((RegionCoprocessorEnvironment)env).getRegion().getTableDesc().getNameAsString();
        final long conglomId;
        try{
            conglomId = Long.parseLong(tableName);
        }catch(NumberFormatException nfe){
            SpliceLogUtils.debug(LOG, "Unable to parse conglomerate id for table %s, " +
                    "index management for batch operations will be diabled",tableName);
            writeContextFactory = WriteContextFactoryPool.getDefaultFactory();
            super.start(env);
            return;
        }

        try {
            writeContextFactory = WriteContextFactoryPool.getContextFactory(conglomId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        SpliceDriver.Service service = new SpliceDriver.Service(){

            @Override
            public boolean start() {
                if(writeContextFactory instanceof LocalWriteContextFactory)
                    ((LocalWriteContextFactory) writeContextFactory).prepare();
                SpliceDriver.driver().deregisterService(this);
                return true;
            }

            @Override
            public boolean shutdown() {
                return true;
            }
        };
        SpliceDriver.driver().registerService(service);
        super.start(env);
    }

    @Override
    public void stop(CoprocessorEnvironment env) {
        try {
            WriteContextFactoryPool.releaseContextFactory((LocalWriteContextFactory) writeContextFactory);
        } catch (Exception e) {
            LOG.error("Unable to close context factory, beware memory leaks!",e);
        }
    }

    @Override
    public MutationResponse batchMutate(MutationRequest mutationsToApply) throws IOException {
        SpliceLogUtils.trace(LOG,"batchMutate %s",mutationsToApply);
        RegionCoprocessorEnvironment rce = (RegionCoprocessorEnvironment)this.getEnvironment();
        WriteContext context;
        try {
            context = writeContextFactory.create(rce);
        } catch (InterruptedException e) {
            //was interrupted while trying to create a write context.
            //we're done, someone else will have to write this batch
            throw new IOException(e);
        }
        for(Mutation mutation:mutationsToApply.getMutations()){
            context.sendUpstream(mutation); //send all writes along the pipeline
        }
        Map<Mutation,MutationResult> resultMap = context.finish();

        MutationResponse response = new MutationResponse();
        List<Mutation> mutations = mutationsToApply.getMutations();
        int pos=0;
        for(Mutation mutation:mutations){
            MutationResult result = resultMap.get(mutation);
            response.addResult(pos,result);
            pos++;
        }

        return response;
    }

    @Override
    public MutationResult deleteFirstAfter(String transactionId, byte[] rowKey, byte[] limit) throws IOException {
        RegionCoprocessorEnvironment rce = (RegionCoprocessorEnvironment)this.getEnvironment();
        final HRegion region = rce.getRegion();
        Scan scan = SpliceUtils.createScan(transactionId);
        scan.setStartRow(rowKey);
        scan.setStopRow(limit);
        scan.setCaching(1);
        scan.setBatch(1);

        RegionScanner scanner = region.getScanner(scan);
        List<KeyValue> row = Lists.newArrayList();
        if(scanner.next(row)){
            //get the row for the first entry
            byte[] rowBytes =  row.get(0).getRow();
            if(Bytes.compareTo(rowBytes,limit)<0){
                Mutation mutation = Mutations.getDeleteOp(transactionId,rowBytes);
                mutation.setAttribute(IndexSet.INDEX_UPDATED,IndexSet.INDEX_ALREADY_UPDATED);
                if(mutation instanceof Put){
                    try{
                        OperationStatus[] statuses = region.put(new Pair[]{Pair.newPair((Put)mutation,null)});
                        OperationStatus status = statuses[0];
                        switch (status.getOperationStatusCode()) {
                            case NOT_RUN:
                                return MutationResult.notRun();
                            case SUCCESS:
                                return MutationResult.success();
                            default:
                                return new MutationResult(MutationResult.Code.FAILED,status.getExceptionMsg());
                        }
                    }catch(IOException ioe){
                        return new MutationResult(MutationResult.Code.FAILED,ioe.getMessage());
                    }
                }else{
                    try{
                        region.delete((Delete)mutation,null,true);
                        return MutationResult.success();
                    }catch(IOException ioe){
                        return new MutationResult(MutationResult.Code.FAILED,ioe.getMessage());
                    }
                }
            }else{
                //we've gone past our stop point, so we can't delete anything
                return MutationResult.notRun();
            }
        }else{
            //there are no rows matching this scan, so we can't delete anything
            return MutationResult.notRun();
        }
    }

}
