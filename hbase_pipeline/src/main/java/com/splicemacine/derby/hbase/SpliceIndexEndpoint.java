package com.splicemacine.derby.hbase;

import com.google.common.base.Function;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.google.protobuf.ZeroCopyLiteralByteString;
import com.splicemachine.access.api.ServerControl;
import com.splicemachine.constants.EnvUtils;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.pipeline.PartitionWritePipeline;
import com.splicemachine.pipeline.PipelineWriter;
import com.splicemachine.pipeline.client.BulkWrites;
import com.splicemachine.pipeline.client.BulkWritesResult;
import com.splicemachine.pipeline.contextfactory.UnmanagedFactoryLoader;
import com.splicemachine.pipeline.contextfactory.WriteContextFactory;
import com.splicemachine.pipeline.contextfactory.WriteContextFactoryManager;
import com.splicemachine.pipeline.utils.PipelineCompressor;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.region.RegionServerControl;
import com.splicemachine.storage.RegionPartition;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemacine.pipeline.server.PipelineDriver;
import com.splicemacine.pipeline.server.PipelineEnvironment;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Endpoint to allow special batch operations that the HBase API doesn't explicitly enable
 * by default (such as bulk-processed mutations)
 *
 * @author Scott Fines
 *         Created on: 3/11/13
 */
public class SpliceIndexEndpoint extends SpliceMessage.SpliceIndexService implements BatchProtocol,Coprocessor, IndexEndpoint{
    private static final Logger LOG=Logger.getLogger(SpliceIndexEndpoint.class);

    private PartitionWritePipeline writePipeline;
    private PipelineWriter pipelineWriter;
    private PipelineCompressor compressor;


//    private static MetricName receptionName = new MetricName("com.splicemachine", "receiverStats", "time");
//    private static MetricName rejectedMeterName = new MetricName("com.splicemachine", "receiverStats", "rejected");


    //private Timer timer = SpliceDriver.driver().getRegistry().newTimer(receptionName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
//    private Meter rejectedMeter = SpliceDriver.driver().getRegistry().newMeter(rejectedMeterName, "rejectedRows", TimeUnit.SECONDS);



    @Override
    @SuppressWarnings("unchecked")
    public void start(CoprocessorEnvironment env){
        RegionCoprocessorEnvironment rce=((RegionCoprocessorEnvironment)env);
        ServerControl serverControl=new RegionServerControl(rce.getRegion());

        String tableName=rce.getRegion().getTableDesc().getTableName().getQualifierAsString();
        SpliceConstants.TableEnv table=EnvUtils.getTableEnv((RegionCoprocessorEnvironment)env);
        if(table.equals(SpliceConstants.TableEnv.USER_TABLE) || table.equals(SpliceConstants.TableEnv.DERBY_SYS_TABLE)){ // DERBY SYS TABLE is temporary (stats)
            final WriteContextFactory<TransactionalRegion> factory;
            long conglomId;
            try{
                conglomId=Long.parseLong(tableName);
            }catch(NumberFormatException nfe){
                SpliceLogUtils.warn(LOG,"Unable to parse conglomerate id for table %s, "+
                        "index management for batch operations will be disabled",tableName);
                conglomId=-1;
            }
            PipelineEnvironment pipelineEnv = HBasePipelineEnvironment.loadEnvironment(null); //TODO -sf- register a factory loader
            RegionPartition baseRegion=new RegionPartition(rce.getRegion());
            PipelineDriver pipelineDriver = pipelineEnv.getPipelineDriver();
            SIDriver siDriver = pipelineEnv.getSIDriver();

            factory=WriteContextFactoryManager.getWriteContext(conglomId,pipelineEnv.configuration(),
                    siDriver.getTableFactory(),
                    pipelineDriver.exceptionFactory(),
                    new Function<TableName, String>(){
                        @Override public String apply(TableName input){ return input.getNameAsString(); }
                    },
                    pipelineDriver.getContextFactoryLoader(conglomId)
            );
            factory.prepare();
            TransactionalRegion txnRegion = siDriver.transactionalPartition(conglomId,baseRegion);

            writePipeline=new PartitionWritePipeline(serverControl,
                    baseRegion,
                    factory,
                    txnRegion,
                    pipelineDriver.meter(),pipelineDriver.exceptionFactory());
            //TODO -sf- register ourselves as a service correctly
            pipelineWriter = pipelineDriver.writer();
            pipelineDriver.registerPipeline(baseRegion.getName(),writePipeline);
            compressor = pipelineDriver.compressor();
//            Service service = new Service() {
//                @Override
//                public boolean shutdown() {
//                    return true;
//                }
//
//                @Override
//                public boolean start() {
//                    factory.prepare();
//                    if (conglomId >= 0) {
//                        region = TransactionalRegions.get((HRegion) serverControl.getRegion());
//                    } else {
//                        region = TransactionalRegions.nonTransactionalRegion((HRegion) serverControl.getRegion());
//                    }
//                    partitionWritePipeline= new PartitionWritePipeline(serverControl, (HRegion) serverControl.getRegion(), factory, region, pipelineMeter);
//                    SpliceDriver.driver().deregisterService(this);
//                    return true;
//                }
//            };
//            SpliceDriver.driver().registerService(service);
        }
    }

    @Override
    public SpliceIndexEndpoint getBaseIndexEndpoint(){
        return this;
    }

    @Override
    public void bulkWrite(RpcController controller,
                          SpliceMessage.BulkWriteRequest request,
                          RpcCallback<SpliceMessage.BulkWriteResponse> done){
        try{
            byte[] bytes=bulkWrites(request.getBytes().toByteArray());
            SpliceMessage.BulkWriteResponse response =SpliceMessage.BulkWriteResponse.newBuilder()
                    .setBytes(ZeroCopyLiteralByteString.wrap(bytes)).build();
            done.run(response);
        }catch(IOException e){
            controller.setFailed(e.getMessage());
        }
    }

    @Override
    public Service getService(){
        return this;
    }

    @Override
    public void stop(CoprocessorEnvironment env){
        if(writePipeline!=null){
            writePipeline.close();
            PipelineDriver.driver().deregisterPipeline(((RegionCoprocessorEnvironment)env).getRegion().getRegionNameAsString());
        }
    }

    @Override
    public BulkWritesResult bulkWrite(BulkWrites bulkWrites) throws IOException{
        return pipelineWriter.bulkWrite(bulkWrites);
    }

    @Override
    public byte[] bulkWrites(byte[] bulkWriteBytes) throws IOException{
        assert bulkWriteBytes!=null;
        BulkWrites bulkWrites=compressor.decompress(bulkWriteBytes,BulkWrites.class);
        return compressor.compress(bulkWrite(bulkWrites));
    }
}