package com.splicemachine.lifecycle;

import com.google.common.base.Function;
import com.splicemachine.access.api.ServerControl;
import com.splicemachine.lifecycle.DatabaseLifecycleService;
import com.splicemachine.pipeline.*;
import com.splicemachine.pipeline.contextfactory.ContextFactoryDriver;
import com.splicemachine.pipeline.contextfactory.WriteContextFactory;
import com.splicemachine.pipeline.contextfactory.WriteContextFactoryManager;
import com.splicemachine.pipeline.utils.PipelineCompressor;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.Partition;

import javax.management.MBeanServer;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/12/16
 */
public abstract class PipelineLoadService<TableNameInfo> implements DatabaseLifecycleService{
    private final ServerControl serverControl;
    private final Partition basePartition;
    private final long conglomId;

    protected PipelineEnvironment pipelineEnv;
    private PipelineCompressor compressor;
    private PipelineWriter pipelineWriter;
    private PartitionWritePipeline writePipeline;

    public PipelineLoadService(ServerControl serverControl,Partition basePartition,long conglomId){
        this.serverControl=serverControl;
        this.basePartition=basePartition;
        this.conglomId=conglomId;
    }

    @Override
    public void start() throws Exception{
        ContextFactoryDriver cfDriver=ContextFactoryDriverService.loadDriver();
        pipelineEnv=loadPipelineEnvironment(cfDriver);
        final PipelineDriver pipelineDriver=pipelineEnv.getPipelineDriver();
        compressor=pipelineDriver.compressor();
        pipelineWriter=pipelineDriver.writer();
        final SIDriver siDriver=pipelineEnv.getSIDriver();
        WriteContextFactory factory=
                WriteContextFactoryManager.getWriteContext(conglomId,pipelineEnv.configuration(),
                        siDriver.getTableFactory(),
                        pipelineDriver.exceptionFactory(),
                        getStringParsingFunction(),
                        pipelineDriver.getContextFactoryLoader(conglomId)
                );
        factory.prepare();

        TransactionalRegion txnRegion=siDriver.transactionalPartition(conglomId,basePartition);
        writePipeline=new PartitionWritePipeline(serverControl,
                basePartition,
                factory,
                txnRegion,
                pipelineDriver.meter(),pipelineDriver.exceptionFactory());
        pipelineDriver.registerPipeline(basePartition.getName(),writePipeline);
    }

    public PipelineCompressor getCompressor(){
        return compressor;
    }

    public PipelineWriter getPipelineWriter(){
        return pipelineWriter;
    }

    public PartitionWritePipeline getWritePipeline(){
        return writePipeline;
    }

    protected abstract Function<TableNameInfo,String> getStringParsingFunction();

    protected abstract PipelineEnvironment loadPipelineEnvironment(ContextFactoryDriver cfDriver) throws IOException;

    @Override
    public void registerJMX(MBeanServer mbs) throws Exception{
        if(pipelineEnv!=null)
            pipelineEnv.getPipelineDriver().registerJMX(mbs);
    }
}
