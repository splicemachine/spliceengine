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

package com.splicemachine.lifecycle;

import org.spark_project.guava.base.Function;
import com.splicemachine.access.api.ServerControl;
import com.splicemachine.pipeline.*;
import com.splicemachine.pipeline.contextfactory.ContextFactoryDriver;
import com.splicemachine.pipeline.contextfactory.ContextFactoryLoader;
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
    private ContextFactoryLoader ctxLoader;

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
        ctxLoader=pipelineDriver.getContextFactoryLoader(conglomId);
        WriteContextFactory factory=
                WriteContextFactoryManager.getWriteContext(conglomId,pipelineEnv.configuration(),
                        siDriver.getTableFactory(),
                        pipelineDriver.exceptionFactory(),
                        getStringParsingFunction(),
                       ctxLoader
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

    @Override
    public void shutdown() throws Exception{
        if(ctxLoader!=null)
            ctxLoader.close();
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
