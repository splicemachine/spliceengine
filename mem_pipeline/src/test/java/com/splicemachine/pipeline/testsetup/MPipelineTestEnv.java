/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.pipeline.testsetup;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.spark_project.guava.base.Function;

import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.configuration.ConfigurationBuilder;
import com.splicemachine.access.configuration.ConfigurationSource;
import com.splicemachine.access.configuration.HConfigurationDefaultsList;
import com.splicemachine.access.configuration.PipelineConfiguration;
import com.splicemachine.access.util.ReflectingConfigurationSource;
import com.splicemachine.pipeline.ManualContextFactoryLoader;
import com.splicemachine.pipeline.MappedPipelineFactory;
import com.splicemachine.pipeline.PartitionWritePipeline;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.api.Writer;
import com.splicemachine.pipeline.client.Monitor;
import com.splicemachine.pipeline.client.WriteCoordinator;
import com.splicemachine.pipeline.context.NoOpPipelineMeter;
import com.splicemachine.pipeline.contextfactory.ContextFactoryLoader;
import com.splicemachine.pipeline.contextfactory.WriteContextFactory;
import com.splicemachine.pipeline.contextfactory.WriteContextFactoryManager;
import com.splicemachine.pipeline.mem.DirectBulkWriterFactory;
import com.splicemachine.pipeline.mem.DirectPipelineExceptionFactory;
import com.splicemachine.pipeline.traffic.AtomicSpliceWriteControl;
import com.splicemachine.pipeline.writer.SynchronousBucketingWriter;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.MemSITestEnv;
import com.splicemachine.si.api.server.TransactionalRegionFactory;
import com.splicemachine.si.impl.server.SITransactor;
import com.splicemachine.storage.MServerControl;
import com.splicemachine.storage.Partition;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class MPipelineTestEnv extends MemSITestEnv implements PipelineTestEnv{
    private final WriteCoordinator writeCoordinator;
    private final Map<Long,ContextFactoryLoader> contextFactoryLoaderMap = new ConcurrentHashMap<>();
    private final MappedPipelineFactory pipelineFactory;
    private final SConfiguration config;
    private final TransactionalRegionFactory trf;

    @SuppressWarnings("unchecked")
    public MPipelineTestEnv() throws IOException{
        this.config = new ConfigurationBuilder().build(new HConfigurationDefaultsList().addConfig(new MPipelineTestConfig()),
                                                       new ReflectingConfigurationSource());

         trf = buildTransactionalRegionFactory();
        pipelineFactory = new MappedPipelineFactory();
        DirectBulkWriterFactory bwf = new DirectBulkWriterFactory(pipelineFactory,
                new AtomicSpliceWriteControl(Integer.MAX_VALUE,Integer.MAX_VALUE,Integer.MAX_VALUE,Integer.MAX_VALUE),
                DirectPipelineExceptionFactory.INSTANCE,NoOpPipelineMeter.INSTANCE);
        Writer writer = new SynchronousBucketingWriter(bwf,DirectPipelineExceptionFactory.INSTANCE,
                getTableFactory(),getClock());
        Monitor monitor = new Monitor(Long.MAX_VALUE,Integer.MAX_VALUE,10,10L,Integer.MAX_VALUE);
        writeCoordinator = new WriteCoordinator(writer,writer,monitor,getTableFactory(),
                DirectPipelineExceptionFactory.INSTANCE,null);
        bwf.setWriteCoordinator(writeCoordinator);
    }

    @Override
    public void initialize() throws IOException{
        createTransactionalTable(Bytes.toBytes(Long.toString(1292)));
        personPartition = getTableFactory().getTable(Long.toString(1292));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void createTransactionalTable(byte[] tableNameBytes) throws IOException{
        super.createTransactionalTable(tableNameBytes);
        PartitionFactory tableFactory=getTableFactory();
        Partition table=tableFactory.getTable(tableNameBytes);
        long conglomerateId=Long.parseLong(Bytes.toString(tableNameBytes));
        WriteContextFactory wcf =WriteContextFactoryManager.getWriteContext(conglomerateId,
                config,tableFactory,pipelineExceptionFactory(),new Function<Object,String>(){
                    @Override
                    public String apply(Object input){
                        return (String)input;
                    }
                },contextFactoryLoader(conglomerateId));
        wcf.prepare();
        PartitionWritePipeline pwp = new PartitionWritePipeline(MServerControl.INSTANCE,table,
                wcf,
                trf.newRegion(table),
                NoOpPipelineMeter.INSTANCE,pipelineExceptionFactory());
        pipelineFactory.registerPipeline(Bytes.toString(tableNameBytes),pwp);
    }

    @Override
    public WriteCoordinator writeCoordinator(){
        return writeCoordinator;
    }

    @Override
    public ContextFactoryLoader contextFactoryLoader(long conglomerateId){
        ContextFactoryLoader contextFactoryLoader=contextFactoryLoaderMap.get(conglomerateId);
        if(contextFactoryLoader==null){
            contextFactoryLoader = new ManualContextFactoryLoader();
            contextFactoryLoaderMap.put(conglomerateId,contextFactoryLoader);
        }

        return contextFactoryLoader;
    }

    @Override
    public PipelineExceptionFactory pipelineExceptionFactory(){
        return DirectPipelineExceptionFactory.INSTANCE;
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    @SuppressWarnings("unchecked")
    private TransactionalRegionFactory buildTransactionalRegionFactory(){
        SITransactor transactor = new SITransactor(getTxnStore(),
                getOperationFactory(),getOperationStatusFactory(),getExceptionFactory());
        return new TransactionalRegionFactory(this.getTxnStore(),
                transactor,getOperationFactory());
    }

    //==============================================================================================================
    // private helper classes
    //==============================================================================================================
    private static class MPipelineTestConfig extends PipelineConfiguration {

        @Override
        public void setDefaults(ConfigurationBuilder builder, ConfigurationSource configurationSource) {
            super.setDefaults(builder, configurationSource);
            // Overwritten for test
            builder.startupLockWaitPeriod = Long.MAX_VALUE;
        }
    }
}
