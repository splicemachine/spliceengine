package com.splicemachine.pipeline.testsetup;

import com.splicemachine.access.api.PartitionCreator;
import com.splicemachine.lifecycle.DatabaseLifecycleManager;
import com.splicemachine.pipeline.ManualContextFactoryLoader;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.client.WriteCoordinator;
import com.splicemachine.pipeline.contextfactory.ContextFactoryDriver;
import com.splicemachine.pipeline.contextfactory.ContextFactoryLoader;
import com.splicemachine.si.testsetup.HBaseSITestEnv;
import com.splicemachine.derby.hbase.HBasePipelineEnvironment;
import com.splicemachine.derby.hbase.SpliceIndexEndpoint;
import com.splicemachine.derby.hbase.SpliceIndexObserver;
import org.apache.log4j.Level;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class HPipelineTestEnv extends HBaseSITestEnv implements PipelineTestEnv{
    private final ConcurrentMap<Long,ContextFactoryLoader> contextFactoryLoaderMap = new ConcurrentHashMap<>();
    private final HBasePipelineEnvironment env;

    public HPipelineTestEnv() throws IOException{
        super(Level.WARN); //don't create SI tables, we'll manually add them once the driver is setup
        this.env = HBasePipelineEnvironment.loadEnvironment(super.getClock(),new ContextFactoryDriver(){
            @Override
            public ContextFactoryLoader getLoader(long conglomerateId){
                ContextFactoryLoader cfl = contextFactoryLoaderMap.get(conglomerateId);
                if(cfl==null){
                    cfl = new ManualContextFactoryLoader();
                    ContextFactoryLoader old=contextFactoryLoaderMap.putIfAbsent(conglomerateId,cfl);
                    if(old!=null)
                        cfl = old;
                }
                return cfl;
            }
        });
        DatabaseLifecycleManager.manager().start(); //start the database
    }

    @Override
    public WriteCoordinator writeCoordinator(){
        return env.getPipelineDriver().writeCoordinator();
    }

    @Override
    public ContextFactoryLoader contextFactoryLoader(long conglomerateId){
        return env.contextFactoryDriver().getLoader(conglomerateId);
    }

    @Override
    public PipelineExceptionFactory pipelineExceptionFactory(){
        return env.pipelineExceptionFactory();
    }

    @Override
    public void initialize() throws IOException{

    }

    @Override
    protected void addCoprocessors(PartitionCreator creator) throws IOException{
        creator.withCoprocessor(SpliceIndexEndpoint.class.getName())
                .withCoprocessor(SpliceIndexObserver.class.getName());
    }
}
