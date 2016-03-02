package com.splicemachine.derby.lifecycle;

import com.google.common.base.Function;
import com.splicemachine.access.api.PartitionCreator;
import com.splicemachine.lifecycle.DatabaseLifecycleManager;
import com.splicemachine.lifecycle.PipelineLoadService;
import com.splicemachine.pipeline.MPipelineEnv;
import com.splicemachine.pipeline.PipelineDriver;
import com.splicemachine.pipeline.PipelineEnvironment;
import com.splicemachine.pipeline.contextfactory.ContextFactoryDriver;
import com.splicemachine.si.MemSIEnvironment;
import com.splicemachine.storage.MServerControl;
import com.splicemachine.storage.Partition;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/12/16
 */
public class PipelinePartitionCreator implements PartitionCreator{
    private static volatile PipelineEnvironment env;
    private PartitionCreator baseCreator;

    public PipelinePartitionCreator(PartitionCreator baseCreator){
        this.baseCreator=baseCreator;
    }

    public PartitionCreator withName(String name){
        baseCreator=baseCreator.withName(name);
        try{
            //noinspection ResultOfMethodCallIgnored
            Long.parseLong(name);
        }catch(NumberFormatException nfe){
            return baseCreator;
        }
        return this;
    }

    public PartitionCreator withCoprocessor(String coprocessor) throws IOException{
        baseCreator = baseCreator.withCoprocessor(coprocessor);
        return this;
    }

    @Override
    public PartitionCreator withDisplayNames(String[] displayNames){
        baseCreator = baseCreator.withDisplayNames(displayNames);
        return this;
    }

    @Override
    public PartitionCreator withPartitionSize(long partitionSize){
        baseCreator =baseCreator.withPartitionSize(partitionSize);
        return this;
    }

    public Partition create() throws IOException{
        Partition p =baseCreator.create(); //create the base table
        long cId;
        try{
            cId = Long.parseLong(p.getName());
        }catch(NumberFormatException nfe){
            return p;
        }

        //register the pipeline
        try{
            final PipelineLoadService<Object> service=new PipelineLoadService<Object>(MServerControl.INSTANCE,p,cId){
                @Override
                protected Function<Object, String> getStringParsingFunction(){
                    return new Function<Object, String>(){
                        @Nullable @Override public String apply(Object o){ return (String)o; }
                    };
                }

                @Override
                protected PipelineEnvironment loadPipelineEnvironment(ContextFactoryDriver cfDriver) throws IOException{
                    PipelineEnvironment pe = env;
                    if(pe==null){
                        synchronized(PipelineEnvironment.class){
                            pe = env;
                            if(pe==null){
                                pe = pipelineEnv = env =new MPipelineEnv(MemSIEnvironment.INSTANCE);
                                PipelineDriver.loadDriver(pe);
                            }
                        }
                    }
                    return pe;
                }
            };
            DatabaseLifecycleManager.manager().registerGeneralService(service);
        }catch(Exception e){
            throw new IOException(e);
        }
        return p;
    }
}

