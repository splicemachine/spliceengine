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

package com.splicemachine.derby.lifecycle;

import org.spark_project.guava.base.Function;
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

