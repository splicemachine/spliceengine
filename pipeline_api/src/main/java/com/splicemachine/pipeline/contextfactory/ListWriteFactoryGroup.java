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

package com.splicemachine.pipeline.contextfactory;

import com.splicemachine.pipeline.context.PipelineWriteContext;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author Scott Fines
 *         Date: 12/22/15
 */
@ThreadSafe
public class ListWriteFactoryGroup implements WriteFactoryGroup{
    private final List<LocalWriteFactory> factories;

    public ListWriteFactoryGroup(){
        this.factories=new CopyOnWriteArrayList<>();
    }
    public ListWriteFactoryGroup(List<LocalWriteFactory> factories){
        this.factories=factories;
    }

    @Override
    public void addFactory(LocalWriteFactory writeFactory){
        factories.add(writeFactory);
    }

    @Override
    public void addFactories(PipelineWriteContext context,boolean keepState,int expectedWrites) throws IOException{
        for(LocalWriteFactory lwf:factories){
            lwf.addTo(context,keepState,expectedWrites);
        }
    }

    @Override
    public void replace(LocalWriteFactory newFactory){
        synchronized(factories){
            for(int i=0;i<factories.size();i++){
                LocalWriteFactory localWriteFactory=factories.get(i);
                if(localWriteFactory.equals(newFactory)){
                    if(localWriteFactory.canReplace(newFactory)){
                        localWriteFactory.replace(newFactory);
                    }else{
                        factories.set(i,newFactory);
                    }
                    return;
                }
            }
            factories.add(newFactory);
        }
    }

    public List<LocalWriteFactory> list(){
        return factories;
    }

    @Override
    public void clear(){
        factories.clear();
    }

    @Override
    public boolean isEmpty(){
        return factories.isEmpty();
    }
}
