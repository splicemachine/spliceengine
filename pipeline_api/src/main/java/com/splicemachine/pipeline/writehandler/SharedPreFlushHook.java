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

package com.splicemachine.pipeline.writehandler;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.splicemachine.pipeline.callbuffer.PreFlushHook;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.storage.Record;
import com.splicemachine.utils.Pair;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Attempt to share the call buffer between contexts using the context to lookup pre flush values...
 */
public class SharedPreFlushHook implements PreFlushHook{

    private static final Logger LOG=Logger.getLogger(SharedPreFlushHook.class);

    private List<Pair<WriteContext, ObjectObjectOpenHashMap<Record, Record>>> sharedMainMutationList=new ArrayList<>();

    @Override
    public Collection<Record> transform(Collection<Record> buffer) throws Exception{
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"transform buffer rows=%d",buffer.size());
        Collection<Record> newList=new ArrayList<>(buffer.size());
        for(Record indexPair : buffer){
            for(Pair<WriteContext, ObjectObjectOpenHashMap<Record, Record>> pair : sharedMainMutationList){
                Record base=pair.getSecond().get(indexPair);
                if(base!=null){
                    if(pair.getFirst().canRun(base))
                        newList.add(indexPair);
                    break;
                }
            }
        }
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"transform returns buffer rows=%d",newList.size());
        return newList;
    }

    public void registerContext(WriteContext context,ObjectObjectOpenHashMap<Record, Record> indexToMainMutationMap){
        sharedMainMutationList.add(Pair.newPair(context,indexToMainMutationMap));
    }

    public void cleanup(){
        sharedMainMutationList.clear();
        sharedMainMutationList=null;
    }

}