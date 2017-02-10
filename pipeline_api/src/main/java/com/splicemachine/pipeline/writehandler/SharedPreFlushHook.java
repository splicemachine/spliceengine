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

package com.splicemachine.pipeline.writehandler;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.callbuffer.PreFlushHook;
import com.splicemachine.pipeline.context.WriteContext;
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

    private List<Pair<WriteContext, ObjectObjectOpenHashMap<KVPair, KVPair>>> sharedMainMutationList=new ArrayList<>();

    @Override
    public Collection<KVPair> transform(Collection<KVPair> buffer) throws Exception{
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"transform buffer rows=%d",buffer.size());
        Collection<KVPair> newList=new ArrayList<>(buffer.size());
        for(KVPair indexPair : buffer){
            for(Pair<WriteContext, ObjectObjectOpenHashMap<KVPair, KVPair>> pair : sharedMainMutationList){
                KVPair base=pair.getSecond().get(indexPair);
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

    public void registerContext(WriteContext context,ObjectObjectOpenHashMap<KVPair, KVPair> indexToMainMutationMap){
        sharedMainMutationList.add(Pair.newPair(context,indexToMainMutationMap));
    }

    public void cleanup(){
        sharedMainMutationList.clear();
        sharedMainMutationList=null;
    }

}