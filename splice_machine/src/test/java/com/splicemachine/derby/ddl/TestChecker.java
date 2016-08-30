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

package com.splicemachine.derby.ddl;

import com.google.common.base.Function;
import org.sparkproject.guava.collect.Lists;
import com.splicemachine.ddl.DDLMessage.*;
import com.splicemachine.utils.Pair;
import org.junit.Assert;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 9/8/15
 */
class TestChecker implements DDLWatchChecker{
    private List<DDLChange> changes;

    private List<CommunicationListener> listeners;

    private final CommunicationListener communicator;
    private final String id;

    public TestChecker(){
        this(new ArrayList<DDLChange>());
    }

    public TestChecker(List<DDLChange> changes){
       this("testChecker",changes,null);
    }

    public TestChecker(String id,List<DDLChange> changes,CommunicationListener communicator){
        this.changes=changes;
        this.listeners = new ArrayList<>();
        this.communicator = communicator;
        this.id = id;
    }

    @Override public boolean initialize(CommunicationListener listener) throws IOException{ return true; }

    public void addChange(DDLChange change){
        this.changes.add(change);
        Iterator<CommunicationListener> iter = listeners.iterator();
        while(iter.hasNext()){
            iter.next().onCommunicationEvent(change.getChangeId());
            iter.remove();
        }
    }

    public void removeChange(String changeId){
        Iterator<DDLChange> changeIter = changes.iterator();
        while(changeIter.hasNext()){
            DDLChange change = changeIter.next();
            if(change.getChangeId().equals(changeId)){
                changeIter.remove();
                return;
            }
        }
    }

    @Override
    public Collection<String> getCurrentChangeIds() throws IOException{
        return Lists.transform(changes,new Function<DDLChange, String>(){
            @Nullable
            @Override
            public String apply(DDLChange ddlChange){
                return ddlChange.getChangeId();
            }
        });
    }

    @Override
    public DDLChange getChange(String changeId) throws IOException{
        for(DDLChange change:changes){
            if(change.getChangeId().equals(changeId)) return change;
        }
        Assert.fail("Did not find the specified change!");
        return null;
    }

    @Override
    public void notifyProcessed(Collection<Pair<DDLChange,String>> processedChanges) throws IOException{
        for(Pair<DDLChange,String> change:processedChanges){
            removeChange(change.getFirst().getChangeId());
            if(communicator!=null){
                communicator.onCommunicationEvent(change.getFirst().getChangeId()+"-"+id); //notify the other side if we have one
            }
        }
    }

    @Override
    public void killDDLTransaction(String key){
        Iterator<DDLChange> changeIter = changes.iterator();
        while(changeIter.hasNext()){
            DDLChange change = changeIter.next();
            if(change.getChangeId().equals(key)){
                changeIter.remove();
                return;
            }
        }
        Assert.fail("Did not find the specified change!");
    }
}
