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

package com.splicemachine.derby.ddl;

import org.spark_project.guava.base.Function;
import org.spark_project.guava.collect.Lists;
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
