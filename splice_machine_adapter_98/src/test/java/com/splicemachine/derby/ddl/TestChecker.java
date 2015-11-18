package com.splicemachine.derby.ddl;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.splicemachine.ddl.DDLMessage.*;
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
    public List<String> getCurrentChangeIds() throws IOException{
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
    public void notifyProcessed(Collection<DDLChange> processedChanges) throws IOException{
        for(DDLChange change:processedChanges){
            removeChange(change.getChangeId());
            if(communicator!=null){
                communicator.onCommunicationEvent(change.getChangeId()+"-"+id); //notify the other side if we have one
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
