package com.splicemachine.derby.ddl;


import com.splicemachine.ddl.DDLMessage.*;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Scott Fines
 *         Date: 9/8/15
 */
class CountingListener implements DDLWatcher.DDLListener{
    private int startGlobalCount = 0;
    private int endGlobalCount = 0;

    private Map<DDLChange,Integer> countMap = new IdentityHashMap<DDLChange,Integer>(){
        @Override
        public Integer get(Object key){
            Integer integer=super.get(key);
            if(integer==null) integer = 0;
            return integer;
        }
    };

    private Set<DDLChange> failedChanges =Collections.newSetFromMap(new IdentityHashMap<DDLChange, Boolean>());

    public int getStartGlobalCount(){
        return startGlobalCount;
    }

    public int getEndGlobalCount(){
        return endGlobalCount;
    }

    public int getCount(DDLChange change){
        return countMap.get(change);
    }

    @Override public void startGlobalChange(){
        startGlobalCount++;
        assertEquals("Initiated change more than once!",1,startGlobalCount);
    }

    @Override public void finishGlobalChange(){
        endGlobalCount++;
        assertEquals("Initiated change more than once!",1,endGlobalCount);
    }

    @Override
    public void startChange(DDLChange change){
        Integer startCount=countMap.get(change);
        startCount++;
        assertEquals("Initiated change more than once!",1,startCount.intValue());
        countMap.put(change,startCount);
    }

    @Override
    public void changeSuccessful(String changeId){
        DDLChange change = null;
        for(Map.Entry<DDLChange,Integer> changeEntry:countMap.entrySet()){
            DDLChange key=changeEntry.getKey();
            if(key.getChangeId().equals(changeId)){
                change = key;
                break;
            }
        }
        assertNotNull("No change found!",change);
        Integer integer=countMap.get(change);
        assertEquals("Incorrect finish count!",1,integer.intValue());
        countMap.remove(change);
    }

    @Override
    public void changeFailed(String changeId){
        DDLChange change = null;
        for(Map.Entry<DDLChange,Integer> changeEntry:countMap.entrySet()){
            DDLChange key=changeEntry.getKey();
            if(key.getChangeId().equals(changeId)){
                change = key;
                break;
            }
        }
        failedChanges.add(change);
    }

    public boolean isFailed(DDLChange testChange){
        return failedChanges.contains(testChange);
    }
}
