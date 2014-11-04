package com.splicemachine.tools;

import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Debug Utility that will check if a particular entity has been added before.
 *
 * Watch out! This uses a lot of memory! If you turn it on in Production, Scott will
 * Hunt you down and give you a ringing dent alongside your left ear!
 *
 * @author Scott Fines
 * Created on: 8/6/13
 */
public class UniqueChecker<T> {
    private final ConcurrentSkipListMap<T,Long> backData;

    public UniqueChecker(Comparator<T> comparator) {
        this.backData = new ConcurrentSkipListMap<T, Long>(comparator);
    }

    public long check(T entity){
        long time = System.currentTimeMillis();
        Long otherTime = backData.putIfAbsent(entity, time);
        if(otherTime==null) return -1l;
        return otherTime;
    }
}
