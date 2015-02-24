package com.splicemachine.stats.order;

import com.splicemachine.stats.Updateable;

/**
 * @author Scott Fines
 *         Date: 2/24/15
 */
public class ComparableMinMaxCollector<T extends Comparable<T>> implements MinMaxCollector<T>,Updateable<T> {
    private T currentMin;
    private long minCount;
    private T currentMax;
    private long maxCount;

    @Override public T minimum() { return currentMin; }
    @Override public T maximum() { return currentMax; }

    @Override public long minCount() { return minCount; }
    @Override public long maxCount() { return maxCount; }

    @Override public void update(T item) { update(item,1l); }

    @Override
    public void update(T item, long count) {
        if(currentMin==null){
            currentMin = item;
            minCount = count;
        }else{
            int compare = currentMin.compareTo(item);
            if(compare==0)
                minCount+=count;
            else if(compare<0) {
                currentMin = item;
                minCount = count;
            }
        }

        if(currentMax==null){
            currentMax = item;
            maxCount = count;
        }else{
            int compare = currentMax.compareTo(item);
            if(compare==0)
                maxCount+=count;
            else if(compare>0) {
                currentMax = item;
                maxCount = count;
            }
        }
    }
}
