package com.splicemachine.stats.order;

import com.splicemachine.stats.LongUpdateable;

/**
 * @author Scott Fines
 *         Date: 2/24/15
 */
public class LongMinMaxCollector implements MinMaxCollector<Long>,LongUpdateable{
    private long currentMin;
    private long currentMinCount;
    private long currentMax;
    private long currentMaxCount;

    @Override
    public void update(long item, long count) {
        if(item==currentMin)
            currentMinCount+=count;
        else if(currentMin>item) {
            currentMin = item;
            currentMinCount = count;
        }
        if(item==currentMax)
            currentMaxCount+=count;
        else if(currentMax<item) {
            currentMax = item;
            currentMaxCount = count;
        }
    }

    @Override public void update(long item) { update(item,1l);
    }

    @Override
    public void update(Long item) {
        assert item!=null: "Cannot order null elements";
        update(item.longValue());
    }

    @Override
    public void update(Long item, long count) {
        assert item!=null: "Cannot order null elements!";
        update(item.longValue(),count);
    }

    @Override public Long minimum() { return currentMin; }
    @Override public Long maximum() { return currentMax; }
    @Override public long minCount() { return currentMinCount; }
    @Override public long maxCount() { return currentMaxCount; }

    public long max(){ return currentMax; }
    public long min(){ return currentMin; }

    public static LongMinMaxCollector newInstance() {
        LongMinMaxCollector collector = new LongMinMaxCollector();
        collector.currentMin = Long.MAX_VALUE;
        collector.currentMax = Long.MIN_VALUE;
        return collector;
    }
}
