package com.splicemachine.stats.order;

import com.splicemachine.stats.ShortUpdateable;

/**
 * @author Scott Fines
 *         Date: 2/24/15
 */
public class ShortMinMaxCollector implements MinMaxCollector<Short>,ShortUpdateable {
    private short currentMin;
    private long currentMinCount;
    private short currentMax;
    private long currentMaxCount;

    @Override
    public void update(short item, long count) {
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

    @Override public void update(short item) { update(item,1l); }

    @Override
    public void update(Short item) {
        assert item!=null: "Cannot order null elements";
        update(item.shortValue());
    }

    @Override
    public void update(Short item, long count) {
        assert item!=null: "Cannot order null elements!";
        update(item.shortValue(),count);
    }

    @Override public Short minimum() { return currentMin; }
    @Override public Short maximum() { return currentMax; }
    @Override public long minCount() { return currentMinCount; }
    @Override public long maxCount() { return currentMaxCount; }

    public short max(){ return currentMax; }
    public short min(){ return currentMin; }

    public static ShortMinMaxCollector newInstance() {
        ShortMinMaxCollector collector = new ShortMinMaxCollector();
        collector.currentMin = Short.MAX_VALUE;
        collector.currentMax = Short.MIN_VALUE;
        return collector;
    }
}
