package com.splicemachine.stats.order;

import com.splicemachine.stats.FloatUpdateable;

/**
 * @author Scott Fines
 *         Date: 2/24/15
 */
public class FloatMinMaxCollector implements MinMaxCollector<Float>,FloatUpdateable {
    private float currentMin;
    private long currentMinCount;
    private float currentMax;
    private long currentMaxCount;

    @Override
    public void update(float item, long count) {
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

    @Override public void update(float item) { update(item,1l); }

    @Override
    public void update(Float item) {
        assert item!=null: "Cannot order null elements";
        update(item.floatValue());
    }

    @Override
    public void update(Float item, long count) {
        assert item!=null: "Cannot order null elements!";
        update(item.floatValue(),count);
    }

    @Override public Float minimum() { return currentMin; }
    @Override public Float maximum() { return currentMax; }
    @Override public long minCount() { return currentMinCount; }
    @Override public long maxCount() { return currentMaxCount; }

    public float max(){ return currentMax; }
    public float min(){ return currentMin; }

    public static FloatMinMaxCollector newInstance() {
        FloatMinMaxCollector collector = new FloatMinMaxCollector();
        collector.currentMin = Float.MAX_VALUE;
        collector.currentMax = Float.MIN_VALUE;
        return collector;
    }
}
