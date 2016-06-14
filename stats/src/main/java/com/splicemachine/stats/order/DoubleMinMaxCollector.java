package com.splicemachine.stats.order;

import com.splicemachine.stats.DoubleUpdateable;

/**
 * @author Scott Fines
 *         Date: 2/24/15
 */
public class DoubleMinMaxCollector implements MinMaxCollector<Double>,DoubleUpdateable {

    private double currentMin;
    private long currentMinCount;
    private double currentMax;
    private long currentMaxCount;

    @Override
    public void update(double item, long count) {
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

    @Override public void update(double item) { update(item,1l); }

    @Override
    public void update(Double item) {
        assert item!=null: "Cannot order null elements";
        update(item.doubleValue());
    }

    @Override
    public void update(Double item, long count) {
        assert item!=null: "Cannot order null elements!";
        update(item.doubleValue(),count);
    }

    @Override public Double minimum() { return currentMin; }
    @Override public Double maximum() { return currentMax; }
    @Override public long minCount() { return currentMinCount; }
    @Override public long maxCount() { return currentMaxCount; }

    public double max(){ return currentMax; }
    public double min(){ return currentMin; }

    public static DoubleMinMaxCollector newInstance() {
        DoubleMinMaxCollector collector = new DoubleMinMaxCollector();
        collector.currentMin = Double.MAX_VALUE;
        collector.currentMax = Double.MIN_VALUE;
        return collector;
    }
}
