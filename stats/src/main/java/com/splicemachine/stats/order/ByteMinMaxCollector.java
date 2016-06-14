package com.splicemachine.stats.order;

import com.splicemachine.stats.ByteUpdateable;

/**
 * @author Scott Fines
 *         Date: 2/24/15
 */
public class ByteMinMaxCollector implements MinMaxCollector<Byte>,ByteUpdateable {
    private byte currentMin;
    private long currentMinCount;
    private byte currentMax;
    private long currentMaxCount;

    @Override
    public void update(byte item, long count) {
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

    @Override public void update(byte item) { update(item,1l); }

    @Override
    public void update(Byte item) {
        assert item!=null: "Cannot order null elements";
        update(item.byteValue());
    }

    @Override
    public void update(Byte item, long count) {
        assert item!=null: "Cannot order null elements!";
        update(item.byteValue(),count);
    }

    @Override public Byte minimum() { return currentMin; }
    @Override public Byte maximum() { return currentMax; }
    @Override public long minCount() { return currentMinCount; }
    @Override public long maxCount() { return currentMaxCount; }

    public byte max(){ return currentMax; }
    public byte min(){ return currentMin; }

    public static ByteMinMaxCollector newInstance() {
        ByteMinMaxCollector collector = new ByteMinMaxCollector();
        collector.currentMin = Byte.MAX_VALUE;
        collector.currentMax = Byte.MIN_VALUE;
        return collector;
    }
}
