package com.splicemachine.stats.histogram;

import com.splicemachine.stats.ByteUpdateable;

/**
 * A simple equi-width histogram for bytes, which keeps
 * a single long counter for each byte value it encounters.
 *
 * This implementation stores a single counter for each possible
 * byte value, which means that it uses 256*8=2KB of heap (plus
 * object overhead).
 *
 * @author Scott Fines
 *         Date: 12/1/14
 */
public class ByteHistogram implements ByteUpdateable,ByteRangeQuerySolver{

    private final long[] counters = new long[256];

    @Override
    public byte max() {
        return (byte)0xEF;
    }

    @Override
    public byte min() {
        return (byte)0x80;
    }

    @Override
    public long equal(byte value) {
        return counters[value &0xff];
    }

    @Override
    public long between(byte startValue, byte endValue, boolean inclusiveStart, boolean inclusiveEnd) {
        if(startValue==endValue){
            if(!inclusiveStart) return 0l;
            return equal(startValue);
        }
        /*
         * We store our counters in unsigned order, which means that -128 comes AFTER 127, rather than
         * BEFORE 0. As a result, we loop around the counters. We must therefore be careful about
         * which counters we select.
         */
        int start = startValue & 0xFF;
        if(!inclusiveStart)
            start = (start+1)&255; //255 = length(counters)-1

        int available = Math.abs(endValue-startValue);
        if(inclusiveEnd)
            available++;

        long sum = 0l;
        int pos = start;
        while(available>0){
            sum+=counters[pos];
            pos=(pos+1)&255;
            available--;
        }
        return sum;
    }

    @Override public long after(byte n, boolean equals) { return between(n,max(),equals,true); }
    @Override public long before(byte n, boolean equals) { return between(min(),n,true,equals); }
    @Override public void update(byte item) { update(item,1l); }

    @Override
    public void update(byte item, long count) {
        counters[item & 0xFF]+=count;
    }

    @Override
    public long getNumElements(Byte start, Byte end, boolean inclusiveStart, boolean inclusiveEnd) {
        byte s = start==null? min(): start.byteValue();
        byte e = end==null? max(): end.byteValue();
        return between(s,e,inclusiveStart,inclusiveEnd);
    }

    @Override public Byte getMin() { return min(); }
    @Override public Byte getMax() { return max(); }

    @Override
    public void update(Byte item) {
        assert item!=null: "Cannot update a null item!";
        update(item.byteValue(),1l);
    }

    @Override
    public void update(Byte item, long count) {
        assert item!=null: "Cannot update a null item!";
        update(item.byteValue(),count);
    }

    public static void main(String... args) throws Exception{

        byte[] signal = new byte[]{0,1,2,3,4,5,6,7};
//        int[] count = new int[]{1,3,5,11,12,13,0,1};
        int[] count = new int[]{2,2,0,2,3,5,4,4};
        ByteHistogram histogram = new ByteHistogram();
        for(int i=0;i<signal.length;i++){
            histogram.update(signal[i], count[i]);
        }

        int rangeSize=1;
        for(byte i=-128;i<128-rangeSize;i+=rangeSize){
            System.out.printf("[%d,%d),est=%d%n", i, i + rangeSize,
                    histogram.between(i, (byte) (i + rangeSize), true, false));
        }
    }

}
