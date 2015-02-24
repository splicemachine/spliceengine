package com.splicemachine.stats.order;

import com.splicemachine.primitives.ByteComparator;

import java.nio.ByteBuffer;

/**
 * @author Scott Fines
 *         Date: 2/24/15
 */
public class BufferMinMaxCollector implements BytesMinMaxCollector {
    private ByteBuffer currentMin;
    private long currentMinCount;
    private ByteBuffer currentMax;
    private long currentMaxCount;

    private final ByteComparator byteComparator;

    public BufferMinMaxCollector(ByteComparator byteComparator) {
        this.byteComparator = byteComparator;
    }

    @Override
    public void update(byte[] bytes, int offset, int length) {

    }

    @Override
    public void update(byte[] bytes, int offset, int length, long count) {
        if(currentMin==null) {
            currentMin = ByteBuffer.wrap(bytes, offset, length);
            currentMinCount = count;
        }else{
            int compare = byteComparator.compare(currentMin,bytes,offset,length);
            if(compare>0){
                currentMin = ByteBuffer.wrap(bytes, offset, length);
                currentMinCount = count;
            }else if(compare==0){
                currentMinCount += count;
            }
        }

        if(currentMax==null) {
            currentMax = ByteBuffer.wrap(bytes, offset, length);
            currentMaxCount = count;
        }else{
            int compare = byteComparator.compare(currentMax,bytes,offset,length);
            if(compare<0){
                currentMax = ByteBuffer.wrap(bytes, offset, length);
                currentMaxCount = count;
            }else if(compare==0){
                currentMaxCount += count;
            }
        }
    }

    @Override public void update(ByteBuffer bytes) { update(bytes,1l);  }

    @Override
    public void update(ByteBuffer bytes, long count) {
        if(currentMin==null) {
            currentMin = bytes;
            currentMinCount = count;
        }else{
            int compare = byteComparator.compare(currentMin,bytes);
            if(compare>0){
                currentMin = bytes;
                currentMinCount = count;
            }else if(compare==0){
                currentMinCount += count;
            }
        }

        if(currentMax==null) {
            currentMax = bytes;
            currentMaxCount = count;
        }else{
            int compare = byteComparator.compare(currentMax,bytes);
            if(compare<0){
                currentMax = bytes;
                currentMaxCount = count;
            }else if(compare==0){
                currentMaxCount += count;
            }
        }

    }

    @Override public void update(byte[] item) { update(item,0,item.length); }
    @Override public void update(byte[] item, long count) { update(item,0,item.length,count); }

    @Override public byte[] minimum() { return toArray(currentMin); }
    @Override public byte[] maximum() { return toArray(currentMax); }

    @Override public long minCount() { return currentMinCount; }
    @Override public long maxCount() { return currentMaxCount; }

    public ByteBuffer max(){ return currentMax; }
    public ByteBuffer min(){ return currentMin; }

    public static BufferMinMaxCollector newInstance(ByteComparator comparator) {
        return new BufferMinMaxCollector(comparator);
    }


    /* ****************************************************************************************************************/
    /*private helper methods*/
    private byte[] toArray(ByteBuffer currentMin) {
        currentMin.mark();
        byte[] copy = new byte[currentMin.remaining()];
        currentMin.get(copy);
        currentMin.reset();
        return copy;
    }
}
