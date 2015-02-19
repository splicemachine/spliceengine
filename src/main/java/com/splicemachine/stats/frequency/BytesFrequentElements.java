package com.splicemachine.stats.frequency;

import com.splicemachine.annotations.Untested;
import com.splicemachine.primitives.ByteComparator;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * @author Scott Fines
 *         Date: 12/5/14
 */
public class BytesFrequentElements implements FrequentElements<byte[]>{
    private static final Comparator<BytesFrequencyEstimate> naturalComparator = new Comparator<BytesFrequencyEstimate>() {
        @Override
        public int compare(BytesFrequencyEstimate o1, BytesFrequencyEstimate o2) {
            int compare = o1.compareTo(o2);
            if (compare != 0) return compare;
            return Long.signum(o1.count() - o2.count());
        }
    };

    private final NavigableSet<BytesFrequencyEstimate> elements;
    private final ByteComparator comparator;

    public BytesFrequentElements(Collection<BytesFrequencyEstimate> elements,ByteComparator comparator) {
        TreeSet<BytesFrequencyEstimate> elems = new TreeSet<BytesFrequencyEstimate>(naturalComparator);
        elems.addAll(elements);
        this.elements = elems;
        this.comparator = comparator;
    }

    public FrequencyEstimate<byte[]> countEqual(byte[] bytes){
       return countEqual(bytes,0,bytes.length);
    }

    public FrequencyEstimate<byte[]> countEqual(byte[] bytes, int offset, int length){
        for(BytesFrequencyEstimate estimate:elements){
            if(estimate.compare(bytes,offset,length)==0)
                return estimate;
        }
        return new ZeroBytes(bytes,offset,length);
    }

    public FrequencyEstimate<byte[]> countEqual(ByteBuffer buffer){
        for(BytesFrequencyEstimate estimate:elements){
            if(estimate.compare(buffer)==0)
                return estimate;
        }
        return new ZeroBuffer(buffer);
    }

    @Override
    public FrequencyEstimate<? extends byte[]> equal(byte[] item) {
        return countEqual(item,0,item.length);
    }

    @Untested
    @Override
    public Set<? extends FrequencyEstimate<byte[]>> frequentElementsBetween(byte[] start, byte[] stop, boolean includeMin, boolean includeStop) {
        return frequentBetween(start,0,start.length,
                stop,0,stop.length,
                includeMin,includeStop);
    }

    @Untested
    public Set<BytesFrequencyEstimate> frequentAfter(byte[] start, int startOffset,int startLength,
                                                   boolean includeStart){
        BytesFrequencyEstimate last = elements.last();
        int lastCompare = last.compare(start,startOffset,startLength);
        if(lastCompare<0 || (!includeStart && lastCompare==0)){
            //start happens after the end of the available data, so nothing is there
            return Collections.emptySet();
        }

        BytesFrequencyEstimate first = elements.first();
        int firstCompare = first.compare(start,startOffset,startLength);
        if(firstCompare>0 || (includeStart && firstCompare==0)){
            //the first element happens after our start, so everything is contained
            return elements;
        }

        BytesFrequencyEstimate floor = elements.ceiling(new ZeroBytes(start,startOffset,startLength));
        return elements.tailSet(floor,includeStart);
    }

    @Untested
    public Set<BytesFrequencyEstimate> frequentBefore(byte[] stop, boolean includeStop){
        return frequentBefore(stop,0,stop.length,includeStop);
    }

    @Untested
    public Set<BytesFrequencyEstimate> frequentBefore(byte[] stop, int stopOffset, int stopLength,
                                                      boolean includeStop){
        BytesFrequencyEstimate first = elements.first();
        int firstCompare = first.compare(stop,stopOffset,stopLength);
        if(firstCompare>0||(!includeStop && firstCompare==0)) {
            return Collections.emptySet();
        }
        BytesFrequencyEstimate last = elements.last();
        int lastCompare = last.compare(stop,stopOffset,stopLength);
        if(lastCompare<0){
            //last elements is before the stop, so return everything
            return elements;
        }else if(lastCompare==0 && includeStop) return elements;

        BytesFrequencyEstimate floor = elements.ceiling(new ZeroBytes(stop,stopOffset,stopLength));
        return elements.headSet(floor, includeStop);
    }

    @Untested
    public Set<BytesFrequencyEstimate> frequentBetween(byte[] start, int startOffset,int startLength,
                                              byte[] stop, int stopOffset, int stopLength,
                                              boolean includeStart, boolean includeStop){
        BytesFrequencyEstimate first = elements.first();
        int firstStopCompare = first.compare(stop,stopOffset,stopLength);
        if(firstStopCompare>0||(!includeStart && firstStopCompare==0)){
            //our entire range is outside the available values, so we are empty
            return Collections.emptySet();
        }
        int firstStartCompare = first.compare(start,startOffset,startLength);
        if(firstStartCompare>0||(includeStart && firstStartCompare==0))
            return frequentBefore(stop,stopOffset,stopLength,includeStop);

        BytesFrequencyEstimate last = elements.last();
        int lastStartCompare = last.compare(start,startOffset,startLength);
        if(lastStartCompare<0||(!includeStart && lastStartCompare==0))
            return Collections.emptySet();
        int lastStopCompare = last.compare(stop,stopOffset,stopLength);
        if(lastStopCompare<0 || (includeStop && lastStopCompare==0))
            return frequentAfter(start,startOffset,startLength,includeStart);

        ZeroBytes zb = new ZeroBytes(start,startOffset,startLength);
        BytesFrequencyEstimate startElem = elements.floor(zb);
        zb.set(stop,stopOffset,stopLength);
        BytesFrequencyEstimate stopElem = elements.floor(zb);
        return elements.subSet(startElem,includeStart,stopElem,includeStop);
    }

    @Untested
    public Set<BytesFrequencyEstimate> frequentBetween(ByteBuffer start,
                                                       ByteBuffer stop,
                                                       boolean includeStart, boolean includeStop){
        BytesFrequencyEstimate first = elements.first();
        int firstStopCompare = first.compare(stop);
        if(firstStopCompare>0||(!includeStart && firstStopCompare==0)){
            //our entire range is outside the available values, so we are empty
            return Collections.emptySet();
        }
        int firstStartCompare = first.compare(start);
        if(firstStartCompare>0||(includeStart && firstStartCompare==0))
            return frequentBefore(stop,includeStop);

        BytesFrequencyEstimate last = elements.last();
        int lastStartCompare = last.compare(start);
        if(lastStartCompare<0||(!includeStart && lastStartCompare==0))
            return Collections.emptySet();
        int lastStopCompare = last.compare(stop);
        if(lastStopCompare<0 || (includeStop && lastStopCompare==0))
            return frequentAfter(start,includeStart);

        ZeroBuffer zb = new ZeroBuffer(start);
        BytesFrequencyEstimate startElem = elements.floor(zb);
        zb.set(stop);
        BytesFrequencyEstimate stopElem = elements.floor(zb);
        return elements.subSet(startElem,includeStart,stopElem,includeStop);
    }

    @Untested
    public Set<BytesFrequencyEstimate> frequentAfter(ByteBuffer start, boolean includeStart){
        BytesFrequencyEstimate first = elements.first();
        int firstCompare = first.compare(start);
        if(firstCompare>0){
            //the first element happens after our start, so everything is contained
            return elements;
        }else if(firstCompare==0 && includeStart) return elements;

        BytesFrequencyEstimate floor = elements.ceiling(new ZeroBuffer(start));
        return elements.tailSet(floor,includeStart);
    }

    @Untested
    public Set<BytesFrequencyEstimate> frequentBefore(ByteBuffer stop, boolean includeStop){
        BytesFrequencyEstimate first = elements.first();
        int firstCompare = first.compare(stop);
        if(firstCompare>0||(!includeStop && firstCompare==0)) {
            return Collections.emptySet();
        }
        BytesFrequencyEstimate last = elements.last();
        int lastCompare = last.compare(stop);
        if(lastCompare<0){
            //last elements is before the stop, so return everything
            return elements;
        }else if(lastCompare==0 && includeStop) return elements;

        BytesFrequencyEstimate floor = elements.ceiling(new ZeroBuffer(stop));
        return elements.headSet(floor, includeStop);
    }

    /*************************************************************************************************************/
    /*private helper methods*/

    private final class ZeroBytes implements BytesFrequencyEstimate{
        private byte[] bytes;
        private int offset;
        private int length;

        private transient byte[] cachedCopy;

        public ZeroBytes() { }

        public ZeroBytes(byte[] bytes, int offset, int length) {
            this.bytes = bytes;
            this.offset = offset;
            this.length = length;
        }

        void set(byte[] bytes, int offset, int length){
            this.bytes = bytes;
            this.offset = offset;
            this.length = length;
            this.cachedCopy = null;
        }

        @Override public ByteBuffer valueBuffer() { return ByteBuffer.wrap(bytes,offset,length); }
        @Override public byte[] valueArrayBuffer() { return bytes; }
        @Override public int valueArrayLength() { return length; }
        @Override public int valueArrayOffset() { return offset; }

        @Override
        public int compare(ByteBuffer buffer) {
            return -1*comparator.compare(buffer,bytes,offset,length);
        }

        @Override
        public int compare(byte[] buffer, int offset, int length) {
            return comparator.compare(this.bytes,this.offset,this.length,buffer,offset,length);
        }

        @Override
        public int compareTo(BytesFrequencyEstimate o) {
            return comparator.compare(bytes,offset,length,
                    o.valueArrayBuffer(),o.valueArrayOffset(),o.valueArrayLength());
        }

        @Override
        public byte[] getValue() {
            if(cachedCopy==null){
                cachedCopy = new byte[length];
                System.arraycopy(bytes,offset,cachedCopy,0,length);
            }
            return cachedCopy;
        }

        @Override public long count() { return 0; }
        @Override public long error() { return 0; }
    }

    private final class ZeroBuffer implements BytesFrequencyEstimate{
        private ByteBuffer buffer;

        private transient byte[] cachedCopy;

        public ZeroBuffer() { }

        public ZeroBuffer(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        void set(ByteBuffer buffer){
            this.buffer = buffer;
            this.cachedCopy = null;
        }

        @Override public ByteBuffer valueBuffer() {  return buffer;}
        @Override public int valueArrayLength() {return buffer.remaining();}
        @Override public byte[] valueArrayBuffer() {
            if(buffer.hasArray()) return buffer.array();
            return getValue();
        }


        @Override
        public int valueArrayOffset() {
            if(buffer.hasArray()) return buffer.position();
            return 0;
        }

        @Override
        public int compareTo(BytesFrequencyEstimate o) {
            return comparator.compare(buffer,o.valueBuffer());
        }

        @Override
        public int compare(ByteBuffer buffer) {
            return comparator.compare(this.buffer,buffer);
        }

        @Override
        public int compare(byte[] buffer, int offset, int length) {
            return comparator.compare(this.buffer,buffer,offset,length);
        }

        @Override
        public byte[] getValue() {
            if(cachedCopy==null){
                cachedCopy = new byte[buffer.remaining()];
                buffer.get(cachedCopy);
            }
            return cachedCopy;
        }

        @Override public long count() { return 0; }
        @Override public long error() { return 0; }
    }
}
