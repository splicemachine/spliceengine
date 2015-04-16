package com.splicemachine.stats.frequency;

import com.google.common.primitives.Longs;
import com.splicemachine.annotations.Untested;
import com.splicemachine.encoding.Encoder;
import com.splicemachine.primitives.ByteComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * @author Scott Fines
 *         Date: 12/5/14
 */
public abstract class BytesFrequentElements implements FrequentElements<ByteBuffer>{
    private static final Comparator<BytesFrequencyEstimate> frequencyComparator = new Comparator<BytesFrequencyEstimate>() {
        @Override
        public int compare(BytesFrequencyEstimate o1, BytesFrequencyEstimate o2) {
            int compare = Longs.compare(o1.count(), o2.count());
            if (compare != 0) return compare;
            return o1.compareTo(o2);
        }
    };
    private static final Comparator<BytesFrequencyEstimate> naturalComparator = new Comparator<BytesFrequencyEstimate>() {
        @Override
        public int compare(BytesFrequencyEstimate o1, BytesFrequencyEstimate o2) {
            int compare = o1.compareTo(o2);
            if (compare != 0) return compare;
            return Long.signum(o1.count() - o2.count());
        }
    };

    private NavigableSet<BytesFrequencyEstimate> elements;
    private final ByteComparator comparator;
    private long totalCount;

    private BytesFrequentElements(long totalCount,Collection<BytesFrequencyEstimate> elements,ByteComparator comparator) {
        TreeSet<BytesFrequencyEstimate> elems = new TreeSet<>(naturalComparator);
        elems.addAll(elements);
        this.elements = elems;
        this.comparator = comparator;
        this.totalCount = totalCount;
    }

    public static BytesFrequentElements topK(int k,
                                             long totalCount,
                                             Collection<BytesFrequencyEstimate> elements,
                                             ByteComparator comparator){
        return new TopK(k,totalCount,elements,comparator);
    }

    public static BytesFrequentElements heavyHitters(float support,
                                                     long totalCount,
                                             Collection<BytesFrequencyEstimate> elements,
                                             ByteComparator comparator){
        return new HeavyItems(support,totalCount,elements,comparator);
    }

    public FrequencyEstimate<ByteBuffer> countEqual(byte[] bytes){
       return countEqual(bytes,0,bytes.length);
    }

    public FrequencyEstimate<ByteBuffer> countEqual(byte[] bytes, int offset, int length){
        for(BytesFrequencyEstimate estimate:elements){
            if(estimate.compare(bytes,offset,length)==0)
                return estimate;
        }
        return new ZeroBytes(bytes,offset,length);
    }

    public FrequencyEstimate<ByteBuffer> countEqual(ByteBuffer buffer){
        for(BytesFrequencyEstimate estimate:elements){
            if(estimate.compare(buffer)==0)
                return estimate;
        }
        return new ZeroBuffer(buffer);
    }

    @Override
    public FrequencyEstimate<? extends ByteBuffer> equal(ByteBuffer item) {
        return countEqual(item);
    }

    @Override
    public Set<? extends FrequencyEstimate<ByteBuffer>> allFrequentElements() {
        return Collections.unmodifiableSet(elements);
    }

    @Override
    public long totalFrequentElements() {
        long count = 0l;
        for(FrequencyEstimate<ByteBuffer> est:allFrequentElements()){
            count+=est.count();
        }
        return count;
    }

    @Override
    public Set<? extends FrequencyEstimate<ByteBuffer>> frequentElementsBetween(ByteBuffer start, ByteBuffer stop, boolean includeMin, boolean includeStop) {
        return frequentBetween(start,stop,includeMin,includeStop);
    }

    @Untested
    public Set<? extends FrequencyEstimate<ByteBuffer>> frequentElementsBetween(byte[] start, byte[] stop, boolean includeMin, boolean includeStop) {
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

    @Override
    public FrequentElements<ByteBuffer> merge(FrequentElements<ByteBuffer> other) {
        assert other instanceof BytesFrequentElements: "Cannot merge instance of type "+ other.getClass();
        return merge((BytesFrequentElements)other);
    }

    public BytesFrequentElements merge(BytesFrequentElements other) {
        NavigableSet<BytesFrequencyEstimate> merged = this.elements;
        NavigableSet<BytesFrequencyEstimate> otherEstimates = other.elements;
        /*
         * We need to pick out *maxSize* most common elements from both sets. Since this
         * is relatively small data, we will maintain a set in occurrance order, and add everything
         * from both sets, merging as we go. In order to effectively do that, we use a simple array,
         * add everything in, then add back in the number of elements that we need.
         */
        int totalSize = merged.size()+otherEstimates.size();
        BytesFrequencyEstimate[] topK = new BytesFrequencyEstimate[totalSize]; //assume no intersection
        int size =0;
        for(BytesFrequencyEstimate estimate:merged){
            topK[size] = estimate;
            size++;
        }
        for(BytesFrequencyEstimate otherEst:otherEstimates){
            boolean found = false;
            for(int i=0;i<size;i++){
                BytesFrequencyEstimate existingEst = topK[i];
                if(existingEst.equals(otherEst)){
                    topK[i] = (BytesFrequencyEstimate)existingEst.merge(otherEst);
                    found=true;
                    break;
                }
            }
            if(!found) {
                topK[size] = otherEst;
                size++;
            }
        }
        this.totalCount+=other.totalCount;
        this.elements = rebuild(totalCount,topK,size);

        return this;
    }

    protected abstract NavigableSet<BytesFrequencyEstimate> rebuild(long mergedCount,BytesFrequencyEstimate[] topK,int size);

    static Encoder<BytesFrequentElements> newEncoder(ByteComparator byteComparator) {
        return new EncoderDecoder(byteComparator);
    }

    public BytesFrequentElements getClone(){
        Collection<BytesFrequencyEstimate> copy = new TreeSet<>(elements);
        return getNew(totalCount,copy,comparator);
    }

    protected abstract BytesFrequentElements getNew(long totalCount, Collection<BytesFrequencyEstimate> copy, ByteComparator comparator);

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private static class TopK extends BytesFrequentElements {
        /*
         * Represents the Top-K version of Bytes FrequentElements
         */
        private int k;
        public TopK(int k,long totalCount, Collection<BytesFrequencyEstimate> elements, ByteComparator comparator) {
            super(totalCount,elements,comparator);
            this.k = k;
        }

        @Override
        protected NavigableSet<BytesFrequencyEstimate> rebuild(long ignored,BytesFrequencyEstimate[] topK,int size) {
            Arrays.sort(topK,0,size,frequencyComparator);
            NavigableSet<BytesFrequencyEstimate> result = new TreeSet<>(naturalComparator);
            int min = Math.min(k, size);
            //noinspection ManualArrayToCollectionCopy
            for(int i=0;i< min;i++){
                result.add(topK[i]);
            }
            return result;
        }

        @Override
        protected BytesFrequentElements getNew(long totalCount, Collection<BytesFrequencyEstimate> copy, ByteComparator comparator) {
            return new TopK(k,totalCount,copy,comparator);
        }
    }

    private static class HeavyItems extends BytesFrequentElements {
        /*
         * Represents the Heavy-Hitters version of the BytesFrequentElements
         */
        private float support;

        public HeavyItems(float support,long totalCount, Collection<BytesFrequencyEstimate> elements, ByteComparator comparator) {
            super(totalCount,elements,comparator);
            this.support = support;
        }

        @Override
        protected NavigableSet<BytesFrequencyEstimate> rebuild(long totalCount,BytesFrequencyEstimate[] topK,int size) {
            NavigableSet<BytesFrequencyEstimate> result = new TreeSet<>(naturalComparator);
            long threshold = (long)(totalCount*support);
            //noinspection ForLoopReplaceableByForEach
            for(int i=0;i< size;i++){
                BytesFrequencyEstimate est = topK[i];
                if(est.count()>threshold)
                    result.add(est);
            }
            return result;
        }

        @Override
        protected BytesFrequentElements getNew(long totalCount, Collection<BytesFrequencyEstimate> copy, ByteComparator comparator) {
            return new HeavyItems(support,totalCount,copy,comparator);
        }
    }

    private final class ZeroBytes implements BytesFrequencyEstimate{
        private byte[] bytes;
        private int offset;
        private int length;

        private transient byte[] cachedCopy;

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
        public FrequencyEstimate<ByteBuffer> merge(FrequencyEstimate<ByteBuffer> otherEst) {
            return otherEst;
        }

        @Override
        public int compareTo(BytesFrequencyEstimate o) {
            return comparator.compare(bytes,offset,length,
                    o.valueArrayBuffer(),o.valueArrayOffset(),o.valueArrayLength());
        }

        @Override
        public ByteBuffer getValue() {
            return valueBuffer();
        }

        public byte[] byteValue() {
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

        public ZeroBuffer(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        void set(ByteBuffer buffer){
            this.buffer = buffer;
            this.cachedCopy = null;
        }

        @Override public FrequencyEstimate<ByteBuffer> merge(FrequencyEstimate<ByteBuffer> otherEst) { return otherEst; }
        @Override public ByteBuffer valueBuffer() {  return buffer;}
        @Override public int valueArrayLength() {return buffer.remaining();}
        @Override public byte[] valueArrayBuffer() {
            if(buffer.hasArray()) return buffer.array();
            if(cachedCopy==null){
                cachedCopy = new byte[buffer.remaining()];
                buffer.get(cachedCopy);
            }
            return cachedCopy;
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

        public ByteBuffer getValue(){
            return valueBuffer();
        }

        @Override public long count() { return 0; }
        @Override public long error() { return 0; }
    }

    static class EncoderDecoder implements Encoder<BytesFrequentElements> {
        private final ByteComparator byteComparator;

        public EncoderDecoder(ByteComparator byteComparator) {
            this.byteComparator = byteComparator;
        }

        @Override
        public void encode(BytesFrequentElements item, DataOutput dataInput) throws IOException {
            dataInput.writeLong(item.totalCount);
            encodeSet(item.elements,dataInput);
            if(item instanceof TopK) {
                dataInput.writeBoolean(true);
                dataInput.writeInt(((TopK)item).k);
            }else {
                dataInput.writeBoolean(false);
                dataInput.writeFloat(((HeavyItems) item).support);
            }
        }

        @Override
        public BytesFrequentElements decode(DataInput input) throws IOException {
            long totalCount = input.readLong();
            Set<BytesFrequencyEstimate> estimates = decodeSet(input);
            if(input.readBoolean()){
                int k = input.readInt();
                return new TopK(k,totalCount,estimates,byteComparator);
            }else{
                float support = input.readFloat();
                return new HeavyItems(support,totalCount,estimates,byteComparator);
            }
        }

        private void encodeSet(NavigableSet<BytesFrequencyEstimate> elements, DataOutput dataInput) throws IOException {
            dataInput.writeInt(elements.size());
            for(BytesFrequencyEstimate element:elements){
                byte[] data = element.valueArrayBuffer(); //does a memcp, but that's probably okay, considering how rarely this is used
                dataInput.writeInt(element.valueArrayLength());
                dataInput.write(data,element.valueArrayOffset(),element.valueArrayLength());
                dataInput.writeLong(element.count());
                dataInput.writeLong(element.error());
            }
        }

        private Set<BytesFrequencyEstimate> decodeSet(DataInput input) throws IOException{
            int size = input.readInt();
            Set<BytesFrequencyEstimate> set = new TreeSet<>(naturalComparator);
            for(int i=0;i<size;i++){
                byte[] data = new byte[input.readInt()];
                input.readFully(data);
                long c = input.readLong();
                long eps = input.readLong();
                set.add(new BytesValueEstimate(data,c,eps,byteComparator));
            }
            return set;
        }
    }
}
