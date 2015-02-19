package com.splicemachine.stats.frequency;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.splicemachine.hash.Hash32;
import com.splicemachine.primitives.ByteComparator;

import java.nio.ByteBuffer;
import java.util.Collection;

/**
 * @author Scott Fines
 *         Date: 2/18/15
 */
public class BytesSpaceSaver extends ObjectSpaceSaver<byte[]> implements BytesFrequencyCounter {
    private static final Function<? super FrequencyEstimate<byte[]>,BytesFrequencyEstimate> castFunction = new Function<FrequencyEstimate<byte[]>, BytesFrequencyEstimate>() {
        @Override
        public BytesFrequencyEstimate apply(FrequencyEstimate<byte[]> input) {
            return (BytesFrequencyEstimate)input;
        }
    };

    private ByteComparator byteComparator;

    private ByteEntry byteHolder = new ByteEntry();
    private BufferEntry bufferHolder = new BufferEntry();

    public BytesSpaceSaver(ByteComparator comparator,
                           Hash32 hashFunction, int maxSize) {
        super(comparator, hashFunction, maxSize);
        this.byteComparator = comparator;
    }

    public BytesSpaceSaver(ByteComparator comparator,
                           Hash32 hashFunction, int maxSize, int initialSize, float loadFactor) {
        super(comparator, hashFunction, maxSize, initialSize, loadFactor);
        this.byteComparator = comparator;
    }

    /*****************************************************************************************************************/
    /*Accessors*/
    @Override
    public BytesFrequentElements frequentElements(int k) {
        Collection<BytesFrequencyEstimate> freqItems = Collections2.transform(topKElements(k),castFunction);
        return new BytesFrequentElements(freqItems,byteComparator);
    }

    @Override
    public BytesFrequentElements heavyHitters(float support) {
        Collection<BytesFrequencyEstimate> heavyItems = Collections2.transform(heavyItems(support),castFunction);
        return new BytesFrequentElements(heavyItems,byteComparator);
    }

    /******************************************************************************************************************/
    /*modifiers*/
    @Override public void update(byte[] item) { update(item,0,item.length); }
    @Override public void update(byte[] item, long count) { update(item,0,item.length,count); }
    @Override public void update(byte[] bytes, int offset, int length) { update(bytes,offset,length,1l); }
    @Override public void update(ByteBuffer bytes) { update(bytes,1l); }

    @Override
    public void update(byte[] bytes, int offset, int length, long count) {
        byteHolder.set(bytes,offset,length);
        holderEntry = byteHolder;
        doUpdate(count);
    }

    @Override
    public void update(ByteBuffer bytes, long count) {
        bufferHolder.set(bytes);
        holderEntry = bufferHolder;
        doUpdate(count);
    }

    @Override
    protected void setValue(Entry holderEntry, Entry entry) {
        if(holderEntry instanceof BufferEntry){
            ((BufferEntry)entry).buffer = ((BufferEntry) holderEntry).buffer;
        }else{
            ByteEntry be = (ByteEntry)entry;
            ByteEntry holder = (ByteEntry)holderEntry;
            be.buffer = holder.buffer;
            be.offset = holder.offset;
            be.length = holder.length;
        }
    }

    @Override
    protected Entry newEntry() {
        if(holderEntry instanceof BufferEntry)
            return new BufferEntry();
        else
            return new ByteEntry();
    }

    private class BufferEntry extends Entry implements BytesFrequencyEstimate{
        private ByteBuffer buffer;
        private transient byte[] cachedCopy;

        public BufferEntry getClone(){
            BufferEntry be = new BufferEntry();
            be.buffer = buffer;
            be.cachedCopy = cachedCopy;
            return be;
        }

        @Override public ByteBuffer valueBuffer() { return buffer; }
        @Override public int valueArrayLength() {  return buffer.remaining(); }

        @Override
        public int valueArrayOffset() {
            if(buffer.hasArray()) return buffer.position();
            return 0;
        }

        @Override
        public int compare(ByteBuffer buffer) {
            return byteComparator.compare(this.buffer,buffer);
        }

        @Override
        public int compare(byte[] buffer, int offset, int length) {
            return byteComparator.compare(this.buffer,buffer,offset,length);
        }

        @Override
        public byte[] valueArrayBuffer() {
            if(buffer.hasArray()) return buffer.array();
            else return getValue();
        }

        @Override
        public byte[] getValue() {
            if(cachedCopy==null){
                buffer.mark();
                cachedCopy = new byte[buffer.remaining()];
                buffer.get(cachedCopy);
                buffer.reset();
            }
            return cachedCopy;
        }

        @Override
        public void set(byte[] item) {
            buffer = ByteBuffer.wrap(item);
            cachedCopy = null;
        }

        public void set(ByteBuffer buffer) {
            this.buffer = buffer;
            cachedCopy = null;
        }

        @Override
        protected int computeHash() {
            buffer.mark();
            int hC =  hashFunction.hash(buffer);
            buffer.reset();
            return hC;
        }

        @Override
        public boolean equals(Entry o) {
            if(o instanceof ByteEntry){
                ByteEntry bEntry = (ByteEntry)o;
                byte[] oBuffer = bEntry.buffer;
                int oOffset = bEntry.offset;
                int oLength = bEntry.length;
                return byteComparator.equals(buffer,oBuffer,oOffset,oLength);
            }else{
                return byteComparator.equals(buffer,((BufferEntry)o).buffer);
            }
        }

        @Override
        public int compareTo(BytesFrequencyEstimate o) {
            if(o instanceof ByteEntry){
                ByteEntry oEntry = (ByteEntry)o;
                byte[] oBuffer = oEntry.buffer;
                int oOffset = oEntry.offset;
                int oLength = oEntry.length;
                return byteComparator.compare(buffer, oBuffer, oOffset, oLength);
            }else if(o instanceof BufferEntry){
                return byteComparator.compare(buffer, ((BufferEntry) o).buffer);
            }else
                return byteComparator.compare(buffer, o.valueBuffer());
        }
    }

    private class ByteEntry extends Entry implements BytesFrequencyEstimate{
        private byte[] buffer;
        private int offset;
        private int length;

        private transient byte[] cachedCopy;

        public ByteEntry getClone(){
            ByteEntry be = new ByteEntry();
            be.buffer = buffer;
            be.offset = offset;
            be.length = length;
            return be;
        }

        @Override
        public ByteBuffer valueBuffer() {
            return ByteBuffer.wrap(buffer,offset,length);
        }

        @Override public byte[] valueArrayBuffer() { return buffer; }
        @Override public int valueArrayLength() { return offset; }
        @Override public int valueArrayOffset() { return length; }

        @Override
        public byte[] getValue() {
            if(cachedCopy==null){
                cachedCopy = new byte[length];
                System.arraycopy(buffer, offset, cachedCopy, 0, length);
            }
            return cachedCopy;
        }

        @Override
        public int compare(ByteBuffer buffer) {
            return -1*byteComparator.compare(buffer,this.buffer,offset,length);
        }

        @Override
        public int compare(byte[] buffer, int offset, int length) {
            return byteComparator.compare(this.buffer, this.offset, this.length, buffer, offset, length);
        }

        @Override
        public void set(byte[] item) {
            set(item,0,item.length);
        }

        public void set(byte[] item, int offset, int length){
            this.buffer = item;
            this.offset = offset;
            this.length = length;
            cachedCopy = null;
        }

        @Override
        protected int computeHash() {
            return hashFunction.hash(buffer,offset,length);
        }

        @Override
        public boolean equals(Entry o) {
            if(o instanceof ByteEntry){
                ByteEntry bEntry = (ByteEntry)o;
                byte[] oBuffer = bEntry.buffer;
                int oOffset = bEntry.offset;
                int oLength = bEntry.length;
                return byteComparator.equals(buffer,offset,length,oBuffer,oOffset,oLength);
            }else{
                BufferEntry bufferEntry = (BufferEntry)o;
                return byteComparator.equals(bufferEntry.buffer,buffer,offset,length);
            }
        }

        @Override
        public int compareTo(BytesFrequencyEstimate o) {
            if(o instanceof ByteEntry){
                ByteEntry oEntry = (ByteEntry)o;
                byte[] oBuffer = oEntry.buffer;
                int oOffset = oEntry.offset;
                int oLength = oEntry.length;
                return byteComparator.compare(buffer, offset, length, oBuffer, oOffset, oLength);
            }else if(o instanceof BufferEntry){
                return -1*byteComparator.compare(((BufferEntry)o).buffer,buffer,offset,length);
            }else
                return byteComparator.compare(
                        buffer, offset, length,
                        o.valueArrayBuffer(), o.valueArrayOffset(), o.valueArrayLength());
        }
    }
}
