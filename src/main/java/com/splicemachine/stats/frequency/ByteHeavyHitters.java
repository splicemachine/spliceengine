package com.splicemachine.stats.frequency;

import java.util.*;

/**
 * @author Scott Fines
 *         Date: 12/5/14
 */
public class ByteHeavyHitters implements ByteFrequentElements {
    private final long[] counts;
    private final ByteFrequencyEstimate[] cachedFrequencies;
    private long threshold;

    public ByteHeavyHitters(final long[] counts,long threshold) {
        this.counts = counts;
        this.threshold = threshold;
        this.cachedFrequencies = new ByteFrequencyEstimate[counts.length];
        for(int i=0;i<counts.length;i++){
            ByteFrequency byteFrequency = new ByteFrequency((byte)i);
            this.cachedFrequencies[i] = byteFrequency;
        }
    }

    @Override
    public ByteFrequencyEstimate countEqual(byte item) {
        return cachedFrequencies[item & 0xff];
    }

    @Override
    public Set<ByteFrequencyEstimate> frequentBetween(byte start, byte stop, boolean includeStart, boolean includeStop) {
        assert stop>=start: "Cannot estimate between: stop < start!";
        if(start==stop){
            if(includeStart || includeStop)
                return Collections.singleton(countEqual(start));
            return Collections.emptySet();
        }

        int startPos = ((int)start & 0xff);
        if(!includeStart) startPos++;
        int stopPos = ((int)stop & 0xff);
        if(includeStop) stopPos++;
        return new InnerSet(startPos,stopPos);
    }

    @Override
    public Set<ByteFrequencyEstimate> frequentAfter(byte start, boolean includeStart) {
        int startPos = ((int)start & 0xff);
        if(!includeStart) startPos++;
        return new InnerSet(startPos,256);
    }

    @Override
    public Set<ByteFrequencyEstimate> frequentBefore(byte stop, boolean includeStop) {
        int startPos = (0x80 & 0xff);
        int stopPos = ((int)stop & 0xff);
        if(includeStop)stopPos++;
        return new InnerSet(startPos,stopPos);
    }

    @Override
    public FrequencyEstimate<? extends Byte> equal(Byte item) {
        assert item!=null: "Cannot determine frequent elements for null item!";
        return countEqual(item);
    }

    @Override
    public Set<? extends FrequencyEstimate<Byte>> frequentElementsBetween(Byte start, Byte stop, boolean includeMin, boolean includeStop) {
        return null;
    }

    @Override
    public ByteFrequentElements merge(ByteFrequentElements other) {
        if(other instanceof ByteHeavyHitters){
            ByteHeavyHitters oHitters = (ByteHeavyHitters)other;
            for(int i=0;i<counts.length;i++){
                counts[i]+=oHitters.counts[i];
            }
        }else {
            for (int i = 0; i < counts.length; i++) {
                ByteFrequencyEstimate oe = other.countEqual((byte) i);
                counts[i] += oe.count();
            }
        }
        return this;
    }

    /******************************************************************************************************************/
    /*private helper classes*/
    private class ByteFrequency implements ByteFrequencyEstimate{
        final byte value;

        public ByteFrequency(byte value) {
            this.value = value;
        }

        @Override public byte value() { return value; }
        @Override public Byte getValue() { return value; }
        @Override public long count() {
            long count = counts[value & 0xff];
            if(count<threshold) return 0l;
            return count;
        }
        @Override public long error() { return 0; }

        @Override
        public int compareTo(ByteFrequencyEstimate o) {
            return o.value()-value;
        }
    }

    private class InnerSet extends AbstractSet<ByteFrequencyEstimate> {
        private final int startPos;
        private final int stopPos;

        public InnerSet(int startPos, int stopPos) {
            this.startPos = startPos;
            this.stopPos = stopPos;
        }

        @Override public Iterator<ByteFrequencyEstimate> iterator() { return new Iter(startPos,stopPos); }

        @Override
        public int size() {
            int s=0;
            for(int i=startPos;i<stopPos;i++){
                if(counts[i]>=threshold)
                    s++;
            }
            return s;
        }
    }

    private class Iter implements Iterator<ByteFrequencyEstimate> {
        private final int stopPos;
        private int pos;
        private boolean hasNextCalled =false;

        public Iter(int startPos, int stopPos) {
            this.stopPos = stopPos;
            this.pos = startPos;
        }

        @Override
        public boolean hasNext() {
            if(hasNextCalled) return pos<stopPos;

            while(pos<stopPos && counts[pos]<threshold){
                pos = (pos+1) & 255;
            }
            hasNextCalled=true;
            return pos<stopPos;
        }

        @Override
        public ByteFrequencyEstimate next() {
            if(!hasNext()) throw new NoSuchElementException();
            hasNextCalled=false;
            return cachedFrequencies[pos];
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Removal not supported");
        }
    }
}
