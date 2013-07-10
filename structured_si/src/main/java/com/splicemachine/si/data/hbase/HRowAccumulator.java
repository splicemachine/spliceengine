package com.splicemachine.si.data.hbase;

import com.splicemachine.si.impl.RowAccumulator;
import com.splicemachine.storage.EntryAccumulator;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.index.BitIndex;

import java.io.IOException;

public class HRowAccumulator implements RowAccumulator<byte[]> {
    private final EntryPredicateFilter predicateFilter;
    private final EntryAccumulator entryAccumulator;
    private final EntryDecoder decoder;

    public HRowAccumulator(EntryPredicateFilter predicateFilter, EntryDecoder decoder) {
        this.predicateFilter = predicateFilter;
        this.entryAccumulator = predicateFilter.newAccumulator();
        this.decoder = decoder;
    }

    @Override
    public boolean isOfInterest(byte[] value) {
        decoder.set(value);
        final BitIndex currentIndex = decoder.getCurrentIndex();
        return currentIndex.intersects(entryAccumulator.getRemainingFields());
    }

    @Override
    public boolean accumulate(byte[] value) throws IOException {
        decoder.set(value);
        return predicateFilter.match(decoder, entryAccumulator);
    }

    @Override
    public boolean isFinished() {
        return entryAccumulator.getRemainingFields().isEmpty();
    }

    @Override
    public byte[] result() {
        final byte[] result = entryAccumulator.finish();
        entryAccumulator.reset();
        return result;
    }
}
