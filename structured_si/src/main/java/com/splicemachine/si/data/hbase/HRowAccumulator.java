package com.splicemachine.si.data.hbase;

import com.splicemachine.si.impl.RowAccumulator;
import com.splicemachine.storage.EntryAccumulator;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.index.BitIndex;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;

public class HRowAccumulator implements RowAccumulator<byte[]> {
    private final EntryAccumulator entryAccumulator;
    private final EntryDecoder decoder;

    public HRowAccumulator(EntryAccumulator entryAccumulator, EntryDecoder decoder) {
        this.entryAccumulator = entryAccumulator;
        this.decoder = decoder;
    }

    @Override
    public boolean isOfInterest(byte[] value) {
        decoder.set(value);
        final BitIndex currentIndex = decoder.getCurrentIndex();
        return currentIndex.intersects(entryAccumulator.getRemainingFields());
    }

    @Override
    public void accumulate(byte[] value) throws IOException {
        decoder.set(value);
        final BitIndex decoderIndexSet = decoder.getCurrentIndex();
        final BitSet accumulatorIndexSet = entryAccumulator.getRemainingFields();
        final BitSet columnsToSet = decoderIndexSet.and(accumulatorIndexSet);
        for (int i = columnsToSet.nextSetBit(0); i >= 0; i = columnsToSet.nextSetBit(i + 1)) {
            final byte[] columnData = decoder.getData(i);
            entryAccumulator.add(i, ByteBuffer.wrap(columnData));
        }
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
