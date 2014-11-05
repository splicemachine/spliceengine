package com.splicemachine.si.data.hbase;

import com.splicemachine.si.api.RowAccumulator;
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.storage.EntryAccumulator;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.index.BitIndex;
import java.io.Closeable;
import java.io.IOException;

public class HRowAccumulator<Data> implements RowAccumulator<Data> {
    private final EntryPredicateFilter predicateFilter;
    private final EntryAccumulator entryAccumulator;
    private final EntryDecoder decoder;
    private final DataStore dataStore;
    private boolean countStar;
	private long bytesAccumulated = 0l;
	public HRowAccumulator(DataStore dataStore, EntryPredicateFilter predicateFilter, EntryDecoder decoder, boolean countStar) {
		this(dataStore, predicateFilter, decoder, predicateFilter.newAccumulator(),countStar);
	}

    public HRowAccumulator(DataStore dataStore, EntryPredicateFilter predicateFilter, EntryDecoder decoder,EntryAccumulator accumulator, boolean countStar) {
        this.predicateFilter = predicateFilter;
        this.entryAccumulator = accumulator;
        this.decoder = decoder;
        this.countStar = countStar;
        this.dataStore = dataStore;
    }

    @Override
    public boolean isOfInterest(Data data) {
    	if (countStar)
    		return false;
        decoder.set(dataStore.dataLib.getDataValueBuffer(data),
        		dataStore.dataLib.getDataValueOffset(data),
        		dataStore.dataLib.getDataValuelength(data));
        final BitIndex currentIndex = decoder.getCurrentIndex();
		return entryAccumulator.isInteresting(currentIndex);
    }

		@Override
		public void close() throws IOException {
				if(entryAccumulator instanceof Closeable)
						((Closeable) entryAccumulator).close();
		}

		@Override
    public boolean accumulate(Data data) throws IOException {
		bytesAccumulated+=dataStore.dataLib.getLength(data);
        boolean pass = predicateFilter.match(decoder, entryAccumulator);
        if(!pass)
            entryAccumulator.reset();
        return pass;
    }

    @Override
    public boolean isFinished() {
    	if (countStar)
    		return true;
        return entryAccumulator.isFinished();
    }

    @Override
    public byte[] result() {
        final byte[] result = entryAccumulator.finish();
        entryAccumulator.reset();
        return result;
    }
	@Override public long getBytesVisited() { return bytesAccumulated; }
	
	@Override
	public boolean isCountStar() {
		return this.countStar;
	}

		@Override
		public void reset() {
				entryAccumulator.reset();
		}
}
