/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.si.impl.filter;

import com.splicemachine.si.api.filter.RowAccumulator;
import com.splicemachine.storage.DataCell;
import com.splicemachine.storage.EntryAccumulator;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.index.BitIndex;
import java.io.Closeable;
import java.io.IOException;

public class HRowAccumulator implements RowAccumulator{
    private final EntryPredicateFilter predicateFilter;
    private final EntryAccumulator entryAccumulator;
    private final EntryDecoder decoder;
    private boolean countStar;
    private long bytesAccumulated = 0l;

    public HRowAccumulator(EntryPredicateFilter predicateFilter,EntryDecoder decoder,boolean countStar) {
        this(predicateFilter, decoder, predicateFilter.newAccumulator(),countStar);
    }

    public HRowAccumulator(EntryPredicateFilter predicateFilter,EntryDecoder decoder,EntryAccumulator accumulator,boolean countStar) {
        this.predicateFilter = predicateFilter;
        this.entryAccumulator = accumulator;
        this.decoder = decoder;
        this.countStar = countStar;
    }

    @Override
    public boolean isInteresting(DataCell data){
        if (countStar)
            return false;
        decoder.set(data.valueArray(),data.valueOffset(),data.valueLength());
        final BitIndex currentIndex = decoder.getCurrentIndex();
        return entryAccumulator.isInteresting(currentIndex);
    }

    @Override
    public boolean accumulateCell(DataCell value) throws IOException{
        bytesAccumulated+=value.encodedLength();
        boolean pass = predicateFilter.match(decoder, entryAccumulator);
        if(!pass)
            entryAccumulator.reset();
        return pass;
    }

    @Override
    public void close() throws IOException {
        if(entryAccumulator instanceof Closeable)
            ((Closeable) entryAccumulator).close();
    }

    @Override
    public boolean isFinished(){
        return countStar || entryAccumulator.isFinished();
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
