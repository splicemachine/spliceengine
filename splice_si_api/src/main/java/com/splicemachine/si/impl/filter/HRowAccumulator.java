/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
