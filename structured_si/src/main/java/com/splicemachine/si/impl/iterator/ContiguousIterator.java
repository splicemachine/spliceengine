package com.splicemachine.si.impl.iterator;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;

/**
 * Wraps an ordered iterator of Data items. Identifies gaps in the sequence of IDs represented by the data. Fills these
 * gaps with calls to the "missing" callback. The net result is that the consumer of this iterator sees a contiguous
 * series of data elements representing all IDs.
 */
public class ContiguousIterator<ID extends Comparable, Data> {
    private ID target;
    private Data buffered;
    private final Iterator<Data> source;
    private final DataIDDecoder<ID, Data> decoder;
    private final ContiguousIteratorFunctions<ID, Data> callbacks;

    /**
     * @param target The first ID expected. This is needed because there may be a gap at the beginning of the sequence.
     */
    public ContiguousIterator(ID target, Iterator<Data> source,
                              DataIDDecoder<ID, Data> decoder,
                              ContiguousIteratorFunctions<ID, Data> callbacks) {
        this.target = target;
        this.source = source;
        this.decoder = decoder;
        this.callbacks = callbacks;
    }

    public boolean hasNext() {
        return buffered != null || source.hasNext();
    }

    public Data next() throws IOException {
        Data result = null;
        while (result == null && hasNext()) {
            final Data next = read();
            final int comparison = compareToTarget(next);
            if (comparison == 0) {
                result = next;
            } else if (comparison > 0) {
                buffered = next;
                result = callbacks.missing(target);
            } else {
                throw new RuntimeException("expected value " + target + " is ahead of actual value " + next);
            }
            target = callbacks.increment(target);
        }
        return result;
    }

    private int compareToTarget(Data next) {
        return decoder.getID(next).compareTo(target);
    }

    private Data read() {
        if (buffered == null) {
            return source.next();
        } else {
            final Data result = buffered;
            buffered = null;
            return result;
        }
    }

}
