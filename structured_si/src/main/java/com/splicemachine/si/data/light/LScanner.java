package com.splicemachine.si.data.light;

import com.splicemachine.si.impl.PushBackIterator;

import java.util.Collections;
import java.util.List;

public class LScanner {
    private final PushBackIterator<List<LKeyValue>> iterator;

    public LScanner(PushBackIterator<List<LKeyValue>> iterator) {
        this.iterator = iterator;
    }

    public List<LKeyValue> next() {
        if (iterator.hasNext()) {
            return iterator.next();
        } else {
            return Collections.EMPTY_LIST;
        }
    }

    public void seek(Object rowKey) {
        boolean done = false;
        while(iterator.hasNext() && !done) {
            final List<LKeyValue> next = iterator.next();
            if (next != null && !next.isEmpty()) {
                final LKeyValue keyValue = next.get(0);
                final int comparison = keyValue.rowKey.compareTo((String) rowKey);
                if(comparison < 0) {
                    // keep looking
                } else if (comparison == 0) {
                    iterator.pushBack(next);
                    done = true;
                } else {
                    done = true;
                }
            } else {
                done = true;
            }
        }
    }
}
