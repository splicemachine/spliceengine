package com.splicemachine.derby.stream.iterator;


import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.kvpair.KVPair;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created by jyuan on 10/16/15.
 */
@NotThreadSafe
public class DirectScannerIterator implements Iterable<KVPair>, Iterator<KVPair>, Closeable {
    private DirectScanner directScanner;
    private KVPair value;

    public DirectScannerIterator(DirectScanner hTableBuilder) throws IOException, StandardException{
        this.directScanner= hTableBuilder;
        directScanner.open();
    }

    @Override
    public Iterator<KVPair> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        boolean hasNext = true;
        try {
            value = directScanner.next();
            if (value == null) {
                close();
                hasNext = false;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return hasNext;
    }

    @Override
    public KVPair next() {
        return value;
    }

    @Override
    public void remove() {
        throw new RuntimeException("Not Implemented");
    }

    @Override
    public void close() throws IOException {
        if (directScanner!= null) {
            try {
                directScanner.close();
            } catch (StandardException se) {
                throw new IOException(se);
            }
        }
    }
}
