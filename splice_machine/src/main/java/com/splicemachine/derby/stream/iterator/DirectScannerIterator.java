package com.splicemachine.derby.stream.iterator;


import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.kvpair.KVPair;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by jyuan on 10/16/15.
 */
@NotThreadSafe
public class DirectScannerIterator implements Iterable<KVPair>, Iterator<KVPair>, Closeable {
    private DirectScanner directScanner;
    private KVPair value;
    private boolean populated=false;

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
        if(populated) return value!=null;
        populated=true;
        try {
            value = directScanner.next();
            if (value == null) {
                close();
                return false;
            }else
                return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public KVPair next() {
        if(!hasNext()) throw new NoSuchElementException();
        populated=false;
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
