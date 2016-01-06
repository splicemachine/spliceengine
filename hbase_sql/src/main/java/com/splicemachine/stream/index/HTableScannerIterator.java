package com.splicemachine.stream.index;


import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.hbase.KVPair;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created by jyuan on 10/16/15.
 */
@NotThreadSafe
public class HTableScannerIterator implements Iterable<KVPair>, Iterator<KVPair>, Closeable {
    private HTableScannerBuilder hTableBuilder;
    private HTableScanner hTableScanner;
    private KVPair value;

    public HTableScannerIterator(HTableScannerBuilder hTableBuilder) {
        this.hTableBuilder = hTableBuilder;
    }

    @Override
    public Iterator<KVPair> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {

        boolean hasNext = true;
        try {
            if (hTableScanner == null) {
                hTableScanner = hTableBuilder.build();
                hTableScanner.open();
            }
            value = hTableScanner.next();
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
        if (hTableScanner != null) {
            try {
                hTableScanner.close();
            } catch (StandardException se) {
                throw new IOException(se);
            }
        }
    }
}
