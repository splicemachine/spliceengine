package com.splicemachine.hbase;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.splicemachine.utils.Source;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * @author Scott Fines
 *         Date: 8/13/14
 */
public class RegionScanIterator<T> implements Source<T> {
    public interface IOFunction<T>{
        public T apply(List<KeyValue> keyValues) throws IOException;
    }
    private final IOFunction<T> conversionFunction;
    private final RegionScanner regionScanner;

    private List<KeyValue> currentResults;
    private boolean shouldContinue = true;

    private T next;
    public RegionScanIterator(RegionScanner scanner, IOFunction<T> conversionFunction) {
        this.conversionFunction = conversionFunction;
        this.regionScanner = scanner;
        this.currentResults = Lists.newArrayListWithExpectedSize(4);
    }

    @Override
    public boolean hasNext() throws IOException {
        if(next!=null) return true;
        if(!shouldContinue) return false;
        do{
            currentResults.clear();
            shouldContinue = regionScanner.next(currentResults,null);
            if(currentResults.size()<=0) return false;

            next = conversionFunction.apply(currentResults);
        }while(shouldContinue && next==null);

        return next!=null;
    }

    @Override
    public T next() throws IOException {
        if(!hasNext()) throw new NoSuchElementException();
        T n = next;
        next = null;
        return n;
    }

    @Override public void close() throws IOException { regionScanner.close(); }
}
