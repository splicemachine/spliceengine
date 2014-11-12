package com.splicemachine.hbase;

import com.google.common.collect.Lists;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.utils.Source;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * @author Scott Fines
 *         Date: 8/13/14
 */
public class RegionScanIterator<Data,Put extends OperationWithAttributes,Delete,Get extends OperationWithAttributes, Scan,T> implements Source<T> {
    public interface IOFunction<T,Data>{
        public T apply(List<Data> keyValues) throws IOException;
    }
    private final IOFunction<T,Data> conversionFunction;
    private final RegionScanner regionScanner;
    private List<Data> currentResults;
    private boolean shouldContinue = true;
    private T next;
    private SDataLib<Data,Put,Delete,Get,Scan> dataLib;
    
    public RegionScanIterator(RegionScanner scanner, IOFunction<T,Data> conversionFunction,SDataLib<Data,Put,Delete,Get,Scan> dataLib) {
        this.conversionFunction = conversionFunction;
        this.regionScanner = scanner;
        this.currentResults = Lists.newArrayListWithExpectedSize(4);
        this.dataLib = dataLib;
    }

    @Override
    public boolean hasNext() throws IOException {
        if(next!=null) return true;
        if(!shouldContinue) return false;
        do{
            currentResults.clear();
            shouldContinue = dataLib.regionScannerNext(regionScanner,currentResults);
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
