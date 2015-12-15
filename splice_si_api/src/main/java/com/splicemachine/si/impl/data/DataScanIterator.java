package com.splicemachine.si.impl.data;

import com.splicemachine.storage.DataCell;
import com.splicemachine.storage.DataScanner;
import com.splicemachine.utils.Source;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Iterator which transforms a Data Cell list into a single entity.
 *
 * @author Scott Fines
 *         Date: 8/13/14
 */
public class DataScanIterator<T> implements Source<T> {
    public interface IOFunction<T>{
        T apply(List<DataCell> keyValues) throws IOException;
    }
    private final IOFunction<T> conversionFunction;
    private final DataScanner regionScanner;
//    private List<DataCell> currentResults;
    private T next;

    public DataScanIterator(DataScanner scanner,IOFunction<T> conversionFunction) {
        this.conversionFunction = conversionFunction;
        this.regionScanner = scanner;
    }

    @Override
    public boolean hasNext() throws IOException {
        if(next!=null) return true;
        do{
            List<DataCell> next=regionScanner.next(-1);
            if(next==null||next.size()<=0) return false;

            this.next= conversionFunction.apply(next);
        }while(next==null);

        return true;
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
