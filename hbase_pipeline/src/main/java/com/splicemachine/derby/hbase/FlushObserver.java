package com.splicemachine.derby.hbase;

import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/28/15
 */
public interface FlushObserver{

    InternalScanner preFlush(ObserverContext<RegionCoprocessorEnvironment> e,
                             Store store,
                             InternalScanner scanner) throws IOException;

    void postFlush(ObserverContext<RegionCoprocessorEnvironment> e,
                          @Nullable Store store,
                          @Nullable StoreFile resultFile) throws IOException ;
}
