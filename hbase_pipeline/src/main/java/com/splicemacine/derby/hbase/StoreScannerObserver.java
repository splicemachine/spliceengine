package com.splicemacine.derby.hbase;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.Store;

import java.io.IOException;
import java.util.NavigableSet;

/**
 * @author Scott Fines
 *         Date: 12/28/15
 */
public interface StoreScannerObserver{

    KeyValueScanner preStoreScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c,
                                        Store store,
                                        Scan scan,
                                        NavigableSet<byte[]> targetCols,
                                        KeyValueScanner s) throws IOException;
}
