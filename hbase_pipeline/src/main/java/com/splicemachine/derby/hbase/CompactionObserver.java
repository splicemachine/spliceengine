package com.splicemachine.derby.hbase;

import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/28/15
 */
public interface CompactionObserver{

    InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e,
                               Store store,
                               InternalScanner scanner,
                               ScanType scanType,
                               CompactionRequest request) throws IOException;

    void postCompact(ObserverContext<RegionCoprocessorEnvironment> e,
                            Store store,StoreFile resultFile,CompactionRequest request) throws IOException;
}
