package com.splicemachine.derby.hbase;

import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/28/15
 */
public interface SplitObserver{

    void preSplit(ObserverContext<RegionCoprocessorEnvironment> c,
                  @Nullable byte[] splitRow) throws IOException;

    void postSplit(ObserverContext<RegionCoprocessorEnvironment> e, HRegion l, HRegion r) throws IOException;

}
