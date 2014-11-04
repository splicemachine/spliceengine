package com.splicemachine.hbase.stats;

/**
 * Central, Singleton reference for updating statistics related to SpliceMachine that
 * HBase doesn't give us.
 *
 * This includes things like Bulk Writes/Second (since the BatchProtocol just registers as a single
 * request), total transactions begun, total transactions ended, that kind of thing.
 *
 * @author Scott Fines
 * Created on: 5/6/13
 */
public class SpliceRegionStats {

}
