package com.splicemachine.storage;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public interface DataGet extends Attributable{
    void setTimeRange(long low,long high);

    void returnAllVersions();

    void setFilter(DataFilter txnFilter);

    byte[] key();

    DataFilter filter();

    long highTimestamp();

    long lowTimestamp();

    void addColumn(byte[] family,byte[] qualifier);

    Map<byte[],Set<byte[]>> familyQualifierMap();
}
