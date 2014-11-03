package com.splicemachine.hbase.regioninfocache;

import com.google.common.collect.Sets;
import com.splicemachine.hbase.RegionCacheComparator;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;

import static com.google.common.base.Preconditions.checkNotNull;

class RegionMetaScannerVisitor implements MetaScanner.MetaScannerVisitor {

    private static final RegionCacheComparator COMPARATOR = new RegionCacheComparator();

    /* We don't need a concurrent data structure here, but we do need one that does not depend
     * on hashCode of the keys being consistent with equality, which is not the case for byte arrays */
    private Map<byte[], SortedSet<Pair<HRegionInfo, ServerName>>> regionPairMap =
            new TreeMap<byte[], SortedSet<Pair<HRegionInfo, ServerName>>>(Bytes.BYTES_COMPARATOR);

    private byte[] updateTableName;

    /**
     * Construct an instance that collects information only for the specified table.
     */
    RegionMetaScannerVisitor(byte[] updateTableName) {
        this.updateTableName = updateTableName;
    }

    @Override
    public boolean processRow(Result rowResult) throws IOException {
        Pair<HRegionInfo, ServerName> infoPair = MetaReader.parseCatalogResult(rowResult);
        HRegionInfo regionInfo = infoPair.getFirst();
        byte[] currentTableName = regionInfo.getTableName();

        if (updateTableName != null && !Bytes.equals(updateTableName, currentTableName)) {
            /* We are looking for one specific table. This isn't it.  Should we continue?  If regionPairMap
             * is empty then we may not have seen our table yet (or its regions were offline), continue. If the map
             * is not empty however, then we know we can stop as we have scanned past the table we are looking for. */
            return regionPairMap.isEmpty();
        }

        if (isRegionAvailable(regionInfo)) {
            checkNotNull(infoPair.getSecond(), "never expect ServerName object to be null");
            SortedSet<Pair<HRegionInfo, ServerName>> regionsForTable = regionPairMap.get(currentTableName);
            if (regionsForTable == null) {
                regionsForTable = Sets.newTreeSet(COMPARATOR);
                regionPairMap.put(currentTableName, regionsForTable);
            }
            regionsForTable.add(infoPair);
        }
        return true;
    }

    @Override
    public void close() throws IOException {
    }

    public Map<byte[], SortedSet<Pair<HRegionInfo, ServerName>>> getRegionPairMap() {
        return regionPairMap;
    }

    public static boolean isRegionAvailable(HRegionInfo info) {
        return !info.isOffline() && !info.isSplit() && !info.isSplitParent();
    }
}
