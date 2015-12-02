package com.splicemachine.db.impl.sql.catalog;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.services.property.PropertyUtil;
import com.splicemachine.db.iapi.sql.dictionary.*;
import java.util.List;
import java.util.Properties;

/**
 *
 * Cache Holder for making sense of data dictionary caching and concurrency.
 *
 * Created by jleach on 12/1/15.
 */
public class DataDictionaryCache {
    private Cache<UUID,TableDescriptor> oidTdCache;
    private Cache<TableKey,TableDescriptor> nameTdCache;
    private Cache<TableKey,SPSDescriptor> spsNameCache;
    private Cache<String,SequenceUpdater> sequenceGeneratorCache;
    private Cache<PermissionsDescriptor,PermissionsDescriptor> permissionsCache;
    private Cache<Long,List<PartitionStatisticsDescriptor>> partitionStatisticsCache;
    private Cache<UUID, SPSDescriptor> storedPreparedStatementCache;
    int tdCacheSize;
    int stmtCacheSize;
    int permissionsCacheSize;


    public DataDictionaryCache(Properties startParams) throws StandardException {
        String value=startParams.getProperty(Property.LANG_TD_CACHE_SIZE);
        tdCacheSize= PropertyUtil.intPropertyValue(Property.LANG_TD_CACHE_SIZE, value,
                0, Integer.MAX_VALUE, Property.LANG_TD_CACHE_SIZE_DEFAULT);

        value=startParams.getProperty(Property.LANG_SPS_CACHE_SIZE);
        stmtCacheSize=PropertyUtil.intPropertyValue(Property.LANG_SPS_CACHE_SIZE,value,
                0,Integer.MAX_VALUE,Property.LANG_SPS_CACHE_SIZE_DEFAULT);

        value=startParams.getProperty(Property.LANG_SEQGEN_CACHE_SIZE);
        int seqgenCacheSize=PropertyUtil.intPropertyValue(Property.LANG_SEQGEN_CACHE_SIZE,value,
                0,Integer.MAX_VALUE,Property.LANG_SEQGEN_CACHE_SIZE_DEFAULT);

        value=startParams.getProperty(Property.LANG_PERMISSIONS_CACHE_SIZE);
        permissionsCacheSize=PropertyUtil.intPropertyValue(Property.LANG_PERMISSIONS_CACHE_SIZE, value,
                0, Integer.MAX_VALUE, Property.LANG_PERMISSIONS_CACHE_SIZE_DEFAULT);

        oidTdCache = CacheBuilder.newBuilder().maximumSize(tdCacheSize).build();
        nameTdCache = CacheBuilder.newBuilder().maximumSize(tdCacheSize).build();
        if(stmtCacheSize>0){
            spsNameCache = CacheBuilder.newBuilder().maximumSize(stmtCacheSize).build();
            storedPreparedStatementCache = CacheBuilder.newBuilder().maximumSize(stmtCacheSize).build();
        }
        sequenceGeneratorCache=CacheBuilder.newBuilder().maximumSize(seqgenCacheSize).build();
        partitionStatisticsCache = CacheBuilder.newBuilder().maximumSize(8092).build();
        permissionsCache=CacheBuilder.newBuilder().maximumSize(permissionsCacheSize).build();

    }

    public TableDescriptor oidTdCacheFind(UUID tableID) throws StandardException {
        TableDescriptor td =  oidTdCache.getIfPresent(tableID);
        if (td!=null) // bind in previous command might have set
            td.setReferencedColumnMap(null);
        return td;
    }

    public TableDescriptor nameTdCacheFind(TableKey tableKey) {
        return nameTdCache.getIfPresent(tableKey);
    }

    public List<PartitionStatisticsDescriptor> partitionStatisticsCacheFind(Long conglomID) {
        return partitionStatisticsCache.getIfPresent(conglomID);
    }

    public void partitionStatisticsCacheAdd(Long conglomID, List<PartitionStatisticsDescriptor> list) {
        partitionStatisticsCache.put(conglomID, list);
    }

    public TableDescriptor nameTdCacheInvalidate(TableKey tableKey) {
        TableDescriptor td = nameTdCache.getIfPresent(tableKey);
        nameTdCache.invalidate(tableKey);
        return td;
    }



    public PermissionsDescriptor permissionCacheFind(PermissionsDescriptor desc) {
        return permissionsCache.getIfPresent(desc);
    }

    public SPSDescriptor spsNameCacheFind(TableKey tableKey) {
        return spsNameCache.getIfPresent(tableKey);
    }

    public void spsNameCacheAdd(TableKey tableKey, SPSDescriptor sps) {
        spsNameCache.put(tableKey,sps);
    }

    public void permissionCacheRemove(PermissionsDescriptor desc) {
        permissionsCache.invalidate(desc);
    }

    public void storedPreparedStatementCacheAdd(SPSDescriptor desc) {
        storedPreparedStatementCache.put(desc.getUUID(),desc);
    }

    public SPSDescriptor storedPreparedStatementCacheFind(UUID uuid) {
        return storedPreparedStatementCache.getIfPresent(uuid);
    }


    public void storedPreparedStatementCacheRemove(SPSDescriptor desc) {
        storedPreparedStatementCache.invalidate(desc.getUUID());
    }

    public void sequenceGeneratorCacheClearAll() {
        sequenceGeneratorCache.invalidateAll();
    }

    public SequenceUpdater sequenceGeneratorCacheFind(String uuid) {
        return sequenceGeneratorCache.getIfPresent(uuid);
    }

    public void clearAll() {
        oidTdCache.invalidateAll();
        nameTdCache.invalidateAll();
        spsNameCache.invalidateAll();
        sequenceGeneratorCache.invalidateAll();
        permissionsCache.invalidateAll();
        partitionStatisticsCache.invalidateAll();
        storedPreparedStatementCache.invalidateAll();
    }

    public void invalidateCachedStatistics(Long conglomID) {
        partitionStatisticsCache.invalidate(conglomID);
    }

}