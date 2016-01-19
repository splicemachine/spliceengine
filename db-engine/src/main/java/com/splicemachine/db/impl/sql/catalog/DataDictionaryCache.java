package com.splicemachine.db.impl.sql.catalog;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.services.property.PropertyUtil;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.store.access.conglomerate.Conglomerate;
import com.splicemachine.db.impl.sql.GenericStatement;
import com.splicemachine.db.impl.sql.GenericStorablePreparedStatement;
import org.apache.log4j.Logger;
import javax.management.MXBean;
import java.util.List;
import java.util.Properties;

/**
 *
 * Cache Holder for making sense of data dictionary caching and concurrency.
 *
 */
public class DataDictionaryCache {
    private static Logger LOG = Logger.getLogger(DataDictionaryCache.class);
    private Cache<UUID,TableDescriptor> oidTdCache;
    private Cache<TableKey,TableDescriptor> nameTdCache;
    private Cache<TableKey,SPSDescriptor> spsNameCache;
    private Cache<String,SequenceUpdater> sequenceGeneratorCache;
    private Cache<PermissionsDescriptor,PermissionsDescriptor> permissionsCache;
    private Cache<Long,List<PartitionStatisticsDescriptor>> partitionStatisticsCache;
    private Cache<UUID, SPSDescriptor> storedPreparedStatementCache;
    private Cache<Long,Conglomerate> conglomerateCache;
    private Cache<GenericStatement,GenericStorablePreparedStatement> statementCache;
    private Cache<String,SchemaDescriptor> schemaCache;
    private Cache<String,Optional<RoleGrantDescriptor>> roleCache;
    private int tdCacheSize;
    private int stmtCacheSize;
    private int permissionsCacheSize;
    private DataDictionary dd;


    public DataDictionaryCache(Properties startParams,DataDictionary dd) throws StandardException {
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
        conglomerateCache = CacheBuilder.newBuilder().maximumSize(1024).build();
        statementCache = CacheBuilder.newBuilder().maximumSize(1024).build();
        schemaCache = CacheBuilder.newBuilder().maximumSize(1024).build();
        roleCache = CacheBuilder.newBuilder().maximumSize(100).build();
        permissionsCache=CacheBuilder.newBuilder().maximumSize(permissionsCacheSize).build();
        this.dd = dd;
    }

    public TableDescriptor nameTdCacheFind(TableKey tableKey) throws StandardException {
        if (LOG.isDebugEnabled())
            LOG.debug("nameTdCacheFind " + tableKey);
        if (!dd.canUseCache())
            return null;
        return nameTdCache.getIfPresent(tableKey);
    }

    public void nameTdCacheAdd(TableKey tableKey, TableDescriptor td) throws StandardException {
        if (LOG.isDebugEnabled())
            LOG.debug("nameTdCacheAdd " + tableKey + " : " + td);
        if (!dd.canUseCache())
            return;
        nameTdCache.put(tableKey,td);
    }

    public TableDescriptor nameTdCacheRemove(TableKey tableKey) throws StandardException {
        if (LOG.isDebugEnabled())
            LOG.debug("nameTdCacheInvalidate " + tableKey);
        TableDescriptor td = nameTdCache.getIfPresent(tableKey);
        nameTdCache.invalidate(tableKey);
        return td;
    }

    public TableDescriptor oidTdCacheFind(UUID tableID) throws StandardException {
        if (LOG.isDebugEnabled())
            LOG.debug("oidTdCacheFind " + tableID);
        if (!dd.canUseCache())
            return null;
        TableDescriptor td =  oidTdCache.getIfPresent(tableID);
        if (td!=null) // bind in previous command might have set
            td.setReferencedColumnMap(null);
        return td;
    }

    public void oidTdCacheAdd(UUID tableID, TableDescriptor td) throws StandardException {
        if (LOG.isDebugEnabled())
            LOG.debug("oidTdCacheAdd " + tableID + " : " + td);
        if (!dd.canUseCache())
            return;
        oidTdCache.put(tableID,td);
    }

    public TableDescriptor oidTdCacheRemove(UUID tableID) throws StandardException {
        if (LOG.isDebugEnabled())
            LOG.debug("oidTdCacheRemove " + tableID);
        TableDescriptor td = oidTdCache.getIfPresent(tableID);
        oidTdCache.invalidate(tableID);
        return td;
    }


    public List<PartitionStatisticsDescriptor> partitionStatisticsCacheFind(Long conglomID) throws StandardException {
        if (LOG.isDebugEnabled())
            LOG.debug("partitionStatisticsCacheFind " + conglomID);
        if (!dd.canUseCache())
            return null;
        return partitionStatisticsCache.getIfPresent(conglomID);
    }

    public void partitionStatisticsCacheAdd(Long conglomID, List<PartitionStatisticsDescriptor> list) throws StandardException {
        if (LOG.isDebugEnabled())
            LOG.debug("partitionStatisticsCacheAdd " + conglomID);
        if (!dd.canUseCache())
            return;
        partitionStatisticsCache.put(conglomID, list);
    }

    public void partitionStatisticsCacheRemove(Long conglomID) throws StandardException {
        if (LOG.isDebugEnabled())
            LOG.debug("invalidateCachedStatistics " + conglomID);
        partitionStatisticsCache.invalidate(conglomID);
    }



    public PermissionsDescriptor permissionCacheFind(PermissionsDescriptor desc) throws StandardException {
        if (LOG.isDebugEnabled())
            LOG.debug("permissionCacheFind " + desc);
        if (!dd.canUseCache())
            return null;
        return permissionsCache.getIfPresent(desc);
    }

    public SPSDescriptor spsNameCacheFind(TableKey tableKey) throws StandardException {
        if (LOG.isDebugEnabled())
            LOG.debug("spsNameCacheFind " + tableKey);
        if (!dd.canUseSPSCache())
            return null;
        return spsNameCache.getIfPresent(tableKey);
    }

    public void spsNameCacheAdd(TableKey tableKey, SPSDescriptor sps) throws StandardException {
        if (LOG.isDebugEnabled())
            LOG.debug("spsNameCacheAdd tableKey=" + tableKey + " descriptor="+sps);
        if (!dd.canUseSPSCache())
            return;
        spsNameCache.put(tableKey,sps);
    }

    public void permissionCacheRemove(PermissionsDescriptor desc) throws StandardException {
        if (LOG.isDebugEnabled())
            LOG.debug("permissionCacheRemove " + desc);
        permissionsCache.invalidate(desc);
    }

    public void storedPreparedStatementCacheAdd(SPSDescriptor desc) throws StandardException {
        if (LOG.isDebugEnabled())
            LOG.debug("storedPreparedStatementCacheAdd " + desc);
        if (!dd.canUseCache())
            return;
        storedPreparedStatementCache.put(desc.getUUID(), desc);
    }

    public SPSDescriptor storedPreparedStatementCacheFind(UUID uuid) throws StandardException {
        if (LOG.isDebugEnabled())
            LOG.debug("storedPreparedStatementCacheFind " + uuid);
        if (!dd.canUseCache())
            return null;
        return storedPreparedStatementCache.getIfPresent(uuid);
    }

    public Conglomerate conglomerateCacheFind(Long conglomId) throws StandardException {
        if (LOG.isDebugEnabled())
            LOG.debug("conglomerateCacheFind " + conglomId);
        if (!dd.canUseCache())
            return null;
        return conglomerateCache.getIfPresent(conglomId);
    }

    public void conglomerateCacheAdd(Long conglomId, Conglomerate conglomerate) throws StandardException {
        if (LOG.isDebugEnabled())
            LOG.debug("conglomerateCacheAdd " + conglomId + " : " + conglomerate);
        if (!dd.canUseCache())
            return;
        conglomerateCache.put(conglomId,conglomerate);
    }

    public void conglomerateCacheRemove(Long conglomId) throws StandardException {
        if (LOG.isDebugEnabled())
            LOG.debug("conglomerateCacheRemove " + conglomId);
        conglomerateCache.invalidate(conglomId);
    }


    public SchemaDescriptor schemaCacheFind(String schemaName) throws StandardException {
        if (LOG.isDebugEnabled())
            LOG.debug("schemaCacheFind " + schemaName);
        if (!dd.canUseCache())
            return null;
        return schemaCache.getIfPresent(schemaName);
    }

    public void schemaCacheAdd(String schemaName, SchemaDescriptor descriptor) throws StandardException {
        if (LOG.isDebugEnabled())
            LOG.debug("schemaCacheAdd " + schemaName + " : " + descriptor);
        if (!dd.canUseCache())
            return;
        schemaCache.put(schemaName,descriptor);
    }

    public void schemaCacheRemove(String schemaName) throws StandardException {
        if (LOG.isDebugEnabled())
            LOG.debug("schemaCacheRemove " + schemaName);
        schemaCache.invalidate(schemaName);
    }

    public void storedPreparedStatementCacheRemove(SPSDescriptor desc) throws StandardException {
        if (LOG.isDebugEnabled())
            LOG.debug("storedPreparedStatementCacheRemove " + desc);
        storedPreparedStatementCache.invalidate(desc.getUUID());
    }

    public void sequenceGeneratorCacheClearAll() throws StandardException {
        if (LOG.isDebugEnabled())
            LOG.debug("sequenceGeneratorCacheClearAll");
        sequenceGeneratorCache.invalidateAll();
    }

    public SequenceUpdater sequenceGeneratorCacheFind(String uuid) throws StandardException {
        if (LOG.isDebugEnabled())
            LOG.debug("sequenceGeneratorCacheFind " + uuid);
        if (!dd.canUseCache())
            return null;
        return sequenceGeneratorCache.getIfPresent(uuid);
    }

    public void clearAll() {
        if (LOG.isDebugEnabled())
            LOG.debug("clearAll");
        oidTdCache.invalidateAll();
        nameTdCache.invalidateAll();
        spsNameCache.invalidateAll();
        sequenceGeneratorCache.invalidateAll();
        permissionsCache.invalidateAll();
        partitionStatisticsCache.invalidateAll();
        storedPreparedStatementCache.invalidateAll();
        schemaCache.invalidateAll();
        statementCache.invalidateAll();
        roleCache.invalidateAll();
    }

    public void clearTableCache(){
        oidTdCache.invalidateAll();
        nameTdCache.invalidateAll();
        partitionStatisticsCache.invalidateAll();
        schemaCache.invalidateAll();
        sequenceGeneratorCache.invalidateAll();
        permissionsCache.invalidateAll();
        statementCache.invalidateAll();
        roleCache.invalidateAll();
    }

    public void clearSchemaCache(){
        schemaCache.invalidateAll();
    }

    public void removeStatement(GenericStatement gs) throws StandardException {
        statementCache.invalidate(gs);
    }

    public void emptyStatementCache() {
        statementCache.invalidateAll();
    }

    public void statementCacheAdd(GenericStatement gs, GenericStorablePreparedStatement gsp) throws StandardException {
        if (LOG.isDebugEnabled())
            LOG.debug("statementCacheAdd " + gs);
        if (!dd.canUseCache())
            return;
        statementCache.put(gs,gsp);
    }

    public GenericStorablePreparedStatement statementCacheFind(GenericStatement gs) throws StandardException {
        if (LOG.isDebugEnabled())
            LOG.debug("statementCacheFind " + gs);
        if (!dd.canUseCache())
            return null;
        return statementCache.getIfPresent(gs);
    }

    public void roleCacheAdd(String roleName, Optional<RoleGrantDescriptor> optional) throws StandardException {
        if (LOG.isDebugEnabled())
            LOG.debug("roleCacheAdd " + roleName);
        if (!dd.canUseCache())
            return;
        roleCache.put(roleName,optional);
    }

    public Optional<RoleGrantDescriptor> roleCacheFind(String roleName) throws StandardException {
        if (LOG.isDebugEnabled())
            LOG.debug("roleCacheFind " + roleName);
        if (!dd.canUseCache())
            return null;
        return roleCache.getIfPresent(roleName);
    }

    public void roleCacheRemove(String roleName) throws StandardException {
        if (LOG.isDebugEnabled())
            LOG.debug("roleCacheRemove " + roleName);
        roleCache.invalidate(roleName);
    }



    @MXBean
    @SuppressWarnings("UnusedDeclaration")
    public interface DataDictionaryCacheIFace {

    }

}