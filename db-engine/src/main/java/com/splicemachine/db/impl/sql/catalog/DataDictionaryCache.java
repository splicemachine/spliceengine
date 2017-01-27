/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.catalog;

import com.google.common.base.Optional;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.services.property.PropertyUtil;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.depend.Dependent;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.access.conglomerate.Conglomerate;
import com.splicemachine.db.impl.sql.GenericStatement;
import com.splicemachine.db.impl.sql.GenericStorablePreparedStatement;
import org.apache.log4j.Logger;
import org.spark_project.guava.cache.Cache;
import org.spark_project.guava.cache.CacheBuilder;
import org.spark_project.guava.cache.RemovalListener;
import org.spark_project.guava.cache.RemovalNotification;

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

        RemovalListener<Object,Dependent> dependentInvalidator = new RemovalListener<Object, Dependent>() {
            @Override
            public void onRemoval(RemovalNotification<Object, Dependent> removalNotification) {
                LanguageConnectionContext lcc=(LanguageConnectionContext)
                        ContextService.getContextOrNull(LanguageConnectionContext.CONTEXT_ID);
                try {
                    removalNotification.getValue().makeInvalid(DependencyManager.INTERNAL_RECOMPILE_REQUEST, lcc);
                } catch (StandardException e) {
                    LOG.error("Failed to invalidate " + removalNotification.getValue(), e);
                }
            }
        };
        oidTdCache = CacheBuilder.newBuilder().maximumSize(tdCacheSize).build();
        nameTdCache = CacheBuilder.newBuilder().maximumSize(tdCacheSize).build();
        if(stmtCacheSize>0){
            spsNameCache = CacheBuilder.newBuilder().maximumSize(stmtCacheSize).removalListener(dependentInvalidator).build();
            storedPreparedStatementCache = CacheBuilder.newBuilder().maximumSize(stmtCacheSize).removalListener(dependentInvalidator).build();
        }
        sequenceGeneratorCache=CacheBuilder.newBuilder().maximumSize(seqgenCacheSize).build();
        partitionStatisticsCache = CacheBuilder.newBuilder().maximumSize(8092).build();
        conglomerateCache = CacheBuilder.newBuilder().maximumSize(1024).build();
        statementCache = CacheBuilder.newBuilder().maximumSize(1024).removalListener(dependentInvalidator).build();
        schemaCache = CacheBuilder.newBuilder().maximumSize(1024).build();
        roleCache = CacheBuilder.newBuilder().maximumSize(100).build();
        permissionsCache=CacheBuilder.newBuilder().maximumSize(permissionsCacheSize).build();
        this.dd = dd;
    }

    public TableDescriptor nameTdCacheFind(TableKey tableKey) throws StandardException {
        if (!dd.canUseCache(null))
            return null;
        if (LOG.isDebugEnabled())
            LOG.debug("nameTdCacheFind " + tableKey);
        return nameTdCache.getIfPresent(tableKey);
    }

    public void nameTdCacheAdd(TableKey tableKey, TableDescriptor td) throws StandardException {
        if (!dd.canUseCache(null))
            return;
        if (LOG.isDebugEnabled())
            LOG.debug("nameTdCacheAdd " + tableKey + " : " + td);
        nameTdCache.put(tableKey,td);
        oidTdCache.put(td.getUUID(),td);
    }

    public TableDescriptor nameTdCacheRemove(TableKey tableKey) throws StandardException {
        TableDescriptor td = nameTdCache.getIfPresent(tableKey);
        if (LOG.isDebugEnabled())
            LOG.debug("nameTdCacheInvalidate " + tableKey + (td != null ? " found" : " null"));
        nameTdCache.invalidate(tableKey);
        return td;
    }

    public TableDescriptor oidTdCacheFind(UUID tableID) throws StandardException {
        if (!dd.canUseCache(null))
            return null;
        TableDescriptor td =  oidTdCache.getIfPresent(tableID);
        if (LOG.isDebugEnabled())
            LOG.debug("oidTdCacheFind " + tableID + (td != null ? " found" : " null"));
        if (td!=null) // bind in previous command might have set
            td.setReferencedColumnMap(null);
        return td;
    }

    public void oidTdCacheAdd(UUID tableID, TableDescriptor td) throws StandardException {
        if (!dd.canUseCache(null))
            return;
        if (LOG.isDebugEnabled())
            LOG.debug("oidTdCacheAdd " + tableID + " : " + td);
        oidTdCache.put(tableID,td);
    }

    public TableDescriptor oidTdCacheRemove(UUID tableID) throws StandardException {
        TableDescriptor td = oidTdCache.getIfPresent(tableID);
        if (LOG.isDebugEnabled())
            LOG.debug("oidTdCacheRemove " + tableID + (td != null ? " found" : " null"));
        oidTdCache.invalidate(tableID);
        return td;
    }


    public List<PartitionStatisticsDescriptor> partitionStatisticsCacheFind(Long conglomID) throws StandardException {
        if (!dd.canUseCache(null))
            return null;
        if (LOG.isDebugEnabled())
            LOG.debug("partitionStatisticsCacheFind " + conglomID);
        return partitionStatisticsCache.getIfPresent(conglomID);
    }

    public void partitionStatisticsCacheAdd(Long conglomID, List<PartitionStatisticsDescriptor> list) throws StandardException {
        if (!dd.canUseCache(null))
            return;
        if (LOG.isDebugEnabled())
            LOG.debug("partitionStatisticsCacheAdd " + conglomID);
        partitionStatisticsCache.put(conglomID, list);
    }

    public void partitionStatisticsCacheRemove(Long conglomID) throws StandardException {
        if (LOG.isDebugEnabled())
            LOG.debug("invalidateCachedStatistics " + conglomID);
        partitionStatisticsCache.invalidate(conglomID);
    }

    public void permissionCacheAdd(PermissionsDescriptor key, PermissionsDescriptor permissions) throws StandardException {
        if (!dd.canUseCache(null))
            return;
        if (LOG.isDebugEnabled())
            LOG.debug("permissionCacheAdd " + key);
        if (key != null && permissions != null) {
            permissionsCache.put(key, permissions);
        }
    }

    public void permissionCacheRemove(PermissionsDescriptor desc) throws StandardException {
        if (LOG.isDebugEnabled())
            LOG.debug("permissionCacheRemove " + desc);
        permissionsCache.invalidate(desc);
    }

    public PermissionsDescriptor permissionCacheFind(PermissionsDescriptor desc) throws StandardException {
        if (!dd.canUseCache(null))
            return null;
        if (LOG.isDebugEnabled())
            LOG.debug("permissionCacheFind " + desc);
        return permissionsCache.getIfPresent(desc);
    }

    public SPSDescriptor spsNameCacheFind(TableKey tableKey) throws StandardException {
        if (!dd.canUseSPSCache())
            return null;
        if (LOG.isDebugEnabled())
            LOG.debug("spsNameCacheFind " + tableKey);
        return spsNameCache.getIfPresent(tableKey);
    }

    public void spsNameCacheAdd(TableKey tableKey, SPSDescriptor sps) throws StandardException {
        if (!dd.canUseSPSCache())
            return;
        if (LOG.isDebugEnabled())
            LOG.debug("spsNameCacheAdd tableKey=" + tableKey + " descriptor="+sps);
        spsNameCache.put(tableKey, sps);
    }

    public void storedPreparedStatementCacheAdd(SPSDescriptor desc) throws StandardException {
        if (!dd.canUseCache(null))
            return;
        if (LOG.isDebugEnabled())
            LOG.debug("storedPreparedStatementCacheAdd " + desc);
        storedPreparedStatementCache.put(desc.getUUID(), desc);
    }

    public SPSDescriptor storedPreparedStatementCacheFind(UUID uuid) throws StandardException {
        if (!dd.canUseCache(null))
            return null;
        if (LOG.isDebugEnabled())
            LOG.debug("storedPreparedStatementCacheFind " + uuid);
        return storedPreparedStatementCache.getIfPresent(uuid);
    }


    public Conglomerate conglomerateCacheFind(TransactionController xactMgr,Long conglomId) throws StandardException {
        if (!dd.canUseCache(xactMgr) && conglomId>=DataDictionary.FIRST_USER_TABLE_NUMBER)
            // Use cache even if dd says we can't as long as it's a system table (conglomID is < FIRST_USER_TABLE_NUMBER)
            return null;
        if (LOG.isDebugEnabled())
            LOG.debug("conglomerateCacheFind " + conglomId);
        return conglomerateCache.getIfPresent(conglomId);
    }

    public Conglomerate conglomerateCacheFind(Long conglomId) throws StandardException {
        return conglomerateCacheFind(null,conglomId);
    }

    public void conglomerateCacheAdd(Long conglomId, Conglomerate conglomerate,TransactionController xactMgr) throws StandardException {
        if (!dd.canUseCache(xactMgr))
            return;
        if (LOG.isDebugEnabled())
            LOG.debug("conglomerateCacheAdd " + conglomId + " : " + conglomerate);
        conglomerateCache.put(conglomId,conglomerate);
    }

    public void conglomerateCacheAdd(Long conglomId, Conglomerate conglomerate) throws StandardException {
        conglomerateCacheAdd(conglomId, conglomerate,null);
    }

    public void conglomerateCacheRemove(Long conglomId) throws StandardException {
        if (LOG.isDebugEnabled())
            LOG.debug("conglomerateCacheRemove " + conglomId);
        conglomerateCache.invalidate(conglomId);
    }


    public SchemaDescriptor schemaCacheFind(String schemaName) throws StandardException {
        if (!dd.canUseCache(null))
            return null;
        if (LOG.isDebugEnabled())
            LOG.debug("schemaCacheFind " + schemaName);
        return schemaCache.getIfPresent(schemaName);
    }

    public void schemaCacheAdd(String schemaName, SchemaDescriptor descriptor) throws StandardException {
        if (!dd.canUseCache(null))
            return;
        if (LOG.isDebugEnabled())
            LOG.debug("schemaCacheAdd " + schemaName + " : " + descriptor);
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
        if (!dd.canUseCache(null))
            return null;
        if (LOG.isDebugEnabled())
            LOG.debug("sequenceGeneratorCacheFind " + uuid);
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

    public void statementCacheRemove(GenericStatement gs) throws StandardException {
        if (LOG.isDebugEnabled()) {
            GenericStorablePreparedStatement gsps = statementCache.getIfPresent(gs);
            LOG.debug("statementCacheRemove " + gs.toString() +(gsps != null ? " found" : " null"));
        }
        statementCache.invalidate(gs);
    }

    public void clearStatementCache() {
        if (LOG.isDebugEnabled())
            LOG.debug("clearStatementCache ");
        statementCache.invalidateAll();
    }

    public void statementCacheAdd(GenericStatement gs, GenericStorablePreparedStatement gsp) throws StandardException {
        if (!dd.canUseCache(null))
            return;
        if (LOG.isDebugEnabled())
            LOG.debug("statementCacheAdd " + gs.toString());
        statementCache.put(gs,gsp);
    }

    public GenericStorablePreparedStatement statementCacheFind(GenericStatement gs) throws StandardException {
        if (!dd.canUseCache(null))
            return null;
        GenericStorablePreparedStatement gsps = statementCache.getIfPresent(gs);
        if (LOG.isDebugEnabled())
            LOG.debug("statementCacheFind " + gs.toString() +(gsps != null ? " found" : " null"));
        return gsps;
    }

    public void roleCacheAdd(String roleName, Optional<RoleGrantDescriptor> optional) throws StandardException {
        if (!dd.canUseCache(null))
            return;
        if (LOG.isDebugEnabled())
            LOG.debug("roleCacheAdd " + roleName);
        roleCache.put(roleName,optional);
    }

    public Optional<RoleGrantDescriptor> roleCacheFind(String roleName) throws StandardException {
        if (!dd.canUseCache(null))
            return null;
        if (LOG.isDebugEnabled())
            LOG.debug("roleCacheFind " + roleName);
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