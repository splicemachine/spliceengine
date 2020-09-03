/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.protobuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.ZeroCopyLiteralByteString;
import com.splicemachine.db.catalog.IndexDescriptor;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.util.ByteArray;
import com.splicemachine.db.impl.services.uuid.BasicUUID;
import com.splicemachine.db.impl.sql.catalog.SYSTABLESRowFactory;
import com.splicemachine.db.impl.sql.execute.ColumnInfo;
import com.splicemachine.ddl.DDLMessage.*;
import com.splicemachine.derby.DerbyMessage;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.utils.DataDictionaryUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.log4j.Logger;
import splice.com.google.common.base.Function;
import splice.com.google.common.base.Joiner;
import splice.com.google.common.collect.Lists;
import splice.com.google.common.primitives.Ints;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * Created by jleach on 11/13/15.
 */
public class ProtoUtil {
    private static final Logger LOG = Logger.getLogger(ProtoUtil.class);
    private static Function TABLEDESCRIPTORTOUUID = new Function<TableDescriptor,DerbyMessage.UUID>() {
        @Override
        public DerbyMessage.UUID apply(@Nullable TableDescriptor td) {
            assert td!=null;
            return transferDerbyUUID((BasicUUID)td.getUUID());
        }
    };

    public static DDLChange alterStats(long txnId, List<TableDescriptor> tableDescriptors) {
        return DDLChange.newBuilder().setTxnId(txnId)
                .setAlterStats(AlterStats.newBuilder().addAllTableId(Lists.transform(tableDescriptors,TABLEDESCRIPTORTOUUID)))
                .setDdlChangeType(DDLChangeType.ALTER_STATS)
                .build();
    }

    public static DDLChange createDropSchema(long txnId, String schemaName, BasicUUID schemaUUID) {
        return DDLChange.newBuilder().setTxnId(txnId).setDropSchema(DropSchema.newBuilder()
                .setSchemaName(schemaName)
                .setSchemaUUID(transferDerbyUUID(schemaUUID))
                .build())
                .setDdlChangeType(DDLChangeType.DROP_SCHEMA)
                .build();
    }

    public static DDLChange createUpdateSchemaOwner(long txnId, String schemaName, String ownerName, BasicUUID schemaUUID) {
        return DDLChange.newBuilder().setTxnId(txnId).setUpdateSchemaOwner(UpdateSchemaOwner.newBuilder()
                .setSchemaName(schemaName)
                .setOwnerName(ownerName)
                .setSchemaUUID(transferDerbyUUID(schemaUUID))
                .build())
                .setDdlChangeType(DDLChangeType.UPDATE_SCHEMA_OWNER)
                .build();
    }

    public static DDLChange createGrantRevokeRole(long txnId, String roleName, String granteeName, String grantorName, boolean isGrant) {
        return DDLChange.newBuilder().setTxnId(txnId).setGrantRevokeRole(GrantRevokeRole.newBuilder()
                .setType(isGrant?GrantRevokeRole.Type.GRANT_OP:GrantRevokeRole.Type.REVOKE_OP)
                .setRoleName(roleName)
                .setGranteeName(granteeName)
                .setGrantorName(grantorName)
                .build())
                .setDdlChangeType(DDLChangeType.GRANT_REVOKE_ROLE)
                .build();
    }

    public static DDLChange createSetDatabaseProperty(long txnId, String key) {
        return DDLChange.newBuilder().setTxnId(txnId).setSetDatabaseProperty(SetDatabaseProperty.newBuilder()
                 .setPropertyName(key)
                 .build())
                 .setDdlChangeType(DDLChangeType.SET_DATABASE_PROPERTY)
                 .build();
    }

    public static DDLChange createRefreshEnterpriseFeatures(long txnId) {
        return DDLChange.newBuilder().setTxnId(txnId).setRefreshEnterpriseFeatures(RefreshEnterpriseFeatures.newBuilder().build())
                .setDdlChangeType(DDLChangeType.REFRESH_ENTRPRISE_FEATURES)
                .build();
    }


    public static DDLChange createSetReplicationRole(long txnId, String role) {
        return DDLChange.newBuilder().setTxnId(txnId).setSetReplicationRole(SetReplicationRole.newBuilder()
                .setRole(role)
                .build())
                .setDdlChangeType(DDLChangeType.SET_REPLICATION_ROLE)
                .build();
    }

    public static DDLChange createNotifyModifyClasspath(long txnId, String classpath) {
        return DDLChange.newBuilder().setTxnId(txnId).setNotifyModifyClasspath(NotifyModifyClasspath.newBuilder()
                .setClasspath(classpath).build())
                .setDdlChangeType(DDLChangeType.NOTIFY_MODIFY_CLASSPATH)
                .build();
    }

    public static DDLChange createNotifyJarLoader(long txnId, boolean reload, boolean drop, String schemaName, String sqlName) {
        return DDLChange.newBuilder().setTxnId(txnId).setNotifyJarLoader(NotifyJarLoader.newBuilder()
                .setReload(reload)
                .setDrop(drop)
                .setSchemaName(schemaName==null?"":schemaName)
                .setSqlName(sqlName==null?"":sqlName)
                .build())
                .setDdlChangeType(DDLChangeType.NOTIFY_JAR_LOADER)
                .build();
    }

    public static DDLChange createTrigger(long txnId, BasicUUID basicUUID) {
        return DDLChange.newBuilder().setTxnId(txnId).setCreateTrigger(CreateTrigger.newBuilder()
                .setTableId(transferDerbyUUID(basicUUID)))
                .setDdlChangeType(DDLChangeType.CREATE_TRIGGER)
                .build();
    }

    public static DDLChange dropTrigger(long txnId, BasicUUID tableUUID, BasicUUID triggerUUID, BasicUUID spsDescriptorUUID) {
        return DDLChange.newBuilder().setTxnId(txnId).setDropTrigger(DropTrigger.newBuilder()
                .setTableId(transferDerbyUUID(tableUUID))
                .setTriggerId(transferDerbyUUID(triggerUUID))
                .setSpsDescriptorUUID(transferDerbyUUID(spsDescriptorUUID)))
                .setDdlChangeType(DDLChangeType.DROP_TRIGGER)
                .build();
    }

    public static DerbyMessage.UUID transferDerbyUUID(BasicUUID basicUUID) {
        return DerbyMessage.UUID.newBuilder().setMajorId(basicUUID.majorId)
                .setSequence(basicUUID.sequence)
                .setTimemillis(basicUUID.timemillis).build();
    }

    public static BasicUUID getDerbyUUID(DerbyMessage.UUID messageUUID) {
        return new BasicUUID(messageUUID.getMajorId(),messageUUID.getTimemillis(),messageUUID.getSequence());
    }


    public static DDLChange createDropTable(long txnId, BasicUUID basicUUID) {
        return DDLChange.newBuilder().setTxnId(txnId).setDropTable(DropTable.newBuilder()
                .setTableId(transferDerbyUUID(basicUUID)))
                .setDdlChangeType(DDLChangeType.DROP_TABLE)
                .build();
    }

    public static DDLChange createTable(long txnId) {
        return DDLChange.newBuilder().setTxnId(txnId)
                .setDdlChangeType(DDLChangeType.CREATE_TABLE)
                .build();
    }

    public static DDLChange createSchema(long txnId) {
        return DDLChange.newBuilder().setTxnId(txnId)
                .setDdlChangeType(DDLChangeType.CREATE_SCHEMA)
                .build();
    }

    public static DDLChange createAlias(long txnId) {
        return DDLChange.newBuilder().setTxnId(txnId)
                .setDdlChangeType(DDLChangeType.CREATE_ALIAS)
                .build();
    }

    public static DDLChange createView(long txnId) {
        return DDLChange.newBuilder().setTxnId(txnId)
                .setDdlChangeType(DDLChangeType.CREATE_VIEW)
                .build();
    }


    public static DDLChange dropAlias(long txnId, String schemaName, String aliasName, String namespace) {
        return DDLChange.newBuilder().setTxnId(txnId).setDropAlias((DropAlias.newBuilder()
                .setSchemaName(schemaName)
                .setAliasName(aliasName)
                .setNamespace(namespace)))
                .setDdlChangeType(DDLChangeType.DROP_ALIAS)
                .build();
    }

    public static DDLChange dropView(long txnId, BasicUUID basicUUID) {
        return DDLChange.newBuilder().setTxnId(txnId).setDropView(DropView.newBuilder()
                .setTableId(transferDerbyUUID(basicUUID)))
                .setDdlChangeType(DDLChangeType.DROP_VIEW)
                .build();
    }

    public static DDLChange dropSequence(long txnId, String schemaName, String sequenceName) {
        return DDLChange.newBuilder().setTxnId(txnId).setDropSequence(DropSequence.newBuilder()
                .setSchemaName(schemaName)
                .setSequenceName(sequenceName))
                .setDdlChangeType(DDLChangeType.DROP_SEQUENCE)
                .build();
    }

    public static DDLChange createAlterTable(long txnId, BasicUUID basicUUID) {
        return DDLChange.newBuilder().setTxnId(txnId).setAlterTable(AlterTable.newBuilder()
                .addTableId(transferDerbyUUID(basicUUID)))
                .setDdlChangeType(DDLChangeType.ALTER_TABLE)
                .build();
    }

    public static DDLChange createRenameTable(long txnId, BasicUUID basicUUID) {
        return DDLChange.newBuilder().setTxnId(txnId).setRenameTable(RenameTable.newBuilder()
                .setTableId(transferDerbyUUID(basicUUID)))
                .setDdlChangeType(DDLChangeType.RENAME_TABLE)
                .build();
    }

    public static DDLChange createRenameIndex(long txnId, BasicUUID basicUUID) {
        return DDLChange.newBuilder().setTxnId(txnId).setRenameIndex(RenameIndex.newBuilder()
                .setTableId(transferDerbyUUID(basicUUID)))
                .setDdlChangeType(DDLChangeType.RENAME_INDEX)
                .build();
    }

    public static DDLChange createRenameColumn(long txnId, BasicUUID basicUUID, String oldColumnName) {
        return DDLChange.newBuilder().setTxnId(txnId).setRenameColumn(RenameColumn.newBuilder()
                .setTableId(transferDerbyUUID(basicUUID))
                .setColumnName(oldColumnName))
                .setDdlChangeType(DDLChangeType.RENAME_COLUMN)
                .build();
    }


    public static FKConstraintInfo createFKConstraintInfo(ForeignKeyConstraintDescriptor fKConstraintDescriptor) {
        try {
            String version = fKConstraintDescriptor.getTableDescriptor().getVersion();
            ColumnDescriptorList columnDescriptors = fKConstraintDescriptor.getColumnDescriptors();
            return FKConstraintInfo.newBuilder().setTableName(fKConstraintDescriptor.getTableDescriptor().getName())
                .addAllFormatIds(Ints.asList(columnDescriptors.getFormatIds()))
                .setParentTableVersion(version!=null?version: SYSTABLESRowFactory.CURRENT_TABLE_VERSION)
                .setConstraintName(fKConstraintDescriptor.getConstraintName())
                .setColumnNames(Joiner.on(",").join(Lists.transform(columnDescriptors, new ColumnDescriptorNameFunction()))).build();
        } catch (StandardException se) {
            throw new RuntimeException(se);
        }
    }

    private static class ColumnDescriptorNameFunction implements Function<ColumnDescriptor, String> {
        @Override
        public String apply(ColumnDescriptor columnDescriptor) {
            return columnDescriptor.getColumnName();
        }
    }

    public static DDLChange createDropIndex(long indexConglomId, long tableConglomId, long txnId, BasicUUID tableUUID, String schemaName, String indexName) {
        return DDLChange.newBuilder().setTxnId(txnId).setDropIndex(DropIndex.newBuilder()
                .setBaseConglomerate(tableConglomId)
                .setTableUUID(transferDerbyUUID(tableUUID))
                .setSchemaName(schemaName)
                .setIndexName(indexName)
                .setConglomerate(indexConglomId))
                .setDdlChangeType(DDLChangeType.DROP_INDEX)
                .build();
    }

    public static Index createIndex(long conglomerate, IndexDescriptor indexDescriptor, DataValueDescriptor defaultValue)
            throws StandardException
    {
        byte [] defaultValuesBytes = null;
        if (defaultValue!=null) {
            defaultValuesBytes = SerializationUtils.serialize(defaultValue);
        }
        boolean[] ascColumns = indexDescriptor.isAscending();
        Index.Builder builder=Index.newBuilder()
                .setConglomerate(conglomerate)
                .setUniqueWithDuplicateNulls(indexDescriptor.isUniqueWithDuplicateNulls())
                .setUnique(indexDescriptor.isUnique())
                .setExcludeDefaults(indexDescriptor.excludeDefaults())
                .setExcludeNulls(indexDescriptor.excludeNulls())
                .setNumExprs(indexDescriptor.getExprBytecode().length);
        if (defaultValuesBytes != null)
            builder.setDefaultValues(ByteString.copyFrom(defaultValuesBytes));
        for(int i=0;i<ascColumns.length;i++){
            builder = builder.addDescColumns(!ascColumns[i]);
        }

        int[] backingArray=indexDescriptor.baseColumnPositions();
        for(int i=0;i<backingArray.length;i++){
            builder = builder.addIndexColsToMainColMap(backingArray[i]);
        }

        ByteArray[] bytecodeArray = indexDescriptor.getExprBytecode();
        for (ByteArray bc : bytecodeArray) {
            builder = builder.addBytecodeExprs(ByteString.copyFrom(bc.getArray(), bc.getOffset(), bc.getLength()));
        }
        String[] classNames = indexDescriptor.getGeneratedClassNames();
        for (String cn : classNames) {
            builder = builder.addGeneratedClassNames(cn);
        }
        DataTypeDescriptor[] indexColumnTypes = indexDescriptor.getIndexColumnTypes();
        for (DataTypeDescriptor dtd : indexColumnTypes) {
            builder = builder.addIndexColumnFormatIds(dtd.getNull().getTypeFormatId());
        }
        return builder.build();
    }

    public static Table createTable(long conglomerate, TableDescriptor td, LanguageConnectionContext lcc) throws StandardException {
        assert td!=null:"TableDescriptor is null";
        assert td.getFormatIds()!=null:"No Format ids";
        SpliceConglomerate sc = (SpliceConglomerate)((SpliceTransactionManager)lcc.getTransactionExecute()).findConglomerate(conglomerate);
        Table.Builder builder=Table.newBuilder()
                .setConglomerate(conglomerate)
                .addAllFormatIds(Ints.asList(td.getFormatIds()))
                .addAllColumnOrdering(Ints.asList(sc.getColumnOrdering()))
                .setTableUuid(transferDerbyUUID((BasicUUID)td.getUUID()));
        String tV = DataDictionaryUtils.getTableVersion(lcc,td.getUUID());
        if(tV!=null)
            builder = builder.setTableVersion(tV);

        return builder.build();
    }

    public static DDLChange createTentativeIndexChange(long txnId, LanguageConnectionContext lcc, long baseConglomerate, long indexConglomerate,
                                                       TableDescriptor td, IndexDescriptor indexDescriptor, DataValueDescriptor defaultValues) throws StandardException {
        SpliceLogUtils.trace(LOG, "create Tentative Index {baseConglomerate=%d, indexConglomerate=%d");
        return DDLChange.newBuilder().setTentativeIndex(TentativeIndex.newBuilder()
                .setIndex(createIndex(indexConglomerate, indexDescriptor,defaultValues))
                .setTable(createTable(baseConglomerate,td,lcc))
                .setTxnId(txnId)
                .build())
                .setTxnId(txnId)
                .setDdlChangeType(DDLChangeType.CREATE_INDEX)
                .build();
    }

    public static TentativeIndex createTentativeIndex(LanguageConnectionContext lcc, long baseConglomerate, long indexConglomerate,
                                                       TableDescriptor td, IndexDescriptor indexDescriptor, DataValueDescriptor defaultValue) throws StandardException {
        SpliceLogUtils.trace(LOG, "create Tentative Index {baseConglomerate=%d, indexConglomerate=%d");
        return TentativeIndex.newBuilder()
                .setIndex(createIndex(indexConglomerate,indexDescriptor,defaultValue))
                .setTable(createTable(baseConglomerate,td,lcc))
                .setTxnId(((SpliceTransactionManager)lcc.getTransactionExecute()).getActiveStateTxn().getTxnId())
                .build();
    }

    public static DDLChange createRestoreMode(long txnId) {
        return DDLChange.newBuilder().setTxnId(txnId).setDdlChangeType(DDLChangeType.ENTER_RESTORE_MODE).build();
    }

    public static DDLChange createDropPKConstraint(long txnId, long newConglomId, long oldConglomId,
                                                   int[] srcColumnOrdering, int[] targetColumnOrdering,
                                                   ColumnInfo[] columInfos, LanguageConnectionContext lcc, BasicUUID tableId) throws StandardException {
        String tableVersion = DataDictionaryUtils.getTableVersion(lcc, tableId);
        return DDLChange.newBuilder().setTxnId(txnId)
                .setDdlChangeType(DDLChangeType.DROP_PRIMARY_KEY)
                .setTentativeDropPKConstraint(
                        TentativeDropPKConstraint.newBuilder()
                                .setNewConglomId(newConglomId)
                                .setOldConglomId(oldConglomId)
                                .setTableVersion(tableVersion)
                                .addAllSrcColumnOrdering(Ints.asList(srcColumnOrdering))
                                .addAllTargetColumnOrdering(Ints.asList(targetColumnOrdering))
                                .setColumnInfos(ZeroCopyLiteralByteString.wrap(DDLUtils.serializeColumnInfoArray(columInfos))
                                )).build();
    }

    public static DDLChange createTentativeAddColumn(long txnId, long newCongNum,
                                                     long oldCongNum, int[] columnOrdering,
                                                     ColumnInfo[] newColumnInfo, LanguageConnectionContext lcc, BasicUUID tableId) throws StandardException {
        String tableVersion = DataDictionaryUtils.getTableVersion(lcc, tableId);
        return DDLChange.newBuilder().setTxnId(txnId).setDdlChangeType(DDLChangeType.ADD_COLUMN)
                .setTentativeAddColumn(TentativeAddColumn.newBuilder()
                                .setTableVersion(tableVersion)
                                .setOldConglomId(oldCongNum)
                                .setNewConglomId(newCongNum)
                                .addAllColumnOrdering(columnOrdering!=null?Ints.asList(columnOrdering):Collections.EMPTY_LIST)
                                .setColumnInfo(ZeroCopyLiteralByteString.wrap(DDLUtils.serializeColumnInfoArray(newColumnInfo)))
                ).build();
    }

    public static DDLChange createTentativeDropColumn(long txnId, long newCongNum,
                                                     long oldCongNum, int[] oldColumnOrdering, int[] newColumnOrdering,
                                                     ColumnInfo[] columnInfo, int droppedColumnPosition, LanguageConnectionContext lcc, BasicUUID tableId) throws StandardException {
        String tableVersion = DataDictionaryUtils.getTableVersion(lcc, tableId);
        return DDLChange.newBuilder().setTxnId(txnId).setDdlChangeType(DDLChangeType.DROP_COLUMN)
                .setTentativeDropColumn(TentativeDropColumn.newBuilder()
                                .setTableVersion(tableVersion)
                                .setOldConglomId(oldCongNum)
                                .setNewConglomId(newCongNum)
                                .addAllOldColumnOrdering(Ints.asList(oldColumnOrdering))
                                .addAllNewColumnOrdering(Ints.asList(newColumnOrdering))
                                .setColumnInfos(ZeroCopyLiteralByteString.wrap(DDLUtils.serializeColumnInfoArray(columnInfo)))
                                .setDroppedColumnPosition(droppedColumnPosition)
                ).build();
    }

    public static DDLChange createTentativeDropConstraint (long txnId, long oldConglomId, long indexConglomId, ColumnInfo[] columnInfo,
                                                           LanguageConnectionContext lcc, BasicUUID tableId) throws StandardException {
        String tableVersion = DataDictionaryUtils.getTableVersion(lcc, tableId);
        return DDLChange.newBuilder().setTxnId(txnId)
                .setDdlChangeType(DDLChangeType.DROP_CONSTRAINT)
                .setTentativeDropConstraint(TentativeDropConstraint.newBuilder()
                                .setTableVersion(tableVersion)
                                .setOldConglomId(oldConglomId)
                                .setColumnInfos(ZeroCopyLiteralByteString.wrap(DDLUtils.serializeColumnInfoArray(columnInfo)))
                                .setIndexConglomerateId(indexConglomId)
                ).build();
    }

    public static DDLChange createTentativeAddConstraint (long txnId, long oldConglomId,
                                                          long newConglomId, long indexConglomerateId,
                                                          int[] srcColumnOrdering, int[] targetColumnOrdering, ColumnInfo[] columnInfo, LanguageConnectionContext lcc, BasicUUID tableId) throws StandardException {
        String tableVersion = DataDictionaryUtils.getTableVersion(lcc, tableId);
    return DDLChange.newBuilder().setTxnId(txnId)
                .setDdlChangeType(DDLChangeType.ADD_UNIQUE_CONSTRAINT)
                .setTentativeAddConstraint(TentativeAddConstraint.newBuilder()
                                .setNewConglomId(newConglomId)
                                .setOldConglomId(oldConglomId)
                                .setIndexConglomerateId(indexConglomerateId)
                                .setTableVersion(tableVersion)
                       .addAllSrcColumnOrdering((srcColumnOrdering != null ? Ints.asList(srcColumnOrdering) : Ints.asList()))
                        .addAllTargetColumnOrdering((targetColumnOrdering != null ? Ints.asList(targetColumnOrdering) : Ints.asList()))
                                .setColumnInfos(ZeroCopyLiteralByteString.wrap(DDLUtils.serializeColumnInfoArray(columnInfo)))
                ).build();


    }

    public static DDLChange createTentativeFKConstraint(ForeignKeyConstraintDescriptor foreignKeyConstraintDescriptor, long txnId,
                                                        long baseConglomerate, String tableName, String tableVersion,
                                                        int[] backingIndexFormatIds, long backingIndexConglomerateId, DDLChangeType changeType) {
        return DDLChange.newBuilder().setTxnId(txnId)
                .setDdlChangeType(changeType)
                .setTentativeFK(TentativeFK.newBuilder()
                                .addAllBackingIndexFormatIds(Ints.asList(backingIndexFormatIds))
                                .setBaseConglomerate(baseConglomerate)
                                .setReferencedTableName(tableName)
                                .setReferencedTableVersion(tableVersion)
                                .setFkConstraintInfo(createFKConstraintInfo(foreignKeyConstraintDescriptor))
                                .setBackingIndexConglomerateId(backingIndexConglomerateId)
                                .setReferencedConglomerateNumber(baseConglomerate)
                                .setReferencingConglomerateNumber(backingIndexConglomerateId)
                ).build();
    }

    public static DDLChange createNoOpDDLChange(long txnId,String changeId,DDLChangeType changeType) {
        return DDLChange.newBuilder()
                .setTxnId(txnId)
                .setChangeId(changeId)
                .setDdlChangeType(changeType)
                .build();
    }

    public static DDLChange createDropRole(long txnId, String roleName) {
        return DDLChange.newBuilder().setDdlChangeType(DDLChangeType.DROP_ROLE)
                .setTxnId(txnId)
                .setDropRole(DropRole.newBuilder().setRoleName(roleName).build())
                .build();
    }

    public static DDLChange createAddRole(long txnId, String roleName) {
        return DDLChange.newBuilder().setDdlChangeType(DDLChangeType.CREATE_ROLE)
                .setTxnId(txnId)
                .setCreateRole(CreateRole.newBuilder().setRoleName(roleName).build())
                .build();
    }

    public static DDLChange createTruncateTable(long txnId, BasicUUID basicUUID) {
        return DDLChange.newBuilder().setDdlChangeType(DDLChangeType.TRUNCATE_TABLE)
                .setTxnId(txnId)
                .setTruncateTable(TruncateTable.newBuilder().setTableId(transferDerbyUUID(basicUUID)).build())
                .build();
    }

    public static DDLChange createRevokeSchemaPrivilege(long txnId, SchemaPermsDescriptor permissionsDescriptor, boolean grant) {
        RevokeSchemaPrivilege revokeSchemaPrivilege = RevokeSchemaPrivilege.newBuilder()
                .setSchemaId(transferDerbyUUID((BasicUUID) permissionsDescriptor.getSchemaUUID()))
                .setSelectPerm(permissionsDescriptor.getSelectPriv())
                .setDeletePerm(permissionsDescriptor.getDeletePriv())
                .setInsertPerm(permissionsDescriptor.getInsertPriv())
                .setUpdatePerm(permissionsDescriptor.getUpdatePriv())
                .setReferencesPerm(permissionsDescriptor.getReferencesPriv())
                .setTriggerPerm(permissionsDescriptor.getTriggerPriv())
                .setModifyPerm(permissionsDescriptor.getModifyPriv())
                .setAccessPerm(permissionsDescriptor.getAccessPriv())
                .setGrantor(permissionsDescriptor.getGrantor())
                .setGrantee(permissionsDescriptor.getGrantee())
                .setPermObjectId(transferDerbyUUID((BasicUUID) permissionsDescriptor.getUUID()))
                .build();
        RevokePrivilege revokePrivilege = RevokePrivilege.newBuilder()
                .setType(RevokePrivilege.Type.REVOKE_SCHEMA_PRIVILEGE)
                .setOp(grant? RevokePrivilege.OpType.GRANT_OP: RevokePrivilege.OpType.REVOKE_OP)
                .setRevokeSchemaPrivilege(revokeSchemaPrivilege)
                .build();

        return DDLChange.newBuilder().setDdlChangeType(DDLChangeType.REVOKE_PRIVILEGE)
                .setTxnId(txnId).setRevokePrivilege(revokePrivilege).build();
    }

    public static DDLChange createRevokeTablePrivilege(long txnId, TablePermsDescriptor permissionsDescriptor, boolean grant) {
        RevokeTablePrivilege revokeTablePrivilege = RevokeTablePrivilege.newBuilder()
                .setTableId(transferDerbyUUID((BasicUUID) permissionsDescriptor.getTableUUID()))
                .setSelectPerm(permissionsDescriptor.getSelectPriv())
                .setDeletePerm(permissionsDescriptor.getDeletePriv())
                .setInsertPerm(permissionsDescriptor.getInsertPriv())
                .setUpdatePerm(permissionsDescriptor.getUpdatePriv())
                .setReferencesPerm(permissionsDescriptor.getReferencesPriv())
                .setTriggerPerm(permissionsDescriptor.getTriggerPriv())
                .setGrantor(permissionsDescriptor.getGrantor())
                .setGrantee(permissionsDescriptor.getGrantee())
                .setPermObjectId(transferDerbyUUID((BasicUUID) permissionsDescriptor.getUUID()))
                .build();
        RevokePrivilege revokePrivilege = RevokePrivilege.newBuilder()
                .setType(RevokePrivilege.Type.REVOKE_TABLE_PRIVILEGE)
                .setOp(grant? RevokePrivilege.OpType.GRANT_OP: RevokePrivilege.OpType.REVOKE_OP)
                .setRevokeTablePrivilege(revokeTablePrivilege)
                .build();

        return DDLChange.newBuilder().setDdlChangeType(DDLChangeType.REVOKE_PRIVILEGE)
                .setTxnId(txnId).setRevokePrivilege(revokePrivilege).build();
    }

    public static DDLChange createRevokeColumnPrivilege(long txnId, ColPermsDescriptor permissionsDescriptor, boolean grant) {

        RevokeColumnPrivilege.Builder builder = RevokeColumnPrivilege.newBuilder()
                .setTableId(transferDerbyUUID((BasicUUID) permissionsDescriptor.getTableUUID()))
                .setType(permissionsDescriptor.getType())
                .setGrantee(permissionsDescriptor.getGrantee())
                .setGrantor(permissionsDescriptor.getGrantor())
                .setPermObjectId(transferDerbyUUID((BasicUUID) permissionsDescriptor.getUUID()));
        if (permissionsDescriptor.getColumns()!=null)
            builder.setColumns(ByteString.copyFrom(permissionsDescriptor.getColumns().getByteArray()));

        RevokeColumnPrivilege revokeColumnPrivilege = builder.build();

        RevokePrivilege revokePrivilege = RevokePrivilege.newBuilder()
                .setType(RevokePrivilege.Type.REVOKE_COLUMN_PRIVILEGE)
                .setOp(grant? RevokePrivilege.OpType.GRANT_OP: RevokePrivilege.OpType.REVOKE_OP)
                .setRevokeColumnPrivilege(revokeColumnPrivilege)
                .build();

        return DDLChange.newBuilder().setDdlChangeType(DDLChangeType.REVOKE_PRIVILEGE)
                .setTxnId(txnId).setRevokePrivilege(revokePrivilege).build();
    }

    public static DDLChange createRevokeRoutinePrivilege(long txnId, RoutinePermsDescriptor permissionsDescriptor, boolean grant) {

        RevokeRoutinePrivilege revokeRoutinePrivilege = RevokeRoutinePrivilege.newBuilder()
                .setRountineId(transferDerbyUUID((BasicUUID) permissionsDescriptor.getRoutineUUID()))
                .setGrantee(permissionsDescriptor.getGrantee())
                .setGrantor(permissionsDescriptor.getGrantor())
                .setPermObjectId(transferDerbyUUID((BasicUUID)permissionsDescriptor.getUUID()))
                .build();

        RevokePrivilege revokePrivilege = RevokePrivilege.newBuilder()
                .setType(RevokePrivilege.Type.REVOKE_ROUTINE_PRIVILEGE)
                .setOp(grant? RevokePrivilege.OpType.GRANT_OP: RevokePrivilege.OpType.REVOKE_OP)
                .setRevokeRoutinePrivilege(revokeRoutinePrivilege)
                .build();

        return DDLChange.newBuilder().setDdlChangeType(DDLChangeType.REVOKE_PRIVILEGE)
                .setTxnId(txnId).setRevokePrivilege(revokePrivilege).build();
    }

    public static DDLChange createRevokeGenericPrivilege(long txnId, PermDescriptor permissionsDescriptor, boolean restrict, boolean grant) {
        RevokeGenericPrivilege revokeGenericPrivilege = RevokeGenericPrivilege.newBuilder()
                .setId(transferDerbyUUID((BasicUUID) permissionsDescriptor.getPermObjectId()))
                .setObjectType(permissionsDescriptor.getObjectType())
                .setPermObjectId(transferDerbyUUID((BasicUUID) permissionsDescriptor.getPermObjectId()))
                .setGrantor(permissionsDescriptor.getGrantor())
                .setGrantee(permissionsDescriptor.getGrantee())
                .setGrantable(permissionsDescriptor.isGrantable())
                .setPermission(permissionsDescriptor.getPermission())
                .setRestrict(restrict)
                .build();

        RevokePrivilege revokePrivilege = RevokePrivilege.newBuilder()
                .setType(RevokePrivilege.Type.REVOKE_GENERIC_PRIVILEGE)
                .setOp(grant? RevokePrivilege.OpType.GRANT_OP: RevokePrivilege.OpType.REVOKE_OP)
                .setRevokeGenericPrivilege(revokeGenericPrivilege)
                .build();
        return DDLChange.newBuilder().setDdlChangeType(DDLChangeType.REVOKE_PRIVILEGE)
                .setTxnId(txnId).setRevokePrivilege(revokePrivilege).build();
    }

    public static DDLChange createUpdateSystemProcedure(long txnId) {
        return DDLChange.newBuilder()
                .setDdlChangeType(DDLChangeType.UPDATE_SYSTEM_PROCEDURES)
                .setTxnId(txnId)
                .setUpdateSystemProcedures(UpdateSystemProcedures.newBuilder())
                .build();
    }
}






