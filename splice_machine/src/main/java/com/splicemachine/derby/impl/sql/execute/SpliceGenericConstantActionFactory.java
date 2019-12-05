/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.db.catalog.AliasInfo;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.depend.ProviderInfo;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.impl.sql.compile.TableName;
import com.splicemachine.db.impl.sql.execute.*;
import com.splicemachine.derby.impl.sql.execute.actions.*;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @author Scott Fines
 *         Created on: 3/1/13
 */
public abstract class SpliceGenericConstantActionFactory extends GenericConstantActionFactory{
    private static Logger LOG=Logger.getLogger(SpliceGenericConstantActionFactory.class);

    @Override
    public ConstantAction getCreateConstraintConstantAction(String constraintName,
                                                            int constraintType,
                                                            boolean forCreateTable,
                                                            String tableName,
                                                            UUID tableId,
                                                            String schemaName,
                                                            String[] columnNames,
                                                            ConstantAction indexAction,
                                                            String constraintText,
                                                            boolean enabled,
                                                            ConstraintInfo otherConstraint,
                                                            ProviderInfo[] providerInfo){
        SpliceLogUtils.trace(LOG,"getConstraintConstantAction with name {%s} for table {%s}",constraintName,tableName);
        return new CreateConstraintConstantOperation
                (constraintName,constraintType,forCreateTable,tableName,
                        tableId,schemaName,columnNames,constraintType==DataDictionary.PRIMARYKEY_CONSTRAINT?null:indexAction,constraintText,
                        enabled,otherConstraint,providerInfo);

    }

    @Override
    public ConstraintConstantOperation[] createConstraintConstantActionArray(int size){
        SpliceLogUtils.trace(LOG,"createConstraintConstantActionArray with size {%d}",size);
        // Will sometimes hold DropConstraintConstantOperation, sometimes CreateConstraintConstantOperation
        return new ConstraintConstantOperation[size];
    }

    @Override
    public ConstantAction getCreateTableConstantAction(String schemaName,
                                                       String tableName,
                                                       int tableType,
                                                       ColumnInfo[] columnInfos,
                                                       ConstantAction[] constantActions,
                                                       Properties properties,
                                                       char lockGranularity,
                                                       boolean onCommitDeleteRows,
                                                       boolean onRollbackDeleteRows,
                                                       String withDataQueryString,
                                                       boolean isExternal,
                                                       String delimited,
                                                       String escaped,
                                                       String lines,
                                                       String storedAs,
                                                       String location,
                                                       String compression,
                                                       boolean mergeSchema,
                                                       boolean presplit,
                                                       boolean isLogicalKey,
                                                       String splitKeyPath,
                                                       String columnDelimiter,
                                                       String characterDelimiter,
                                                       String timestampFormat,
                                                       String dateFormat,
                                                       String timeFormat) {
        SpliceLogUtils.trace(LOG, "getCreateTableConstantAction for {%s.%s} with columnInfo %s and constraintActions",
            schemaName, tableName, Arrays.toString(columnInfos),Arrays.toString(constantActions));
        return new SpliceCreateTableOperation(schemaName,tableName,tableType,columnInfos,
            constantActions,properties,lockGranularity,
            onCommitDeleteRows,onRollbackDeleteRows,withDataQueryString, isExternal,
                delimited,escaped,lines,storedAs,location, compression, mergeSchema,presplit,isLogicalKey,splitKeyPath,
                columnDelimiter,characterDelimiter,timestampFormat,dateFormat,timeFormat);
    }


    @Override
    public ConstantAction getCreateIndexConstantAction(boolean      forCreateTable,
                                                       boolean		unique,
                                                       boolean		uniqueWithDuplicateNulls,
                                                       String		indexType,
                                                       String		schemaName,
                                                       String		indexName,
                                                       String		tableName,
                                                       UUID			tableId,
                                                       String[]		columnNames,
                                                       boolean[]    isAscending,
                                                       boolean		isConstraint,
                                                       UUID			conglomerateUUID,
                                                       boolean		excludeNulls,
                                                       boolean 		excludeDefaults,
                                                       boolean      preSplit,
                                                       boolean      isLogicalKey,
                                                       boolean      sampling,
                                                       double       sampleFraction,
                                                       String       splitKeyPath,
                                                       String       hfilePath,
                                                       String       columnDelimiter,
                                                       String       characterDelimiter,
                                                       String       timestampFormat,
                                                       String       dateFormat,
                                                       String       timeFormat,
                                                       Properties	properties){
        SpliceLogUtils.trace(LOG,"getCreateIndexConstantAction for index {%s.%s} on {%s.%s} with columnNames %s",schemaName,indexName,schemaName,tableName,Arrays.toString(columnNames));
        return new CreateIndexConstantOperation(forCreateTable,unique,uniqueWithDuplicateNulls,indexType, schemaName,
                indexName,tableName,tableId,columnNames,isAscending,isConstraint, conglomerateUUID, excludeNulls,
                excludeDefaults,preSplit,isLogicalKey,sampling,sampleFraction,splitKeyPath,hfilePath,columnDelimiter,characterDelimiter,
                timestampFormat, dateFormat,timeFormat,properties);
    }

    @Override
    public ConstantAction getSetConstraintsConstantAction(ConstraintDescriptorList cdl,boolean enable,boolean unconditionallyEnforce,Object[] ddlList){
        SpliceLogUtils.trace(LOG,"getSetConstraintsConstantAction for {%s} on ddlList {%s}",cdl,Arrays.toString(ddlList));
        return new SetConstraintsConstantOperation(cdl,enable,unconditionallyEnforce);
    }


    @Override
    public ConstantAction getAlterTableConstantAction(SchemaDescriptor sd,
                                                      String tableName,UUID tableId,long tableConglomerateId,
                                                      int tableType,ColumnInfo[] columnInfo,
                                                      ConstantAction[] constraintActions,char lockGranularity,
                                                      boolean compressTable,int behavior,boolean sequential,
                                                      boolean truncateTable,boolean purge,boolean defragment,
                                                      boolean truncateEndOfTable,boolean updateStatistics,
                                                      boolean updateStatisticsAll,boolean dropStatistics,
                                                      boolean dropStatisticsAll,String indexNameForStatistics){
        SpliceLogUtils.trace(LOG,"getAlterTableConstantAction for {%s.%s} with columnInfo {%s}",(sd==null?"none":sd.getSchemaName()),tableName,Arrays.toString(columnInfo));
        if(truncateTable){
            return new TruncateTableConstantOperation(sd,tableName,tableId,
                    lockGranularity,behavior,indexNameForStatistics);
        }else if(columnInfo!=null && columnInfo.length>0){
            return new ModifyColumnConstantOperation(sd,tableName,tableId,
                    columnInfo,constraintActions,
                    lockGranularity,behavior,indexNameForStatistics);
        }else{
            return new AlterTableConstantOperation(sd,tableName,tableId,
                    columnInfo,constraintActions,
                    behavior,
                    indexNameForStatistics);
        }
    }

    @Override
    public ConstantAction getCreateAliasConstantAction(String aliasName,String schemaName,String javaClassName,AliasInfo aliasInfo,char aliasType){
        SpliceLogUtils.trace(LOG,"getCreateAliasConstantAction for alias {%s} in schema {%s} with javaClassName %s and aliasInfo {%s}",aliasName,schemaName,javaClassName,aliasInfo);
        return new CreateAliasConstantOperation(aliasName,schemaName,javaClassName,aliasInfo,aliasType);
    }

    @Override
    public ConstantAction getCreateSchemaConstantAction(String schemaName,String aid){
        SpliceLogUtils.trace(LOG,"getCreateSchemaConstantAction for schema {%s} with aid {%s}",schemaName,aid);
        return new CreateSchemaConstantOperation(schemaName,aid);
    }

    @Override
    public ConstantAction getCreateRoleConstantAction(String roleName){
        SpliceLogUtils.trace(LOG,"getCreateRoleConstantAction for role {%s}",roleName);
        return new CreateRoleConstantOperation(roleName);
    }

    @Override
    public ConstantAction getSetRoleConstantAction(String roleName,int type){
        SpliceLogUtils.trace(LOG,"getSetRoleConstantAction for role {%s} with type {%d}",roleName,type);
        return new SetRoleConstantOperation(roleName,type);
    }

    @Override
    public ConstantAction getCreateSequenceConstantAction(TableName sequenceName,DataTypeDescriptor dataType,long initialValue,long stepValue,long maxValue,long minValue,
                                                          boolean cycle){
        SpliceLogUtils.trace(LOG,"getCreateSequenceConstantAction for sequenceName {%s} with dataType {%s} with initialValue {%s}, stepValue {%s}, maxValue {%s}, minValue {%s}",
                sequenceName,dataType,initialValue,stepValue,maxValue,minValue);
        return new CreateSequenceConstantOperation(sequenceName.getSchemaName(),
                sequenceName.getTableName(),
                dataType,
                initialValue,
                stepValue,
                maxValue,
                minValue,
                cycle);
    }

    @Override
    public ConstantAction getSavepointConstantAction(String savepointName,int statementType){
        SpliceLogUtils.trace(LOG,"--ignored -- getSavepointConstantAction for savepoint {%s} with type {%d}",savepointName,statementType);
        return new SavepointConstantOperation(savepointName,statementType);
    }

    @Override
    public ConstantAction getCreateViewConstantAction(String schemaName,String tableName,int tableType,String viewText,int checkOption,
                                                      ColumnInfo[] columnInfo,ProviderInfo[] providerInfo,UUID compSchemaId){
        SpliceLogUtils.trace(LOG,"getCreateViewConstantAction for {%s.%s} with view text {%s}",schemaName,tableName,viewText);
        return new CreateViewConstantOperation(schemaName,tableName,tableType,
                viewText,checkOption,columnInfo,
                providerInfo,compSchemaId);
    }

    @Override
    public ConstantAction getDeleteConstantAction(long conglomId,
                                                  int tableType,StaticCompiledOpenConglomInfo heapSCOCI,
                                                  int[] pkColumns,IndexRowGenerator[] irgs,long[] indexCIDS,
                                                  StaticCompiledOpenConglomInfo[] indexSCOCIs,ExecRow emptyHeapRow,
                                                  boolean deferred,boolean tableIsPublished,UUID tableID,
                                                  int lockMode,Object deleteToken,Object keySignature,
                                                  int[] keyPositions,long keyConglomId,String schemaName,
                                                  String tableName,ResultDescription resultDescription,
                                                  FKInfo[] fkInfo,TriggerInfo triggerInfo,
                                                  FormatableBitSet baseRowReadList,int[] baseRowReadMap,
                                                  int[] streamStorableHeapColIds,int numColumns,UUID dependencyId,
                                                  boolean singleRowSource,ConstantAction[] dependentConstantActions)
            throws StandardException{
        SpliceLogUtils.trace(LOG,"getDeleteConstantAction for {%s.%s}",schemaName,tableName);
        return new DeleteConstantOperation(
                conglomId,
                heapSCOCI,
                pkColumns,
                irgs,
                indexCIDS,
                indexSCOCIs,
                emptyHeapRow,
                deferred,
                tableID,
                lockMode,
                fkInfo,
                triggerInfo,
                baseRowReadList,
                baseRowReadMap,
                streamStorableHeapColIds,
                numColumns,
                singleRowSource,
                resultDescription,
                dependentConstantActions
        );
    }

    @Override
    public ConstantAction getDropConstraintConstantAction(
            String constraintName,String constraintSchemaName,
            String tableName,UUID tableId,String tableSchemaName,
            ConstantAction indexAction,int behavior,int verifyType){
        SpliceLogUtils.trace(LOG,"getDropConstraintConstantAction for {%s.%s} on {%s.%s}",constraintSchemaName,constraintName,tableSchemaName,tableName);
        return new DropConstraintConstantOperation(constraintName,constraintSchemaName,tableName,
                tableId,tableSchemaName,indexAction,behavior,verifyType);
    }

    @Override
    public ConstantAction getDropAliasConstantAction(SchemaDescriptor sd,
                                                     String aliasName,char aliasType){
        SpliceLogUtils.trace(LOG,"getDropAliasConstantAction for {%s.%s}",(sd==null?"none":sd.getSchemaName()),aliasName);
        //-sf- I checked with derby, and sd will never be null, it is ensured. This assertion protects against regression on that
        assert sd!=null: "Cannot drop an alias with a null schema descriptor";
        return new DropAliasConstantOperation(sd,aliasName,aliasType);
    }

    @Override
    public ConstantAction getDropRoleConstantAction(String roleName){
        SpliceLogUtils.trace(LOG,"getDropRoleConstantAction for {%s}",roleName);
        return new DropRoleConstantOperation(roleName);
    }

    @Override
    public ConstantAction getDropSequenceConstantAction(SchemaDescriptor sd,String seqName){
        SpliceLogUtils.trace(LOG,"getDropSequenceConstantAction for {%s.%s}",(sd==null?"none":sd.getSchemaName()),seqName);
        return new DropSequenceConstantOperation(sd,seqName);
    }

    @Override
    public ConstantAction getDropSchemaConstantAction(String schemaName){
        SpliceLogUtils.trace(LOG,"getDropSchemaConstantAction for {%s}",schemaName);
        return new DropSchemaConstantOperation(schemaName);
    }

    @Override
    public ConstantAction getDropTableConstantAction(String fullTableName,
                                                     String tableName,SchemaDescriptor sd,long conglomerateNumber,
                                                     UUID tableId,int behavior){
        SpliceLogUtils.trace(LOG,"getDropTableConstantAction for {%s.%s}",(sd==null?"none":sd.getSchemaName()),tableName);
        return new DropTableConstantOperation(fullTableName,tableName,sd,conglomerateNumber,tableId,behavior);
    }

    @Override
    public ConstantAction getDropViewConstantAction(String fullTableName,String tableName,SchemaDescriptor sd){
        assert sd!=null: "SchemaDescriptor cannot be null!";
        SpliceLogUtils.trace(LOG,"getDropViewConstantAction for {%s.%s}",sd.getSchemaName(),tableName);
        return new DropViewConstantOperation(fullTableName,tableName,sd);
    }

    @Override
    public ConstantAction getRenameConstantAction(String fullTableName,
                                                  String tableName,String oldObjectName,String newObjectName,
                                                  SchemaDescriptor sd,UUID tableId,boolean usedAlterTable,
                                                  int renamingWhat){
        SpliceLogUtils.trace(LOG,"getRenameConstantAction for {%s.%s} with old {%s} and new {%s}",(sd==null?"none":sd.getSchemaName()),tableName,oldObjectName,newObjectName);
        return new RenameConstantOperation(fullTableName,tableName,oldObjectName,newObjectName,
                sd,tableId,usedAlterTable,renamingWhat);
    }

    @Override
    public ConstantAction getInsertConstantAction(
            TableDescriptor tableDescriptor,long conglomId,
            StaticCompiledOpenConglomInfo heapSCOCI,int[] pkColumns,
            IndexRowGenerator[] irgs,long[] indexCIDS,
            StaticCompiledOpenConglomInfo[] indexSCOCIs,String[] indexNames,
            boolean deferred,boolean tableIsPublished,UUID tableID,
            int lockMode,Object insertToken,Object rowSignature,
            Properties targetProperties,FKInfo[] fkInfo,
            TriggerInfo triggerInfo,int[] streamStorableHeapColIds,
            boolean[] indexedCols,UUID dependencyId,Object[] stageControl,
            Object[] ddlList,boolean singleRowSource,
            RowLocation[] autoincRowLocation) throws StandardException{
        SpliceLogUtils.trace(LOG,"getInsertConstantAction for {%s}",tableDescriptor);
        return new InsertConstantOperation(tableDescriptor,
                conglomId,
                heapSCOCI,
                pkColumns,
                irgs,
                indexCIDS,
                indexSCOCIs,
                indexNames,
                deferred,
                targetProperties,
                tableID,
                lockMode,
                fkInfo,
                triggerInfo,
                streamStorableHeapColIds,
                indexedCols,
                singleRowSource,
                autoincRowLocation
        );
    }

    @Override
    public ConstantAction getUpdatableVTIConstantAction(int statementType,boolean deferred) throws StandardException{
        SpliceLogUtils.trace(LOG,"getUpdatableVTIConstantAction for {%d}",statementType);
        return new UpdatableVTIConstantAction(statementType,deferred,null);
    }

    @Override
    public ConstantAction getUpdatableVTIConstantAction(int statementType,boolean deferred,int[] changedColumnIds) throws StandardException{
        SpliceLogUtils.trace(LOG,"getUpdatableVTIConstantAction for {%d}",statementType);
        return new UpdatableVTIConstantOperation(statementType,deferred,changedColumnIds);
    }

    @Override
    public ConstantAction getLockTableConstantAction(String fullTableName,long conglomerateNumber,boolean exclusiveMode){
        SpliceLogUtils.trace(LOG,"getLockTableConstantAction for {%s}",fullTableName);
        return new LockTableConstantOperation(fullTableName,conglomerateNumber,exclusiveMode);
    }

    @Override
    public ConstantAction getSetSchemaConstantAction(String schemaName,int type){
        SpliceLogUtils.trace(LOG,"getSetSchemaConstantAction for {%s}",schemaName);
        return new SetSchemaConstantOperation(schemaName,type);
    }

    @Override
    public ConstantAction getSetTransactionIsolationConstantAction(int isolationLevel){
        SpliceLogUtils.trace(LOG,"getSetTransactionIsolationConstantAction at {%d}",isolationLevel);
        return new SetTransactionIsolationConstantOperation(isolationLevel);
    }

    @Override
    public ConstantAction getSetSessionPropertyConstantAction(Properties properties) {
        SpliceLogUtils.trace(LOG,"getSetSessionPropertyConstantAction {%s}", properties.toString());
        return new SetSessionPropertyConstantOperation(properties);
    }

    @Override
    public ConstantAction getUpdateConstantAction(long conglomId,
                                                  int tableType,
                                                  StaticCompiledOpenConglomInfo heapSCOCI,
                                                  int[] pkColumns,
                                                  IndexRowGenerator[] irgs,
                                                  long[] indexCIDS,
                                                  StaticCompiledOpenConglomInfo[] indexSCOCIs,
                                                  String[] indexNames,
                                                  ExecRow emptyHeapRow,
                                                  boolean deferred,
                                                  UUID targetUUID,
                                                  int lockMode,
                                                  boolean tableIsPublished,
                                                  int[] changedColumnIds,
                                                  int[] keyPositions,
                                                  Object updateToken,
                                                  FKInfo[] fkInfo,
                                                  TriggerInfo triggerInfo,
                                                  FormatableBitSet baseRowReadList,
                                                  int[] baseRowReadMap,
                                                  int[] streamStorableHeapColIds,
                                                  int numColumns,
                                                  boolean positionedUpdate,
                                                  boolean singleRowSource,
                                                  int[] storagePositionArray) throws StandardException{
        return new UpdateConstantOperation(
                conglomId,
                heapSCOCI,
                pkColumns,
                irgs,
                indexCIDS,
                indexSCOCIs,
                indexNames,
                emptyHeapRow,
                deferred,
                targetUUID,
                lockMode,
                changedColumnIds,
                fkInfo,
                triggerInfo,
                baseRowReadList,
                baseRowReadMap,
                streamStorableHeapColIds,
                numColumns,
                positionedUpdate,
                singleRowSource,
                storagePositionArray
        );
    }

    @Override
    public ConstantAction getCreateTriggerConstantAction(
            String triggerSchemaName,String triggerName,TriggerEventDML eventMask,
            boolean isBefore,boolean isRow,boolean isEnabled,
            TableDescriptor triggerTable,UUID whenSPSId,String whenText,
            UUID actionSPSId,String actionText,UUID spsCompSchemaId,
            Timestamp creationTimestamp,int[] referencedCols,
            int[] referencedColsInTriggerAction,
            String originalWhenText,
            String originalActionText,
            boolean referencingOld,boolean referencingNew,
            String oldReferencingName,String newReferencingName){
        SpliceLogUtils.trace(LOG,"getCreateTriggerConstantAction for trigger {%s.%s}",triggerSchemaName,triggerName);
        return new CreateTriggerConstantOperation(triggerSchemaName,triggerName,
                eventMask,isBefore,isRow,isEnabled,triggerTable,whenSPSId,
                whenText,actionSPSId,actionText,spsCompSchemaId,creationTimestamp,
                referencedCols,referencedColsInTriggerAction,
                originalWhenText, originalActionText,
                referencingOld,referencingNew,oldReferencingName,newReferencingName);
    }

    @Override
    public ConstantAction getDropTriggerConstantAction(SchemaDescriptor sd,String triggerName,UUID tableId){
        SpliceLogUtils.trace(LOG,"getDropTriggerConstantAction for trigger {%s.%s}",(sd==null?"none":sd.getSchemaName()),triggerName);
        return new DropTriggerConstantOperation(sd,triggerName,tableId);
    }

    @Override
    public ConstantAction getDropStatisticsConstantAction(SchemaDescriptor sd,
                                                          String fullTableName,String objectName,boolean forTable){
        SpliceLogUtils.trace(LOG,"getDropStatisticsConstantAction for trigger {%s.%s}",(sd==null?"none":sd.getSchemaName()),fullTableName);
        return new DropStatisticsConstantOperation(sd,fullTableName,objectName,forTable);
    }

    @Override
    public ConstantAction getDropPinConstantAction(String fullTableName, String tableName, SchemaDescriptor sd, long conglomerateNumber, UUID tableId, int behavior) {
        SpliceLogUtils.trace(LOG,"getDropPinConstantAction for {%s.%s}",(sd==null?"none":sd.getSchemaName()),tableName);
        return new DropPinConstantOperation(fullTableName, tableName, sd, conglomerateNumber, tableId, behavior);
    }

    @Override
    public ConstantAction getGrantConstantAction(PrivilegeInfo privileges,List grantees){
        SpliceLogUtils.trace(LOG,"getGrantConstantAction for privileges {%s}",privileges);
        return new GrantRevokeConstantOperation(true,privileges,grantees);
    }

    @Override
    public ConstantAction getGrantRoleConstantAction(List roleNames,List grantees, boolean isDefaultRole){
        SpliceLogUtils.trace(LOG,"getGrantRoleConstantAction for %s roles {%s} and grantees {%s}",(isDefaultRole?"default":"non-default"), roleNames,grantees);
        return new GrantRoleConstantOperation(roleNames,grantees,isDefaultRole);
    }

    @Override
    public ConstantAction getRevokeConstantAction(PrivilegeInfo privileges,List grantees){
        SpliceLogUtils.trace(LOG,"getRevokeConstantAction for privileges {%s} and grantees {%s}",privileges,grantees);
        return new GrantRevokeConstantOperation(false,privileges,grantees);
    }

    @Override
    public ConstantAction getRevokeRoleConstantAction(List roleNames,List grantees){
        SpliceLogUtils.trace(LOG,"getRevokeRoleConstantAction for roles {%s} and grantees {%s}",roleNames,grantees);
        return new RevokeRoleConstantOperation(roleNames,grantees);
    }


    @Override
    public ConstantAction getPinTableConstantAction(String schemaName, String tableName) {
        return new CreatePinConstantOperation(schemaName,tableName);
    }
}
