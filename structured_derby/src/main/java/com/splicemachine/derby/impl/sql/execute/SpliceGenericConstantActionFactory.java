package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.derby.impl.sql.execute.actions.CreateTableAction;
import com.splicemachine.derby.impl.sql.execute.actions.DropIndexOperation;

import org.apache.derby.catalog.AliasInfo;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.depend.ProviderInfo;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptorList;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.impl.sql.compile.TableName;
import org.apache.derby.impl.sql.execute.*;
import org.apache.log4j.Logger;

import com.splicemachine.derby.impl.sql.execute.actions.CreateIndexOperation;
import com.splicemachine.utils.SpliceLogUtils;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @author Scott Fines
 * Created on: 3/1/13
 */
public class SpliceGenericConstantActionFactory extends GenericConstantActionFactory {
	private static Logger LOG = Logger.getLogger(SpliceGenericConstantActionFactory.class);
    @Override
    public CreateConstraintConstantAction getCreateConstraintConstantAction(String constraintName,
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
                                                                            ProviderInfo[] providerInfo) {
    	SpliceLogUtils.trace(LOG, "getConstraintConstantAction with name {%s} for table {%s}",constraintName,tableName);
        if(constraintType== DataDictionary.PRIMARYKEY_CONSTRAINT){
            return super.getCreateConstraintConstantAction(constraintName, constraintType,
                    forCreateTable, tableName, tableId,
                    schemaName, columnNames, null,
                    constraintText, enabled, otherConstraint, providerInfo);
        }else
            return super.getCreateConstraintConstantAction(constraintName, constraintType,
                    forCreateTable, tableName, tableId,
                    schemaName, columnNames, indexAction,
                    constraintText, enabled, otherConstraint, providerInfo);
    }

    @Override
    public ConstantAction getCreateTableConstantAction(String schemaName, String tableName,
                                                       int tableType, ColumnInfo[] columnInfo,
                                                       CreateConstraintConstantAction[] constraintActions,
                                                       Properties properties, char lockGranularity,
                                                       boolean onCommitDeleteRows, boolean onRollbackDeleteRows) {
    	SpliceLogUtils.trace(LOG, "getCreateTableConstantAction for {%s.%s} with columnInfo %s and constraintActions",schemaName, tableName, Arrays.toString(columnInfo),Arrays.toString(constraintActions));
        return new CreateTableAction(schemaName,tableName,tableType,columnInfo,
                constraintActions,properties,lockGranularity,
                onCommitDeleteRows,onRollbackDeleteRows);
    }

    @Override
    public ConstantAction getCreateIndexConstantAction(boolean forCreateTable,
                                                       boolean unique,
                                                       boolean uniqueWithDuplicateNulls,
                                                       String indexType, String schemaName,
                                                       String indexName, String tableName,
                                                       UUID tableId, String[] columnNames,
                                                       boolean[] isAscending, boolean isConstraint,
                                                       UUID conglomerateUUID, Properties properties) {
    	SpliceLogUtils.trace(LOG, "getCreateIndexConstantAction for index {%s.%s} on {%s.%s} with columnNames %s",schemaName, indexName, schemaName, tableName, Arrays.toString(columnNames));
        return new CreateIndexOperation(schemaName,indexName,tableName,columnNames,isAscending,tableId,conglomerateUUID,unique,indexType,properties);
    }

    @Override
    public ConstantAction getDropIndexConstantAction(String fullIndexName,
                                                     String indexName,
                                                     String tableName,
                                                     String schemaName,
                                                     UUID tableId,
                                                     long tableConglomerateId) {
    	SpliceLogUtils.trace(LOG, "getDropIndexConstantAction for index {%s} on {%s.%s} with columnNames %s",fullIndexName, schemaName, tableName);
        return new DropIndexOperation(fullIndexName,indexName,
                tableName,schemaName,tableId,tableConglomerateId);
    }

	@Override
	public ConstantAction getSetConstraintsConstantAction(ConstraintDescriptorList cdl, boolean enable,boolean unconditionallyEnforce, Object[] ddlList) {
    	SpliceLogUtils.trace(LOG, "getSetConstraintsConstantAction for {%s} on ddlList {%s}",cdl,Arrays.toString(ddlList));
		return super.getSetConstraintsConstantAction(cdl, enable,unconditionallyEnforce, ddlList);
	}

	@Override
	public ConstantAction getAlterTableConstantAction(SchemaDescriptor sd,
			String tableName, UUID tableId, long tableConglomerateId,
			int tableType, ColumnInfo[] columnInfo,
			ConstraintConstantAction[] constraintActions, char lockGranularity,
			boolean compressTable, int behavior, boolean sequential,
			boolean truncateTable, boolean purge, boolean defragment,
			boolean truncateEndOfTable, boolean updateStatistics,
			boolean updateStatisticsAll, boolean dropStatistics,
			boolean dropStatisticsAll, String indexNameForStatistics) {
    	SpliceLogUtils.trace(LOG, "getAlterTableConstantAction for {%s.%s} with columnInfo {%s}",(sd==null?"none":sd.getSchemaName()),tableName, Arrays.toString(columnInfo));
		return super.getAlterTableConstantAction(sd, tableName, tableId,
				tableConglomerateId, tableType, columnInfo, constraintActions,
				lockGranularity, compressTable, behavior, sequential, truncateTable,
				purge, defragment, truncateEndOfTable, updateStatistics,
				updateStatisticsAll, dropStatistics, dropStatisticsAll,
				indexNameForStatistics);
	}

	@Override
	public ConstantAction getCreateAliasConstantAction(String aliasName,String schemaName, String javaClassName, AliasInfo aliasInfo, char aliasType) {
    	SpliceLogUtils.trace(LOG, "getCreateAliasConstantAction for alias {%s} in schema {%s} with javaClassName %s and aliasInfo {%s}",aliasName, schemaName, javaClassName, aliasInfo);
		return super.getCreateAliasConstantAction(aliasName, schemaName, javaClassName,
				aliasInfo, aliasType);
	}

	@Override
	public ConstantAction getCreateSchemaConstantAction(String schemaName,String aid) {
    	SpliceLogUtils.trace(LOG, "getCreateSchemaConstantAction for schema {%s} with aid {%s}",schemaName, aid);
		return super.getCreateSchemaConstantAction(schemaName, aid);
	}

	@Override
	public ConstantAction getCreateRoleConstantAction(String roleName) {
    	SpliceLogUtils.trace(LOG, "getCreateRoleConstantAction for role {%s}",roleName);
		return super.getCreateRoleConstantAction(roleName);
	}

	@Override
	public ConstantAction getSetRoleConstantAction(String roleName, int type) {
    	SpliceLogUtils.trace(LOG, "getSetRoleConstantAction for role {%s} with type {%d}",roleName, type);
		return super.getSetRoleConstantAction(roleName, type);
	}

	@Override
	public ConstantAction getCreateSequenceConstantAction(TableName sequenceName, DataTypeDescriptor dataType,long initialValue, long stepValue, long maxValue, long minValue,
			boolean cycle) {
    	SpliceLogUtils.trace(LOG, "getCreateSequenceConstantAction for sequenceName {%s} with dataType {%s} with initialValue {%s}, stepValue {%s}, maxValue {%s}, minValue {%s}",
    			sequenceName, dataType, initialValue, stepValue, maxValue, minValue);
		return super.getCreateSequenceConstantAction(sequenceName, dataType,
				initialValue, stepValue, maxValue, minValue, cycle);
	}

	@Override
	public ConstantAction getSavepointConstantAction(String savepointName,int statementType) {
    	SpliceLogUtils.trace(LOG, "--ignored -- getSavepointConstantAction for savepoint {%s} with type {%d}",savepointName, statementType);
		return super.getSavepointConstantAction(savepointName, statementType);
	}

	@Override
	public ConstantAction getCreateViewConstantAction(String schemaName,String tableName, int tableType, String viewText, int checkOption,
			ColumnInfo[] columnInfo, ProviderInfo[] providerInfo,UUID compSchemaId) {
    	SpliceLogUtils.trace(LOG, "getCreateViewConstantAction for {%s.%s} with view text {%s}",schemaName, tableName, viewText);
		return super.getCreateViewConstantAction(schemaName, tableName, tableType,viewText, checkOption, columnInfo, providerInfo, compSchemaId);
	}

	@Override
	public ConstantAction getDeleteConstantAction(long conglomId,
			int tableType, StaticCompiledOpenConglomInfo heapSCOCI,
			int[] pkColumns, IndexRowGenerator[] irgs, long[] indexCIDS,
			StaticCompiledOpenConglomInfo[] indexSCOCIs, ExecRow emptyHeapRow,
			boolean deferred, boolean tableIsPublished, UUID tableID,
			int lockMode, Object deleteToken, Object keySignature,
			int[] keyPositions, long keyConglomId, String schemaName,
			String tableName, ResultDescription resultDescription,
			FKInfo[] fkInfo, TriggerInfo triggerInfo,
			FormatableBitSet baseRowReadList, int[] baseRowReadMap,
			int[] streamStorableHeapColIds, int numColumns, UUID dependencyId,
			boolean singleRowSource, ConstantAction[] dependentConstantActions)
			throws StandardException {
    	SpliceLogUtils.trace(LOG, "getDeleteConstantAction for {%s.%s}",schemaName, tableName);
		return super.getDeleteConstantAction(conglomId, tableType, heapSCOCI,
				pkColumns, irgs, indexCIDS, indexSCOCIs, emptyHeapRow, deferred,
				tableIsPublished, tableID, lockMode, deleteToken, keySignature,
				keyPositions, keyConglomId, schemaName, tableName, resultDescription,
				fkInfo, triggerInfo, baseRowReadList, baseRowReadMap,
				streamStorableHeapColIds, numColumns, dependencyId, singleRowSource,
				dependentConstantActions);
	}

	@Override
	public ConstantAction getDropConstraintConstantAction(
			String constraintName, String constraintSchemaName,
			String tableName, UUID tableId, String tableSchemaName,
			ConstantAction indexAction, int behavior, int verifyType) {
    	SpliceLogUtils.trace(LOG, "getDropConstraintConstantAction for {%s.%s} on {%s.%s}",constraintSchemaName, constraintName, tableSchemaName, tableName);
		return super.getDropConstraintConstantAction(constraintName,
				constraintSchemaName, tableName, tableId, tableSchemaName, indexAction,
				behavior, verifyType);
	}

	@Override
	public ConstantAction getDropAliasConstantAction(SchemaDescriptor sd,
			String aliasName, char aliasType) {
    	SpliceLogUtils.trace(LOG, "getDropAliasConstantAction for {%s.%s}",(sd==null?"none":sd.getSchemaName()), aliasName);
		return super.getDropAliasConstantAction(sd, aliasName, aliasType);
	}

	@Override
	public ConstantAction getDropRoleConstantAction(String roleName) {
	   	SpliceLogUtils.trace(LOG, "getDropRoleConstantAction for {%s}",roleName);
		return super.getDropRoleConstantAction(roleName);
	}

	@Override
	public ConstantAction getDropSequenceConstantAction(SchemaDescriptor sd,String seqName) {
	   	SpliceLogUtils.trace(LOG, "getDropSequenceConstantAction for {%s.%s}",(sd==null?"none":sd.getSchemaName()), seqName);
		return super.getDropSequenceConstantAction(sd, seqName);
	}

	@Override
	public ConstantAction getDropSchemaConstantAction(String schemaName) {
	   	SpliceLogUtils.trace(LOG, "getDropSchemaConstantAction for {%s}",schemaName);
		return super.getDropSchemaConstantAction(schemaName);
	}

	@Override
	public ConstantAction getDropTableConstantAction(String fullTableName,
			String tableName, SchemaDescriptor sd, long conglomerateNumber,
			UUID tableId, int behavior) {
	   	SpliceLogUtils.trace(LOG, "getDropTableConstantAction for {%s.%s}",(sd==null?"none":sd.getSchemaName()), tableName);
		return super.getDropTableConstantAction(fullTableName, tableName, sd,
				conglomerateNumber, tableId, behavior);
	}

	@Override
	public ConstantAction getDropViewConstantAction(String fullTableName, String tableName, SchemaDescriptor sd) {
	   	SpliceLogUtils.trace(LOG, "getDropViewConstantAction for {%s.%s}",(sd==null?"none":sd.getSchemaName()), tableName);
		return super.getDropViewConstantAction(fullTableName, tableName, sd);
	}

	@Override
	public ConstantAction getRenameConstantAction(String fullTableName,
			String tableName, String oldObjectName, String newObjectName,
			SchemaDescriptor sd, UUID tableId, boolean usedAlterTable,
			int renamingWhat) {
	   	SpliceLogUtils.trace(LOG, "getRenameConstantAction for {%s.%s} with old {%s} and new {%s}",(sd==null?"none":sd.getSchemaName()), tableName, oldObjectName, newObjectName);
		return super.getRenameConstantAction(fullTableName, tableName, oldObjectName,
				newObjectName, sd, tableId, usedAlterTable, renamingWhat);
	}

	@Override
	public ConstantAction getInsertConstantAction(
			TableDescriptor tableDescriptor, long conglomId,
			StaticCompiledOpenConglomInfo heapSCOCI, int[] pkColumns,
			IndexRowGenerator[] irgs, long[] indexCIDS,
			StaticCompiledOpenConglomInfo[] indexSCOCIs, String[] indexNames,
			boolean deferred, boolean tableIsPublished, UUID tableID,
			int lockMode, Object insertToken, Object rowSignature,
			Properties targetProperties, FKInfo[] fkInfo,
			TriggerInfo triggerInfo, int[] streamStorableHeapColIds,
			boolean[] indexedCols, UUID dependencyId, Object[] stageControl,
			Object[] ddlList, boolean singleRowSource,
			RowLocation[] autoincRowLocation) throws StandardException {
	   	SpliceLogUtils.trace(LOG, "getInsertConstantAction for {%s}",tableDescriptor);
		return super.getInsertConstantAction(tableDescriptor, conglomId, heapSCOCI,
				pkColumns, irgs, indexCIDS, indexSCOCIs, indexNames, deferred,
				tableIsPublished, tableID, lockMode, insertToken, rowSignature,
				targetProperties, fkInfo, triggerInfo, streamStorableHeapColIds,
				indexedCols, dependencyId, stageControl, ddlList, singleRowSource,
				autoincRowLocation);
	}

	@Override
	public ConstantAction getUpdatableVTIConstantAction(int statementType,boolean deferred) throws StandardException {
	   	SpliceLogUtils.trace(LOG, "getUpdatableVTIConstantAction for {%d}",statementType);
		return super.getUpdatableVTIConstantAction(statementType, deferred);
	}

	@Override
	public ConstantAction getUpdatableVTIConstantAction(int statementType,boolean deferred, int[] changedColumnIds) throws StandardException {
	   	SpliceLogUtils.trace(LOG, "getUpdatableVTIConstantAction for {%d}",statementType);
		return super.getUpdatableVTIConstantAction(statementType, deferred,
				changedColumnIds);
	}

	@Override
	public ConstantAction getLockTableConstantAction(String fullTableName,long conglomerateNumber, boolean exclusiveMode) {
	   	SpliceLogUtils.trace(LOG, "getLockTableConstantAction for {%s}",fullTableName);
		return super.getLockTableConstantAction(fullTableName, conglomerateNumber,
				exclusiveMode);
	}

	@Override
	public ConstantAction getSetSchemaConstantAction(String schemaName, int type) {
	   	SpliceLogUtils.trace(LOG, "getSetSchemaConstantAction for {%s}",schemaName);
		return super.getSetSchemaConstantAction(schemaName, type);
	}

	@Override
	public ConstantAction getSetTransactionIsolationConstantAction(int isolationLevel) {
	   	SpliceLogUtils.trace(LOG, "getSetTransactionIsolationConstantAction at {%d}",isolationLevel);
		return super.getSetTransactionIsolationConstantAction(isolationLevel);
	}

	@Override
	public UpdateConstantAction getUpdateConstantAction(long conglomId,
			int tableType, StaticCompiledOpenConglomInfo heapSCOCI,
			int[] pkColumns, IndexRowGenerator[] irgs, long[] indexCIDS,
			StaticCompiledOpenConglomInfo[] indexSCOCIs, String[] indexNames,
			ExecRow emptyHeapRow, boolean deferred, UUID targetUUID,
			int lockMode, boolean tableIsPublished, int[] changedColumnIds,
			int[] keyPositions, Object updateToken, FKInfo[] fkInfo,
			TriggerInfo triggerInfo, FormatableBitSet baseRowReadList,
			int[] baseRowReadMap, int[] streamStorableHeapColIds,
			int numColumns, boolean positionedUpdate, boolean singleRowSource)
			throws StandardException {
	   	SpliceLogUtils.trace(LOG, "getUpdateConstantAction with triggerinfo {%s}",triggerInfo);
		return super.getUpdateConstantAction(conglomId, tableType, heapSCOCI,
				pkColumns, irgs, indexCIDS, indexSCOCIs, indexNames, emptyHeapRow,
				deferred, targetUUID, lockMode, tableIsPublished, changedColumnIds,
				keyPositions, updateToken, fkInfo, triggerInfo, baseRowReadList,
				baseRowReadMap, streamStorableHeapColIds, numColumns, positionedUpdate,
				singleRowSource);
	}

	@Override
	public ConstantAction getCreateTriggerConstantAction(
			String triggerSchemaName, String triggerName, int eventMask,
			boolean isBefore, boolean isRow, boolean isEnabled,
			TableDescriptor triggerTable, UUID whenSPSId, String whenText,
			UUID actionSPSId, String actionText, UUID spsCompSchemaId,
			Timestamp creationTimestamp, int[] referencedCols,
			int[] referencedColsInTriggerAction, String originalActionText,
			boolean referencingOld, boolean referencingNew,
			String oldReferencingName, String newReferencingName) {
	   	SpliceLogUtils.trace(LOG, "getCreateTriggerConstantAction for trigger {%s.%s}",triggerSchemaName, triggerName);
		return super.getCreateTriggerConstantAction(triggerSchemaName, triggerName,
				eventMask, isBefore, isRow, isEnabled, triggerTable, whenSPSId,
				whenText, actionSPSId, actionText, spsCompSchemaId, creationTimestamp,
				referencedCols, referencedColsInTriggerAction, originalActionText,
				referencingOld, referencingNew, oldReferencingName, newReferencingName);
	}

	@Override
	public ConstantAction getDropTriggerConstantAction(SchemaDescriptor sd, String triggerName, UUID tableId) {
	   	SpliceLogUtils.trace(LOG, "getDropTriggerConstantAction for trigger {%s.%s}",(sd==null?"none":sd.getSchemaName()), triggerName);
		return super.getDropTriggerConstantAction(sd, triggerName, tableId);
	}

	@Override
	public ConstantAction getDropStatisticsConstantAction(SchemaDescriptor sd,
			String fullTableName, String objectName, boolean forTable) {
	   	SpliceLogUtils.trace(LOG, "getDropStatisticsConstantAction for trigger {%s.%s}",(sd==null?"none":sd.getSchemaName()), fullTableName);
		return super.getDropStatisticsConstantAction(sd, fullTableName, objectName,
				forTable);
	}

	@Override
	public ConstantAction getGrantConstantAction(PrivilegeInfo privileges, List grantees) {
	   	SpliceLogUtils.trace(LOG, "getGrantConstantAction for privileges {%s}",privileges);
		return super.getGrantConstantAction(privileges, grantees);
	}

	@Override
	public ConstantAction getGrantRoleConstantAction(List roleNames,
			List grantees) {
	   	SpliceLogUtils.trace(LOG, "getGrantRoleConstantAction for roles {%s} and grantees {%s}",roleNames, grantees);
		return super.getGrantRoleConstantAction(roleNames, grantees);
	}

	@Override
	public ConstantAction getRevokeConstantAction(PrivilegeInfo privileges, List grantees) {
	   	SpliceLogUtils.trace(LOG, "getRevokeConstantAction for privileges {%s} and grantees {%s}",privileges, grantees);
		return super.getRevokeConstantAction(privileges, grantees);
	}

	@Override
	public ConstantAction getRevokeRoleConstantAction(List roleNames, List grantees) {
	   	SpliceLogUtils.trace(LOG, "getRevokeRoleConstantAction for roles {%s} and grantees {%s}",roleNames, grantees);
		return super.getRevokeRoleConstantAction(roleNames, grantees);
	}
    
    
}
