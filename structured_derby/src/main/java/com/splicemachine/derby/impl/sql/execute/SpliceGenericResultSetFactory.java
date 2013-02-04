package com.splicemachine.derby.impl.sql.execute;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.impl.sql.execute.GenericResultSetFactory;
import org.apache.derby.impl.sql.execute.ScrollInsensitiveResultSet;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.log4j.Logger;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.BulkTableScanOperation;
import com.splicemachine.derby.impl.sql.execute.operations.CallStatementOperation;
import com.splicemachine.derby.impl.sql.execute.operations.DeleteOperation;
import com.splicemachine.derby.impl.sql.execute.operations.DependentOperation;
import com.splicemachine.derby.impl.sql.execute.operations.DistinctGroupedAggregateOperation;
import com.splicemachine.derby.impl.sql.execute.operations.DistinctScalarAggregateOperation;
import com.splicemachine.derby.impl.sql.execute.operations.DistinctScanOperation;
import com.splicemachine.derby.impl.sql.execute.operations.GroupedAggregateOperation;
import com.splicemachine.derby.impl.sql.execute.operations.HashJoinOperation;
import com.splicemachine.derby.impl.sql.execute.operations.HashLeftOuterJoinOperation;
import com.splicemachine.derby.impl.sql.execute.operations.HashScanOperation;
import com.splicemachine.derby.impl.sql.execute.operations.HashTableOperation;
import com.splicemachine.derby.impl.sql.execute.operations.IndexRowToBaseRowOperation;
import com.splicemachine.derby.impl.sql.execute.operations.InsertOperation;
import com.splicemachine.derby.impl.sql.execute.operations.MergeSortJoinOperation;
import com.splicemachine.derby.impl.sql.execute.operations.MergeSortLeftOuterJoinOperation;
import com.splicemachine.derby.impl.sql.execute.operations.MiscOperation;
import com.splicemachine.derby.impl.sql.execute.operations.MultiProbeTableScanOperation;
import com.splicemachine.derby.impl.sql.execute.operations.NestedLoopJoinOperation;
import com.splicemachine.derby.impl.sql.execute.operations.NestedLoopLeftOuterJoinOperation;
import com.splicemachine.derby.impl.sql.execute.operations.NormalizeOperation;
import com.splicemachine.derby.impl.sql.execute.operations.OnceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.OperationTree;
import com.splicemachine.derby.impl.sql.execute.operations.ProjectRestrictOperation;
import com.splicemachine.derby.impl.sql.execute.operations.RowOperation;
import com.splicemachine.derby.impl.sql.execute.operations.ScalarAggregateOperation;
import com.splicemachine.derby.impl.sql.execute.operations.SetTransactionOperation;
import com.splicemachine.derby.impl.sql.execute.operations.SortOperation;
import com.splicemachine.derby.impl.sql.execute.operations.TableScanOperation;
import com.splicemachine.derby.impl.sql.execute.operations.UnionOperation;
import com.splicemachine.derby.impl.sql.execute.operations.UpdateOperation;
import com.splicemachine.derby.impl.sql.execute.operations.VTIOperation;
import com.splicemachine.utils.SpliceLogUtils;

public class SpliceGenericResultSetFactory extends GenericResultSetFactory {
	private static Logger LOG = Logger.getLogger(SpliceGenericResultSetFactory.class);
	HTablePool htablePool = new HTablePool();
	
	public SpliceGenericResultSetFactory() {
		super();
		SpliceLogUtils.trace(LOG, "instantiating SpliceGenericResultSetFactory");
	}
	
	
	
	@Override
	public NoPutResultSet getOnceResultSet(NoPutResultSet source,
			GeneratedMethod emptyRowFun, int cardinalityCheck,
			int resultSetNumber, int subqueryNumber, int pointOfAttachment,
			double optimizerEstimatedRowCount, double optimizerEstimatedCost)
			throws StandardException {
		SpliceLogUtils.trace(LOG, "getOnceResultSet");
		OnceOperation op = new OnceOperation(source, source.getActivation(), emptyRowFun, cardinalityCheck,
				resultSetNumber, subqueryNumber, pointOfAttachment,
				optimizerEstimatedRowCount, optimizerEstimatedCost);

		OperationTree operationTree = new OperationTree();
		operationTree.traverse(op);
		return (NoPutResultSet) operationTree.execute();
	}



	@Override
	public NoPutResultSet getIndexRowToBaseRowResultSet(long conglomId,
			int scociItem, NoPutResultSet source,
			GeneratedMethod resultRowAllocator, int resultSetNumber,
			String indexName, int heapColRefItem, int allColRefItem,
			int heapOnlyColRefItem, int indexColMapItem,
			GeneratedMethod restriction, boolean forUpdate,
			double optimizerEstimatedRowCount, double optimizerEstimatedCost)
			throws StandardException {
		SpliceLogUtils.trace(LOG, "getIndexRowToBaseRowResultSet");
		return new IndexRowToBaseRowOperation(
				conglomId,
				scociItem,
				source.getActivation(),
				source,
				resultRowAllocator,
				resultSetNumber,
				indexName,
				heapColRefItem,
				allColRefItem,
				heapOnlyColRefItem,
				indexColMapItem,
				restriction,
				forUpdate,
			    optimizerEstimatedRowCount,
				optimizerEstimatedCost);
	}

	@Override
	public NoPutResultSet getProjectRestrictResultSet(NoPutResultSet source,
			GeneratedMethod restriction, GeneratedMethod projection,
			int resultSetNumber, GeneratedMethod constantRestriction,
			int mapRefItem, int cloneMapItem, boolean reuseResult,
			boolean doesProjection, double optimizerEstimatedRowCount,
			double optimizerEstimatedCost) throws StandardException {
		SpliceLogUtils.trace(LOG, "getProjectRestrictResultSet");
				return new ProjectRestrictOperation(source, source.getActivation(), 
						restriction, projection, resultSetNumber, 
			            constantRestriction, mapRefItem, cloneMapItem,
						reuseResult,
						doesProjection,
					    optimizerEstimatedRowCount,
						optimizerEstimatedCost);
	}

	@Override
	public NoPutResultSet getHashJoinResultSet(NoPutResultSet leftResultSet,
			int leftNumCols, NoPutResultSet rightResultSet, int rightNumCols,
			GeneratedMethod joinClause, int resultSetNumber,
			boolean oneRowRightSide, boolean notExistsRightSide,
			double optimizerEstimatedRowCount, double optimizerEstimatedCost,
			String userSuppliedOptimizerOverrides) throws StandardException {
		// TODO Auto-generated method stub
		SpliceLogUtils.trace(LOG, "getHashJoinResultSet");
		return new HashJoinOperation(leftResultSet, leftNumCols,
				   rightResultSet, rightNumCols,
				   leftResultSet.getActivation(), joinClause,
				   resultSetNumber, 
				   oneRowRightSide, 
				   notExistsRightSide, 
				   optimizerEstimatedRowCount,
				   optimizerEstimatedCost,
				   userSuppliedOptimizerOverrides);
	}
	
	@Override
	public NoPutResultSet getHashScanResultSet(Activation activation,
			long conglomId, int scociItem, GeneratedMethod resultRowAllocator,
			int resultSetNumber, GeneratedMethod startKeyGetter,
			int startSearchOperator, GeneratedMethod stopKeyGetter,
			int stopSearchOperator, boolean sameStartStopPosition,
			String scanQualifiersField, String nextQualifierField,
			int initialCapacity, float loadFactor, int maxCapacity,
			int hashKeyColumn, String tableName,
			String userSuppliedOptimizerOverrides, String indexName,
			boolean isConstraint, boolean forUpdate, int colRefItem,
			int indexColItem, int lockMode, boolean tableLocked,
			int isolationLevel, double optimizerEstimatedRowCount,
			double optimizerEstimatedCost) throws StandardException {
	SpliceLogUtils.trace(LOG, "getHashScanResultSet");
	StaticCompiledOpenConglomInfo scoci = (StaticCompiledOpenConglomInfo)(activation.getPreparedStatement().
					getSavedObject(scociItem));

	return new HashScanOperation(
							conglomId,
							scoci,
							activation,
							resultRowAllocator,
							resultSetNumber,
							startKeyGetter,
							startSearchOperator,
							stopKeyGetter,
							stopSearchOperator,
							sameStartStopPosition,
							scanQualifiersField,
							nextQualifierField,
							initialCapacity,
							loadFactor,
							maxCapacity,
							hashKeyColumn,
							tableName,
							userSuppliedOptimizerOverrides,
							indexName,
							isConstraint,
							forUpdate,
							colRefItem,
							lockMode,
							tableLocked,
							isolationLevel,
							true,		// Skip rows with 1 or more null key columns
							optimizerEstimatedRowCount,
							optimizerEstimatedCost);
	}

	@Override
	public NoPutResultSet getNestedLoopLeftOuterJoinResultSet(
		NoPutResultSet leftResultSet, int leftNumCols,
		NoPutResultSet rightResultSet, int rightNumCols,
		GeneratedMethod joinClause, int resultSetNumber,
		GeneratedMethod emptyRowFun, boolean wasRightOuterJoin,
		boolean oneRowRightSide, boolean notExistsRightSide,
		double optimizerEstimatedRowCount, double optimizerEstimatedCost,
		String userSuppliedOptimizerOverrides) throws StandardException {
		SpliceLogUtils.trace(LOG, "getNestedLoopLeftOuterJoinResultSet");
		return new NestedLoopLeftOuterJoinOperation(leftResultSet, leftNumCols,
				   rightResultSet, rightNumCols,
				   leftResultSet.getActivation(), joinClause,
				   resultSetNumber, 
				   emptyRowFun, 
				   wasRightOuterJoin,
				   oneRowRightSide,
				   notExistsRightSide,
				   optimizerEstimatedRowCount,
				   optimizerEstimatedCost,
				   userSuppliedOptimizerOverrides);
	}

	@Override
	public NoPutResultSet getScrollInsensitiveResultSet(NoPutResultSet source,
			Activation activation, int resultSetNumber, int sourceRowWidth,
			boolean scrollable, double optimizerEstimatedRowCount,
			double optimizerEstimatedCost) throws StandardException {
		SpliceLogUtils.trace(LOG, "getScrollInsensitiveResultSet");
			NoPutResultSet top;
			if (scrollable) {
				top = new ScrollInsensitiveResultSet(source, activation, 
										  resultSetNumber, 
										  sourceRowWidth,
										  optimizerEstimatedRowCount,
										  optimizerEstimatedCost);
			}
			else {
				top = source;
			}
			
			OperationTree operationTree = new OperationTree();
			operationTree.traverse((SpliceOperation) top);
			return (NoPutResultSet) operationTree.execute();
	}

	@Override
	public NoPutResultSet getTableScanResultSet(Activation activation,
			long conglomId, int scociItem, GeneratedMethod resultRowAllocator,
			int resultSetNumber, GeneratedMethod startKeyGetter,
			int startSearchOperator, GeneratedMethod stopKeyGetter,
			int stopSearchOperator, boolean sameStartStopPosition,
			String qualifiersField, String tableName,
			String userSuppliedOptimizerOverrides, String indexName,
			boolean isConstraint, boolean forUpdate, int colRefItem,
			int indexColItem, int lockMode, boolean tableLocked,
			int isolationLevel, boolean oneRowScan,
			double optimizerEstimatedRowCount, double optimizerEstimatedCost)
			throws StandardException {
		SpliceLogUtils.trace(LOG, "getTableScanResultSet");
        StaticCompiledOpenConglomInfo scoci = (StaticCompiledOpenConglomInfo)(activation.getPreparedStatement().
						getSavedObject(scociItem));
		return new TableScanOperation(
								conglomId,
								scoci,
								activation,
								resultRowAllocator,
								resultSetNumber,
								startKeyGetter,
								startSearchOperator,
								stopKeyGetter,
								stopSearchOperator,
								sameStartStopPosition,
								qualifiersField,
								tableName,
								userSuppliedOptimizerOverrides,
								indexName,
								isConstraint,
								forUpdate,
								colRefItem,
								indexColItem,
								lockMode,
								tableLocked,
								isolationLevel,
								1,	// rowsPerRead is 1 if not a bulkTableScan
								oneRowScan,
								optimizerEstimatedRowCount,
								optimizerEstimatedCost);
	}


	@Override
	public NoPutResultSet getBulkTableScanResultSet(Activation activation,
			long conglomId, int scociItem, GeneratedMethod resultRowAllocator,
			int resultSetNumber, GeneratedMethod startKeyGetter,
			int startSearchOperator, GeneratedMethod stopKeyGetter,
			int stopSearchOperator, boolean sameStartStopPosition,
			String qualifiersField, String tableName,
			String userSuppliedOptimizerOverrides, String indexName,
			boolean isConstraint, boolean forUpdate, int colRefItem,
			int indexColItem, int lockMode, boolean tableLocked,
			int isolationLevel, int rowsPerRead, boolean disableForHoldable,
			boolean oneRowScan, double optimizerEstimatedRowCount,
			double optimizerEstimatedCost) throws StandardException {
		SpliceLogUtils.trace(LOG, "getBulkTableScanResultSet");
	       StaticCompiledOpenConglomInfo scoci = (StaticCompiledOpenConglomInfo)(activation.getPreparedStatement().
							getSavedObject(scociItem));
			return new BulkTableScanOperation(
									conglomId,
									scoci,
									activation,
									resultRowAllocator,
									resultSetNumber,
									startKeyGetter,
									startSearchOperator,
									stopKeyGetter,
									stopSearchOperator,
									sameStartStopPosition,
									qualifiersField,
									tableName,
									userSuppliedOptimizerOverrides,
									indexName,
									isConstraint,
									forUpdate,
									colRefItem,
									indexColItem,
									lockMode,
									tableLocked,
									isolationLevel,
									rowsPerRead,
	                                disableForHoldable,
									oneRowScan,
									optimizerEstimatedRowCount,
									optimizerEstimatedCost);
	}

	@Override
	public NoPutResultSet getHashLeftOuterJoinResultSet(
			NoPutResultSet leftResultSet, int leftNumCols,
			NoPutResultSet rightResultSet, int rightNumCols,
			GeneratedMethod joinClause, int resultSetNumber,
			GeneratedMethod emptyRowFun, boolean wasRightOuterJoin,
			boolean oneRowRightSide, boolean notExistsRightSide,
			double optimizerEstimatedRowCount, double optimizerEstimatedCost,
			String userSuppliedOptimizerOverrides) throws StandardException {
		SpliceLogUtils.trace(LOG, "getHashLeftOuterJoinResultSet");
		return new HashLeftOuterJoinOperation(leftResultSet, leftNumCols,
				   rightResultSet, rightNumCols,
				   leftResultSet.getActivation(), joinClause,
				   resultSetNumber, 
				   emptyRowFun, 
				   wasRightOuterJoin,
				   oneRowRightSide,
				   notExistsRightSide,
				   optimizerEstimatedRowCount,
				   optimizerEstimatedCost,
				   userSuppliedOptimizerOverrides);
	}

	@Override
	public NoPutResultSet getGroupedAggregateResultSet(NoPutResultSet source,
			boolean isInSortedOrder, int aggregateItem, int orderItem,
			GeneratedMethod rowAllocator, int maxRowSize, int resultSetNumber,
			double optimizerEstimatedRowCount, double optimizerEstimatedCost,
			boolean isRollup) throws StandardException {
		SpliceLogUtils.trace(LOG, "getGroupedAggregateResultSet");
		return new GroupedAggregateOperation(source, isInSortedOrder, aggregateItem, orderItem, source.getActivation(),
		rowAllocator, maxRowSize, resultSetNumber, optimizerEstimatedRowCount,
		optimizerEstimatedCost, isRollup);
	}

	@Override
	public NoPutResultSet getScalarAggregateResultSet(NoPutResultSet source,
			boolean isInSortedOrder, int aggregateItem, int orderItem,
			GeneratedMethod rowAllocator, int maxRowSize, int resultSetNumber,
			boolean singleInputRow, double optimizerEstimatedRowCount,
			double optimizerEstimatedCost) throws StandardException {
		SpliceLogUtils.trace(LOG, "getScalarAggregateResultSet");		
		return new ScalarAggregateOperation(
				source, isInSortedOrder, aggregateItem, source.getActivation(),
				rowAllocator, resultSetNumber, singleInputRow,
				optimizerEstimatedRowCount,
				optimizerEstimatedCost);
	}
	
	@Override
	public NoPutResultSet getSortResultSet(NoPutResultSet source, 
											boolean distinct,
											boolean isInSortedOrder,
											int orderingItem,
											GeneratedMethod ra,
											int numColumns,
											int resultSetNumber,
											double optimizerEstimatedRowCount,
											double optimizerEstimatedCost) throws StandardException{
		SpliceLogUtils.trace(LOG, "getSortResultSet");	
		return new SortOperation(source,distinct,
								 orderingItem,numColumns,
								 source.getActivation(),ra,
								 resultSetNumber,
								 optimizerEstimatedRowCount,
								 optimizerEstimatedCost);
	}
	
	@Override
	public NoPutResultSet getUnionResultSet(NoPutResultSet leftResultSet,
			NoPutResultSet rightResultSet,
			int resultSetNumber,
			double optimizerEstimatedRowCount,
			double optimizerEstimatedCost) throws StandardException
	{
		SpliceLogUtils.trace(LOG, "getUnionResultSet");	
		return new UnionOperation(leftResultSet, 
				rightResultSet, 
				leftResultSet.getActivation(),
				resultSetNumber, 
				optimizerEstimatedRowCount,
				optimizerEstimatedCost);
	}

	@Override
	public NoPutResultSet getRowResultSet(Activation activation,
			GeneratedMethod row, boolean canCacheRow, int resultSetNumber,
			double optimizerEstimatedRowCount, double optimizerEstimatedCost) {
		SpliceLogUtils.trace(LOG, "getRowResultSet");
		try {
			return new RowOperation(activation, row, canCacheRow, resultSetNumber,optimizerEstimatedRowCount, optimizerEstimatedCost);
		} catch (StandardException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, "Cannot get Row Result Set",e);
			return null;
		}
	}

	@Override
	public NoPutResultSet getNormalizeResultSet(NoPutResultSet source,
			int resultSetNumber, int erdNumber,
			double optimizerEstimatedRowCount, double optimizerEstimatedCost,
			boolean forUpdate) throws StandardException {
		return new NormalizeOperation(source,source.getActivation(),resultSetNumber,erdNumber,
								optimizerEstimatedRowCount,optimizerEstimatedCost,forUpdate);
	}
	
	@Override
	public NoPutResultSet getDistinctScanResultSet(
			Activation activation,
			long conglomId,
			int scociItem,
			GeneratedMethod resultRowAllocator,
			int resultSetNumber,
			int hashKeyColumn,
			String tableName,
			String userSuppliedOptimizerOverrides,
			String indexName,
			boolean isConstraint,
			int colRefItem,
			int lockMode,
			boolean tableLocked,
			int isolationLevel,
			double optimizerEstimatedRowCount,
			double optimizerEstimatedCost) throws StandardException {
		
		StaticCompiledOpenConglomInfo scoci = (StaticCompiledOpenConglomInfo)(activation.getPreparedStatement().getSavedObject(scociItem));
		return new DistinctScanOperation(
				conglomId,
				scoci,
				activation,
				resultRowAllocator,
				resultSetNumber,
				hashKeyColumn,
				tableName,
				userSuppliedOptimizerOverrides,
				indexName,
				isConstraint,
				colRefItem,
				lockMode,
				tableLocked,
				isolationLevel,
				optimizerEstimatedRowCount,
				optimizerEstimatedCost);
	}

	@Override
	public NoPutResultSet getHashTableResultSet(NoPutResultSet source,
			GeneratedMethod singleTableRestriction,
			String equijoinQualifiersField, GeneratedMethod projection,
			int resultSetNumber, int mapRefItem, boolean reuseResult,
			int keyColItem, boolean removeDuplicates, long maxInMemoryRowCount,
			int initialCapacity, float loadFactor,
			double optimizerEstimatedRowCount, double optimizerEstimatedCost)
			throws StandardException {
		return new HashTableOperation(source, source.getActivation(), 
				singleTableRestriction, 
				equijoinQualifiersField,
				projection, resultSetNumber, 
				mapRefItem, 
				reuseResult,
				keyColItem, removeDuplicates,
				maxInMemoryRowCount,
				initialCapacity,
				loadFactor,
				true,		// Skip rows with 1 or more null key columns
			    optimizerEstimatedRowCount,
				optimizerEstimatedCost);
	}

	@Override
	public NoPutResultSet getVTIResultSet(Activation activation,
			GeneratedMethod row, int resultSetNumber,
			GeneratedMethod constructor, String javaClassName,
			String pushedQualifiersField, int erdNumber, boolean version2,
			boolean reuseablePs, int ctcNumber, boolean isTarget,
			int scanIsolationLevel, double optimizerEstimatedRowCount,
			double optimizerEstimatedCost, boolean isDerbyStyleTableFunction,
			int returnTypeNumber, int vtiProjectionNumber,
			int vtiRestrictionNumber) throws StandardException {
		return new VTIOperation(activation, row, resultSetNumber, 
				constructor,
				javaClassName,
				pushedQualifiersField,
				erdNumber,
				version2, reuseablePs,
				ctcNumber,
				isTarget,
				scanIsolationLevel,
			    optimizerEstimatedRowCount,
				optimizerEstimatedCost,
				isDerbyStyleTableFunction,
                returnTypeNumber,
                vtiProjectionNumber,
                vtiRestrictionNumber
                );
	}

	@Override
	public NoPutResultSet getMultiProbeTableScanResultSet(
			Activation activation, long conglomId, int scociItem,
			GeneratedMethod resultRowAllocator, int resultSetNumber,
			GeneratedMethod startKeyGetter, int startSearchOperator,
			GeneratedMethod stopKeyGetter, int stopSearchOperator,
			boolean sameStartStopPosition, String qualifiersField,
			DataValueDescriptor[] probeVals, int sortRequired,
			String tableName, String userSuppliedOptimizerOverrides,
			String indexName, boolean isConstraint, boolean forUpdate,
			int colRefItem, int indexColItem, int lockMode,
			boolean tableLocked, int isolationLevel, boolean oneRowScan,
			double optimizerEstimatedRowCount, double optimizerEstimatedCost)
			throws StandardException {
		StaticCompiledOpenConglomInfo scoci = (StaticCompiledOpenConglomInfo)
				activation.getPreparedStatement().getSavedObject(scociItem);

			return new MultiProbeTableScanOperation(
									conglomId,
									scoci,
									activation,
									resultRowAllocator,
									resultSetNumber,
									startKeyGetter,
									startSearchOperator,
									stopKeyGetter,
									stopSearchOperator,
									sameStartStopPosition,
									qualifiersField,
									probeVals,
									sortRequired,
									tableName,
									userSuppliedOptimizerOverrides,
									indexName,
									isConstraint,
									forUpdate,
									colRefItem,
									indexColItem,
									lockMode,
									tableLocked,
									isolationLevel,
									oneRowScan,
									optimizerEstimatedRowCount,
									optimizerEstimatedCost);
	}

	@Override
	public NoPutResultSet getRaDependentTableScanResultSet(
			Activation activation, long conglomId, int scociItem,
			GeneratedMethod resultRowAllocator, int resultSetNumber,
			GeneratedMethod startKeyGetter, int startSearchOperator,
			GeneratedMethod stopKeyGetter, int stopSearchOperator,
			boolean sameStartStopPosition, String qualifiersField,
			String tableName, String userSuppliedOptimizerOverrides,
			String indexName, boolean isConstraint, boolean forUpdate,
			int colRefItem, int indexColItem, int lockMode,
			boolean tableLocked, int isolationLevel, boolean oneRowScan,
			double optimizerEstimatedRowCount, double optimizerEstimatedCost,
			String parentResultSetId, long fkIndexConglomId,
			int fkColArrayItem, int rltItem) throws StandardException {
		SpliceLogUtils.trace(LOG, "getRaDependentTableScanResultSet");
        StaticCompiledOpenConglomInfo scoci = (StaticCompiledOpenConglomInfo)(activation.getPreparedStatement().
						getSavedObject(scociItem));
		return new DependentOperation(
								conglomId,
								scoci,
								activation,
								resultRowAllocator,
								resultSetNumber,
								startKeyGetter,
								startSearchOperator,
								stopKeyGetter,
								stopSearchOperator,
								sameStartStopPosition,
								qualifiersField,
								tableName,
								userSuppliedOptimizerOverrides,
								indexName,
								isConstraint,
								forUpdate,
								colRefItem,
								lockMode,
								tableLocked,
								isolationLevel,
								1,
								oneRowScan,
								optimizerEstimatedRowCount,
								optimizerEstimatedCost,
								parentResultSetId,
								fkIndexConglomId,
								fkColArrayItem,
								rltItem);
	}
	
	@Override
	public NoPutResultSet getDistinctScalarAggregateResultSet(NoPutResultSet source,
			boolean isInSortedOrder,
			int aggregateItem,
			int orderItem,
			GeneratedMethod rowAllocator, 
			int maxRowSize,
			int resultSetNumber, 
			boolean singleInputRow,
			double optimizerEstimatedRowCount,
			double optimizerEstimatedCost) throws StandardException {
		SpliceLogUtils.trace(LOG, "getDistinctScalarAggregateResultSet");
		return new DistinctScalarAggregateOperation(
				source, isInSortedOrder, aggregateItem, orderItem, source.getActivation(),
				rowAllocator, maxRowSize, resultSetNumber, singleInputRow,
				optimizerEstimatedRowCount,
				optimizerEstimatedCost);
	}

	@Override
	public NoPutResultSet getDistinctGroupedAggregateResultSet(NoPutResultSet source,
			boolean isInSortedOrder,
			int aggregateItem,
			int orderItem,
			GeneratedMethod rowAllocator, 
			int maxRowSize,
			int resultSetNumber, 
			double optimizerEstimatedRowCount,
			double optimizerEstimatedCost,
			boolean isRollup) throws StandardException {
		SpliceLogUtils.trace(LOG, "getDistinctGroupedAggregateResultSet");
		return new DistinctGroupedAggregateOperation (
				source, isInSortedOrder, aggregateItem, orderItem, source.getActivation(),
				rowAllocator, maxRowSize, resultSetNumber, optimizerEstimatedRowCount,
				optimizerEstimatedCost, isRollup);
	}

	@Override
	public NoPutResultSet getMergeSortLeftOuterJoinResultSet(
			NoPutResultSet leftResultSet, int leftNumCols,
			NoPutResultSet rightResultSet, int rightNumCols, int leftHashKeyItem, int rightHashKeyItem,
			GeneratedMethod joinClause, int resultSetNumber,
			GeneratedMethod emptyRowFun, boolean wasRightOuterJoin,
			boolean oneRowRightSide, boolean notExistsRightSide,
			double optimizerEstimatedRowCount, double optimizerEstimatedCost,
			String userSuppliedOptimizerOverrides) throws StandardException {
		SpliceLogUtils.trace(LOG, "getMergeSortLeftOuterJoinResultSet");
		return new MergeSortLeftOuterJoinOperation(leftResultSet, leftNumCols,
				   rightResultSet, rightNumCols,leftHashKeyItem,rightHashKeyItem,
				   leftResultSet.getActivation(), joinClause,
				   resultSetNumber, 
				   emptyRowFun, 
				   wasRightOuterJoin,
				   oneRowRightSide,
				   notExistsRightSide,
				   optimizerEstimatedRowCount,
				   optimizerEstimatedCost,
				   userSuppliedOptimizerOverrides);
	}

	@Override
	public NoPutResultSet getNestedLoopJoinResultSet(
			NoPutResultSet leftResultSet, int leftNumCols,
			NoPutResultSet rightResultSet, int rightNumCols,
			GeneratedMethod joinClause, int resultSetNumber,
			boolean oneRowRightSide, boolean notExistsRightSide,
			double optimizerEstimatedRowCount, double optimizerEstimatedCost,
			String userSuppliedOptimizerOverrides) throws StandardException {
		SpliceLogUtils.trace(LOG, "getNestedLoopJoinResultSet");
		return new NestedLoopJoinOperation(leftResultSet, leftNumCols,
				rightResultSet, rightNumCols, leftResultSet.getActivation(), joinClause, resultSetNumber,
				oneRowRightSide, notExistsRightSide, optimizerEstimatedRowCount,
				optimizerEstimatedCost, userSuppliedOptimizerOverrides);
	}

	@Override
	public NoPutResultSet getMergeSortJoinResultSet(
			NoPutResultSet leftResultSet, int leftNumCols,
			NoPutResultSet rightResultSet, int rightNumCols,
			int leftHashKeyItem, int rightHashKeyItem, GeneratedMethod joinClause,
			int resultSetNumber, boolean oneRowRightSide,
			boolean notExistsRightSide, double optimizerEstimatedRowCount,
			double optimizerEstimatedCost, String userSuppliedOptimizerOverrides)
			throws StandardException {
		SpliceLogUtils.trace(LOG, "getMergeSortJoinResultSet");
		return new MergeSortJoinOperation(leftResultSet, leftNumCols,
				rightResultSet, rightNumCols, leftHashKeyItem, rightHashKeyItem, leftResultSet.getActivation(), joinClause, resultSetNumber,
				oneRowRightSide, notExistsRightSide, optimizerEstimatedRowCount,
				optimizerEstimatedCost, userSuppliedOptimizerOverrides);
	}

	@Override
	public NoPutResultSet getDDLResultSet(Activation activation)
			throws StandardException {
		SpliceLogUtils.trace(LOG, "getDDLResultSet");
		return getMiscResultSet(activation);
	}

	@Override
	public NoPutResultSet getMiscResultSet(Activation activation)
			throws StandardException {
		SpliceLogUtils.trace(LOG, "getMiscResultSet");
		SpliceOperation top = new MiscOperation(activation);	
		OperationTree opTree = new OperationTree();
		opTree.traverse(top);
		return (NoPutResultSet)opTree.execute();
	}

	@Override
	public NoPutResultSet getCallStatementResultSet(GeneratedMethod methodCall,
			Activation activation) throws StandardException {
		SpliceLogUtils.trace(LOG, "getCallStatementResultSet");
		SpliceOperation top = new CallStatementOperation(methodCall, activation);	
		OperationTree opTree = new OperationTree();
		opTree.traverse(top);
		return (NoPutResultSet)opTree.execute();
	}

	@Override
	public ResultSet getSetTransactionResultSet(Activation activation) 
			throws StandardException {	
		SpliceLogUtils.trace(LOG, "getSetTransactionResultSet");
		SpliceOperation top = new SetTransactionOperation(activation);	
		OperationTree opTree = new OperationTree();
		opTree.traverse(top);
		return (NoPutResultSet)opTree.execute();
	}

	@Override
	public NoPutResultSet getInsertResultSet(NoPutResultSet source,
			GeneratedMethod generationClauses, GeneratedMethod checkGM)
			throws StandardException {
		SpliceOperation top = new InsertOperation(source, generationClauses, checkGM);	
		OperationTree opTree = new OperationTree();
		opTree.traverse(top);
		return (NoPutResultSet)opTree.execute();
	}
	
	@Override
	public NoPutResultSet getUpdateResultSet(NoPutResultSet source, GeneratedMethod generationClauses,
			GeneratedMethod checkGM) throws StandardException {
		SpliceOperation top = new UpdateOperation(source, generationClauses, checkGM, source.getActivation());
		OperationTree opTree = new OperationTree();
		opTree.traverse(top);
		return (NoPutResultSet)opTree.execute();
	}
	
	@Override
	public NoPutResultSet getDeleteResultSet(NoPutResultSet source)
			throws StandardException
	{
		SpliceOperation top = new DeleteOperation(source, source.getActivation());		
		OperationTree opTree = new OperationTree();
		opTree.traverse(top);
		return (NoPutResultSet)opTree.execute();
	}
	
	/*@Override
	public NoPutResultSet getDeleteCascadeResultSet(NoPutResultSet source, 
			   int constantActionItem,
			   ResultSet[] dependentResultSets,
			   String resultSetId)
			throws StandardException
	{
		return new DeleteCascadeOperation(source, source.getActivation(), 
				constantActionItem,
				dependentResultSets, 
				resultSetId);
	}*/
}
