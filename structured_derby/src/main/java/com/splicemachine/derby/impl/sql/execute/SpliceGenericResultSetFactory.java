package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.sql.execute.OperationResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.*;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.impl.sql.execute.GenericResultSetFactory;
import org.apache.log4j.Logger;

public class SpliceGenericResultSetFactory extends GenericResultSetFactory {
	private static Logger LOG = Logger.getLogger(SpliceGenericResultSetFactory.class);
//	HTablePool htablePool = new HTablePool();

    private final OperationTree treeManager;

	public SpliceGenericResultSetFactory() {
		super();
		SpliceLogUtils.trace(LOG, "instantiating SpliceGenericResultSetFactory");
        int maxTreeThreads = SpliceConstants.maxTreeThreads;

        treeManager = OperationTree.create(maxTreeThreads);
    }


    @Override
    public NoPutResultSet getAnyResultSet(NoPutResultSet source,
                                          GeneratedMethod emptyRowFun,
                                          int resultSetNumber,
                                          int subqueryNumber,
                                          int pointOfAttachment,
                                          double optimizerEstimatedRowCount,
                                          double optimizerEstimatedCost) throws StandardException {
        try{
            OperationResultSet below = (OperationResultSet)source;
            AnyOperation anyOp = new AnyOperation(below.getTopOperation(),
                    source.getActivation(),emptyRowFun,
                    resultSetNumber,subqueryNumber,
                    pointOfAttachment,optimizerEstimatedRowCount,
                    optimizerEstimatedCost);
            below.setTopOperation(anyOp);
            return below;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

    @Override
	public NoPutResultSet getOnceResultSet(NoPutResultSet source,
			GeneratedMethod emptyRowFun, int cardinalityCheck,
			int resultSetNumber, int subqueryNumber, int pointOfAttachment,
			double optimizerEstimatedRowCount, double optimizerEstimatedCost)
			throws StandardException {
        try{
            SpliceLogUtils.trace(LOG, "getOnceResultSet");
            OperationResultSet below = (OperationResultSet)source;
            OnceOperation op = new OnceOperation(below.getTopOperation(), source.getActivation(), emptyRowFun, cardinalityCheck,
                    resultSetNumber, subqueryNumber, pointOfAttachment,
                    optimizerEstimatedRowCount, optimizerEstimatedCost);
            below.setTopOperation(op);
            below.markAsTopResultSet();
            return below;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }



	@Override
	public NoPutResultSet getIndexRowToBaseRowResultSet(long conglomId,
			int scociItem,
            NoPutResultSet source,
			GeneratedMethod resultRowAllocator,
            int resultSetNumber,
			String indexName,
            int heapColRefItem,
            int allColRefItem,
			int heapOnlyColRefItem,
            int indexColMapItem,
			GeneratedMethod restriction,
            boolean forUpdate,
			double optimizerEstimatedRowCount,
            double optimizerEstimatedCost)
			throws StandardException {
		SpliceLogUtils.trace(LOG, "getIndexRowToBaseRowResultSet");
        try{
            SpliceOperation belowOp = ((OperationResultSet)source).getTopOperation();
            SpliceOperation op = new IndexRowToBaseRowOperation(
                    conglomId,
                    scociItem,
                    source.getActivation(),
                    belowOp,
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
            ((OperationResultSet)source).setTopOperation(op);
            return source;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

	@Override
	public NoPutResultSet getProjectRestrictResultSet(NoPutResultSet source,
			GeneratedMethod restriction,
            GeneratedMethod projection,
			int resultSetNumber,
            GeneratedMethod constantRestriction,
			int mapRefItem,
            int cloneMapItem,
            boolean reuseResult,
			boolean doesProjection,
            double optimizerEstimatedRowCount,
			double optimizerEstimatedCost) throws StandardException {
		SpliceLogUtils.trace(LOG, "getProjectRestrictResultSet");
        try{
            OperationResultSet opSet = (OperationResultSet)source;
            SpliceOperation op =  new ProjectRestrictOperation(opSet.getTopOperation(),
                    source.getActivation(),
                    restriction, projection, resultSetNumber,
                    constantRestriction, mapRefItem, cloneMapItem,
                    reuseResult,
                    doesProjection,
                    optimizerEstimatedRowCount,
                    optimizerEstimatedCost);
            opSet.setTopOperation(op);
            return opSet;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

	@Override
	public NoPutResultSet getHashJoinResultSet(NoPutResultSet leftResultSet,
			int leftNumCols,
            NoPutResultSet rightResultSet,
            int rightNumCols,
			GeneratedMethod joinClause,
            int resultSetNumber,
			boolean oneRowRightSide,
            boolean notExistsRightSide,
			double optimizerEstimatedRowCount,
            double optimizerEstimatedCost,
			String userSuppliedOptimizerOverrides) throws StandardException {
		SpliceLogUtils.trace(LOG, "getHashJoinResultSet");
        OperationResultSet left = (OperationResultSet)leftResultSet;
        OperationResultSet right = (OperationResultSet)rightResultSet;
		SpliceOperation op =  new HashJoinOperation(left.getTopOperation(), leftNumCols,
				   right.getTopOperation(), rightNumCols,
				   leftResultSet.getActivation(), joinClause,
				   resultSetNumber, 
				   oneRowRightSide, 
				   notExistsRightSide, 
				   optimizerEstimatedRowCount,
				   optimizerEstimatedCost,
				   userSuppliedOptimizerOverrides);
        left.setTopOperation(op);
        return left;
	}
	
	@Override
	public NoPutResultSet getHashScanResultSet(Activation activation,
			long conglomId,
            int scociItem,
            GeneratedMethod resultRowAllocator,
			int resultSetNumber,
            GeneratedMethod startKeyGetter,
			int startSearchOperator,
            GeneratedMethod stopKeyGetter,
			int stopSearchOperator,
            boolean sameStartStopPosition,
			String scanQualifiersField,
            String nextQualifierField,
			int initialCapacity,
            float loadFactor,
            int maxCapacity,
			int hashKeyColumn,
            String tableName,
			String userSuppliedOptimizerOverrides,
            String indexName,
			boolean isConstraint,
            boolean forUpdate,
            int colRefItem,
			int indexColItem,
            int lockMode,
            boolean tableLocked,
			int isolationLevel,
            double optimizerEstimatedRowCount,
			double optimizerEstimatedCost) throws StandardException {
	SpliceLogUtils.trace(LOG, "getHashScanResultSet");
        try{
            StaticCompiledOpenConglomInfo scoci = (StaticCompiledOpenConglomInfo)(activation.getPreparedStatement().
                    getSavedObject(scociItem));

            SpliceOperation op = new HashScanOperation(
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

            return new OperationResultSet(activation,treeManager,op);
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

	@Override
	public NoPutResultSet getNestedLoopLeftOuterJoinResultSet(
		NoPutResultSet leftResultSet,
        int leftNumCols,
		NoPutResultSet rightResultSet,
        int rightNumCols,
		GeneratedMethod joinClause,
        int resultSetNumber,
		GeneratedMethod emptyRowFun,
        boolean wasRightOuterJoin,
		boolean oneRowRightSide,
        boolean notExistsRightSide,
		double optimizerEstimatedRowCount,
        double optimizerEstimatedCost,
		String userSuppliedOptimizerOverrides) throws StandardException {
        try{
            SpliceLogUtils.trace(LOG, "getNestedLoopLeftOuterJoinResultSet");
            OperationResultSet left = (OperationResultSet)leftResultSet;
            OperationResultSet right = (OperationResultSet)rightResultSet;
            SpliceOperation newOp = new NestedLoopLeftOuterJoinOperation(left.getTopOperation(), leftNumCols,
                    right.getTopOperation(), rightNumCols,
                    leftResultSet.getActivation(), joinClause,
                    resultSetNumber,
                    emptyRowFun,
                    wasRightOuterJoin,
                    oneRowRightSide,
                    notExistsRightSide,
                    optimizerEstimatedRowCount,
                    optimizerEstimatedCost,
                    userSuppliedOptimizerOverrides);
            left.setTopOperation(newOp);
            return left;
        }catch(Exception e){
            if(e instanceof StandardException) throw (StandardException)e;
            throw StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,e);
        }
    }

    @Override
    public NoPutResultSet getScrollInsensitiveResultSet(NoPutResultSet source,
                                                        Activation activation, int resultSetNumber, int sourceRowWidth,
                                                        boolean scrollable, double optimizerEstimatedRowCount,
                                                        double optimizerEstimatedCost) throws StandardException {
        try{
            SpliceLogUtils.trace(LOG, "getScrollInsensitiveResultSet");
            OperationResultSet opSet = (OperationResultSet)source;
            opSet.markAsTopResultSet();
            return opSet;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

	@Override
	public NoPutResultSet getTableScanResultSet(Activation activation,
			long conglomId,
            int scociItem,
            GeneratedMethod resultRowAllocator,
			int resultSetNumber,
            GeneratedMethod startKeyGetter,
			int startSearchOperator,
            GeneratedMethod stopKeyGetter,
			int stopSearchOperator,
            boolean sameStartStopPosition,
			String qualifiersField,
            String tableName,
			String userSuppliedOptimizerOverrides,
            String indexName,
			boolean isConstraint,
            boolean forUpdate,
            int colRefItem,
			int indexColItem,
            int lockMode,
            boolean tableLocked,
			int isolationLevel,
            boolean oneRowScan,
			double optimizerEstimatedRowCount,
            double optimizerEstimatedCost)
			throws StandardException {
		SpliceLogUtils.trace(LOG, "getTableScanResultSet");
        try{
            StaticCompiledOpenConglomInfo scoci = (StaticCompiledOpenConglomInfo)(activation.getPreparedStatement().
                    getSavedObject(scociItem));
            SpliceOperation baseOp =  new TableScanOperation(
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

            return new OperationResultSet(activation,treeManager,baseOp);
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }


    @Override
    public NoPutResultSet getBulkTableScanResultSet(Activation activation,
                                                    long conglomId,
                                                    int scociItem,
                                                    GeneratedMethod resultRowAllocator,
                                                    int resultSetNumber,
                                                    GeneratedMethod startKeyGetter,
                                                    int startSearchOperator,
                                                    GeneratedMethod stopKeyGetter,
                                                    int stopSearchOperator,
                                                    boolean sameStartStopPosition,
                                                    String qualifiersField,
                                                    String tableName,
                                                    String userSuppliedOptimizerOverrides,
                                                    String indexName,
                                                    boolean isConstraint,
                                                    boolean forUpdate,
                                                    int colRefItem,
                                                    int indexColItem,
                                                    int lockMode,
                                                    boolean tableLocked,
                                                    int isolationLevel,
                                                    int rowsPerRead,
                                                    boolean disableForHoldable,
                                                    boolean oneRowScan,
                                                    double optimizerEstimatedRowCount,
                                                    double optimizerEstimatedCost) throws StandardException {
        SpliceLogUtils.trace(LOG, "getBulkTableScanResultSet");
        try{
            StaticCompiledOpenConglomInfo scoci = (StaticCompiledOpenConglomInfo)(activation.getPreparedStatement().
                    getSavedObject(scociItem));
            SpliceOperation op =  new BulkTableScanOperation(
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

            return new OperationResultSet(activation,treeManager,op);
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

	@Override
	public NoPutResultSet getHashLeftOuterJoinResultSet(
			NoPutResultSet leftResultSet,
            int leftNumCols,
			NoPutResultSet rightResultSet,
            int rightNumCols,
			GeneratedMethod joinClause,
            int resultSetNumber,
			GeneratedMethod emptyRowFun,
            boolean wasRightOuterJoin,
			boolean oneRowRightSide,
            boolean notExistsRightSide,
			double optimizerEstimatedRowCount,
            double optimizerEstimatedCost,
			String userSuppliedOptimizerOverrides) throws StandardException {
		SpliceLogUtils.trace(LOG, "getHashLeftOuterJoinResultSet");
        try{
            OperationResultSet left = (OperationResultSet)leftResultSet;
            OperationResultSet right = (OperationResultSet)rightResultSet;
            SpliceOperation op = new HashLeftOuterJoinOperation(left.getTopOperation(), leftNumCols,
                    right.getTopOperation(), rightNumCols,
                    leftResultSet.getActivation(), joinClause,
                    resultSetNumber,
                    emptyRowFun,
                    wasRightOuterJoin,
                    oneRowRightSide,
                    notExistsRightSide,
                    optimizerEstimatedRowCount,
                    optimizerEstimatedCost,
                    userSuppliedOptimizerOverrides);

            left.setTopOperation(op);
            return left;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

	@Override
	public NoPutResultSet getGroupedAggregateResultSet(NoPutResultSet source,
			boolean isInSortedOrder, int aggregateItem, int orderItem,
			GeneratedMethod rowAllocator, int maxRowSize, int resultSetNumber,
			double optimizerEstimatedRowCount, double optimizerEstimatedCost,
			boolean isRollup) throws StandardException {
        try{
            SpliceLogUtils.trace(LOG, "getGroupedAggregateResultSet");
            OperationResultSet below = (OperationResultSet)source;
            SpliceOperation op =  new GroupedAggregateOperation(below.getTopOperation(), isInSortedOrder, aggregateItem, orderItem, source.getActivation(),
                    rowAllocator, maxRowSize, resultSetNumber, optimizerEstimatedRowCount,
                    optimizerEstimatedCost, isRollup);

            below.setTopOperation(op);
            return below;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
	}

	@Override
	public NoPutResultSet getScalarAggregateResultSet(NoPutResultSet source,
			boolean isInSortedOrder,
            int aggregateItem,
            int orderItem,
			GeneratedMethod rowAllocator,
            int maxRowSize,
            int resultSetNumber,
			boolean singleInputRow,
            double optimizerEstimatedRowCount,
			double optimizerEstimatedCost) throws StandardException {
		SpliceLogUtils.trace(LOG, "getScalarAggregateResultSet");
        try{
            OperationResultSet below = (OperationResultSet)source;
            SpliceOperation op =  new ScalarAggregateOperation(
                    below.getTopOperation(), isInSortedOrder, aggregateItem, source.getActivation(),
                    rowAllocator, resultSetNumber, singleInputRow,
                    optimizerEstimatedRowCount,
                    optimizerEstimatedCost);

            below.setTopOperation(op);
            return below;
        }catch(Exception e){
            if(e instanceof StandardException) throw (StandardException)e;
            throw StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,e);
        }
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
        try{
            OperationResultSet below = (OperationResultSet)source;
            SpliceOperation op =  new SortOperation(below.getTopOperation(),distinct,
                    orderingItem,numColumns,
                    source.getActivation(),ra,
                    resultSetNumber,
                    optimizerEstimatedRowCount,
                    optimizerEstimatedCost);
            below.setTopOperation(op);
            return below;
        }catch(Exception e){
            if(e instanceof StandardException) throw (StandardException)e;
            throw StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,e);
        }
	}
	
	@Override
	public NoPutResultSet getUnionResultSet(NoPutResultSet leftResultSet,
			NoPutResultSet rightResultSet,
			int resultSetNumber,
			double optimizerEstimatedRowCount,
			double optimizerEstimatedCost) throws StandardException {
        try{
            SpliceLogUtils.trace(LOG, "getUnionResultSet");
            OperationResultSet left = (OperationResultSet)leftResultSet;
            OperationResultSet right = (OperationResultSet)rightResultSet;
            SpliceOperation op = new UnionOperation(left.getTopOperation(),
                    right.getTopOperation(),
                    leftResultSet.getActivation(),
                    resultSetNumber,
                    optimizerEstimatedRowCount,optimizerEstimatedCost);
            left.setTopOperation(op);
            return left;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
	}

	@Override
	public NoPutResultSet getRowResultSet(Activation activation,
			GeneratedMethod row, boolean canCacheRow, int resultSetNumber,
			double optimizerEstimatedRowCount, double optimizerEstimatedCost) {
		SpliceLogUtils.trace(LOG, "getRowResultSet");
		try {
			SpliceOperation op = new RowOperation(activation, row, canCacheRow, resultSetNumber,optimizerEstimatedRowCount, optimizerEstimatedCost);

            return new OperationResultSet(activation,treeManager,op);
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
        try{
            OperationResultSet below = (OperationResultSet)source;
            SpliceOperation op = new NormalizeOperation(below.getTopOperation(),source.getActivation(),resultSetNumber,erdNumber,
                    optimizerEstimatedRowCount,optimizerEstimatedCost,forUpdate);
            below.setTopOperation(op);
            return below;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
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
        try{
            StaticCompiledOpenConglomInfo scoci = (StaticCompiledOpenConglomInfo)(activation.getPreparedStatement().getSavedObject(scociItem));
            SpliceOperation op = new DistinctScanOperation(
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
            return new OperationResultSet(activation,treeManager,op);
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
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
        try{
            OperationResultSet below = (OperationResultSet)source;
            SpliceOperation op =  new HashTableOperation(below.getTopOperation(), source.getActivation(),
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
            below.setTopOperation(op);
            return below;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
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
        try{
            SpliceOperation op = new VTIOperation(activation, row, resultSetNumber,
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

            return new OperationResultSet(activation,treeManager,op);
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
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
        try{
            StaticCompiledOpenConglomInfo scoci = (StaticCompiledOpenConglomInfo)
                    activation.getPreparedStatement().getSavedObject(scociItem);

            SpliceOperation op =  new MultiProbeTableScanOperation(
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

            return new OperationResultSet(activation,treeManager,op);
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
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
		SpliceOperation op =  new DependentOperation(
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

        return new OperationResultSet(activation,treeManager,op);
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
        try{
            OperationResultSet below = (OperationResultSet)source;
            SpliceOperation op  = new DistinctScalarAggregateOperation(below.getTopOperation(),
                    isInSortedOrder,
                    aggregateItem,
                    orderItem,
                    rowAllocator,
                    maxRowSize,
                    resultSetNumber,
                    singleInputRow,
                    optimizerEstimatedRowCount,
                    optimizerEstimatedCost);

            below.setTopOperation(op);
            return below;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
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
        try{
            OperationResultSet below = (OperationResultSet)source;
            SpliceOperation op =  new DistinctGroupedAggregateOperation (
                    below.getTopOperation(), isInSortedOrder, aggregateItem, orderItem, source.getActivation(),
                    rowAllocator, maxRowSize, resultSetNumber, optimizerEstimatedRowCount,
                    optimizerEstimatedCost, isRollup);

            below.setTopOperation(op);
            return below;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
	}

    @Override
    public NoPutResultSet getMergeSortLeftOuterJoinResultSet(
            NoPutResultSet leftResultSet, int leftNumCols,
            NoPutResultSet rightResultSet, int rightNumCols,
            int leftHashKeyItem, int rightHashKeyItem,
            GeneratedMethod joinClause, int resultSetNumber,
            GeneratedMethod emptyRowFun, boolean wasRightOuterJoin,
            boolean oneRowRightSide, boolean notExistsRightSide,
            double optimizerEstimatedRowCount, double optimizerEstimatedCost,
            String userSuppliedOptimizerOverrides) throws StandardException {
        SpliceLogUtils.trace(LOG, "getMergeSortLeftOuterJoinResultSet");
        try{
            OperationResultSet left = (OperationResultSet)leftResultSet;
            OperationResultSet right = (OperationResultSet)rightResultSet;
            SpliceOperation op = new MergeSortLeftOuterJoinOperation(left.getTopOperation(), leftNumCols,
                    right.getTopOperation(), rightNumCols,leftHashKeyItem,rightHashKeyItem,
                    leftResultSet.getActivation(), joinClause,
                    resultSetNumber,
                    emptyRowFun,
                    wasRightOuterJoin,
                    oneRowRightSide,
                    notExistsRightSide,
                    optimizerEstimatedRowCount,
                    optimizerEstimatedCost,
                    userSuppliedOptimizerOverrides);
            left.setTopOperation(op);
            return left;

        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }
    @Override
    public NoPutResultSet getBroadcastLeftOuterJoinResultSet(
            NoPutResultSet leftResultSet, int leftNumCols,
            NoPutResultSet rightResultSet, int rightNumCols,
            int leftHashKeyItem, int rightHashKeyItem,
            GeneratedMethod joinClause, int resultSetNumber,
            GeneratedMethod emptyRowFun, boolean wasRightOuterJoin,
            boolean oneRowRightSide, boolean notExistsRightSide,
            double optimizerEstimatedRowCount, double optimizerEstimatedCost,
            String userSuppliedOptimizerOverrides) throws StandardException {
        SpliceLogUtils.trace(LOG, "getMergeSortLeftOuterJoinResultSet");
        try{
            OperationResultSet left = (OperationResultSet)leftResultSet;
            OperationResultSet right = (OperationResultSet)rightResultSet;
            SpliceOperation op =  new BroadcastLeftOuterJoinOperation(left.getTopOperation(), leftNumCols,
                    right.getTopOperation(), rightNumCols,leftHashKeyItem,rightHashKeyItem,
                    leftResultSet.getActivation(), joinClause,
                    resultSetNumber,
                    emptyRowFun,
                    wasRightOuterJoin,
                    oneRowRightSide,
                    notExistsRightSide,
                    optimizerEstimatedRowCount,
                    optimizerEstimatedCost,
                    userSuppliedOptimizerOverrides);

            left.setTopOperation(op);
            return left;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
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
       try{
           OperationResultSet left = (OperationResultSet)leftResultSet;
           OperationResultSet right = (OperationResultSet)rightResultSet;
		SpliceOperation op =  new NestedLoopJoinOperation(left.getTopOperation(), leftNumCols,
				right.getTopOperation(), rightNumCols, leftResultSet.getActivation(), joinClause, resultSetNumber,
				oneRowRightSide, notExistsRightSide, optimizerEstimatedRowCount,
				optimizerEstimatedCost, userSuppliedOptimizerOverrides);
           left.setTopOperation(op);
           return left;
       }catch(Exception e){
           throw Exceptions.parseException(e);
       }
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
        try{
            OperationResultSet left = (OperationResultSet)leftResultSet;
            OperationResultSet right = (OperationResultSet)rightResultSet;
            SpliceOperation op =  new MergeSortJoinOperation(left.getTopOperation(), leftNumCols,
                    right.getTopOperation(), rightNumCols, leftHashKeyItem, rightHashKeyItem, leftResultSet.getActivation(), joinClause, resultSetNumber,
                    oneRowRightSide, notExistsRightSide, optimizerEstimatedRowCount,
                    optimizerEstimatedCost, userSuppliedOptimizerOverrides);
            left.setTopOperation(op);
            return left;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
	}

	@Override
	public NoPutResultSet getBroadcastJoinResultSet(
			NoPutResultSet leftResultSet, int leftNumCols,
			NoPutResultSet rightResultSet, int rightNumCols,
			int leftHashKeyItem, int rightHashKeyItem, GeneratedMethod joinClause,
			int resultSetNumber, boolean oneRowRightSide,
			boolean notExistsRightSide, double optimizerEstimatedRowCount,
			double optimizerEstimatedCost, String userSuppliedOptimizerOverrides)
			throws StandardException {
		SpliceLogUtils.trace(LOG, "getBroadcastJoinResultSet");
        try{
            OperationResultSet left = (OperationResultSet)leftResultSet;
            OperationResultSet right = (OperationResultSet)rightResultSet;
            SpliceOperation op =  new BroadcastJoinOperation(left.getTopOperation(), leftNumCols,
                    right.getTopOperation(), rightNumCols, leftHashKeyItem, rightHashKeyItem, leftResultSet.getActivation(), joinClause, resultSetNumber,
                    oneRowRightSide, notExistsRightSide, optimizerEstimatedRowCount,
                    optimizerEstimatedCost, userSuppliedOptimizerOverrides);

            left.setTopOperation(op);
            return left;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
	}

	@Override
	public NoPutResultSet getDDLResultSet(Activation activation)
			throws StandardException {
        SpliceLogUtils.trace(LOG, "getDDLResultSet");
        try{
            return getMiscResultSet(activation);
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
	}

	@Override
	public NoPutResultSet getMiscResultSet(Activation activation)
			throws StandardException {
        try{
            SpliceLogUtils.trace(LOG, "getMiscResultSet");
            SpliceOperation top = new MiscOperation(activation);
            top.markAsTopResultSet();
            return new OperationResultSet(activation,treeManager,top);
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

	@Override
	public NoPutResultSet getCallStatementResultSet(GeneratedMethod methodCall,
                                                    Activation activation) throws StandardException {
        SpliceLogUtils.trace(LOG, "getCallStatementResultSet");
        try{
            SpliceOperation top = new CallStatementOperation(methodCall, activation);
            top.markAsTopResultSet();
            return new OperationResultSet(activation,treeManager,top);
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
	}

	@Override
	public ResultSet getSetTransactionResultSet(Activation activation) 
			throws StandardException {	
		SpliceLogUtils.trace(LOG, "getSetTransactionResultSet");
        try{
            SpliceOperation top = new SetTransactionOperation(activation);
            top.markAsTopResultSet();
            return new OperationResultSet(activation,treeManager,top);
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
	}

	@Override
	public NoPutResultSet getInsertResultSet(NoPutResultSet source,
			GeneratedMethod generationClauses, GeneratedMethod checkGM)
			throws StandardException {
        try{
            OperationResultSet below = (OperationResultSet)source;
            SpliceOperation top = new InsertOperation(below.getTopOperation(), generationClauses, checkGM);
            below.setTopOperation(top);
            below.markAsTopResultSet();
            return below;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
     }
	
	@Override
	public NoPutResultSet getUpdateResultSet(NoPutResultSet source, GeneratedMethod generationClauses,
			GeneratedMethod checkGM) throws StandardException {
        try{
            OperationResultSet below = (OperationResultSet)source;
            SpliceOperation top = new UpdateOperation(below.getTopOperation(), generationClauses, checkGM, source.getActivation());
            below.setTopOperation(top);
            below.markAsTopResultSet();
            return below;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
	}
	
	@Override
	public NoPutResultSet getDeleteResultSet(NoPutResultSet source)
			throws StandardException {
        try{
            OperationResultSet below = (OperationResultSet)source;
            SpliceOperation top = new DeleteOperation(below.getTopOperation(), source.getActivation());
            below.setTopOperation(top);
            below.markAsTopResultSet();
            return below;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }
	
	@Override
	public NoPutResultSet getDeleteCascadeResultSet(NoPutResultSet source, 
			   int constantActionItem,
			   ResultSet[] dependentResultSets,
			   String resultSetId)
			throws StandardException
	{
        throw StandardException.newException(SQLState.HEAP_UNIMPLEMENTED_FEATURE);
	}
	
	public NoPutResultSet getRowCountResultSet(
			NoPutResultSet source,
			Activation activation,
			int resultSetNumber,
			GeneratedMethod offsetMethod,
			GeneratedMethod fetchFirstMethod,
			boolean hasJDBClimitClause,
			double optimizerEstimatedRowCount,
			double optimizerEstimatedCost) throws StandardException {		 
		SpliceLogUtils.trace(LOG, "getRowCountResultSet");
		SpliceOperation op =  new RowCountOperation((SpliceOperation)source,
				activation,
				resultSetNumber,
				offsetMethod,
				fetchFirstMethod,
				hasJDBClimitClause,
				optimizerEstimatedRowCount,
				optimizerEstimatedCost);

        return new OperationResultSet(activation,treeManager,op);
	}
}
