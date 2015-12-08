package com.splicemachine.derby.impl.sql.execute;

import java.util.List;

import com.splicemachine.db.iapi.sql.execute.ResultSetFactory;
import com.splicemachine.derby.impl.sql.execute.operations.*;
import com.splicemachine.derby.impl.sql.execute.operations.batchonce.BatchOnceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.export.ExportOperation;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.ResultSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.NoPutResultSet;
import com.splicemachine.db.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.GenericResultDescription;

import org.apache.log4j.Logger;

import com.splicemachine.derby.iapi.sql.execute.ConvertedResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;

public class SpliceGenericResultSetFactory implements ResultSetFactory {
    private static Logger LOG = Logger.getLogger(SpliceGenericResultSetFactory.class);

    public SpliceGenericResultSetFactory() {
        super();
        SpliceLogUtils.trace(LOG, "instantiating SpliceGenericResultSetFactory");
    }

    public NoPutResultSet getSetOpResultSet( NoPutResultSet leftSource,
                                             NoPutResultSet rightSource,
                                             Activation activation,
                                             int resultSetNumber,
                                             long optimizerEstimatedRowCount,
                                             double optimizerEstimatedCost,
                                             int opType,
                                             boolean all,
                                             int intermediateOrderByColumnsSavedObject,
                                             int intermediateOrderByDirectionSavedObject,
                                             int intermediateOrderByNullsLowSavedObject)
            throws StandardException
    {
        ConvertedResultSet left = (ConvertedResultSet)leftSource;
        ConvertedResultSet right = (ConvertedResultSet)rightSource;
        return new SetOpOperation( left.getOperation(),
                right.getOperation(),
                activation,
                resultSetNumber,
                optimizerEstimatedRowCount,
                optimizerEstimatedCost,
                opType,
                all,
                intermediateOrderByColumnsSavedObject,
                intermediateOrderByDirectionSavedObject,
                intermediateOrderByNullsLowSavedObject);

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
            ConvertedResultSet below = (ConvertedResultSet)source;
            AnyOperation anyOp = new AnyOperation(below.getOperation(),
                    source.getActivation(),emptyRowFun,
                    resultSetNumber,subqueryNumber,
                    pointOfAttachment,optimizerEstimatedRowCount,
                    optimizerEstimatedCost);
            anyOp.markAsTopResultSet();
            return anyOp;
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
            ConvertedResultSet below = (ConvertedResultSet)source;
            OnceOperation op = new OnceOperation(below.getOperation(), source.getActivation(), emptyRowFun, cardinalityCheck,
                    resultSetNumber, subqueryNumber, pointOfAttachment,
                    optimizerEstimatedRowCount, optimizerEstimatedCost);
            op.markAsTopResultSet();
            return op;
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
                                                        double optimizerEstimatedCost,
                                                        String explainPlan)
            throws StandardException {
        SpliceLogUtils.trace(LOG, "getIndexRowToBaseRowResultSet");
        try{
            SpliceOperation belowOp = ((ConvertedResultSet)source).getOperation();
            SpliceOperation indexOp = new IndexRowToBaseRowOperation(
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
            indexOp.setExplainPlan(explainPlan);
            return indexOp;
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
                                                      double optimizerEstimatedCost,
                                                      String explainPlan) throws StandardException {
        assert source!=null:"passed in source is null";
        SpliceLogUtils.trace(LOG, "getProjectRestrictResultSet");
        try{
            ConvertedResultSet opSet = (ConvertedResultSet)source;
            ProjectRestrictOperation op = new ProjectRestrictOperation(opSet.getOperation(),
                    source.getActivation(),
                    restriction, projection, resultSetNumber,
                    constantRestriction, mapRefItem, cloneMapItem,
                    reuseResult,
                    doesProjection,
                    optimizerEstimatedRowCount,
                    optimizerEstimatedCost);
            op.setExplainPlan(explainPlan);
            return op;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public NoPutResultSet getHashJoinResultSet(NoPutResultSet leftResultSet,
                                               int leftNumCols,
                                               NoPutResultSet rightResultSet,
                                               int rightNumCols,
                                               int leftHashKeyItem,
                                               int righthashKeyItem,
                                               GeneratedMethod joinClause,
                                               int resultSetNumber,
                                               boolean oneRowRightSide,
                                               boolean notExistsRightSide,
                                               double optimizerEstimatedRowCount,
                                               double optimizerEstimatedCost,
                                               String userSuppliedOptimizerOverrides,
                                               String explainPlan) throws StandardException {
        throw new UnsupportedOperationException("HashJoin operation shouldn't be called");
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
                                               boolean rowIdKey,
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
        throw new UnsupportedOperationException("HashScan operation shouldn't be called");
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
            String userSuppliedOptimizerOverrides,
            String explainPlan) throws StandardException {
        try{
            SpliceLogUtils.trace(LOG, "getNestedLoopLeftOuterJoinResultSet");
            ConvertedResultSet left = (ConvertedResultSet)leftResultSet;
            ConvertedResultSet right = (ConvertedResultSet)rightResultSet;
            JoinOperation op = new NestedLoopLeftOuterJoinOperation(left.getOperation(), leftNumCols,
                    right.getOperation(), rightNumCols,
                    leftResultSet.getActivation(), joinClause,
                    resultSetNumber,
                    emptyRowFun,
                    wasRightOuterJoin,
                    oneRowRightSide,
                    notExistsRightSide,
                    optimizerEstimatedRowCount,
                    optimizerEstimatedCost,
                    userSuppliedOptimizerOverrides);
            op.setExplainPlan(explainPlan);
            return op;
        }catch(Exception e){
            if(e instanceof StandardException) throw (StandardException)e;
            throw StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION, e);
        }
    }

    @Override
    public NoPutResultSet getScrollInsensitiveResultSet(NoPutResultSet source,
                                                        Activation activation, int resultSetNumber, int sourceRowWidth,
                                                        boolean scrollable, double optimizerEstimatedRowCount,
                                                        double optimizerEstimatedCost,
                                                        String explainPlan) throws StandardException {
        try{
            SpliceLogUtils.trace(LOG, "getScrollInsensitiveResultSet");
            ConvertedResultSet opSet = (ConvertedResultSet)source;
            ScrollInsensitiveOperation op = new ScrollInsensitiveOperation(opSet.getOperation(),activation,resultSetNumber,sourceRowWidth,scrollable,optimizerEstimatedRowCount,optimizerEstimatedCost);
            op.markAsTopResultSet();
            op.setExplainPlan(explainPlan);
            return op;
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
                                                boolean rowIdKey,
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
                                                double optimizerEstimatedCost,
                                                String explainPlan)
            throws StandardException {
        SpliceLogUtils.trace(LOG, "getTableScanResultSet");
        try{
            StaticCompiledOpenConglomInfo scoci = (StaticCompiledOpenConglomInfo)(activation.getPreparedStatement().
                    getSavedObject(scociItem));
            TableScanOperation op = new TableScanOperation(
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
                    rowIdKey,
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
            op.setExplainPlan(explainPlan);
            return op;
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
                                                    boolean rowIdKey,
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
                                                    double optimizerEstimatedCost,
                                                    String explainPlan) throws StandardException {
        SpliceLogUtils.trace(LOG, "getBulkTableScanResultSet");
        try{
            StaticCompiledOpenConglomInfo scoci = (StaticCompiledOpenConglomInfo)(activation.getPreparedStatement().
                    getSavedObject(scociItem));
            TableScanOperation op = new TableScanOperation(
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
                    rowIdKey,
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
                    oneRowScan,
                    optimizerEstimatedRowCount,
                    optimizerEstimatedCost);
            op.setExplainPlan(explainPlan);
            return op;
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
            int leftHashKeyItem,
            int rightHashKeyItem,
            GeneratedMethod joinClause,
            int resultSetNumber,
            GeneratedMethod emptyRowFun,
            boolean wasRightOuterJoin,
            boolean oneRowRightSide,
            boolean notExistsRightSide,
            double optimizerEstimatedRowCount,
            double optimizerEstimatedCost,
            String userSuppliedOptimizerOverrides,
            String explainPlan) throws StandardException {
        throw new UnsupportedOperationException("HashLeftOuterJoin operation shouldn't be called");
    }

    @Override
    public NoPutResultSet getGroupedAggregateResultSet(NoPutResultSet source,
                                                       boolean isInSortedOrder, int aggregateItem, int orderItem,
                                                       GeneratedMethod rowAllocator, int maxRowSize, int resultSetNumber,
                                                       double optimizerEstimatedRowCount, double optimizerEstimatedCost,
                                                       boolean isRollup, String explainPlan) throws StandardException {
        try{
            SpliceLogUtils.trace(LOG, "getGroupedAggregateResultSet");
            ConvertedResultSet below = (ConvertedResultSet)source;
            GroupedAggregateOperation op = new GroupedAggregateOperation(below.getOperation(), isInSortedOrder, aggregateItem, orderItem, source.getActivation(),
                    rowAllocator, maxRowSize, resultSetNumber, optimizerEstimatedRowCount,
                    optimizerEstimatedCost, isRollup);
            op.setExplainPlan(explainPlan);
            return op;
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
                                                      double optimizerEstimatedCost,
                                                      String explainPlan) throws StandardException {
        SpliceLogUtils.trace(LOG, "getScalarAggregateResultSet");
        try{
            ConvertedResultSet below = (ConvertedResultSet)source;
            ScalarAggregateOperation op = new ScalarAggregateOperation(
                    below.getOperation(), isInSortedOrder, aggregateItem, source.getActivation(),
                    rowAllocator, resultSetNumber, singleInputRow,
                    optimizerEstimatedRowCount,
                    optimizerEstimatedCost);
            op.setExplainPlan(explainPlan);
            return op;
        }catch(Exception e){
            if(e instanceof StandardException) throw (StandardException)e;
            throw StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION, e);
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
                                           double optimizerEstimatedCost,
                                           String explainPlan) throws StandardException {
        SpliceLogUtils.trace(LOG, "getSortResultSet");
        try{
            ConvertedResultSet below = (ConvertedResultSet)source;
            SortOperation op = new SortOperation(below.getOperation(),distinct,
                    orderingItem,numColumns,
                    source.getActivation(),ra,
                    resultSetNumber,
                    optimizerEstimatedRowCount,
                    optimizerEstimatedCost);
            op.setExplainPlan(explainPlan);
            return op;
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
                                            double optimizerEstimatedCost,
                                            String explainPlan) throws StandardException {
        try{
            SpliceLogUtils.trace(LOG, "getUnionResultSet");
            ConvertedResultSet left = (ConvertedResultSet)leftResultSet;
            ConvertedResultSet right = (ConvertedResultSet)rightResultSet;
            SpliceOperation op = new UnionOperation(left.getOperation(),
                    right.getOperation(),
                    leftResultSet.getActivation(),
                    resultSetNumber,
                    optimizerEstimatedRowCount,optimizerEstimatedCost);
            op.setExplainPlan(explainPlan);
            return op;
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
            return new RowOperation(activation, row, canCacheRow, resultSetNumber,optimizerEstimatedRowCount, optimizerEstimatedCost);
        } catch (StandardException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG, "Cannot get Row Result Set",e);
            return null;
        }
    }


    @Override
    public NoPutResultSet getRowResultSet(Activation activation,
                                          ExecRow row, boolean canCacheRow, int resultSetNumber,
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
    public NoPutResultSet getCachedResultSet(Activation activation, List rows, int resultSetNumber) throws StandardException {
        try {
            return new CachedOperation(activation, (List<ExecRow>)rows, resultSetNumber);
        } catch (StandardException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG, "Cannot get Cached Result Set", e);
            return null;
        }
    }

    @Override
    public NoPutResultSet getNormalizeResultSet(NoPutResultSet source,
                                                int resultSetNumber,
                                                int erdNumber,
                                                double optimizerEstimatedRowCount,
                                                double optimizerEstimatedCost,
                                                boolean forUpdate,
                                                String explainPlan) throws StandardException {
        try{
            ConvertedResultSet below = (ConvertedResultSet)source;
            NormalizeOperation op = new NormalizeOperation(below.getOperation(),source.getActivation(),resultSetNumber,erdNumber,
                    optimizerEstimatedRowCount,optimizerEstimatedCost,forUpdate);
            op.setExplainPlan(explainPlan);
            return op;
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
        throw new UnsupportedOperationException("HashTable operation shouldn't be called");
    }

    @Override
    public NoPutResultSet getVTIResultSet(Activation activation,
                                          GeneratedMethod row, int resultSetNumber,
                                          GeneratedMethod constructor, String javaClassName,
                                          String pushedQualifiersField, int erdNumber,
                                          int ctcNumber, boolean isTarget,
                                          int scanIsolationLevel, double optimizerEstimatedRowCount,
                                          double optimizerEstimatedCost, boolean isDerbyStyleTableFunction,
                                          int returnTypeNumber, int vtiProjectionNumber,
                                          int vtiRestrictionNumber,
                                          String explainPlan) throws StandardException {

        VTIOperation op =  new VTIOperation(activation, row, resultSetNumber,
                constructor,
                javaClassName,
                pushedQualifiersField,
                erdNumber,
                ctcNumber,
                isTarget,
                scanIsolationLevel,
                optimizerEstimatedRowCount,
                optimizerEstimatedCost,
                isDerbyStyleTableFunction,
                returnTypeNumber,
                vtiProjectionNumber,
                vtiRestrictionNumber);
        op.setExplainPlan(explainPlan);
        return op;
    }

    @Override
    public NoPutResultSet getVTIResultSet(
            Activation activation,
            GeneratedMethod row,
            int resultSetNumber,
            GeneratedMethod constructor,
            String javaClassName,
            com.splicemachine.db.iapi.store.access.Qualifier[][] pushedQualifiersField,
            int erdNumber,
            int ctcNumber,
            boolean isTarget,
            int scanIsolationLevel,
            double optimizerEstimatedRowCount,
            double optimizerEstimatedCost,
            boolean isDerbyStyleTableFunction,
            int returnTypeNumber,
            int vtiProjectionNumber,
            int vtiRestrictionNumber,
            String explainPlan)
            throws StandardException {
        
        return getVTIResultSet(
                activation,
                row,
                resultSetNumber,
                constructor,
                javaClassName,
                (String) null,
                erdNumber,
                ctcNumber,
                isTarget,
                scanIsolationLevel,
                optimizerEstimatedRowCount,
                optimizerEstimatedCost,
                isDerbyStyleTableFunction,
                returnTypeNumber,
                vtiProjectionNumber,
                vtiRestrictionNumber,
                explainPlan
        );
    }


    @Override
    public NoPutResultSet getMultiProbeTableScanResultSet(
            Activation activation, long conglomId, int scociItem,
            GeneratedMethod resultRowAllocator, int resultSetNumber,
            GeneratedMethod startKeyGetter, int startSearchOperator,
            GeneratedMethod stopKeyGetter, int stopSearchOperator,
            boolean sameStartStopPosition, boolean rowIdKey, String qualifiersField,
            DataValueDescriptor[] probeVals, int sortRequired,
            String tableName, String userSuppliedOptimizerOverrides,
            String indexName, boolean isConstraint, boolean forUpdate,
            int colRefItem, int indexColItem, int lockMode,
            boolean tableLocked, int isolationLevel, boolean oneRowScan,
            double optimizerEstimatedRowCount, double optimizerEstimatedCost,
            String explainPlan)
            throws StandardException {
        try{
            StaticCompiledOpenConglomInfo scoci = (StaticCompiledOpenConglomInfo)
                    activation.getPreparedStatement().getSavedObject(scociItem);

            TableScanOperation op = new MultiProbeTableScanOperation(
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
                    rowIdKey,
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
            op.setExplainPlan(explainPlan);
            return op;
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
            boolean sameStartStopPosition, boolean rowIdKey, String qualifiersField,
            String tableName, String userSuppliedOptimizerOverrides,
            String indexName, boolean isConstraint, boolean forUpdate,
            int colRefItem, int indexColItem, int lockMode,
            boolean tableLocked, int isolationLevel, boolean oneRowScan,
            double optimizerEstimatedRowCount, double optimizerEstimatedCost,
            String parentResultSetId, long fkIndexConglomId,
            int fkColArrayItem, int rltItem) throws StandardException {
        throw new UnsupportedOperationException("Dependant operation is not implemented");
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
                                                              double optimizerEstimatedCost,
                                                              String explainPlan) throws StandardException {
        SpliceLogUtils.trace(LOG, "getDistinctScalarAggregateResultSet");
        try{
            ConvertedResultSet below = (ConvertedResultSet)source;
            DistinctScalarAggregateOperation op = new DistinctScalarAggregateOperation(below.getOperation(),
                    isInSortedOrder,
                    aggregateItem,
                    orderItem,
                    rowAllocator,
                    maxRowSize,
                    resultSetNumber,
                    singleInputRow,
                    optimizerEstimatedRowCount,
                    optimizerEstimatedCost);
            op.setExplainPlan(explainPlan);
            return op;
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
                                                               boolean isRollup,
                                                               String explainPlan) throws StandardException {
        SpliceLogUtils.trace(LOG, "getDistinctGroupedAggregateResultSet");
        try{
            ConvertedResultSet below = (ConvertedResultSet)source;
            DistinctGroupedAggregateOperation op = new DistinctGroupedAggregateOperation (
                    below.getOperation(), isInSortedOrder, aggregateItem, orderItem, source.getActivation(),
                    rowAllocator, maxRowSize, resultSetNumber, optimizerEstimatedRowCount,
                    optimizerEstimatedCost, isRollup);
            op.setExplainPlan(explainPlan);
            return op;
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
            String userSuppliedOptimizerOverrides,
            String explainPlan) throws StandardException {
        SpliceLogUtils.trace(LOG, "getMergeSortLeftOuterJoinResultSet");
        try{
            ConvertedResultSet left = (ConvertedResultSet)leftResultSet;
            ConvertedResultSet right = (ConvertedResultSet)rightResultSet;
            JoinOperation op = new MergeSortLeftOuterJoinOperation(left.getOperation(), leftNumCols,
                    right.getOperation(), rightNumCols,leftHashKeyItem,rightHashKeyItem,
                    leftResultSet.getActivation(), joinClause,
                    resultSetNumber,
                    emptyRowFun,
                    wasRightOuterJoin,
                    oneRowRightSide,
                    notExistsRightSide,
                    optimizerEstimatedRowCount,
                    optimizerEstimatedCost,
                    userSuppliedOptimizerOverrides);
            op.setExplainPlan(explainPlan);
            return op;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public NoPutResultSet getMergeLeftOuterJoinResultSet(
            NoPutResultSet leftResultSet, int leftNumCols,
            NoPutResultSet rightResultSet, int rightNumCols,
            int leftHashKeyItem, int rightHashKeyItem,
            GeneratedMethod joinClause, int resultSetNumber,
            GeneratedMethod emptyRowFun, boolean wasRightOuterJoin,
            boolean oneRowRightSide, boolean notExistsRightSide,
            double optimizerEstimatedRowCount, double optimizerEstimatedCost,
            String userSuppliedOptimizerOverrides,
            String explainPlan) throws StandardException {
        SpliceLogUtils.trace(LOG, "getMergeSortLeftOuterJoinResultSet");
        try{
            ConvertedResultSet left = (ConvertedResultSet)leftResultSet;
            ConvertedResultSet right = (ConvertedResultSet)rightResultSet;
            JoinOperation op = new MergeLeftOuterJoinOperation(left.getOperation(), leftNumCols,
                    right.getOperation(), rightNumCols,leftHashKeyItem,rightHashKeyItem,
                    leftResultSet.getActivation(), joinClause,
                    resultSetNumber,
                    emptyRowFun,
                    wasRightOuterJoin,
                    oneRowRightSide,
                    notExistsRightSide,
                    optimizerEstimatedRowCount,
                    optimizerEstimatedCost,
                    userSuppliedOptimizerOverrides);
            op.setExplainPlan(explainPlan);
            return op;
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
            String userSuppliedOptimizerOverrides,
            String explainPlan) throws StandardException {
        SpliceLogUtils.trace(LOG, "getMergeSortLeftOuterJoinResultSet");
        try{
            ConvertedResultSet left = (ConvertedResultSet)leftResultSet;
            ConvertedResultSet right = (ConvertedResultSet)rightResultSet;
            JoinOperation op = new BroadcastLeftOuterJoinOperation(left.getOperation(), leftNumCols,
                    right.getOperation(), rightNumCols,leftHashKeyItem,rightHashKeyItem,
                    leftResultSet.getActivation(), joinClause,
                    resultSetNumber,
                    emptyRowFun,
                    wasRightOuterJoin,
                    oneRowRightSide,
                    notExistsRightSide,
                    optimizerEstimatedRowCount,
                    optimizerEstimatedCost,
                    userSuppliedOptimizerOverrides);
            op.setExplainPlan(explainPlan);
            return op;
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
            String userSuppliedOptimizerOverrides,
            String explainPlan) throws StandardException {
        SpliceLogUtils.trace(LOG, "getNestedLoopJoinResultSet");
        try{
            ConvertedResultSet left = (ConvertedResultSet)leftResultSet;
            ConvertedResultSet right = (ConvertedResultSet)rightResultSet;
            JoinOperation op = new NestedLoopJoinOperation(left.getOperation(), leftNumCols,
                    right.getOperation(), rightNumCols, leftResultSet.getActivation(), joinClause, resultSetNumber,
                    oneRowRightSide, notExistsRightSide, optimizerEstimatedRowCount,
                    optimizerEstimatedCost, userSuppliedOptimizerOverrides);
            op.setExplainPlan(explainPlan);
            return op;
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
            double optimizerEstimatedCost,
            String userSuppliedOptimizerOverrides,
            String explainPlan) throws StandardException {
        SpliceLogUtils.trace(LOG, "getMergeSortJoinResultSet");
        try{
            ConvertedResultSet left = (ConvertedResultSet)leftResultSet;
            ConvertedResultSet right = (ConvertedResultSet)rightResultSet;
            JoinOperation op = new MergeSortJoinOperation(left.getOperation(), leftNumCols,
                    right.getOperation(), rightNumCols, leftHashKeyItem, rightHashKeyItem, leftResultSet.getActivation(), joinClause, resultSetNumber,
                    oneRowRightSide, notExistsRightSide, optimizerEstimatedRowCount,
                    optimizerEstimatedCost, userSuppliedOptimizerOverrides);
            op.setExplainPlan(explainPlan);
            return op;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public NoPutResultSet getMergeJoinResultSet(
            NoPutResultSet leftResultSet, int leftNumCols,
            NoPutResultSet rightResultSet, int rightNumCols,
            int leftHashKeyItem, int rightHashKeyItem, GeneratedMethod joinClause,
            int resultSetNumber, boolean oneRowRightSide,
            boolean notExistsRightSide, double optimizerEstimatedRowCount,
            double optimizerEstimatedCost,
            String userSuppliedOptimizerOverrides,
            String explainPlan) throws StandardException {
        SpliceLogUtils.trace(LOG, "getMergeSortJoinResultSet");
        try{
            ConvertedResultSet left = (ConvertedResultSet)leftResultSet;
            ConvertedResultSet right = (ConvertedResultSet)rightResultSet;
            JoinOperation op = new MergeJoinOperation(left.getOperation(), leftNumCols,
                    right.getOperation(), rightNumCols, leftHashKeyItem, rightHashKeyItem, leftResultSet.getActivation(), joinClause, resultSetNumber,
                    oneRowRightSide, notExistsRightSide, optimizerEstimatedRowCount,
                    optimizerEstimatedCost, userSuppliedOptimizerOverrides);
            op.setExplainPlan(explainPlan);
            return op;
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
            double optimizerEstimatedCost,
            String userSuppliedOptimizerOverrides,
            String explainPlan) throws StandardException {
        SpliceLogUtils.trace(LOG, "getBroadcastJoinResultSet");
        try{
            ConvertedResultSet left = (ConvertedResultSet)leftResultSet;
            ConvertedResultSet right = (ConvertedResultSet)rightResultSet;
            JoinOperation op = new BroadcastJoinOperation(left.getOperation(), leftNumCols,
                    right.getOperation(), rightNumCols, leftHashKeyItem, rightHashKeyItem, leftResultSet.getActivation(), joinClause, resultSetNumber,
                    oneRowRightSide, notExistsRightSide, optimizerEstimatedRowCount,
                    optimizerEstimatedCost, userSuppliedOptimizerOverrides);
            op.setExplainPlan(explainPlan);
            return op;
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
            activation.getLanguageConnectionContext().getAuthorizer().authorize(activation, 1);
            return top;
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
            return top;
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
            return top;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public NoPutResultSet getInsertResultSet(NoPutResultSet source,
                                             GeneratedMethod generationClauses, GeneratedMethod checkGM,
                                             String insertMode, String statusDirectory, int failBadRecordCount,
                                             double optimizerEstimatedRowCount,
                                             double optimizerEstimatedCost,
                                             String explainPlan)
            throws StandardException {
        try{
            ConvertedResultSet below = (ConvertedResultSet)source;
            SpliceOperation top = new InsertOperation(below.getOperation(), generationClauses, checkGM, insertMode,
                    statusDirectory, failBadRecordCount,optimizerEstimatedRowCount,optimizerEstimatedCost);
            source.getActivation().getLanguageConnectionContext().getAuthorizer().authorize(source.getActivation(), 1);
            top.markAsTopResultSet();
            top.setExplainPlan(explainPlan);
            return top;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public NoPutResultSet getUpdateResultSet(NoPutResultSet source, GeneratedMethod generationClauses,
                                             GeneratedMethod checkGM,double optimizerEstimatedRowCount,
                                             double optimizerEstimatedCost, String explainPlan) throws StandardException {
        try{
            ConvertedResultSet below = (ConvertedResultSet)source;
            SpliceOperation top = new UpdateOperation(below.getOperation(), generationClauses, checkGM, source.getActivation(),optimizerEstimatedCost,optimizerEstimatedRowCount);
            source.getActivation().getLanguageConnectionContext().getAuthorizer().authorize(source.getActivation(), 1);
            top.markAsTopResultSet();
            top.setExplainPlan(explainPlan);
            return top;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public ResultSet getDeleteCascadeUpdateResultSet(NoPutResultSet source, GeneratedMethod generationClauses, GeneratedMethod checkGM, int constantActionItem, int rsdItem
                                                     ) throws StandardException {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public NoPutResultSet getDeleteResultSet(NoPutResultSet source,double optimizerEstimatedRowCount,
                                             double optimizerEstimatedCost, String explainPlan)
            throws StandardException {
        try{
            ConvertedResultSet below = (ConvertedResultSet)source;
            SpliceOperation top = new DeleteOperation(below.getOperation(), source.getActivation(),optimizerEstimatedRowCount,optimizerEstimatedCost);
            top.markAsTopResultSet();
            top.setExplainPlan(explainPlan);
            return top;
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
            double optimizerEstimatedCost,
            String explainPlan) throws StandardException {
        SpliceLogUtils.trace(LOG, "getRowCountResultSet");
        ConvertedResultSet below = (ConvertedResultSet)source;
        RowCountOperation op = new RowCountOperation(below.getOperation(),
                activation,
                resultSetNumber,
                offsetMethod,
                fetchFirstMethod,
                hasJDBClimitClause,
                optimizerEstimatedRowCount,
                optimizerEstimatedCost);
        op.setExplainPlan(explainPlan);
        return op;
	}

	public NoPutResultSet getLastIndexKeyResultSet
	(
		Activation 			activation,
		int 				resultSetNumber,
		GeneratedMethod 	resultRowAllocator,
		long 				conglomId,
		String 				tableName,
		String 				userSuppliedOptimizerOverrides,
		String 				indexName,
		int 				colRefItem,
		int 				lockMode,
		boolean				tableLocked,
		int					isolationLevel,
		double				optimizerEstimatedRowCount,
		double 				optimizerEstimatedCost
	) throws StandardException
	{
		SpliceLogUtils.trace(LOG, "getLastIndexKeyResultSet");
		return new LastIndexKeyOperation(
					activation,
					resultSetNumber,
					resultRowAllocator,
					conglomId,
					tableName,
					userSuppliedOptimizerOverrides,
					indexName,
					colRefItem,
					lockMode,
					tableLocked,
					isolationLevel,
					optimizerEstimatedRowCount,
					optimizerEstimatedCost);
	}

    public NoPutResultSet getWindowResultSet(NoPutResultSet source,
                                             boolean isInSortedOrder,
                                             int aggregateItem,
                                             GeneratedMethod rowAllocator,
                                             int maxRowSize,
                                             int resultSetNumber,
                                             double optimizerEstimatedRowCount,
                                             double optimizerEstimatedCost,
                                             String explainPlan)
        throws StandardException {
        SpliceLogUtils.trace(LOG, "getWindowResultSet");

        ConvertedResultSet below = (ConvertedResultSet)source;
        SpliceOperation windowOp = new WindowOperation(
            below.getOperation(),
            isInSortedOrder,
            aggregateItem,
            source.getActivation(),
            rowAllocator,
            resultSetNumber,
            optimizerEstimatedRowCount,
            optimizerEstimatedCost);
        windowOp.setExplainPlan(explainPlan);
        return windowOp;
    }

    @Override
    public NoPutResultSet getExplainResultSet(ResultSet source, Activation activation, int resultSetNumber) throws StandardException {
        ConvertedResultSet opSet = (ConvertedResultSet)source;
        return new ExplainOperation(opSet.getOperation(), activation, resultSetNumber);
    }

    @Override
    public NoPutResultSet getExplainResultSet(NoPutResultSet source, Activation activation, int resultSetNumber) throws StandardException {
        ConvertedResultSet opSet = (ConvertedResultSet)source;
        return new ExplainOperation(opSet.getOperation(), activation, resultSetNumber);
    }

    @Override
    public NoPutResultSet getExportResultSet(NoPutResultSet source,
                                             Activation activation,
                                             int resultSetNumber,
                                             String exportPath,
                                             boolean compression,
                                             int replicationCount,
                                             String encoding,
                                             String fieldSeparator,
                                             String quoteChar,
                                             int srcResultDescriptionSavedObjectNum) throws StandardException {

        // If we ask the activation prepared statement for ResultColumnDescriptors we get the two columns that
        // export operation returns (exported row count, and export time) not the columns of the source operation.
        // Not what we need to format the rows during export.  So ExportNode now saves the source
        // ResultColumnDescriptors and we retrieve them here.
        Object resultDescription = activation.getPreparedStatement().getSavedObject(srcResultDescriptionSavedObjectNum);
        ResultColumnDescriptor[] columnDescriptors = ((GenericResultDescription) resultDescription).getColumnInfo();

        ConvertedResultSet convertedResultSet = (ConvertedResultSet) source;
        SpliceBaseOperation op = new ExportOperation(
                convertedResultSet.getOperation(),
                columnDescriptors,
                activation,
                resultSetNumber,
                exportPath,
                compression,
                replicationCount,
                encoding,
                fieldSeparator,
                quoteChar
        );
        op.markAsTopResultSet();
        return op;
    }

    /**
     * BatchOnce
     */
    @Override
    public NoPutResultSet getBatchOnceResultSet(NoPutResultSet source,
                                                Activation activation,
                                                int resultSetNumber,
                                                NoPutResultSet subqueryResultSet,
                                                String updateResultSetFieldName,
                                                int sourceRowLocationColumnPosition,
                                                int sourceCorrelatedColumnPosition,
                                                int subqueryCorrelatedColumnPosition) throws StandardException {

        ConvertedResultSet convertedResultSet = (ConvertedResultSet) source;
        ConvertedResultSet convertedSubqueryResultSet = (ConvertedResultSet) subqueryResultSet;

        BatchOnceOperation batchOnceOperation = new BatchOnceOperation(
                convertedResultSet.getOperation(),
                activation,
                resultSetNumber,
                convertedSubqueryResultSet.getOperation(),
                updateResultSetFieldName,
                sourceRowLocationColumnPosition,
                sourceCorrelatedColumnPosition,
                subqueryCorrelatedColumnPosition
        );

        return batchOnceOperation;
    }

    @Override
    public ResultSet getInsertVTIResultSet(NoPutResultSet source, NoPutResultSet vtiRS, double optimizerEstimatedRowCount, double optimizerEstimatedCost) throws StandardException {
        throw new RuntimeException("Not Implemented");
    }

    @Override
    public ResultSet getDeleteVTIResultSet(NoPutResultSet source, double optimizerEstimatedRowCount, double optimizerEstimatedCost) throws StandardException {
        throw new RuntimeException("Not Implemented");
    }

    @Override
    public ResultSet getUpdateVTIResultSet(NoPutResultSet source, double optimizerEstimatedRowCount, double optimizerEstimatedCost) throws StandardException {
        throw new RuntimeException("Not Implemented");
    }

    @Override
    public NoPutResultSet getMaterializedResultSet(NoPutResultSet source, int resultSetNumber, double optimizerEstimatedRowCount, double optimizerEstimatedCost) throws StandardException {
        throw new RuntimeException("Not Implemented");
    }

    @Override
    public NoPutResultSet getCurrentOfResultSet(String cursorName, Activation activation, int resultSetNumber) {
        throw new RuntimeException("Not Implemented");
    }
}
