package com.splicemachine.derby.impl.store.access;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.AccessPath;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.store.access.AggregateCostController;
import com.splicemachine.db.iapi.store.access.StoreCostController;
import com.splicemachine.db.impl.sql.compile.*;

/**
 * CostController for a TEMP-table based algorithm for computing Grouped Aggregates.
 *
 * The TEMP based algorithm is separated into a <em>parallel</em> and <em>sequential</em> phase. The
 * parallel phase <em>always</em> occurs, and contributes its cost to the local portion of the sequential
 * phase.
 *
 * --------
 * <h2>Costing the Parallel Phase</h2>
 * For each partition in the base cost, we submit a parallel task which reads all the data,
 * then computes intermediate results and emits them to TEMP (for simplicity, we ignore the
 * size of the underlying hashtable or the sort order of the incoming data, which can dramatically
 * affect the actual size of the intermediate results). Therefore, we read all the rows, but we
 * write only the number of <em>unique</em> rows.
 *
 * Again for simplicity, we assume that all partitions have roughly the same number of rows of interest,
 * which means that all tasks will run in roughly the same amount of time. Therefore, the cost to perform
 * the parallel phase is divided by the partition count to reflect this. As a result, the cost is
 *
 * parallelOutputRows = cardinalityFraction*baseCost.outputRows
 * parallelCost = (baseCost.localCost+baseCost.remoteCost/parallelOutputRows)/numPartitions
 *
 * where cardinalityFraction is the number of unique values for <em>all</em> grouped columns. In general,
 * cardinality estimates of two multisets follow the "row count rule". Consider to columns A and B. The
 * cardinality of A is C(A), and the cardinality of B is C(B). If we let C by the cardinality of the intersection
 * (e.g. the number of distinct values in common between the two sets), and D be the cardinality of the
 * union (e.g. what we're looking for), then we have
 *
 * C(D) = C(A) + C(B) - C(C)
 *
 * and C(D) is the cardinalityFraction. Note that this isn't perfect, but it's fairly close.
 *
 * ------
 * <h2>Costing the Sequential Phase</h2>
 * The Sequential phase reads all data in sorted order and merges them together to form the final output row.
 * This should be considered the cost to read the TEMP data (since we aren't accounting for duplication
 * in this costing strategy).
 *
 * @author Scott Fines
 *         Date: 3/26/15
 */
public class TempGroupedAggregateCostController implements AggregateCostController{
    private final GroupByList groupingList;

    public TempGroupedAggregateCostController(GroupByList groupingList){
        this.groupingList=groupingList;
    }

    @Override
    public CostEstimate estimateAggregateCost(CostEstimate baseCost) throws StandardException{
        double overallCardinalityFraction = 1d;
        double sumCardFrac = 0d;
        for(OrderedColumn oc:groupingList){
            ValueNode colExprValue=oc.getColumnExpression();
            assert colExprValue!=null;
            assert colExprValue instanceof ColumnReference: "Programmer error: unexpected type "+ colExprValue.getClass();

            ColumnReference ref = (ColumnReference)colExprValue;
            StoreCostController storeCostController=getStoreCostForColumn(ref);
            double c=storeCostController.cardinalityFraction(ref.getSourceResultColumn().getColumnPosition());
            overallCardinalityFraction *=c;
            sumCardFrac+=c;
        }

        double cardFraction = sumCardFrac-overallCardinalityFraction;
        if(overallCardinalityFraction==1d||cardFraction>1d){
            /*
             * If the overallCardinalityFraction (e.g. the cardinality of the intersection) is 1,
             * then we will return an entry for every row in the base table, in which case
             * we really shouldn't multiply or divide by anything.
             */
            cardFraction=1d;
        }
        double baseRc = baseCost.rowCount();

        double outputRows;
        if(baseRc==0d)
            outputRows = 0d;
        else
            outputRows = cardFraction*baseRc;
        /*
         * If the baseCost claims it's not returning any rows, or our cardinality
         * fraction is too aggressive, we may think that we don't need to do anything. This
         * is rarely correct. To make out math easier, we just always assume that we'll have at least
         * one row returned.
         */
        if(outputRows<1d) outputRows=1;
        double parallelCost = (baseCost.localCost()+baseCost.remoteCost()/outputRows)/baseCost.partitionCount();

        double localCostPerRow;
        double remoteCostPerRow;
        double heapPerRow;
        if(baseRc==0d){
            localCostPerRow = 0d;
            remoteCostPerRow = 0d;
            heapPerRow = 0d;
        }else{
            localCostPerRow=baseCost.localCost()/baseRc;
            remoteCostPerRow=baseCost.remoteCost()/baseRc;
            heapPerRow=baseCost.getEstimatedHeapSize()/baseRc;
        }

        double seqLocalCost = parallelCost+outputRows*localCostPerRow;
        double seqRemoteCost = outputRows*remoteCostPerRow;
        double finalHeap = outputRows*heapPerRow; //TODO -sf- include the cost of the aggregate columns in the row size

        int numPartitions = 16; //since we write to TEMP's buckets

        CostEstimate newEstimate = baseCost.cloneMe();
        //output information
        newEstimate.setRowCount(outputRows);
        newEstimate.setSingleScanRowCount(outputRows);
        newEstimate.setNumPartitions(numPartitions);
        newEstimate.setEstimatedHeapSize((long)finalHeap);

        //cost information
        newEstimate.setLocalCost(seqLocalCost);
        newEstimate.setRemoteCost(seqRemoteCost);

        //because this is a parallel sink algorithm, the underlying cost should not include any remote cost
        baseCost.setRemoteCost(0d);

        return newEstimate;
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private StoreCostController getStoreCostForColumn(ColumnReference ref) throws StandardException{
        ValueNode exprNode = ref.getSource().getExpression();
        assert exprNode!=null;
        assert exprNode instanceof VirtualColumnNode: "Programmer error: unexpected type "+ exprNode.getClass();

        VirtualColumnNode col = (VirtualColumnNode)exprNode;

        ResultSetNode rsn = col.getSourceResultSet();
        assert rsn!=null;
        assert rsn instanceof FromTable: "Programmer error: unexpected type "+ rsn.getClass();
        ConglomerateDescriptor cd = findConglomerateDescriptor(col.getSourceResultSet());
        //get the underlying store controller for this node.
        return rsn.getCompilerContext().getStoreCostController(cd);
    }

    private ConglomerateDescriptor findConglomerateDescriptor(ResultSetNode rsn){
        if(rsn instanceof FromBaseTable){
            FromBaseTable fbt = (FromBaseTable)rsn;
            AccessPath ap = fbt.getCurrentAccessPath();
            ConglomerateDescriptor cd;
            if(ap!=null){
                cd = ap.getConglomerateDescriptor();
                if(cd==null){
                    /*
                     * -sf- Derby sometimes does this to us if the current Access path hasn't been
                     * initialized yet (which appears to happen if there are no joins in the query).
                     * In this case, we defer to the bestAccessPath, which appears to always be populated.
                     */
                    cd = fbt.getBestAccessPath().getConglomerateDescriptor();
                }
            }else{
                /*
                 * -sf- We don't even HAVE a current access path. I don't believe that this
                 * ever happens, but I'm not yet comfortable enough with how Derby constructs
                 * access paths to be sure.
                 */
                ap = fbt.getBestAccessPath();
                cd = ap.getConglomerateDescriptor();
            }
            return cd;
        }else if(rsn instanceof JoinNode){
            JoinNode joinNode=(JoinNode)rsn;
            ConglomerateDescriptor cd = findConglomerateDescriptor(joinNode.getLeftResultSet());
            if(cd==null)
                cd = findConglomerateDescriptor(joinNode.getRightResultSet());
            return cd;
        }else if(rsn instanceof SingleChildResultSetNode){
            return findConglomerateDescriptor(((SingleChildResultSetNode)rsn).getChildResult());
        }else{
            throw new IllegalStateException("Programmer Error: Unexpected node type: "+rsn.getClass());
        }
    }
}
