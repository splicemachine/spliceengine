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
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.reference.Limits;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.store.access.ColumnOrdering;
import com.splicemachine.db.iapi.store.access.SortCostController;
import com.splicemachine.db.iapi.util.JBitSet;

/**
 * An OrderByList is an ordered list of columns in the ORDER BY clause.
 * That is, the order of columns in this list is significant - the
 * first column in the list is the most significant in the ordering,
 * and the last column in the list is the least significant.
 */
public class OrderByList extends OrderedColumnList implements RequiredRowOrdering{

    private boolean allAscending=true;
    private boolean alwaysSort;
    private SortCostController scc;
    private ColumnOrdering[] columnOrdering;
    private boolean sortNeeded=true;
    private int resultSetNumber=-1;

        /**
          * {@code true} if this instance orders a
          * {@literal <table value constructor>}.
          * See {@link #isTableValueCtorOrdering}.
          */
        private boolean isTableValueCtorOrdering;
    
                /**
          * Initialize with the type of the result set this {@code OrderByList} is
          * attached to, e.g. {@code SELECT}, {@code VALUES} or a set operation.
          * @param rs The result set this {@code OrderByList} is ordering.
         */
                public void init(Object rs) {
                this.isTableValueCtorOrdering =
                                (rs instanceof UnionNode &&
                                        ((UnionNode)rs).tableConstructor()) ||
                                        rs instanceof RowResultSetNode;
            }
    
    /**
     * Add a column to the list
     *
     * @param column The column to add to the list
     */
    public void addOrderByColumn(OrderByColumn column){
        addElement(column);

        if(!column.isAscending())
            allAscending=false;
    }

    /**
     * Get a column from the list
     *
     * @param position The column to get from the list
     */
    public OrderByColumn getOrderByColumn(int position){
        assert position>=0 && position<size();
        return (OrderByColumn)elementAt(position);
    }

    /**
     * Bind the update columns by their names to the target resultset of the
     * cursor specification.
     *
     * @param target The underlying result set
     * @throws StandardException Thrown on error
     */
    public void bindOrderByColumns(ResultSetNode target) throws StandardException{

		/* Remember the target for use in optimization */

        int size=size();

		/* Only 1012 columns allowed in ORDER BY clause */
        if(size>Limits.DB2_MAX_ELEMENTS_IN_ORDER_BY){
            throw StandardException.newException(SQLState.LANG_TOO_MANY_ELEMENTS);
        }

        for(int index=0;index<size;index++){
            OrderByColumn obc=(OrderByColumn)elementAt(index);
            obc.bindOrderByColumn(target,this);

			/*
            ** Always sort if we are ordering on an expression, and not
			** just a column.
			*/
            if(!(obc.getResultColumn().getExpression() instanceof ColumnReference)){
                alwaysSort=true;
            }
        }
    }

    /**
     * Pull up Order By columns by their names to the target resultset
     * of the cursor specification.
     *
     * @param target The underlying result set
     */
    public void pullUpOrderByColumns(ResultSetNode target) throws StandardException{

		/* Remember the target for use in optimization */

        int size=size();
        for(int index=0;index<size;index++){
            OrderByColumn obc=(OrderByColumn)elementAt(index);
            obc.pullUpOrderByColumn(target);
        }

    }

    /**
     * generate the sort result set operating over the source
     * expression.
     *
     * @param acb the tool for building the class
     * @param mb  the method the generated code is to go into
     * @throws StandardException thrown on failure
     */
    public void generate(ActivationClassBuilder acb,
                         MethodBuilder mb,
                         ResultSetNode orderByNode,
                         ResultSetNode child,
                         CostEstimate sortEstimate) throws StandardException{
        /*
		** If sorting is not required, don't generate a sort result set -
		** just return the child result set.
		*/
        if(!sortNeeded){
            child.generate(acb,mb);
            return;
        }

		/* Get the next ResultSet#, so we can number this ResultSetNode, its
		 * ResultColumnList and ResultSet.
		 *
		 * REMIND: to do this properly (if order bys can live throughout
		 * the tree) there ought to be an OrderByNode that holds its own
		 * ResultColumnList that is a lsit of virtual column nodes pointing
		 * to the source's result columns.  But since we know it is outermost,
		 * we just gloss over that and get ourselves a resultSetNumber
		 * directly.
		 */
        CompilerContext cc=getCompilerContext();


        //create the orderItem and stuff it in.
        int orderItem=acb.addItem(acb.getColumnOrdering(this));


		/* Generate the SortResultSet:
		 *	arg1: childExpress - Expression for childResultSet
		 *  arg2: distinct - always false, we have a separate node
		 *				for distincts
		 *  arg3: isInSortedOrder - is the source result set in sorted order
		 *  arg4: orderItem - entry in saved objects for the ordering
		 *  arg5: rowAllocator - method to construct rows for fetching
		 *			from the sort
		 *  arg6: row size
		 *  arg7: resultSetNumber
		 *  arg8: estimated row count
		 *  arg9: estimated cost
         *  arg10: explain plan
		 */

        acb.pushGetResultSetFactoryExpression(mb);

        child.generate(acb,mb);

        resultSetNumber=cc.getNextResultSetNumber();

        // is a distinct query
        mb.push(false);

        // not in sorted order
        mb.push(false);

        mb.push(orderItem);

        // row allocator
        child.getResultColumns().generateHolder(acb,mb);

        mb.push(child.getResultColumns().getTotalColumnSize());

        mb.push(resultSetNumber);

        mb.push(sortEstimate.rowCount());
        mb.push(sortEstimate.getEstimatedCost());
        mb.push(orderByNode == null ? "" : orderByNode.printExplainInformationForActivation());

        mb.callMethod(VMOpcode.INVOKEINTERFACE,null,"getSortResultSet",ClassName.NoPutResultSet,10);
    }

    @Override
    public int sortRequired(RowOrdering rowOrdering,OptimizableList optimizableList) throws StandardException{
        return sortRequired(rowOrdering,null,optimizableList);
    }

    @Override
    public int sortRequired(RowOrdering rowOrdering,
                            JBitSet tableMap,
                            OptimizableList optimizableList) throws StandardException{
		/*
		** Currently, all indexes are ordered ascending, so a descending
		** ORDER BY always requires a sort.
		*/
        if(alwaysSort){
            return RequiredRowOrdering.SORT_REQUIRED;
        }

		/*
		** Step through the columns in this list, and ask the
		** row ordering whether it is ordered on each column.
		*/
        int position=0;
        int size=size();
        for(int loc=0;loc<size;loc++) {
            OrderByColumn obc = getOrderByColumn(loc);

            // If the user specified NULLS FIRST or NULLS LAST in such a way
            // as to require NULL values to be re-sorted to be lower than
            // non-NULL values, then a sort is required, as the index holds
            // NULL values unconditionally higher than non-NULL values
            //
            if (obc.isNullsOrderedLow())
                return RequiredRowOrdering.SORT_REQUIRED;

            // ResultColumn rc = obc.getResultColumn();

			/*
			** This presumes that the OrderByColumn refers directly to
			** the base column, i.e. there is no intervening VirtualColumnNode.
			*/
            // ValueNode expr = obc.getNonRedundantExpression();
            ValueNode expr = obc.getResultColumn().getExpression();

            if (!(expr instanceof ColumnReference)) {
                return RequiredRowOrdering.SORT_REQUIRED;
            }

            ColumnReference cr = (ColumnReference) expr;

			/*
			** Check whether the table referred to is in the table map (if any).
			** If it isn't, we may have an ordering that does not require
			** sorting for the tables in a partial join order.  Look for
			** columns beyond this column to see whether a referenced table
			** is found - if so, sorting is required (for example, in a
			** case like ORDER BY S.A, T.B, S.C, sorting is required).
			*/
            int tableNumber = cr.getTableNumber();
            if (tableMap != null) {
                if (!tableMap.get(tableNumber)) {
					/* Table not in partial join order */
                    for (int remainingPosition = loc + 1;
                         remainingPosition < size();
                         remainingPosition++) {
                        OrderByColumn remainingobc = getOrderByColumn(loc);

                        ResultColumn remainingrc = remainingobc.getResultColumn();

                        ValueNode remainingexpr = remainingrc.getExpression();

                        if (remainingexpr instanceof ColumnReference) {
                            ColumnReference remainingcr = (ColumnReference) remainingexpr;
                            if (tableMap.get(remainingcr.getTableNumber())) {
                                return RequiredRowOrdering.SORT_REQUIRED;
                            }
                        }
                    }
                    return RequiredRowOrdering.NOTHING_REQUIRED;
                }
            }
			/*
			 * Does the order by columns match the sort order of the joins?  Big savings possible my friends...
			 *
             */

            int newPosition = rowOrdering.orderedOnColumn(obc.isAscending() ? RowOrdering.ASCENDING : RowOrdering.DESCENDING, position, tableNumber, cr.getColumnNumber());
            if (newPosition >= position)
                position = newPosition + 1;
            else
                return RequiredRowOrdering.SORT_REQUIRED;
        }
        return RequiredRowOrdering.NOTHING_REQUIRED;
    }

    @Override
    public void estimateCost(Optimizer optimizer,
                             RowOrdering rowOrdering,
                             CostEstimate baseCost,
                             CostEstimate sortCost) throws StandardException{
		/*
		** Do a bunch of set-up the first time: get the SortCostController,
		** the template row, the ColumnOrdering array, and the estimated
		** row size.
		*/
        if(scc==null){
            if(baseCost.isUninitialized()) return;
            if (optimizer == null) {
                double parallelCost = (baseCost.localCost()+baseCost.remoteCost())/baseCost.partitionCount();
                baseCost.setLocalCost(baseCost.localCost()+parallelCost);
                baseCost.setLocalCostPerPartition(baseCost.localCost());
                /* only local cost is changed, remote cost is not changed as sort does not change
                   the rowcount, so no need to change the remote cost nor remoteCostPerPartition
                 */
                // set sortCost
                if (sortCost != null) {
                    sortCost.setLocalCost(parallelCost);
                    sortCost.setLocalCostPerPartition(parallelCost);
                }
                return;
            } else {
                scc = optimizer.newSortCostController(this);
            }
        }
        scc.estimateSortCost(baseCost, sortCost);
    }

    @Override
    public boolean getSortNeeded(){
        return sortNeeded;
    }

    @Override
    public void sortNeeded(){
        sortNeeded=true;
    }

    @Override
    public void sortNotNeeded(){
        sortNeeded=false;
    }

    @Override
    public String toString(){
        StringBuilder buff=new StringBuilder();
        buff=buff.append("allAscending: ").append(allAscending).append("\n");
        buff=buff.append("alwaysSort: ").append(alwaysSort).append("\n");
        buff=buff.append("sortNeeded: ").append(sortNeeded).append("\n");
        buff=buff.append("columnOrdering: ").append("\n");
        if(columnOrdering!=null){
            for(int i=0;i<columnOrdering.length;i++){
                buff.append("[").append(i).append("] ").append(columnOrdering[i]).append("\n");
            }
        }
        buff=buff.append("\n").append(super.toString());

        return buff.toString();
    }

	/* RequiredRowOrdering interface */

    public int getResultSetNumber(){
        return resultSetNumber;
    }

    /**
      * @return {@code true} if the {@code ORDER BY} is attached to a
      * {@literal <table value constructor>}, i.e. a {@code VALUES} clause.
      */
    public boolean isTableValueCtorOrdering() {
        return isTableValueCtorOrdering;
    }

    public void setAlwaysSort(){
        alwaysSort=true;
    }

    /**
     * Are all columns in the list ascending.
     *
     * @return Whether or not all columns in the list ascending.
     */
    boolean allAscending(){
        return allAscending;
    }

    /**
     * Adjust addedColumnOffset values due to removal of a duplicate column
     * <p/>
     * This routine is called by bind processing when it identifies and
     * removes a column from the result column list which was pulled up due
     * to its presence in the ORDER BY clause, but which was later found to
     * be a duplicate. The OrderByColumn instance for the removed column
     * has been adjusted to point to the true column in the result column
     * list and its addedColumnOffset has been reset to -1. This routine
     * finds any other OrderByColumn instances which had an offset greater
     * than that of the column that has been deleted, and decrements their
     * addedColumOffset to account for the deleted column's removal.
     *
     * @param gap column which has been removed from the result column list
     */
    void closeGap(int gap){
        for(int index=0;index<size();index++){
            OrderByColumn obc=(OrderByColumn)elementAt(index);
            obc.collapseAddedColumnGap(gap);
        }
    }

    /**
     * Is this order by list an in order prefix of the specified RCL.
     * This is useful when deciding if an order by list can be eliminated
     * due to a sort from an underlying distinct or union.
     *
     * @param sourceRCL The source RCL.
     * @return Whether or not this order by list an in order prefix of the specified RCL.
     */
    boolean isInOrderPrefix(ResultColumnList sourceRCL){
        assert size()<=sourceRCL.size():"size()("+size()+") expected to be <= "+sourceRCL.size();

        int size=size();
        for(int index=0;index<size;index++){
            if(((OrderByColumn)elementAt(index)).getResultColumn()!=sourceRCL.elementAt(index)){
                return false;
            }
        }
        return true;
    }

    /**
     * Order by columns now point to the PRN above the node of interest.
     * We need them to point to the RCL under that one.  This is useful
     * when combining sorts where we need to reorder the sorting
     * columns.
     */
    void resetToSourceRCs(){
        int size=size();
        for(int index=0;index<size;index++){
            OrderByColumn obc=(OrderByColumn)elementAt(index);
            obc.resetToSourceRC();
        }
    }

    /**
     * Build a new RCL with the same RCs as the passed in RCL
     * but in an order that matches the ordering columns.
     *
     * @param resultColumns The RCL to reorder.
     * @throws StandardException Thrown on error
     */
    ResultColumnList reorderRCL(ResultColumnList resultColumns) throws StandardException{
        ResultColumnList newRCL=(ResultColumnList)getNodeFactory().getNode(C_NodeTypes.RESULT_COLUMN_LIST,getContextManager());

		/* The new RCL starts with the ordering columns */
        int size=size();
        for(int index=0;index<size;index++){
            OrderByColumn obc=(OrderByColumn)elementAt(index);
            newRCL.addElement(obc.getResultColumn());
            resultColumns.removeElement(obc.getResultColumn());
        }

		/* And ends with the non-ordering columns */
        newRCL.destructiveAppend(resultColumns);
        newRCL.resetVirtualColumnIds();
        newRCL.copyOrderBySelect(resultColumns);
        return newRCL;
    }

    /**
     * Remove any constant columns from this order by list.
     * Constant columns are ones where all of the column references
     * are equal to constant expressions according to the given
     * predicate list.
     */
    void removeConstantColumns(PredicateList whereClause){
		/* Walk the list backwards so we can remove elements safely */
        for(int loc=size()-1;
            loc>=0;
            loc--){
            OrderByColumn obc=(OrderByColumn)elementAt(loc);

            if(obc.constantColumn(whereClause)){
                removeElementAt(loc);
            }
        }
    }

    /**
     * Remove any duplicate columns from this order by list.
     * For example, one may "ORDER BY 1, 1, 2" can be reduced
     * to "ORDER BY 1, 2".
     * Beetle 5401.
     */
    void removeDupColumns(){
		/* Walk the list backwards so we can remove elements safely */
        for(int loc=size()-1;loc>0;loc--){
            OrderByColumn obc=(OrderByColumn)elementAt(loc);
            int colPosition=obc.getColumnPosition();

            for(int inner=0;inner<loc;inner++){
                OrderByColumn prev_obc=(OrderByColumn)elementAt(inner);
                if(colPosition==prev_obc.getColumnPosition()){
                    removeElementAt(loc);
                    break;
                }
            }
        }
    }

    /**
     * Remap all ColumnReferences in this tree to be clones of the
     * underlying expression.
     *
     * @throws StandardException Thrown on error
     */
    void remapColumnReferencesToExpressions() throws StandardException{
    }

    /**
     * Determine whether or not this RequiredRowOrdering has a
     * DESCENDING requirement for the column referenced by the
     * received ColumnReference.
     */
    boolean requiresDescending(ColumnReference cRef,int numOptimizables) throws StandardException{
        int size=size();

		/* Start by getting the table number and column position for
		 * the table to which the ColumnReference points.
		 */
        JBitSet tNum=new JBitSet(numOptimizables);
        BaseTableNumbersVisitor btnVis=new BaseTableNumbersVisitor(tNum);

        cRef.accept(btnVis);
        int crTableNumber=tNum.getFirstSetBit();
        int crColPosition=btnVis.getColumnNumber();

        assert crTableNumber>=0 || crColPosition>=0:"Failed to find table/column number for column "+cRef.getColumnName();
        assert tNum.hasSingleBitSet():"Expected ColumnReference '"+cRef.getColumnName()+"' to reference"+
                "exactly one table, but tables found were "+tNum;

		/* Walk through the various ORDER BY elements to see if
		 * any of them point to the same table and column that
		 * we found above.
		 */
        for(int loc=0;loc<size;loc++){
            OrderByColumn obc=getOrderByColumn(loc);
            ResultColumn rcOrderBy=obc.getResultColumn();

            btnVis.reset();
            rcOrderBy.accept(btnVis);
            int obTableNumber=tNum.getFirstSetBit();
            int obColPosition=btnVis.getColumnNumber();

			/* ORDER BY target should always have a table number and
			 * a column position.  It may not necessarily be a base
			 * table, but there should be some FromTable for which
			 * we have a ResultColumnList, and the ORDER BY should
			 * reference one of the columns in that list (otherwise
			 * we shouldn't have made it this far).
			 */
            assert tNum.hasSingleBitSet():"Expected ResultColumn '"+rcOrderBy.getColumnName()+"' to reference"+
                    "exactly one table";
            assert obColPosition>=0:"Failed to find orderBy column number for ORDER BY check on column '"+cRef.getColumnName()+"'.";

            if(crTableNumber!=obTableNumber)
                continue;

			/* They point to the same base table, so check the
			 * column positions.
			 */

            if(crColPosition==obColPosition){
				/* This ORDER BY element points to the same table
				 * and column as the received ColumnReference.  So
				 * return whether or not this ORDER BY element is
				 * descending.
				 */
                return !obc.isAscending();
            }
        }

		/* None of the ORDER BY elements referenced the same table
		 * and column as the received ColumnReference, so there
		 * is no descending requirement for the ColumnReference's
		 * source (at least not from this OrderByList).
		 */
        return false;
    }
}
