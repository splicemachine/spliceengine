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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.catalog.types.ReferencedColumnsDescriptorImpl;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.store.access.StaticCompiledOpenConglomInfo;
import java.util.Collection;
import java.util.Vector;

/**
 * This node type translates an index row to a base row.  It takes a
 * FromBaseTable as its source ResultSetNode, and generates an
 * IndexRowToBaseRowResultSet that takes a TableScanResultSet on an
 * index conglomerate as its source.
 */
public class IndexToBaseRowNode extends FromTable{
    protected FromBaseTable source;
    protected ConglomerateDescriptor baseCD;
    protected boolean cursorTargetTable;
    public PredicateList restrictionList;
    protected boolean forUpdate;
    private FormatableBitSet heapReferencedCols;
    private FormatableBitSet indexReferencedCols;
    private FormatableBitSet allReferencedCols;
    private FormatableBitSet heapOnlyReferencedCols;

    public void init(
            Object source,
            Object baseCD,
            Object resultColumns,
            Object cursorTargetTable,
            Object heapReferencedCols,
            Object indexReferencedCols,
            Object restrictionList,
            Object forUpdate,
            Object tableProperties){
        super.init(null,tableProperties);
        this.source=(FromBaseTable)source;
        this.baseCD=(ConglomerateDescriptor)baseCD;
        this.resultColumns=(ResultColumnList)resultColumns;
        this.cursorTargetTable=(Boolean)cursorTargetTable;
        this.restrictionList=(PredicateList)restrictionList;
        this.forUpdate=(Boolean)forUpdate;
        this.heapReferencedCols=(FormatableBitSet)heapReferencedCols;
        this.indexReferencedCols=(FormatableBitSet)indexReferencedCols;
        if(this.indexReferencedCols==null){
            this.allReferencedCols=this.heapReferencedCols;
            heapOnlyReferencedCols=this.heapReferencedCols;
        }else{
            this.allReferencedCols= new FormatableBitSet(this.heapReferencedCols);
            this.allReferencedCols.or(this.indexReferencedCols);
            heapOnlyReferencedCols= new FormatableBitSet(allReferencedCols);
            heapOnlyReferencedCols.xor(this.indexReferencedCols);
        }
    }

    @Override
    public boolean forUpdate(){
        return source.forUpdate();
    }

    @Override
    public AccessPath getTrulyTheBestAccessPath(){
        // Get AccessPath comes from base table.
        return source.getTrulyTheBestAccessPath();
    }

    @Override
    public CostEstimate getCostEstimate(){
        return source.getTrulyTheBestAccessPath().getCostEstimate();
    }

    @Override
    public CostEstimate getFinalCostEstimate(){
        return source.getFinalCostEstimate();
    }

    /**
     * Return whether or not the underlying ResultSet tree
     * is ordered on the specified columns.
     * RESOLVE - This method currently only considers the outermost table
     * of the query block.
     *
     * @throws StandardException Thrown on error
     * @param    crs                    The specified ColumnReference[]
     * @param    permuteOrdering        Whether or not the order of the CRs in the array can be permuted
     * @param    fbtVector            Vector that is to be filled with the FromBaseTable
     * @return Whether the underlying ResultSet tree
     * is ordered on the specified column.
     */
    @Override
    boolean isOrderedOn(ColumnReference[] crs,boolean permuteOrdering,Vector fbtVector) throws StandardException{
        return source.isOrderedOn(crs,permuteOrdering,fbtVector);
    }

    /**
     * Generation of an IndexToBaseRowNode creates an
     * IndexRowToBaseRowResultSet, which uses the RowLocation in the last
     * column of an index row to get the row from the base conglomerate (heap).
     *
     * @param acb The ActivationClassBuilder for the class being built
     * @param mb  the method  for the method to be built
     * @throws StandardException Thrown on error
     */
    @Override
    public void generate(ActivationClassBuilder acb, MethodBuilder mb) throws StandardException{
        ValueNode restriction=null;

		/*
        ** Get the next ResultSet #, so that we can number this ResultSetNode,
		** its ResultColumnList and ResultSet.
		*/
        assignResultSetNumber();

        // Get the CostEstimate info for the underlying scan
        costEstimate=getFinalCostEstimate();

		/* Put the predicates back into the tree */
        if(restrictionList!=null){
            restriction=restrictionList.restorePredicates();
			/* Allow the restrictionList to get garbage collected now
			 * that we're done with it.
			 */
            restrictionList=null;
        }

        // for the restriction, we generate an exprFun
        // that evaluates the expression of the clause
        // against the current row of the child's result.
        // if the restriction is empty, simply pass null
        // to optimize for run time performance.

        // generate the function and initializer:
        // Note: Boolean lets us return nulls (boolean would not)
        // private Boolean exprN()
        // {
        //   return <<restriction.generate(ps)>>;
        // }
        // static Method exprN = method pointer to exprN;


        int heapColRefItem=-1;
        if(heapReferencedCols!=null){
            heapColRefItem=acb.addItem(heapReferencedCols);
        }
        int allColRefItem=-1;
        if(allReferencedCols!=null){
            allColRefItem=acb.addItem(allReferencedCols);
        }
        int heapOnlyColRefItem=-1;
        if(heapOnlyReferencedCols!=null){
            heapOnlyColRefItem=acb.addItem(heapOnlyReferencedCols);
        }

		/* Create the ReferencedColumnsDescriptorImpl which tells which columns
		 * come from the index.
		 */
        int indexColMapItem=acb.addItem(new ReferencedColumnsDescriptorImpl(getIndexColMapping()));
        long heapConglomNumber=baseCD.getConglomerateNumber();
        StaticCompiledOpenConglomInfo scoci=getLanguageConnectionContext()
                .getTransactionCompile().getStaticCompiledConglomInfo(heapConglomNumber);

        acb.pushGetResultSetFactoryExpression(mb);

        mb.push(heapConglomNumber);
        mb.push(acb.addItem(scoci));
        source.generate(acb,mb);

        mb.upCast(ClassName.NoPutResultSet);

        resultColumns.generateHolder(acb,mb,heapReferencedCols,indexReferencedCols);
        mb.push(resultSetNumber);
        mb.push(source.getBaseTableName());
        mb.push(heapColRefItem);

        mb.push(allColRefItem);
        mb.push(heapOnlyColRefItem);

        mb.push(indexColMapItem);

        // if there is no restriction, we just want to pass null.
        if(restriction==null){
            mb.pushNull(ClassName.GeneratedMethod);
        }else{
            // this sets up the method and the static field.
            // generates:
            // 	Object userExprFun { }
            MethodBuilder userExprFun=acb.newUserExprFun();

            // restriction knows it is returning its value;
	
			/* generates:
			 *    return <restriction.generate(acb)>;
			 * and adds it to userExprFun
			 * NOTE: The explicit cast to DataValueDescriptor is required
			 * since the restriction may simply be a boolean column or subquery
			 * which returns a boolean.  For example:
			 *		where booleanColumn
			 */
            restriction.generate(acb,userExprFun);
            userExprFun.methodReturn();

            // we are done modifying userExprFun, complete it.
            userExprFun.complete();

            // restriction is used in the final result set as an access of the new static
            // field holding a reference to this new method.
            // generates:
            //	ActivationClass.userExprFun
            // which is the static field that "points" to the userExprFun
            // that evaluates the where clause.
            acb.pushMethodReference(mb,userExprFun);
        }

        mb.push(forUpdate);
        mb.push(costEstimate.rowCount());
        mb.push(costEstimate.getEstimatedCost());
        mb.push(source.getTableDescriptor().getVersion());
        mb.push(printExplainInformationForActivation());

        mb.callMethod(VMOpcode.INVOKEINTERFACE,null,"getIndexRowToBaseRowResultSet", ClassName.NoPutResultSet,16);

		/* The IndexRowToBaseRowResultSet generator is what we return */

		/*
		** Remember if this result set is the cursor target table, so we
		** can know which table to use when doing positioned update and delete.
		*/
        if(cursorTargetTable){
            acb.rememberCursorTarget(mb);
        }
    }

    /**
     * Return whether or not the underlying ResultSet tree will return
     * a single row, at most.
     * This is important for join nodes where we can save the extra next
     * on the right side if we know that it will return at most 1 row.
     *
     * @return Whether or not the underlying ResultSet tree will return a single row.
     * @throws StandardException Thrown on error
     */
    @Override
    public boolean isOneRowResultSet() throws StandardException{
        // Default is false
        return source.isOneRowResultSet();
    }

    public FromBaseTable getSource(){
        return source;
    }

    public ConglomerateDescriptor getBaseConglomerateDescriptor(){
        return baseCD;
    }

    /**
     * Return whether or not the underlying FBT is for NOT EXISTS.
     *
     * @return Whether or not the underlying FBT is for NOT EXISTS.
     */
    @Override
    public boolean isNotExists(){
        return source.isNotExists();
    }

    /**
     * Decrement (query block) level (0-based) for this FromTable.
     * This is useful when flattening a subquery.
     *
     * @param decrement The amount to decrement by.
     */
    @Override
    void decrementLevel(int decrement){
        source.decrementLevel(decrement);
    }

    /**
     * Get the lock mode for the target of an update statement
     * (a delete or update).  The update mode will always be row for
     * CurrentOfNodes.  It will be table if there is no where clause.
     *
     * @return The lock mode
     */
    @Override
    public int updateTargetLockMode(){
        return source.updateTargetLockMode();
    }

    @Override
    void adjustForSortElimination(){
		/* NOTE: We use a different method to tell a FBT that
		 * it cannot do a bulk fetch as the ordering issues are
		 * specific to a FBT being under an IRTBR as opposed to a
		 * FBT being under a PRN, etc.
		 */
        source.disableBulkFetch();
    }

    @Override
    void adjustForSortElimination(RequiredRowOrdering rowOrdering) throws StandardException{
		/* rowOrdering is not important to this specific node, so
		 * just call the no-arg version of the method.
		 */
        adjustForSortElimination();

		/* Now pass the rowOrdering down to source, which may
		 * need to do additional work. DERBY-3279.
		 */
        source.adjustForSortElimination(rowOrdering);
    }

    /**
     * Fill in the column mapping for those columns coming from the index.
     *
     * @return The int[] with the mapping.
     */
    private int[] getIndexColMapping(){
        int rclSize=resultColumns.size();
        int[] indexColMapping=new int[rclSize];

        for(int index=0;index<rclSize;index++){
            ResultColumn rc=(ResultColumn)resultColumns.elementAt(index);
            if(indexReferencedCols!=null && rc.getExpression() instanceof VirtualColumnNode){
                // Column is coming from index
                VirtualColumnNode vcn=(VirtualColumnNode)rc.getExpression();
                indexColMapping[index]=
                        vcn.getSourceColumn().getVirtualColumnId()-1;
            }else{
                // Column is not coming from index
                indexColMapping[index]=-1;
            }
        }

        return indexColMapping;
    }

    /**
     * Accept the visitor for all visitable children of this node.
     *
     * @param v the visitor
     */
    @Override
    public void acceptChildren(Visitor v) throws StandardException{
        super.acceptChildren(v);
        if(source!=null){
            source=(FromBaseTable)source.accept(v, this);
        }
    }
    @Override
    public void buildTree(Collection<QueryTreeNode> tree, int depth) throws StandardException{
        setDepth(depth);
        tree.add(this);
        source.buildTree(tree,depth+1);
    }

    @Override
    public String printExplainInformation(String attrDelim, int order) throws StandardException {
        StringBuilder sb = new StringBuilder();
        sb.append(spaceToLevel())
                .append("IndexLookup").append("(")
                .append("n=").append(order)
                .append(attrDelim).append(getFinalCostEstimate().prettyIndexLookupString(attrDelim));
        sb.append(")");
        return sb.toString();
    }

}
