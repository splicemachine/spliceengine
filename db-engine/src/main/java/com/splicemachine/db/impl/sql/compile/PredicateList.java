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

import com.splicemachine.db.catalog.IndexDescriptor;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.LocalField;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.io.FormatableArrayHolder;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.store.access.ScanController;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.iapi.util.JBitSet;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.splicemachine.db.iapi.types.Orderable.*;

/**
 * A PredicateList represents the list of top level predicates.
 * Each top level predicate consists of an AndNode whose leftOperand is the
 * top level predicate and whose rightOperand is true.  It extends
 * QueryTreeNodeVector.
 */

public class PredicateList extends QueryTreeNodeVector<Predicate> implements OptimizablePredicateList{
    private int numberOfStartPredicates;
    private int numberOfStopPredicates;
    private int numberOfQualifiers;

    public PredicateList(){
    }

	/*
     * OptimizableList interface
	 */

    @Override
    public OptimizablePredicate getOptPredicate(int index){
        return elementAt(index);
    }

    @Override
    public final void removeOptPredicate(int predCtr) throws StandardException{
        Predicate predicate=remove(predCtr);

        if(predicate.isStartKey())
            numberOfStartPredicates--;
        if(predicate.isStopKey())
            numberOfStopPredicates--;
        if(predicate.isQualifier())
            numberOfQualifiers--;
    }

    /**
     * Another version of removeOptPredicate that takes the Predicate to be
     * removed, rather than the position of the Predicate.  This is not part
     * any interface (yet).
     */
    public final void removeOptPredicate(OptimizablePredicate pred){
        removeElement((Predicate)pred);

        if(pred.isStartKey())
            numberOfStartPredicates--;
        if(pred.isStopKey())
            numberOfStopPredicates--;
        if(pred.isQualifier())
            numberOfQualifiers--;
    }

    @Override
    public void addOptPredicate(OptimizablePredicate optPredicate){
        addElement((Predicate)optPredicate);

        if(optPredicate.isStartKey())
            numberOfStartPredicates++;
        if(optPredicate.isStopKey())
            numberOfStopPredicates++;
        if(optPredicate.isQualifier())
            numberOfQualifiers++;
    }

    /**
     * Another flavor of addOptPredicate that inserts the given predicate
     * at a given position.  This is not yet part of any interface.
     */
    public void addOptPredicate(OptimizablePredicate optPredicate,int position){
        insertElementAt((Predicate)optPredicate,position);

        if(optPredicate.isStartKey())
            numberOfStartPredicates++;
        if(optPredicate.isStopKey())
            numberOfStopPredicates++;
        if(optPredicate.isQualifier())
            numberOfQualifiers++;
    }

    @Override
    public boolean useful(Optimizable optTable,ConglomerateDescriptor cd) throws StandardException{
        boolean retval=false;

		/*
		** Most of this assumes BTREE,
		** so should move into a configurable module
		*/

		/* If the conglomerate isn't an index, the predicate isn't useful */
        if(!cd.isIndex())
            return false;

		/*
		** A PredicateList is useful for a BTREE if it contains a relational
		** operator directly below a top-level AND comparing the first column
		** in the index to an expression that does not contain a reference
		** to the table in question.  Let's look for that.
		*/
        int size=size();
        for(int index=0;index<size;index++){
            Predicate pred=elementAt(index);
            RelationalOperator relop=pred.getRelop();

			/* InListOperatorNodes, while not relational operators, may still
			 * be useful.  There are two cases: a) we transformed the IN-list
			 * into a probe predicate of the form "col = ?", which can then be
			 * optimized/generated as a start/stop key and used for "multi-
			 * probing" at execution; or b) we did *not* transform the IN-list,
			 * in which case we'll generate _dynamic_ start and stop keys in
			 * order to improve scan performance (beetle 3858).  In either case
			 * the IN-list may still prove "useful".
			 */
            InListOperatorNode inNode=pred.getSourceInList();
            boolean isIn=(inNode!=null);

			/* If it's not a relational operator and it's not "in", then it's
			 * not useful.
			 */
            if(!isIn && (relop==null))
                continue;

			/*
			** If the relational operator is neither a useful start key
			** nor a useful stop key for this table, it is not useful
			** for limiting an index scan.
			*/
            if((!isIn) && (!relop.usefulStartKey(optTable)) && (!relop.usefulStopKey(optTable))){
                continue;
            }

			/*
			** Look for a the first column of the index on one side of the
			** relop.  If it's not found, this Predicate is not optimizable.
			*/
            ColumnReference indexCol=null;

            if(isIn){
                if(inNode.getLeftOperand() instanceof ColumnReference){
                    indexCol=(ColumnReference)inNode.getLeftOperand();
                    if(indexCol.getColumnNumber()!=cd.getIndexDescriptor().baseColumnPositions()[0]){
                        indexCol=null;
                    }
                }
            }else{
                indexCol=relop.getColumnOperand(
                        optTable,
                        cd.getIndexDescriptor().baseColumnPositions()[0]);
            }

            if(indexCol==null){
                continue;
            }

			/*
			** Look at the expression that the index column is compared to.
			** If it contains columns from the table in question, the
			** Predicate is not optimizable.
			*/
            if((isIn && inNode.selfReference(indexCol)) || (!isIn && relop.selfComparison(indexCol))){
                continue;
            }
			
			/* The Predicate is optimizable */
            retval=true;
            break;
        }

        return retval;
    }

    @Override
    public void pushUsefulPredicates(Optimizable optTable) throws StandardException{
        AccessPath ap=optTable.getTrulyTheBestAccessPath();

        orderUsefulPredicates(optTable,
                ap.getConglomerateDescriptor(),
                true,
                ap.getNonMatchingIndexScan(),
                ap.getCoveringIndexScan(),
                true);
    }

    @Override
    public void classify(Optimizable optTable,ConglomerateDescriptor cd, boolean considerJoinPredicateAsKey) throws StandardException{
		/*
		** Don't push the predicates - at this point, we are only determining
		** which predicates are useful.  Also, we don't know yet whether
		** we have a non-matching index scan or a covering index scan -
		** this method call will help determine that.  So, let's say they're
		** false for now.
		*/
        orderUsefulPredicates(optTable,cd,false,false,false, considerJoinPredicateAsKey);
    }

    @Override
    public void markAllPredicatesQualifiers(){
        int size=size();
        for(int index=0;index<size;index++){
            elementAt(index).markQualifier();
        }

        numberOfQualifiers=size;
    }

    @Override
    public int hasEqualityPredicateOnOrderedColumn(Optimizable optTable,
                                                   int columnNumber,
                                                   boolean isNullOkay) throws StandardException{
        ValueNode opNode;
        int size=size();
        for(int index=0;index<size;index++){
            AndNode andNode;
            Predicate predicate;
            predicate=elementAt(index);
            //We are not looking at constant comparison predicate.
            if(predicate.getReferencedMap().hasSingleBitSet()){
                continue;
            }

            andNode=predicate.getAndNode();

            // skip non-equality predicates
            opNode=andNode.getLeftOperand();

            if(opNode.optimizableEqualityNode(optTable,columnNumber,isNullOkay)){
                return index;
            }
        }

        return -1;
    }

    @Override
    public boolean hasOptimizableEqualityPredicate(Optimizable optTable,
                                                   int columnNumber,
                                                   boolean isNullOkay) throws StandardException{
        return getOptimizableEqualityPredicate(optTable,columnNumber,isNullOkay)!=null;
    }

    @Override
    public OptimizablePredicate getOptimizableEqualityPredicate(Optimizable optTable,int columnNumber,boolean isNullOkay) throws StandardException{
        int size=size();
        for(int index=0;index<size;index++){
            AndNode andNode;
            Predicate predicate;
            predicate=elementAt(index);

            andNode=predicate.getAndNode();

            // skip non-equality predicates
            ValueNode opNode=andNode.getLeftOperand();

            if(opNode.optimizableEqualityNode(optTable,columnNumber,isNullOkay)){
                return predicate;
            }
        }

        return null;
    }

    public List<Predicate> getOptimizableEqualityPredicateList(Optimizable optTable,int columnNumber,boolean isNullOkay) throws StandardException{
        int size=size();
        List<Predicate> predicateList = null;
        for(int index=0;index<size;index++){
            AndNode andNode;
            Predicate predicate;
            predicate=elementAt(index);

            andNode=predicate.getAndNode();

            // skip non-equality predicates
            ValueNode opNode=andNode.getLeftOperand();

            if(opNode.optimizableEqualityNode(optTable,columnNumber,isNullOkay)){
                if (predicateList == null) {
                    predicateList = new ArrayList<>();
                }
                predicateList.add(predicate);
            }
        }

        return predicateList;
    }

    @Override
    public boolean hasOptimizableEquijoin(Optimizable optTable,int columnNumber) throws StandardException{
        int size=size();
        for(int index=0;index<size;index++){
            AndNode andNode;
            Predicate predicate=elementAt(index);

            // This method is used by HashJoinStrategy to determine if
            // there are any equality predicates that can be used to
            // perform a hash join (see the findHashKeyColumns()
            // method in HashJoinStrategy.java).  That said, if the
            // predicate was scoped and pushed down from an outer query,
            // then it's no longer possible to perform the hash join
            // because one of the operands is in an outer query and
            // the other (scoped) operand is down in a subquery. Thus
            // we skip this predicate if it has been scoped.
            if(predicate.isScopedForPush()){
                continue;
            }

            andNode=predicate.getAndNode();

            ValueNode leftNode=andNode.getLeftOperand();

            if(!leftNode.optimizableEqualityNode(optTable,columnNumber,false)){
                continue;
            }

			/*
			** Skip comparisons that are not qualifiers for the table
			** in question.
			*/
            if (!(leftNode instanceof BinaryRelationalOperatorNode)) {
                continue;
            }
            if(!((BinaryRelationalOperatorNode)leftNode).isQualifierForHashableJoin(optTable,false)){
                continue;
            }

			/*
			** Skip non-join comparisons.
			*/
            if(predicate.getReferencedMap().hasSingleBitSet()){
                continue;
            }

            // We found a match
            return true;
        }

        return false;
    }

    @Override
    public void putOptimizableEqualityPredicateFirst(Optimizable optTable,int columnNumber) throws StandardException{
        int size=size();
        for(int index=0;index<size;index++){
            Predicate predicate=elementAt(index);

            AndNode andNode=predicate.getAndNode();

            // skip non-equality predicates
            ValueNode opNode=andNode.getLeftOperand();

            if(!opNode.optimizableEqualityNode(optTable,columnNumber,false))
                continue;

            // We found a match - make this entry first in the list
            if(index!=0){
                removeElementAt(index);
                insertElementAt(predicate, 0);
            }

            return;
        }

		/* We should never get here since this method only called when we
		 * know that the desired equality predicate exists.
		 */
        if(SanityManager.DEBUG){
            SanityManager.THROWASSERT("Could  not find the expected equality predicate on column #"+columnNumber);
        }
    }

    @SuppressWarnings("ConstantConditions")
    public static boolean isQualifier(Predicate pred,Optimizable optTable,boolean pushPreds) throws StandardException{
		/*
		** Skip over it if it's not a relational operator (this includes
		** BinaryComparisonOperators and IsNullNodes.
		*/
        if(!pred.isRelationalOpPredicate()){
            // possible OR clause, check for it.
            if(!pred.isPushableOrClause(optTable)){
                /* NOT an OR or AND, so go on to next predicate.
                 *
                 * Note: if "pred" (or any predicates in the tree
                 * beneath "pred") is an IN-list probe predicate
                 * then we'll "revert" it to its original form
                 * (i.e. to the source InListOperatorNode from
                 * which it originated) as part of code generation.
                 * See generateExpression() in BinaryOperatorNode.
                 */
                return false;
            }
        }else if(!pred.getRelop().isQualifier(optTable,pushPreds)){
            // NOT a qualifier, go on to next predicate.
            return false;
        }
        return true;
    }

    private static boolean isQualifierForHashableJoin(Predicate pred,Optimizable optTable,boolean pushPreds) throws StandardException{
		/*
		** Skip over it if it's not a relational operator (this includes
		** BinaryComparisonOperators and IsNullNodes.
		*/
        if(!pred.isRelationalOpPredicate()) {
            // possible OR clause, check for it.
            if (!pred.isPushableOrClause(optTable)) {
                /* NOT an OR or AND, so go on to next predicate.
                 *
                 * Note: if "pred" (or any predicates in the tree
                 * beneath "pred") is an IN-list probe predicate
                 * then we'll "revert" it to its original form
                 * (i.e. to the source InListOperatorNode from
                 * which it originated) as part of code generation.
                 * See generateExpression() in BinaryOperatorNode.
                 */
                return false;
            }
        } else if (!(pred.getRelop() instanceof BinaryRelationalOperatorNode)) {
            return false;
        }else if(!((BinaryRelationalOperatorNode) pred.getRelop()).isQualifierForHashableJoin(optTable,pushPreds)){
            // NOT a qualifier, go on to next predicate.
            return false;
        }
        return true;
    }

    public static Integer isIndexUseful(Predicate pred,
                                        Optimizable optTable,
                                        boolean pushPreds,
                                        boolean skipProbePreds,
                                        int[] baseColumnPositions) throws StandardException{
        ColumnReference indexCol=null;
        int indexPosition = -1;
        RelationalOperator relop=pred.getRelop();
		
		/* InListOperatorNodes, while not relational operators, may still
		 * be useful.  There are two cases: a) we transformed the IN-list
		 * into a probe predicate of the form "col = ?", which can then be
		 * optimized/generated as a start/stop key and used for "multi-
		 * probing" at execution; or b) we did *not* transform the IN-list,
		 * in which case we'll generate _dynamic_ start and stop keys in
		 * order to improve scan performance (beetle 3858).  In either case
		 * the IN-list may still prove "useful".
		 */
        InListOperatorNode inNode=pred.getSourceInList();
        boolean isIn=(inNode!=null);

		/* If it's not an "in" operator and either a) it's not a relational
		 * operator or b) it's not a qualifier, then it's not useful for
		 * limiting the scan, so skip it.
		 */
        if(!isIn && ((relop==null) || !relop.isQualifier(optTable,pushPreds))){
            return null;
        }

		/* Skip it if we're doing a hash join and it's a probe predicate.
		 * Then, since the probe predicate is deemed not useful, it will
		 * be implicitly "reverted" to its underlying IN-list as part of
		 * code generation.
		 */
        if(skipProbePreds && pred.isInListProbePredicate())
            return null;

		/* Look for an index column on one side of the relop */
        if (baseColumnPositions != null) {
            for (indexPosition = 0; indexPosition < baseColumnPositions.length; indexPosition++) {
                if (isIn) {
                    if (inNode.getLeftOperand() instanceof ColumnReference) {
                        indexCol = (ColumnReference) inNode.getLeftOperand();
                        if ((optTable.getTableNumber() != indexCol.getTableNumber())
                                || (indexCol.getColumnNumber() != baseColumnPositions[indexPosition])
                                || inNode.selfReference(indexCol))
                            indexCol = null;
                    }
                } else {
                    indexCol = relop.getColumnOperand(optTable, baseColumnPositions[indexPosition]);
                }
                if (indexCol != null)
                    break;
            }
        }
		/*
		** Skip over it if there is no index column on one side of the
		** operand.
		*/
        if(indexCol==null){
			/* If we're pushing predicates then this is the last time
			 * we'll get here before code generation.  So if we have
			 * any IN-list probe predicates that are not useful, we'll
			 * need to "revert" them back to their original IN-list
			 * form so that they can be generated as regular IN-list
			 * restrictions.  That "revert" operation happens in
			 * the generateExpression() method of BinaryOperatorNode.
			 */
            if (relop instanceof BinaryRelationalOperatorNode) {
                BinaryRelationalOperatorNode brelop = (BinaryRelationalOperatorNode)relop;
                if (brelop.hasRowId()) {
                    pred.markRowId();
                    return -1;
                }
            }
            return null;
        }
        return indexPosition;
    }


    public boolean isRowIdScan() {
        boolean ret = false;
        for (int i = 0; i < size(); ++i) {
            Predicate p = elementAt(i);
            RelationalOperator relop = p.getRelop();
            if (relop instanceof BinaryRelationalOperatorNode) {
                BinaryRelationalOperatorNode brelop = (BinaryRelationalOperatorNode)relop;
                if (brelop.hasRowId()) {
                    ret = true;
                    break;
                }
            }
        }
        return ret;
    }

    private Predicate[] checkRowIdPredicate(Predicate[] predicates) {
        if (predicates == null || predicates.length == 0)
            return predicates;

        List<Predicate> predicateList = new ArrayList<>();
        for (Predicate p : predicates) {
            RelationalOperator relop = p.getRelop();
            if (relop instanceof BinaryRelationalOperatorNode) {
                BinaryRelationalOperatorNode brelop = (BinaryRelationalOperatorNode)relop;
                if (brelop.hasRowId()) {
                    predicateList.add(p);
                }
            }
        }

        if (!predicateList.isEmpty()) {
            predicates = predicateList.toArray(new Predicate[predicateList.size()]);
        }

        return predicates;
    }
    
    private boolean
    genListValueNodeOrRecurse(Optimizable optTable, ValueNode constantToAdd, ValueNodeList constList,
                                 ValueNodeList groupedConstants, ArrayList<Predicate> predList, int level)
                            throws StandardException {
        boolean retval = true;
        ValueNodeList localConstList =
         (ValueNodeList) getNodeFactory().getNode(
            C_NodeTypes.VALUE_NODE_LIST,
            getContextManager());
        if (level != 0) {
            localConstList.nondestructiveAppend(constList);
        }
        localConstList.addValueNode(constantToAdd);
        if (level == predList.size() - 1) {
            // We've grabbed a ConstantNode (or other type of ValueNode) from each level.
            // Time to materialize this combination into a new
            // ListValueNode.
            ValueNode lcn = (ListValueNode) getNodeFactory().getNode(
                C_NodeTypes.LIST_VALUE_NODE,
                localConstList,
                getContextManager());
            groupedConstants.addValueNode(lcn);
            
        } else {
            retval =
                addConstantsToList(optTable, localConstList, groupedConstants,
                    predList, level + 1);
        }
        return retval;
    }
    
    private boolean addConstantsToList(Optimizable optTable, ValueNodeList constList, ValueNodeList groupedConstants,
                                       ArrayList<Predicate> predList, int level)
                            throws StandardException {
        
        Predicate pred = predList.get(level);
        InListOperatorNode inNode = pred.getSourceInList();

        boolean retval = true;
        
        if (inNode == null) {
            RelationalOperator relop = pred.getRelop();
            if (!(relop instanceof BinaryRelationalOperatorNode))
                return false;
            
            // Fold an equality predicate into the IN list.
            BinaryRelationalOperatorNode brop = (BinaryRelationalOperatorNode)relop;
            ValueNode constant = null;
            if (brop.keyColumnOnLeft(optTable))
                constant = (ValueNode) brop.getRightOperand();
            else
                constant = (ValueNode) brop.getLeftOperand();
            retval =
                genListValueNodeOrRecurse(optTable, constant,
                    constList, groupedConstants,
                    predList, level);
        }
        else {
            for (Object constant : inNode.rightOperandList) {
                retval =
                    genListValueNodeOrRecurse(optTable, (ValueNode) constant,
                        constList, groupedConstants,
                        predList, level);
                if (retval == false)
                    break;
            }
        }
        return retval;
    }

    private void orderUsefulPredicates(Optimizable optTable,
                                       ConglomerateDescriptor cd,
                                       boolean pushPreds,
                                       boolean nonMatchingIndexScan,
                                       boolean coveringIndexScan,
                                       boolean considerJoinPredicateAsKey) throws StandardException{
        boolean primaryKey=false;
        int[] baseColumnPositions=null;
        boolean[] isAscending=null;
        int size=size();
        Predicate[] usefulPredicates=new Predicate[size];
        int usefulCount=0;
        Predicate predicate;
        boolean rowIdScan;

        if(cd!=null && !cd.isIndex() && !cd.isConstraint()){
            List<ConglomerateDescriptor> cdl=optTable.getTableDescriptor().getConglomerateDescriptorList();
            for(ConglomerateDescriptor primaryKeyCheck : cdl){
                IndexDescriptor indexDec=primaryKeyCheck.getIndexDescriptor();
                if(indexDec!=null){
                    String indexType=indexDec.indexType();
                    if(indexType!=null && indexType.contains("PRIMARY")){
                        primaryKey=true;
                        baseColumnPositions=indexDec.baseColumnPositions();
                        isAscending=indexDec.isAscending();
                    }
                }
            }
        }

        rowIdScan = isRowIdScan();

		/*
		** Clear all the scan flags for this predicate list, so that the
		** flags that get set are only for the given conglomerate.
		*/
        for(int index=0;index<size;index++){
            predicate=elementAt(index);
            predicate.clearScanFlags();
        }

        JoinStrategy joinStrategy = optTable.getTrulyTheBestAccessPath().getJoinStrategy();
        if (joinStrategy == null) {
            joinStrategy = optTable.getCurrentAccessPath().getJoinStrategy();
        }
        boolean isHashableJoin = joinStrategy instanceof HashableJoinStrategy;

		/*
		** RESOLVE: For now, not pushing any predicates for heaps.  When this
		** changes, we also need to make the scan in
		** TableScanResultSet.getCurrentRow() check the qualifiers to see
		** if the row still qualifies (there is a new method in ScanController
		** for this.
		*/

		/* Is a heap scan or a non-matching index scan on a covering index? */
        if(!rowIdScan && ((cd==null) || (!cd.isIndex() && !primaryKey) || (nonMatchingIndexScan && coveringIndexScan))){

			/*
			** For the heap, the useful predicates are the relational
			** operators that have a column from the table on one side,
			** and an expression that doesn't have a column from that
			** table on the other side.
			**
			** For the heap, all useful predicates become Qualifiers, so
			** they don't have to be in any order.
			**
			** NOTE: We can logically delete the current element when
			** traversing the Vector in the next loop,
			** so we must build an array of elements to
			** delete while looping and then delete them
			** in reverse order after completing the loop.
			*/
            Predicate[] preds=new Predicate[size];
            for(int index=0;index<size;index++){
                Predicate pred=elementAt(index);
                if(isQualifier(pred,optTable,pushPreds) ||
                        (isHashableJoin && isQualifierForHashableJoin(pred, optTable, pushPreds))
                        ) {
                    pred.markQualifier();
                    if(SanityManager.DEBUG){
                        if(pred.isInListProbePredicate()){
                            SanityManager.THROWASSERT("Found an IN-list probe "+
                                    "predicate ("+pred.binaryRelOpColRefsToString()+
                                    ") that was marked as a qualifier, which should "+
                                    "not happen.");
                        }
                    }
                    if(pushPreds){
                        // Push the predicate down. (Just record for now.)
                        if(optTable.pushOptPredicate(pred)){
                            preds[index]=pred;
                        }
                    }
                }
            }

			      /* Now we actually push the predicates down */
            for(int inner=size-1;inner>=0;inner--){
                if(preds[inner]!=null)
                    removeOptPredicate(preds[inner]);
            }
            return;
        }
        if(!primaryKey && !rowIdScan){
            baseColumnPositions=cd.getIndexDescriptor().baseColumnPositions();
            isAscending=cd.getIndexDescriptor().isAscending();
        }
		/* If we have a "useful" IN list probe predicate we will generate a
		 * start/stop key for optTable of the form "col = <val>", where <val>
		 * is the first value in the IN-list.  Then during normal index multi-
		 * probing (esp. as implemented by exec/MultiProbeTableScanResultSet)
		 * we will use that start/stop key as a "placeholder" into which we'll
		 * plug the values from the IN-list one at a time.
		 *
		 * That said, if we're planning to do a hash join with optTable then
		 * we won't generate a MultiProbeTableScanResult; instead we'll
		 * generate a HashScanResultSet, which does not (yet) account for
		 * IN-list multi-probing.  That means the start/stop key "col = <val>"
		 * would be treated as a regular restriction, which could lead to
		 * incorrect results.  So if we're dealing with a hash join, we do
		 * not consider IN-list probe predicates to be "useful". DERBY-2500.
		 *
		 * Note that it should be possible to enhance HashScanResultSet to
		 * correctly perform index multi-probing at some point, and there
		 * would indeed be benefits to doing so (namely, we would scan fewer
		 * rows from disk and build a smaller hash table). But until that
		 * happens we have to make sure we do not consider probe predicates
		 * to be "useful" for hash joins.
		 *
		 * Only need to do this check if "pushPreds" is true, i.e. if we're
		 * modifying access paths and thus we know for sure that we are going
		 * to generate a hash join.
		 */
        boolean skipProbePreds=pushPreds && optTable.getTrulyTheBestAccessPath().getJoinStrategy().isHashJoin();
        boolean[] isEquality =
            new boolean[baseColumnPositions != null ?
                        (size > baseColumnPositions.length ? size : baseColumnPositions.length) :size];
		/*
		** Create an array of useful predicates.  Also, count how many
		** useful predicates there are.
		*/
		Integer inlistPosition = -2;

        boolean multiColumnMultiProbeEnabled =
            (optTable instanceof FromBaseTable &&
            !((FromBaseTable) optTable).isSpark(((FromBaseTable) optTable).getDataSetProcessorType())) ||
                getCompilerContext().getMulticolumnInlistProbeOnSparkEnabled();

        TreeMap<Integer, Predicate> inlistPreds = new TreeMap<>();
        List<Predicate> predicates=new ArrayList<>();
        for(int index=0;index<size;index++){
            Predicate pred=elementAt(index);
            Integer position=isIndexUseful(pred,optTable,pushPreds,skipProbePreds,baseColumnPositions);
            if(position!=null){
                if (pred.isInListProbePredicate()) {
                    inlistPreds.put(position, pred);
                    if (position >= 0 && multiColumnMultiProbeEnabled)
                        isEquality[position] = true;
                    // we keep track of the inlist at highest index position excluding rowid whose position is -1
                    // if MultiProbeScan on multiple columns is enabled, otherwise we keep track
                    // of the lowest index position.
                    if (inlistPosition == -2 ||
                        (multiColumnMultiProbeEnabled && inlistPosition < position) ||
                        (!multiColumnMultiProbeEnabled && inlistPosition > position)) {
                        inlistPosition = position;
                    }
                } else {
                    pred.setIndexPosition(position);
                /* Remember the useful predicate */
                    usefulPredicates[usefulCount++] = pred;
                }
            }else{
                if(primaryKey && isQualifier(pred,optTable,pushPreds) ||
                isHashableJoin && isQualifierForHashableJoin(pred, optTable, pushPreds)){
                    pred.markQualifier();
                    if(pushPreds){
                        if(optTable.pushOptPredicate(pred)){
                            predicates.add(pred);
                        }
                    }
                }
            }
        }
        if (inlistPosition >= 0)
            isEquality[inlistPosition] = true;

        /** inlistPosition of -1 means inlist on rowid, however, currently MultiProbeTableScan for inlist on rowid
		  returns wrong result, so do not consider MultiProbeTableScan for inlist on rowid.
		  eligible inlist for MultiProbeScan are:
		  1. inlist with indexPosition =0, that is, inlist on the leading index/PK column; or
          2. inlist with indexPosition > 0 if all predicates with indexPosition prior to the inlist all having equality conditions
        */
        boolean inlistQualified = false;

        ArrayList<Predicate> inListNonQualifiedPreds = new ArrayList<>();
        
        if (inlistPosition >= 0) {

            inlistQualified = true;
            if (inlistPosition > 0) {
                for (int i = 0; i < usefulCount; i++) {
                    Predicate pred = usefulPredicates[i];
                    int pos = pred.getIndexPosition();
                    if (pos <= inlistPosition && pos >= 0 && !isEquality[pos]) {
                        RelationalOperator relop = pred.getRelop();
                        if (relop != null && relop.getOperator() == RelationalOperator.EQUALS_RELOP &&
                                pred.compareWithKnownConstant(optTable, true))
                            isEquality[pos] = true;
                    }
                }
            }
            boolean unsetRemainder = false;
            for (int pos = 0; pos <= inlistPosition; pos++) {
                if (unsetRemainder)
                    isEquality[pos] = false;
        
                if (!isEquality[pos])
                    unsetRemainder = true;
            }
            TreeMap<Integer, Predicate> inlistPredsCopy = (TreeMap < Integer, Predicate>)inlistPreds.clone();
            for (Map.Entry<Integer, Predicate> p : inlistPredsCopy.entrySet()) {
                if (isEquality[p.getKey()]) {
                    p.getValue().setIndexPosition(p.getKey());
                    inlistQualified = true;
                    /* Remember the useful predicate */
                    usefulPredicates[usefulCount++] = p.getValue();
                }
                else {
                    inListNonQualifiedPreds.add(p.getValue());
                    inlistPreds.remove(p.getKey());
                }
            }
        } else
            inlistQualified = false;

        if (inlistPreds.isEmpty())
            inlistQualified = false;
    
        if (usefulCount > 0) {
            /* The array of useful predicates may have unused slots.  Shrink it */
            if (usefulPredicates.length > usefulCount) {
                Predicate[] shrink = new Predicate[usefulCount];
                System.arraycopy(usefulPredicates, 0, shrink, 0, usefulCount);
                usefulPredicates = shrink;
            }
    
            /* Sort the array of useful predicates in index position order */
            java.util.Arrays.sort(usefulPredicates);
            usefulPredicates = checkRowIdPredicate(usefulPredicates);
            usefulCount = usefulPredicates.length;
        }
        ValueNode andNode = null;
        InListOperatorNode ilon = null;
        Predicate multiColumnInListPred = null;
        ArrayList<Predicate> predsForNewInList = null;
    
        if (inlistQualified) {
          // Only combine multiple IN lists if not using Spark.
          // Adding extra RDDs and unioning them together hinders performance.
          if (inlistPreds.size() > 1) {
            predsForNewInList = new ArrayList<>();
            int firstPred = -1, lastPred = -1, lastIndexPos = -1, firstInListPred = -1;

            boolean foundPred[] = new boolean[usefulCount];
            int numConstants = 1;
            int maxMulticolumnProbeValues = getCompilerContext().getMaxMulticolumnProbeValues();
            for (int i = 0; i < usefulCount; i++) {
                final Predicate pred = usefulPredicates[i];

                if (pred.getIndexPosition() == lastIndexPos + 1) {
                    BinaryRelationalOperatorNode bron =
                        (BinaryRelationalOperatorNode) pred.getRelop();
                    boolean inList = pred.isInListProbePredicate();
                    if (inList ||
                        (bron != null && bron.getOperator() == RelationalOperator.EQUALS_RELOP)) {
                        if (inList) {
                            InListOperatorNode inNode = pred.getSourceInList(true);
    
                            if (firstInListPred == -1)
                                firstInListPred = i;
                            
                            // A max setting of -1 means there is no limit to the size of
                            // IN list that we generate.
                            if (maxMulticolumnProbeValues != -1 &&
                                (numConstants * inNode.rightOperandList.size()) > maxMulticolumnProbeValues)
                                break;
                            numConstants *= inNode.rightOperandList.size();
                        }
                        if (firstPred == -1)
                            firstPred = i;
                        lastPred = i;
                        predsForNewInList.add(usefulPredicates[i]);
                        lastIndexPos = pred.getIndexPosition();
                        foundPred[i] = true;
                    }
                }
                // Can't have any gaps in index position.
                if (pred.getIndexPosition() > lastIndexPos+1)
                    break;
            }

            ValueNodeList vnl = (ValueNodeList)getNodeFactory().getNode(
                                 C_NodeTypes.VALUE_NODE_LIST,
                                  getContextManager());
            ValueNodeList groupedConstants =
                   (ValueNodeList) getNodeFactory().getNode(
                                   C_NodeTypes.VALUE_NODE_LIST,
                                   getContextManager());
            boolean multiColumnInListBuilt = false;
            if (firstPred != lastPred &&
                addConstantsToList(optTable, null, groupedConstants, predsForNewInList, 0)) {
                if (numConstants != groupedConstants.size())
                    SanityManager.THROWASSERT("Wrong number of constants built for IN list.");

                for (Predicate pred : predsForNewInList) {
                    InListOperatorNode inNode = pred.getSourceInList(true);
                    if (inNode != null)
                        vnl.addValueNode(inNode.getLeftOperand().getClone());
                    else {
                        RelationalOperator relop = pred.getRelop();
                        if (! (relop instanceof BinaryRelationalOperatorNode))
                            SanityManager.THROWASSERT("Expected equality predicate, but none found.");
                        BinaryRelationalOperatorNode brop = (BinaryRelationalOperatorNode) relop;
                        ValueNode colRef;
                        if (brop.keyColumnOnLeft(optTable))
                            colRef = brop.getLeftOperand();
                        else
                            colRef = brop.getRightOperand();
                        vnl.addValueNode(colRef.getClone());
                    }
                }
                ilon =
                    (InListOperatorNode) getNodeFactory().getNode(
                        C_NodeTypes.IN_LIST_OPERATOR_NODE,
                        vnl,
                        groupedConstants,
                        getContextManager());
        
                boolean nullableResult = ilon.leftOperandList.isNullable() ||
                                         ilon.rightOperandList.isNullable();
                ilon.setType(new DataTypeDescriptor(TypeId.BOOLEAN_ID, nullableResult));
                
                andNode = ilon.convertToAndedEqualityProbePredicate();

                if (andNode instanceof AndNode) {
                    multiColumnInListBuilt = true;
                    JBitSet newJBitSet = new JBitSet(getCompilerContext().getNumTables());
                    Predicate newPred = (Predicate) getNodeFactory().getNode(C_NodeTypes.PREDICATE,
                            andNode, newJBitSet, getContextManager());

                    // The index position is the position of the first column in the
                    // multicolumn IN list.
                    newPred.setIndexPosition(usefulPredicates[firstPred].getIndexPosition());
                    usefulPredicates[firstPred] = newPred;
                    multiColumnInListPred = newPred;

                    // Pack the remaining useful preds in usefulPredicates so
                    // there are no gaps.
                    int j = firstPred + 1;
                    for (int i = firstPred + 1; i < usefulCount; i++) {
                        while(i < usefulCount && foundPred[i])
                            i++;
                        if (i != j && i < usefulCount) {
                            usefulPredicates[j] = usefulPredicates[i];
                            j++;
                        }
                    }
                    usefulCount -= (predsForNewInList.size() - 1);
                }
            }

            // At this point, we have multiple IN list predicates in the
            // usefulPredicates array, but we might not have combined them
            // all into a single multicolumn IN list predicate.
            // Put the rest into inListNonQualifiedPreds
            // so they may be possibly pushed as qualifiers, but more
            // importantly, remove them from the usefulPredicates so they
            // are not considered for use as probe predicates, as this may
            // cause wrong results.
            boolean copyNeeded = false;
            int oldUsefulCount = usefulCount;
            if (firstInListPred == -1)
                SanityManager.THROWASSERT("Logic failure in building multicolumn IN list.");
            int firstUnusedPred = multiColumnInListBuilt ? firstInListPred + 1 : lastPred + 1;
            for (int i = firstUnusedPred; i < oldUsefulCount; i++) {
                final Predicate pred = usefulPredicates[i];
                if (pred != null && pred.isInListProbePredicate()) {
                    inListNonQualifiedPreds.add(pred);
                    usefulPredicates[i] = null;
                    copyNeeded = true;
                    usefulCount--;
                }
            }
            if (copyNeeded) {
                Predicate [] newUsefulPredicates = new Predicate[usefulCount];
                int j = 0;
                for (int i = 0; i < oldUsefulCount; i++) {
                    final Predicate pred = usefulPredicates[i];
                    if (pred != null) {
                        newUsefulPredicates[j++] = pred;
                    }
                }
                usefulPredicates = newUsefulPredicates;
            }
          }
        }

        //we still need to mark the remaining inlist conditions
        for (Predicate pred : inListNonQualifiedPreds) {
            if (!inlistQualified || pred.getIndexPosition() < 0) {
                if(primaryKey && isQualifier(pred,optTable,pushPreds) ||
                        isHashableJoin && isQualifierForHashableJoin(pred, optTable, pushPreds)){
                    pred.markQualifier();
                    if(pushPreds){
                        if(optTable.pushOptPredicate(pred)){
                            predicates.add(pred);
                        }
                    }
                }
            }
        }

        for(Predicate pred : predicates){
            removeOptPredicate(pred);
        }

		/* We can end up with no useful
		 * predicates with a force index override -
		 * Each predicate is on a non-key column or both
		 * sides of the operator are columns from the same table.
		 * There's no predicates to push down, so return and we'll
		 * evaluate them in a PRN.
		 */
        if(usefulCount==0)
            return;


		/* Push the sorted predicates down to the Optimizable table */
        int currentStartPosition=-1;
        boolean gapInStartPositions=false;
        int currentStopPosition=-1;
        boolean gapInStopPositions=false;
        boolean seenNonEquals=false;
        int firstNonEqualsPosition=-1;
        int lastStartEqualsPosition=-1;

        if (rowIdScan) {
            currentStartPosition = -2;
            currentStopPosition = -2;
            firstNonEqualsPosition = -2;
            lastStartEqualsPosition = -2;
        }
		    /* beetle 4572. We need to truncate if necessary potential multi-column
         * start key up to the first one whose start operator is GT, and make 
         * start operator GT;
		     * or start operator is GE if there's no such column.  We need to
         * truncate if necessary potential multi-column stop key up to the 
         * first one whose stop operator is GE, and make stop operator GE; or 
         * stop operator is GT if no such column.
		     * eg., start key (a,b,c,d,e,f), potential start operators
         * (GE,GE,GE,GT,GE,GT)
		     * then start key should really be (a,b,c,d) with start operator GT.
		     */
        boolean seenGE=false, seenGT=false;
    
        int numColsInStartPred = 1;
        int numColsInStopPred = 1;
        
        for(int i=0;i<usefulCount;i++){
            Predicate thisPred=usefulPredicates[i];
            int thisIndexPosition=thisPred.getIndexPosition();
            boolean thisPredMarked=false;
            RelationalOperator relop=thisPred.getRelop();
            int thisOperator=-1;

            boolean isIn=(thisPred.getSourceInList()!=null);

            if(relop!=null)
                thisOperator=relop.getOperator();

			      /* Allow only one start and stop position per index column */
            if(currentStartPosition+numColsInStartPred<=thisIndexPosition){
				/*
				** We're working on a new index column for the start position.
				** Is it just one more than the previous position?
				*/
                if((thisIndexPosition-currentStartPosition)> numColsInStartPred ||
                        !considerJoinPredicateAsKey && thisPred.isJoinPredicate()){
					/*
					** There's a gap in the start positions.  Don't mark any
					** more predicates as start predicates.
					*/
                    gapInStartPositions=true;
                }else if((thisOperator==RelationalOperator.EQUALS_RELOP)||(thisOperator==RelationalOperator.IS_NULL_RELOP)){
					/* Remember the last "=" or IS NULL predicate in the start
					 * position.  (The sort on the predicates above has ensured
					 * that these predicates appear 1st within the predicates on
					 * a specific column.)
					 */
                    lastStartEqualsPosition=thisIndexPosition;
                }

                if(!gapInStartPositions){
					/*
					** There is no gap in start positions.  Is this predicate
					** useful as a start position?  This depends on the
					** operator - for example, indexCol = <expr> is useful,
					** while indexCol < <expr> is not useful with asc index
					** we simply need to reverse the logic for desc indexes
					**
					** The relop has to figure out whether the index column
					** is on the left or the right, so pass the Optimizable
					** table to help it.
					*/
                    if(!seenGT
                            && (isIn || ((relop.usefulStartKey(optTable) && (thisIndexPosition == -1 || isAscending[thisIndexPosition]))
                                || (relop.usefulStopKey(optTable) && (thisIndexPosition != -1 && !isAscending[thisIndexPosition]))))){
                        thisPred.markStartKey();
                        currentStartPosition=thisIndexPosition;
                        numColsInStartPred = thisPred.numColumnsInQualifier();
                        thisPredMarked=true;
                        seenGT=(thisPred.getStartOperator(optTable)==ScanController.GT);
                        thisPred.markQualifier();
                    }
                }
            }

			/* Same as above, except for stop keys */
            if(currentStopPosition + numColsInStopPred <= thisIndexPosition || thisIndexPosition == -1){
                if((thisIndexPosition-currentStopPosition)> numColsInStopPred ||
                        !considerJoinPredicateAsKey && thisPred.isJoinPredicate()){
                    gapInStopPositions=true;
                }

                if(!gapInStopPositions){
                    if(!seenGE
                            && (isIn || ((relop.usefulStopKey(optTable) && (thisIndexPosition == -1 || isAscending[thisIndexPosition]))
                                || (relop.usefulStartKey(optTable) && (thisIndexPosition != -1 && !isAscending[thisIndexPosition]))))){
                        thisPred.markStopKey();
                        currentStopPosition=thisIndexPosition;
                        numColsInStopPred = thisPred.numColumnsInQualifier();
                        thisPredMarked=true;
                        seenGE=(thisPred.getStopOperator(optTable)==ScanController.GE);
                        thisPred.markQualifier();
                    }
                }
            }

        	/* Mark this predicate as a qualifier if it is not a start/stop
             * position or if we have already seen a previous column whose 
             * RELOPS do not include "=" or IS NULL.  For example, if
			 * the index is on (a, b, c) and the predicates are a > 1 and b = 1
             * and c = 1, then b = 1 and c = 1 also need to be a qualifications,
             * otherwise we may match on (2, 0, 3).
			 */
            if((!isIn) &&    // store can never treat "in" as qualifier
                    ((!thisPredMarked) || (seenNonEquals && thisIndexPosition!=firstNonEqualsPosition))){
                thisPred.markQualifier();
            }

            /* Remember if we have seen a column without an "=" */
            if(lastStartEqualsPosition+numColsInStartPred<=thisIndexPosition
                    && firstNonEqualsPosition==(rowIdScan? -2:-1)
                    && (thisOperator!=RelationalOperator.EQUALS_RELOP)
                    && (thisOperator!=RelationalOperator.IS_NULL_RELOP)){
                seenNonEquals=true;
        		/* Remember the column */
                firstNonEqualsPosition=thisIndexPosition;
            }

            if(pushPreds){
         		/* we only roughly detected that the predicate may be useful
                 * earlier, it may turn out that it's not actually start/stop 
                 * key because another better predicate on the column is chosen.
                 * We don't want to push "in" in this case, since it's not a 
                 * qualifier.  Beetle 4316.
			     */
                if(isIn && !thisPredMarked ||
                thisPred.isRowId() && !thisPred.isStartKey() && !thisPred.isStopKey()){
					/* If we get here for an IN-list probe pred then we know
					 * that we are *not* using the probe predicate as a
					 * start/stop key.  We also know that we're in the middle
					 * of modifying access paths (because pushPreds is true),
					 * which means we are preparing to generate code.  Those
					 * two facts together mean we have to "revert" the
					 * probe predicate back to its original state so that
					 * it can be generated as normal IN-list.  That "revert"
					 * operation happens from within the generateExpression()
					 * method of BinaryOperatorNode.java.
					 */
                    continue;
                }

				/*
				** Push the predicate down.  They get pushed down in the
				** order of the index.
				*/

				/* If this is an InListOperator predicate, make a copy of the
				 * the predicate (including the AND node contained within it)
				 * and then push the _copy_ (instead of the original) into
				 * optTable.  We need to do this to avoid having the exact
				 * same Predicate object (and in particular, the exact same
				 * AndNode object) be referenced in both optTable and this.v,
				 * which can lead to an infinite recursion loop when we get to
				 * restorePredicates(), and thus to stack overflow
				 * (beetle 4974).
				 *
				 * Note: we don't do this if the predicate is an IN-list
				 * probe predicate.  In that case we want to push the 
				 * predicate down to the base table for special handling.
				 */
                Predicate predToPush;
                if(isIn && !thisPred.isInListProbePredicate()){
                    AndNode andCopy=(AndNode)getNodeFactory().getNode(
                            C_NodeTypes.AND_NODE,
                            thisPred.getAndNode().getLeftOperand(),
                            thisPred.getAndNode().getRightOperand(),
                            getContextManager());
                    andCopy.copyFields(thisPred.getAndNode());
                    Predicate predCopy=(Predicate)getNodeFactory().getNode(
                            C_NodeTypes.PREDICATE,
                            andCopy,
                            thisPred.getReferencedSet(),
                            getContextManager());
                    predCopy.copyFields(thisPred);
                    predToPush=predCopy;
                }else{
                    predToPush=thisPred;
                }

                if(optTable.pushOptPredicate(predToPush)){
					          /* Although we generated dynamic start and stop keys
					           * for "in", we still need this predicate for further
					           * restriction--*unless* we're dealing with a probe
					           * predicate, in which case the restriction is handled
					           * via execution-time index probes (for more see
					           * execute/MultiProbeTableScanResultSet.java).
					           */
                    if(!isIn || thisPred.isInListProbePredicate()) {
                        if (thisPred == multiColumnInListPred) {
                            for (Predicate predToRemove:predsForNewInList) {
                                removeOptPredicate(predToRemove);
                            }
                        }
                        else
                            removeOptPredicate(thisPred);
                    }
                }else if(SanityManager.DEBUG){
                    SanityManager.THROWASSERT("pushOptPredicate expected to be true");
                }
            }else{
                /*
			     * We're not pushing the predicates down, so put them at the
			     * beginning of this predicate list in index order.
			     */
                if (thisPred != multiColumnInListPred) {
                    removeOptPredicate(thisPred);
                    addOptPredicate(thisPred, i);
                }
            }
        }
    }

    /**
     * Add a Predicate to the list.
     *
     * @param predicate A Predicate to add to the list
     * @throws StandardException Thrown on error
     */
    public void addPredicate(Predicate predicate) throws StandardException{
        if(predicate.isStartKey())
            numberOfStartPredicates++;
        if(predicate.isStopKey())
            numberOfStopPredicates++;
        if(predicate.isQualifier())
            numberOfQualifiers++;

        addElement(predicate);
    }

    /**
     * Transfer the non-qualifiers from this predicate list to the specified
     * predicate list.
     * This is useful for arbitrary hash join, where we need to separate the 2
     * as the qualifiers get applied when probing the hash table and the
     * non-qualifiers get * applied afterwards.
     *
     * @param optTable The optimizable that we want qualifiers for
     * @param otherPL  ParameterList for non-qualifiers
     * @throws StandardException Thrown on error
     */
    protected void transferNonQualifiers(Optimizable optTable,PredicateList otherPL) throws StandardException{
        //Walk list backwards since we can delete while traversing the list.
        for(int index=size()-1;index>=0;index--){
            Predicate pred=elementAt(index);

            // Transfer each non-qualifier
            //noinspection ConstantConditions
            if(!pred.isRelationalOpPredicate() || !pred.getRelop().isQualifier(optTable,false)){
                pred.clearScanFlags();
                removeElementAt(index);
                otherPL.addElement(pred);
            }
        }

        // Mark all remaining predicates as qualifiers
        markAllPredicatesQualifiers();
    }

    /**
     * Categorize the predicates in the list.  Initially, this means
     * building a bit map of the referenced tables for each predicate.
     *
     * @throws StandardException Thrown on error
     */
    public void categorize() throws StandardException{
        int size=size();

        for(int index=0;index<size;index++){
            elementAt(index).categorize();
        }
    }

    /**
     * Eliminate predicates of the form:
     * AndNode
     * /	   \
     * true BooleanConstantNode		true BooleanConstantNode
     * This is useful when checking for a NOP PRN as the
     * Like transformation on c1 like 'ASDF%' can leave
     * one of these predicates in the list.
     */
    public void eliminateBooleanTrueAndBooleanTrue(){
        //Walk list backwards since we can delete while traversing the list.
        for(int index=size()-1;index>=0;index--){
     			  /* Look at the current predicate from the predicate list */
            AndNode nextAnd=elementAt(index).getAndNode();

            if((nextAnd.getLeftOperand().isBooleanTrue()) && (nextAnd.getRightOperand().isBooleanTrue())){
                removeElementAt(index);
            }
        }
    }

    /**
     * Rebuild a constant expression tree from the remaining constant
     * predicates and delete those entries from the PredicateList.
     * The rightOperand of every top level AndNode is always a true
     * BooleanConstantNode, so we can blindly overwrite that pointer.
     * Optimizations:
     * <p/>
     * We take this opportunity to eliminate:
     * AndNode
     * /		   \
     * true BooleanConstantNode	true BooleanConstantNode
     * <p/>
     * We remove the AndNode if the predicate list is a single AndNode:
     * AndNode
     * /	   \
     * LeftOperand			RightOperand
     * <p/>
     * becomes:
     * LeftOperand
     * <p/>
     * If the leftOperand of any AndNode is False, then the entire expression
     * will be False.  The expression simple becomes:
     * false BooleanConstantNode
     *
     * @return ValueNode    The rebuilt expression tree.
     */
    public ValueNode restoreConstantPredicates() throws StandardException{
        AndNode nextAnd;
        AndNode falseAnd=null;
        ValueNode restriction=null;

		/* Walk list backwards since we can delete while
		 * traversing the list.
		 */
        for(int index=size()-1;index>=0;index--){
			/* Look at the current predicate from the predicate list */
            nextAnd=elementAt(index).getAndNode();

            // Skip over the predicate if it is not a constant expression
            if(!nextAnd.isConstantExpression()){
                continue;
            }

            // This node is a constant expression, so we can remove it from the list
            removeElementAt(index);

			      /* We can skip over TRUE AND TRUE */
            if((nextAnd.getLeftOperand().isBooleanTrue()) && (nextAnd.getRightOperand().isBooleanTrue())){
                continue;
            }

       			/* Remember if we see a false BooleanConstantNode */
            if(nextAnd.getLeftOperand().isBooleanFalse()){
                falseAnd=nextAnd;
            }

            if(restriction!=null){
                nextAnd.setRightOperand(restriction);
				        /* If any of the predicates is nullable, then the resulting
				         * tree must be nullable.
				         */
                if(restriction.getTypeServices().isNullable()){
                    nextAnd.setNullability(true);
                }
            }
            restriction=nextAnd;
        }

		    /* If restriction is a single AndNode, then it's rightOperand must be
		     * a true BooleanConstantNode.  We simply chop out the AndNode and set
         * restriction to AndNode.leftOperand.
		     */
        if((restriction!=null) && (((AndNode)restriction).getRightOperand().isBooleanTrue())){
            restriction=((AndNode)restriction).getLeftOperand();
        }else if(falseAnd!=null){
        			/* Expression is ... AND FALSE AND ...
			         * Replace the entire expression with a false BooleanConstantNode.
			         */
            restriction=falseAnd.getLeftOperand();
        }

        return restriction;
    }

    /**
     * Rebuild an expression tree from the remaining predicates and delete those
     * entries from the PredicateList.
     * The rightOperand of every top level AndNode is always a true
     * BooleanConstantNode, so we can blindly overwrite that pointer.
     * Optimizations:
     * <p/>
     * We take this opportunity to eliminate:
     * AndNode
     * /	   \
     * true BooleanConstantNode	true BooleanConstantNode
     * <p/>
     * We remove the AndNode if the predicate list is a single AndNode:
     * AndNode
     * /	   \
     * LeftOperand			RightOperand
     * <p/>
     * becomes:
     * LeftOperand
     * <p/>
     * If the leftOperand of any AndNode is False, then the entire expression
     * will be False.  The expression simple becomes:
     * false BooleanConstantNode
     *
     * @return ValueNode    The rebuilt expression tree.
     */
    public ValueNode restorePredicates() throws StandardException{
        AndNode nextAnd;
        AndNode falseAnd=null;
        ValueNode restriction=null;

        int size=size();
        for(int index=0;index<size;index++){
            Predicate p = elementAt(index);

            // Skip rowid predicates that are either start or stop key
            if (p.isRowId()&& (p.isStartKey() || p.isStopKey())) {
                continue;
            }
            nextAnd=p.getAndNode();

            /* We can skip over TRUE AND TRUE */
            if((nextAnd.getLeftOperand().isBooleanTrue()) && (nextAnd.getRightOperand().isBooleanTrue())){
                continue;
            }

			/* Remember if we see a false BooleanConstantNode */
            if(nextAnd.getLeftOperand().isBooleanFalse()){
                falseAnd=nextAnd;
            }

            if(restriction!=null){
                nextAnd.setRightOperand(restriction);
                /* If any of the predicates is nullable, then the resulting
				 * tree must be nullable.
				 */
                //added restriction.getTypeServices()!=null&& to this if block to prevent null pointer exception if TaskService is NULL.
                // I am assuming that there is no need to set a TypeService nullable if the type service is NULL.
                if(restriction.getTypeServices()!=null && restriction.getTypeServices().isNullable()){
                    nextAnd.setNullability(true);
                }
            }
            restriction=nextAnd;
        }

		    /* If restriction is a single AndNode, then it's rightOperand must be
		     * a true BooleanConstantNode.  We simply chop out the AndNode and set
             * restriction to AndNode.leftOperand.
		     */
        if((restriction!=null) && (((AndNode)restriction).getRightOperand().isBooleanTrue())){
            restriction=((AndNode)restriction).getLeftOperand();
        }else if(falseAnd!=null){
			/* Expression is ... AND FALSE AND ...
			 * Replace the entire expression with a simple false
             * BooleanConstantNode.
	      	 */
            restriction=falseAnd.getLeftOperand();
        }

        /* Remove all predicates from the list */
        //removeAllElements();
        return restriction;
    }

    /**
     * Remap all ColumnReferences in this tree to be clones of the
     * underlying expression.
     *
     * @throws StandardException Thrown on error
     */
    public void remapColumnReferencesToExpressions() throws StandardException{
        Predicate pred;

        int size=size();
        for(int index=0;index<size;index++){
            pred=elementAt(index);

            pred.setAndNode((AndNode)pred.getAndNode().remapColumnReferencesToExpressions());
        }
    }

    /**
     * Break apart the search clause into matching a PredicateList
     * where each top level predicate is a separate element in the list.
     * Build a bit map to represent the FromTables referenced within each
     * top level predicate.
     * NOTE: We want the rightOperand of every AndNode to be true, in order
     * to simplify the algorithm for putting the predicates back into the tree.
     * (As we put an AndNode back into the tree, we can ignore it's rightOperand.)
     *
     * @param numTables    Number of tables in the DML Statement
     * @param searchClause The search clause to operate on.
     * @throws StandardException Thrown on error
     */
    void pullExpressions(int numTables,ValueNode searchClause) throws StandardException{
        AndNode thisAnd;
        AndNode topAnd;
        JBitSet newJBitSet;
        Predicate newPred;

        if(searchClause!=null){
            topAnd=(AndNode)searchClause;
            ContextManager contextManager = getContextManager();
            BooleanConstantNode trueNode=(BooleanConstantNode)getNodeFactory().getNode(C_NodeTypes.BOOLEAN_CONSTANT_NODE,
                    Boolean.TRUE,contextManager);

            AndNode firstAndInProbeSet = null;
            while(topAnd.getRightOperand() instanceof AndNode){
                /* Break out the next top AndNode */
                thisAnd=topAnd;
                topAnd = (AndNode) topAnd.getRightOperand();
                if (!thisAnd.isNextAndedPredInSameMultiprobeSet()) {
                    /* Set the rightOperand to true */
                    thisAnd.setRightOperand(trueNode);

                    /* Add the top AndNode to the PredicateList */
                    /* If firstAndInProbeSet is not null, add it as a chain of And'ed nodes
                     * in the probe set as a single predicate.
                     */
                    newJBitSet=new JBitSet(numTables);
                    newPred=(Predicate)getNodeFactory().getNode(C_NodeTypes.PREDICATE,
                              firstAndInProbeSet != null ? firstAndInProbeSet : thisAnd,
                              newJBitSet, contextManager);
                    addPredicate(newPred);
    
                    firstAndInProbeSet = null;
                }
                else {
                    if (firstAndInProbeSet == null)
                        firstAndInProbeSet = thisAnd;
                }
            }
			
			      /* Add the last top AndNode to the PredicateList */
            newJBitSet=new JBitSet(numTables);
            newPred=(Predicate)getNodeFactory().getNode( C_NodeTypes.PREDICATE, topAnd, newJBitSet, contextManager);
            addPredicate(newPred);
        }
    }

    private void countScanFlags(){
        Predicate predicate;

        int size=size();
        for(int index=0;index<size;index++){
            predicate=elementAt(index);
            if(predicate.isStartKey())
                numberOfStartPredicates++;
            if(predicate.isStopKey())
                numberOfStopPredicates++;
            if(predicate.isQualifier())
                numberOfQualifiers++;
        }
    }

    /**
     * Check if a node is representing a constant or a parameter.
     *
     * @param node the node to check
     * @return {@code true} if the node is a constant or a parameter, {@code
     * false} otherwise
     */
    private static boolean isConstantOrParameterNode(ValueNode node){
        if (node instanceof ListValueNode)
        {
            ListValueNode lcn = (ListValueNode)node;
            for (int i = 0; i < lcn.numValues(); i++){
                if (!(lcn.getValue(i) instanceof ConstantNode) &&
                    !(lcn.getValue(i) instanceof ParameterNode))
                    return false;
            }
            return true;
        }
        else
            return node instanceof ConstantNode || node instanceof ParameterNode;
    }

    private boolean canPruneForPredicatePushedToUnion(DataTypeDescriptor unionType, DataTypeDescriptor branchType) {
        if (unionType.getTypeName().equals(TypeId.VARCHAR_NAME) && branchType.getTypeName().equals(TypeId.CHAR_NAME))
            return true;

        return false;
    }

    /**
     * Push all predicates, which can be pushed, into the underlying select.
     * A predicate can be pushed into an underlying select if the source of
     * every ColumnReference in the predicate is itself a ColumnReference.
     * <p/>
     * This is useful when attempting to push predicates into non-flattenable
     * views or derived tables or into unions.
     *
     * @param select        The underlying SelectNode.
     * @param copyPredicate Whether to make a copy of the predicate
     *                      before pushing
     * @throws StandardException Thrown on error
     */
    void pushExpressionsIntoSelect(SelectNode select,boolean copyPredicate) throws StandardException{
		/* Walk list backwards since we can delete while
		 * traversing the list.
		 */
        for(int index=size()-1;index>=0;index--){
            Predicate predicate;
            predicate=elementAt(index);

            CollectNodesVisitor getCRs= new CollectNodesVisitor(ColumnReference.class);

            predicate.getAndNode().accept(getCRs);
            @SuppressWarnings("unchecked") List<ColumnReference> colRefs=getCRs.getList();

			/* state doesn't become true until we find the 1st
			 * ColumnReference.  (We probably will always find
			 * at least 1 CR, but just to be safe, ...)
			 */
            boolean state= !colRefs.isEmpty();
            if(state){
                for(ColumnReference ref : colRefs){
                    if(!ref.pointsToColumnReference()){
                        state=false;
                        break;
                    }
                }
            }

            if(!state)
                continue;

            if(copyPredicate){ // this path is for push predicate into union branches
                // Copy this predicate and push this instead
                AndNode andNode=predicate.getAndNode();
                ValueNode leftOperand;
                ColumnReference crNode;
                BinaryRelationalOperatorNode opNode=null;
                InListOperatorNode inNode=null;

                // Make sure we are only pushing binary relations and InList for
                // copyPredicate case. It should be benificial to push expressions that
                // can be pushed, so they can be applied closer to the data.

                if(andNode.getLeftOperand() instanceof BinaryRelationalOperatorNode){
                    opNode=(BinaryRelationalOperatorNode)andNode.getLeftOperand();
                    // Investigate using invariant interface to check rightOperand
                    if(!(opNode.getLeftOperand() instanceof ColumnReference)
                            || !opNode.getRightOperand().isConstantOrParameterTreeNode())
                        continue;

                    crNode=(ColumnReference)opNode.getLeftOperand();
                }else if(andNode.getLeftOperand() instanceof InListOperatorNode){
                    inNode=(InListOperatorNode)andNode.getLeftOperand();
                    // Don't push multicolumn IN list into a SELECT for now.
                    if (inNode.leftOperandList.size() > 1)
                        continue;
                    if(!(inNode.getRightOperandList().containsOnlyConstantAndParamNodes()))
                        continue;

                    if (!(inNode.getLeftOperand() instanceof ColumnReference))
                        continue;

                    crNode=(ColumnReference)inNode.getLeftOperand();
                }else
                    continue;

                // Remap this crNode to underlying column reference in the select, if possible.
                // At this point, the column references in the predicate points to the result column
                // of the union node, we need it to point to the result column of the SELECT node
                // corresponding to the branch underneath the union node
                ColumnReference newCRNode=select.findColumnReferenceInUnionSelect(crNode);
                if(newCRNode==null)
                    continue;

                // Create a copy of the predicate to push down
                // <column> <relop> <value> AND TRUE
                ContextManager contextManager = getContextManager();
                if(andNode.getLeftOperand() instanceof BinaryRelationalOperatorNode){
                    boolean unsatisfiableCondition = false;
					/* If the operator is a binary relational operator that was
					 * created for a probe predicate then we have to make a
					 * copy of the underlying IN-list as well, so that we can
					 * give it the correct left operand (i.e. the new Column
					 * Reference node).  Then we pass that copy into the new
					 * relational operator node.
					 */
                    //noinspection ConstantConditions
                    inNode=opNode.getInListOp();
                    if(inNode!=null){
                        // Don't push multicolumn IN list into a SELECT for now.
                        if (inNode.leftOperandList.size() > 1)
                            continue;
                        inNode=inNode.shallowCopy();
                        inNode.setLeftOperand(newCRNode);

                        // extra processing and optimization for predicate on char type
                        if (canPruneForPredicatePushedToUnion(crNode.getTypeServices(), newCRNode.getTypeServices())) {
                            ValueNodeList newValueNodeList = (ValueNodeList)getNodeFactory().getNode(C_NodeTypes.VALUE_NODE_LIST, contextManager);
                            ValueNodeList oldValueNodeList = inNode.getRightOperandList();
                            int columnLen = newCRNode.getTypeServices().getMaximumWidth();
                            for (int i=0; i < oldValueNodeList.size(); i++) {
                                ValueNode valueNode = (ValueNode)oldValueNodeList.elementAt(i);
                                if (valueNode instanceof CharConstantNode) {
                                    DataValueDescriptor dvd = ((CharConstantNode) valueNode).getValue();
                                    // for fixed char type, discard string value of length different
                                    // from that defined in the column definition
                                    if (dvd.getLength() != columnLen)
                                        continue;
                                }
                                newValueNodeList.addValueNode(valueNode);
                            }
                            inNode.setRightOperandList(newValueNodeList);
                            if (newValueNodeList.isEmpty())
                                unsatisfiableCondition = true;

                            // we may push down an inlist against a varchar to a branch with fixed char column, so we cannot assume
                            // the inlist elements have no duplicates and are sorted
                            inNode.markAsOrdered(false);
                        }
                    }

                    ValueNode rightOp = opNode.getRightOperand();
                    // equality predicate for char column can be pruned with length mismatch
                    if (opNode.getOperator() == RelationalOperator.EQUALS_RELOP &&
                            canPruneForPredicatePushedToUnion(crNode.getTypeServices(), newCRNode.getTypeServices())) {
                        if (rightOp instanceof CharConstantNode) {
                            DataValueDescriptor dvd = ((CharConstantNode) rightOp).getValue();
                            if (dvd.getLength() != newCRNode.getTypeServices().getMaximumWidth())
                                unsatisfiableCondition = true;
                        }
                    }

                    if (!unsatisfiableCondition) {
                        BinaryRelationalOperatorNode newRelop = (BinaryRelationalOperatorNode)
                                getNodeFactory().getNode(
                                        opNode.getNodeType(),
                                        newCRNode,
                                        rightOp,
                                        inNode,
                                        contextManager);
                        newRelop.bindComparisonOperator();
                        leftOperand = newRelop;
                    } else {
                        BooleanConstantNode falseNode=(BooleanConstantNode)getNodeFactory().
                                getNode(C_NodeTypes.BOOLEAN_CONSTANT_NODE,
                                        Boolean.FALSE,
                                        contextManager);
                        leftOperand = falseNode;
                    }
                }else{
                    // pushable inlist condition should have been represented as BinaryRelationalOperatorNode
                    // in the above path
                    continue;
                }

                // Convert the predicate into CNF form
                ValueNode trueNode=(ValueNode)getNodeFactory().getNode(
                        C_NodeTypes.BOOLEAN_CONSTANT_NODE,
                        Boolean.TRUE,
                        contextManager);
                AndNode newAnd=(AndNode)getNodeFactory().getNode(
                        C_NodeTypes.AND_NODE,
                        leftOperand,
                        trueNode,
                        contextManager);
                newAnd.postBindFixup();
                JBitSet tableMap=new JBitSet(select.referencedTableMap.size());

                // Use newly constructed predicate
                predicate=(Predicate)getNodeFactory().getNode(C_NodeTypes.PREDICATE,newAnd,tableMap, contextManager);
            }else{
                // keep the counters up to date when removing a predicate
                if(predicate.isStartKey())
                    numberOfStartPredicates--;
                if(predicate.isStopKey())
                    numberOfStopPredicates--;
                if(predicate.isQualifier())
                    numberOfQualifiers--;

        				/* Clear all of the scan flags since they may be different
			         	 * due to the splitting of the list.
				         */
                predicate.clearScanFlags();
                // Remove this predicate from the list
                removeElementAt(index);
            }

            // Push it into the select
            select.pushExpressionsIntoSelect(predicate);
        }
    }

    /**
     * Mark all of the RCs and the RCs in their RC/VCN chain
     * referenced in the predicate list as referenced.
     *
     * @throws StandardException Thrown on error
     */
    void markReferencedColumns() throws StandardException{
        CollectNodesVisitor collectCRs= new CollectNodesVisitor(ColumnReference.class);

        int size=size();
        for(int index=0;index<size;index++){
            Predicate predicate=elementAt(index);
            predicate.getAndNode().accept(collectCRs);
        }

        @SuppressWarnings("unchecked") List<ColumnReference> colRefs=collectCRs.getList();
        for(ColumnReference ref : colRefs){
            ResultColumn source=ref.getSource();

            // DERBY-4391: Don't try to call markAllRCsInChainReferenced() if
            // source is null. This can happen if the ColumnReference is
            // pointing to a column that is not from a base table. For instance
            // if we have a VALUES clause like (VALUES (1, 2), (3, 4)) V1(I, J)
            // then a column reference to V1.I won't have a source.
            if(source!=null){
                source.markAllRCsInChainReferenced();
            }
        }
    }

    /**
     * Update the array of columns in = conditions with constants
     * or correlation or join columns.  This is useful when doing
     * subquery flattening on the basis of an equality condition.
     *
     * @param tableNumber    The tableNumber of the table from which
     *                       the columns of interest come from.
     * @param eqOuterCols    Array of booleans for noting which columns
     *                       are in = predicates with constants or
     *                       correlation columns.
     * @param tableNumbers   Array of table numbers in this query block.
     * @param resultColTable tableNumber is the table the result columns are
     *                       coming from
     * @throws StandardException Thrown on error
     */
    void checkTopPredicatesForEqualsConditions(int tableNumber,
                                               boolean[] eqOuterCols,
                                               int[] tableNumbers,
                                               JBitSet[] tableColMap,
                                               boolean resultColTable) throws StandardException{
        int size=size();
        for(int index=0;index<size;index++){
            AndNode and=elementAt(index).getAndNode();
            and.checkTopPredicatesForEqualsConditions(
                    tableNumber,eqOuterCols,tableNumbers,tableColMap,
                    resultColTable);
        }
    }

    /**
     * Check if all of the predicates in the list are pushable.
     *
     * @return Whether or not all of the predicates in the list are pushable.
     */
    boolean allPushable(){
        int size=size();
        for(int index=0;index<size;index++){
            Predicate predicate=elementAt(index);
            if(!predicate.getPushable()){
                return false;
            }
        }
        return true;
    }

    /**
     * Check if all the predicates reference a given {@code FromBaseTable}.
     *
     * @param fbt the {@code FromBaseTable} to check for
     * @return {@code true} if the table is referenced by all predicates,
     * {@code false} otherwise
     */
    boolean allReference(FromBaseTable fbt){
        int tableNumber=fbt.getTableNumber();

        for(int i=0;i<size();i++){
            Predicate p=elementAt(i);
            if(!p.getReferencedSet().get(tableNumber)){
                return false;
            }
        }

        return true;
    }

    /**
     * Build a list of pushable predicates, if any,
     * that satisfy the referencedTableMap.
     *
     * @param referencedTableMap The referenced table map
     * @return A list of pushable predicates, if any,
     * that satisfy the referencedTableMap.
     * @throws StandardException Thrown on error
     */
    PredicateList getPushablePredicates(JBitSet referencedTableMap) throws StandardException{
        PredicateList pushPList=null;

        // Walk the list backwards because of possible deletes
        for(int index=size()-1;index>=0;index--){
            Predicate predicate=elementAt(index);
            if(!predicate.getPushable()){
                continue;
            }

            JBitSet curBitSet=predicate.getReferencedSet();
			
            /* Do we have a match? */
            if(referencedTableMap.contains(curBitSet)){
                /* Add the matching predicate to the push list */
                if(pushPList==null){
                    pushPList=(PredicateList)getNodeFactory().getNode(C_NodeTypes.PREDICATE_LIST,getContextManager());
                }
                pushPList.addPredicate(predicate);

                /* Remap all of the ColumnReferences to point to the
				 * source of the values.
				 */
                RemapCRsVisitor rcrv=new RemapCRsVisitor(true);
                predicate.getAndNode().accept(rcrv);

                /* Remove the matching predicate from the outer list */
                removeElementAt(index);
            }
        }
        return pushPList;
    }

    /**
     * Decrement the level of any CRs from the subquery's
     * FROM list that are interesting to transitive closure.
     *
     * @param fromList  The subquery's FROM list.
     * @param decrement Decrement size.
     */
    void decrementLevel(FromList fromList,int decrement){
        int[] tableNumbers=fromList.getTableNumbers();

		/* For each top level relop, find all top level
		 * CRs from the subquery and decrement their 
		 * nesting level.
		 */
        int size=size();
        for(int index=0;index<size;index++){
            ColumnReference cr1=null;
            ColumnReference cr2=null;
            Predicate predicate=elementAt(index);
            ValueNode vn=predicate.getAndNode().getLeftOperand();

            if(vn instanceof BinaryOperatorNode){
                BinaryOperatorNode bon=(BinaryOperatorNode)vn;
                if(bon.getLeftOperand() instanceof ColumnReference){
                    cr1=(ColumnReference)bon.getLeftOperand();
                }
                if(bon.getRightOperand() instanceof ColumnReference){
                    cr2=(ColumnReference)bon.getRightOperand();
                }
            }else if(vn instanceof UnaryOperatorNode){
                UnaryOperatorNode uon=(UnaryOperatorNode)vn;
                if(uon.getOperand() instanceof ColumnReference){
                    cr1=(ColumnReference)uon.getOperand();
                }
            }

            /* See if any of the CRs need to have their
	         * source level decremented.
			 */
            if(cr1!=null){
                int sourceTable=cr1.getTableNumber();
                cr1.setNestingLevel(cr1.getNestingLevel() - decrement);
                //noinspection ForLoopReplaceableByForEach
                for(int inner=0;inner<tableNumbers.length;inner++){
                    if(tableNumbers[inner]==sourceTable && cr1.getSourceLevel()>0){
                        cr1.setSourceLevel(cr1.getSourceLevel()-decrement);
                        break;
                    }
                }
            }

            if(cr2!=null){
                int sourceTable=cr2.getTableNumber();
                cr2.setNestingLevel(cr2.getNestingLevel() - decrement);
                //noinspection ForLoopReplaceableByForEach
                for(int inner=0;inner<tableNumbers.length;inner++){
                    if(tableNumbers[inner]==sourceTable && cr2.getSourceLevel()>0){
                        cr2.setSourceLevel(cr2.getSourceLevel()-decrement);
                        break;
                    }
                }
            }
        }
    }

    /**
     * Perform transitive closure on join clauses.  For each table in the query,
     * we build a list of equijoin clauses of the form:
     * <ColumnReference> <=> <ColumnReference>
     * Each join clause is put on 2 lists since it joins 2 tables.
     * <p/>
     * We then walk the array of lists.  We first walk it as the outer list.
     * For each equijoin predicate, we assign an equivalence class if it does
     * not yet have one.  We then walk the predicate list (as middle) for the
     * other table, searching for other equijoins with the middle table number
     * and column number.  All such predicates are assigned the same
     * equivalence class. We then walk the predicate list (as inner) for the
     * other side of the middle predicate to see if we can find an equijoin
     * between outer and inner.  If so, then we simply assign it to the same
     * equivalence class.  If not, then we add the new equijoin clause.
     * <p/>
     * Note that an equijoin predicate between two tables CANNOT be
     * used for transitive closure, if either of the tables is in the
     * fromlist for NOT EXISTS. In that case, the join predicate
     * actually specifies that the rows from the indicated table must
     * NOT exist, and therefore those non-existent rows cannot be
     * transitively joined to the other matching tables. See DERBY-3033
     * for a description of a situation in which this actually arises.
     *
     * @param numTables The number of tables in the query
     * @param fromList  The FromList in question.
     * @param cc        The CompilerContext to use
     * @throws StandardException Thrown on error
     */
    void joinClauseTransitiveClosure(int numTables,FromList fromList,CompilerContext cc) throws StandardException{
        // Nothing to do if < 3 tables
        if(fromList.size()<3){
            return;
        }

		/* Create an array of numTables PredicateLists to hold the join clauses. */
        PredicateList[] joinClauses=new PredicateList[numTables];
        for(int index=0;index<numTables;index++){
            joinClauses[index]=new PredicateList();
        }

		/* Pull the equijoin clauses, putting each one in the list for
		 * each of the tables being joined.
		 */
        int size=size();
        for(int index=0;index<size;index++){
            Predicate predicate=elementAt(index);
            ValueNode vn=predicate.getAndNode().getLeftOperand();

            if(!(vn.isBinaryEqualsOperatorNode())){
                continue;
            }

			/* Is this an equijoin clause between 2 ColumnReferences? */
            BinaryRelationalOperatorNode equals=(BinaryRelationalOperatorNode)vn;
            ValueNode left=equals.getLeftOperand();
            ValueNode right=equals.getRightOperand();

            if((left instanceof ColumnReference && right instanceof ColumnReference)){
                ColumnReference leftCR=(ColumnReference)left;
                ColumnReference rightCR=(ColumnReference)right;
                if(leftCR.getSourceLevel()==rightCR.getSourceLevel()
                        && leftCR.getTableNumber()!=rightCR.getTableNumber()
                        && !fromList.tableNumberIsNotExists(leftCR.getTableNumber())
                        && !fromList.tableNumberIsNotExists(rightCR.getTableNumber())){
                    // Add the equijoin clause to each of the lists
                    joinClauses[leftCR.getTableNumber()].addElement(predicate);
                    joinClauses[rightCR.getTableNumber()].addElement(predicate);
                }
            }
        }

		    /* Walk each of the PredicateLists, using each 1 as the starting point
         * of an equivalence class.
		     */
        for(int index=0;index<numTables;index++){
            PredicateList outerJCL=joinClauses[index];

            // Skip the empty lists
            if(outerJCL.size()==0){
                continue;
            }

            /* Put all of the join clauses that already have an equivalence
             * class at the head of the outer list to optimize search.
		     */
            List<Predicate> movePreds=new ArrayList<>();
            for(int jcIndex=outerJCL.size()-1;jcIndex>=0;jcIndex--){
                Predicate predicate=outerJCL.elementAt(jcIndex);
                if(predicate.getEquivalenceClass()!=-1){
                    outerJCL.removeElementAt(jcIndex);
                    movePreds.add(predicate);
                }
            }
            //noinspection ForLoopReplaceableByForEach
            for(int mpIndex=0;mpIndex<movePreds.size();mpIndex++){
                outerJCL.insertElementAt(movePreds.get(mpIndex),0);
            }

            // Walk this list as the outer
            for(int outerIndex=0;outerIndex<outerJCL.size();outerIndex++){
                ColumnReference innerCR=null;
                ColumnReference outerCR;
                int middleTableNumber;
                int outerColumnNumber;
                int middleColumnNumber;
                int outerEC;

                /* Assign an equivalence class to those Predicates
			     * that have not already been assigned an equivalence class.
			     */
                Predicate outerP=outerJCL.elementAt(outerIndex);
                if(outerP.getEquivalenceClass()==-1){
                    outerP.setEquivalenceClass(cc.getNextEquivalenceClass());
                }
                outerEC=outerP.getEquivalenceClass();

                // Get the table and column numbers
                BinaryRelationalOperatorNode equals=(BinaryRelationalOperatorNode)outerP.getAndNode().getLeftOperand();
                ColumnReference leftCR=(ColumnReference)equals.getLeftOperand();
                ColumnReference rightCR=(ColumnReference)equals.getRightOperand();

                if(leftCR.getTableNumber()==index){
                    outerColumnNumber=leftCR.getColumnNumber();
                    middleTableNumber=rightCR.getTableNumber();
                    middleColumnNumber=rightCR.getColumnNumber();
                    outerCR=leftCR;
                }else{
                    outerColumnNumber=rightCR.getColumnNumber();
                    middleTableNumber=leftCR.getTableNumber();
                    middleColumnNumber=leftCR.getColumnNumber();
                    outerCR=rightCR;
                }

        		/* Walk the other list as the middle to find other join clauses
			     * in the chain/equivalence class
				 */
                PredicateList middleJCL=joinClauses[middleTableNumber];
                for(int middleIndex=0;middleIndex<middleJCL.size();middleIndex++){
                    /* Skip those Predicates that have already been
				     * assigned a different equivalence class.
				     */
                    Predicate middleP=middleJCL.elementAt(middleIndex);
                    if(middleP.getEquivalenceClass()!=-1 && middleP.getEquivalenceClass()!=outerEC){
                        continue;
                    }

                    int innerTableNumber;
                    int innerColumnNumber;

                    // Get the table and column numbers
                    BinaryRelationalOperatorNode middleEquals=
                            (BinaryRelationalOperatorNode)middleP.getAndNode().getLeftOperand();
                    ColumnReference mLeftCR=(ColumnReference)middleEquals.getLeftOperand();
                    ColumnReference mRightCR=(ColumnReference)middleEquals.getRightOperand();

                    /* Find the other side of the equijoin, skipping this predicate if
				     * not on middleColumnNumber.
				     */
                    if(mLeftCR.getTableNumber()==middleTableNumber){
                        if(mLeftCR.getColumnNumber()!=middleColumnNumber){
                            continue;
                        }
                        innerTableNumber=mRightCR.getTableNumber();
                        innerColumnNumber=mRightCR.getColumnNumber();
                    }else{
                        if(mRightCR.getColumnNumber()!=middleColumnNumber){
                            continue;
                        }
                        innerTableNumber=mLeftCR.getTableNumber();
                        innerColumnNumber=mLeftCR.getColumnNumber();
                    }

                    // Skip over outerTableNumber.outerColumnNumber = middleTableNumber.middleColumnNumber
                    if(index==innerTableNumber && outerColumnNumber==innerColumnNumber){
                        continue;
                    }

                    // Put this predicate into the outer equivalence class
                    middleP.setEquivalenceClass(outerEC);

					/* Now go to the inner list and see if there is an equijoin
					 * between inner and outer on innerColumnNumber and outerColumnNumber.
					 * If so, then we continue our walk across middle, otherwise we
					 * add a new equijoin to both the inner and outer lists before
					 * continuing to walk across middle.
					 */

                    int newTableNumber;
                    int newColumnNumber;
                    Predicate innerP=null;
                    PredicateList innerJCL=joinClauses[innerTableNumber];
                    int innerIndex=0;
                    for(;innerIndex<innerJCL.size();innerIndex++){
                        innerP=innerJCL.elementAt(innerIndex);

                        // Skip over predicates with other equivalence classes
                        if(innerP.getEquivalenceClass()!=-1 && innerP.getEquivalenceClass()!=outerEC){
                            continue;
                        }

						/* Now we see if the inner predicate completes the loop.
						 * If so, then add it to the outer equivalence class
						 * and stop.
						 */

                        // Get the table and column numbers
                        BinaryRelationalOperatorNode innerEquals=(BinaryRelationalOperatorNode)innerP.getAndNode().getLeftOperand();
                        ColumnReference iLeftCR=(ColumnReference)innerEquals.getLeftOperand();
                        ColumnReference iRightCR=(ColumnReference)innerEquals.getRightOperand();

                        if(iLeftCR.getTableNumber()==innerTableNumber){
                            if(iLeftCR.getColumnNumber()!=innerColumnNumber){
                                continue;
                            }
                            newTableNumber=iRightCR.getTableNumber();
                            newColumnNumber=iRightCR.getColumnNumber();
                            innerCR=iLeftCR;
                        }else{
                            if(iRightCR.getColumnNumber()!=innerColumnNumber){
                                continue;
                            }
                            newTableNumber=iLeftCR.getTableNumber();
                            newColumnNumber=iLeftCR.getColumnNumber();
                            innerCR=iRightCR;
                        }

                        // Did we find the equijoin between inner and outer
                        if(newTableNumber==index && newColumnNumber==outerColumnNumber){
                            break;
                        }
                    }

                    // Did we find an equijoin on inner and outer
                    if(innerIndex!=innerJCL.size()){
                        // match found
                        // Put this predicate into the outer equivalence class
                        innerP.setEquivalenceClass(outerEC);
                        continue;
                    }

                    // No match, add new equijoin
                    // Build a new predicate
                    BinaryRelationalOperatorNode newEquals=(BinaryRelationalOperatorNode)
                            getNodeFactory().getNode(C_NodeTypes.BINARY_EQUALS_OPERATOR_NODE,
                                    outerCR.getClone(),
                                    innerCR.getClone(),
                                    getContextManager());
                    newEquals.bindComparisonOperator();
           					/* Create the AND */
                    ValueNode trueNode=(ValueNode)getNodeFactory().getNode(
                            C_NodeTypes.BOOLEAN_CONSTANT_NODE,
                            Boolean.TRUE,
                            getContextManager());
                    AndNode newAnd=(AndNode)getNodeFactory().getNode(
                            C_NodeTypes.AND_NODE,
                            newEquals,
                            trueNode,
                            getContextManager());
                    newAnd.postBindFixup();
                    // Add a new predicate to both the equijoin clauses and this list
                    JBitSet tableMap=new JBitSet(numTables);
                    newAnd.categorize(tableMap,false);
                    Predicate newPred=(Predicate)getNodeFactory().getNode(
                            C_NodeTypes.PREDICATE,
                            newAnd,
                            tableMap,
                            getContextManager());
                    newPred.setEquivalenceClass(outerEC);
                    addPredicate(newPred);
            				/* Add the new predicate right after the outer position
					           * so that we can follow all of the predicates in equivalence
					           * classes before those that do not yet have equivalence classes.
					           */
                    if(outerIndex!=outerJCL.size()-1){
                        outerJCL.insertElementAt(newPred,outerIndex+1);
                    }else{
                        outerJCL.addElement(newPred);
                    }

                    if(outerJCL!=innerJCL){
                        // DERBY-4387: Avoid adding <t1>.a = <t1>.b twice to
                        // the same predicate list, so do nothing since we
                        // already added predicate to outerJCL above.
                        innerJCL.addElement(newPred);
                    }
                }
            }
        }
    }

    /**
     * Perform transitive closure on search clauses.  We build a
     * list of search clauses of the form:
     * <ColumnReference> <RelationalOperator> [<ConstantNode>]
     * We also build a list of equijoin conditions of form:
     * <ColumnReference1> = <ColumnReference2>
     * where both columns are from different tables in the same query block.
     * For each search clause in the list, we search the equijoin list to see
     * if there is an equijoin clause on the same column.  If so, then we
     * search the search clause list for a search condition on the column
     * being joined against with the same relation operator and constant.  If
     * a match is found, then there is no need to add a new predicate.
     * Otherwise, we add a new search condition on the column being joined
     * with.  In either case, if the relational operator in the search
     * clause is an "=" then we mark the equijoin clause as being redundant.
     * Redundant equijoin clauses will be removed at the end of the search as
     * they are * unnecessary.
     *
     * @param numTables         The number of tables in the query
     * @param hashJoinSpecified Whether or not user specified a hash join
     * @throws StandardException Thrown on error
     */
    void searchClauseTransitiveClosure(int numTables,boolean hashJoinSpecified) throws StandardException{
        PredicateList equijoinClauses=new PredicateList();
        PredicateList searchClauses=new PredicateList();
        RelationalOperator equalsNode=null;

        int size=size();
        for(int index=0;index<size;index++){
            Predicate predicate=elementAt(index);
            AndNode andNode=predicate.getAndNode();

            // Skip anything that's not a RelationalOperator
            if(!predicate.isRelationalOpPredicate()){
                continue;
            }

            RelationalOperator operator=(RelationalOperator)andNode.getLeftOperand();
            // Is this an equijoin?
            if(((ValueNode)operator).isBinaryEqualsOperatorNode()){
                BinaryRelationalOperatorNode equals=(BinaryRelationalOperatorNode)operator;
                // Remember any equals node for redundancy check at end
                equalsNode=equals;
                ValueNode left=equals.getLeftOperand();
                ValueNode right=equals.getRightOperand();
                if((left instanceof ColumnReference && right instanceof ColumnReference)){
                    ColumnReference leftCR=(ColumnReference)left;
                    ColumnReference rightCR=(ColumnReference)right;
                    if(leftCR.getSourceLevel()==rightCR.getSourceLevel() &&
                            leftCR.getTableNumber()!=rightCR.getTableNumber()){
                        equijoinClauses.addElement(predicate);
                    }
                    continue;
                }
            }

            // Is this a usable search clause?
            if(operator instanceof UnaryComparisonOperatorNode){
                if(((UnaryComparisonOperatorNode)operator).getOperand() instanceof ColumnReference){
                    searchClauses.addElement(predicate);
                }
            }else if(operator instanceof BinaryComparisonOperatorNode){
                BinaryComparisonOperatorNode bcon=(BinaryComparisonOperatorNode)operator;
                ValueNode left=bcon.getLeftOperand();
                ValueNode right=bcon.getRightOperand();

                // RESOLVE: Consider using variant type of the expression, instead of
                // ConstantNode or ParameterNode in the future.
                if(left instanceof ColumnReference && isConstantOrParameterNode(right)){
                    searchClauses.addElement(predicate);
                }else if(isConstantOrParameterNode(left) && right instanceof ColumnReference){
                    // put the ColumnReference on the left to simplify things
                    andNode.setLeftOperand(bcon.getSwappedEquivalent());
                    searchClauses.addElement(predicate);
                }
            }
        }

        // Nothing to do if no search clauses or equijoin clauses
        if(equijoinClauses.size()==0 || searchClauses.size()==0){
            return;
        }

    		/* Now we do the real work.
	  	   * NOTE: We can append to the searchClauses while walking
	  	   * them, thus we cannot cache the value of size().
	       */
        for(int scIndex=0;scIndex<searchClauses.size();scIndex++){
            ColumnReference searchCR;
            DataValueDescriptor searchODV=null;
            RelationalOperator ro=(RelationalOperator)searchClauses.elementAt(scIndex).getAndNode().getLeftOperand();

            // Find the ColumnReference and constant value, if any, in the search clause
            if(ro instanceof UnaryComparisonOperatorNode){
                searchCR=(ColumnReference)((UnaryComparisonOperatorNode)ro).getOperand();
            }else{
                searchCR=(ColumnReference)((BinaryComparisonOperatorNode)ro).getLeftOperand();

                // Don't get value for parameterNode since not known yet.
                if(((BinaryComparisonOperatorNode)ro).getRightOperand() instanceof ConstantNode){
                    ConstantNode currCN=(ConstantNode)((BinaryComparisonOperatorNode)ro).getRightOperand();
                    searchODV=currCN.getValue();
                }else searchODV=null;
            }
            // Cache the table and column numbers of searchCR
            int tableNumber=searchCR.getTableNumber();
            int colNumber=searchCR.getColumnNumber();

            // Look for any equijoin clauses of interest
            int ejcSize=equijoinClauses.size();
            for(int ejcIndex=0;ejcIndex<ejcSize;ejcIndex++){
				        /* Skip the current equijoin clause if it has already been used
				         * when adding a new search clause of the same type
				         * via transitive closure.
				         * NOTE: We check the type of the search clause instead of just the
				         * fact that a search clause was added because multiple search clauses
				         * can get added when preprocessing LIKE and BETWEEN.
				         */
                Predicate predicate=equijoinClauses.elementAt(ejcIndex);
                if(predicate.transitiveSearchClauseAdded(ro)){
                    continue;
                }

                BinaryRelationalOperatorNode equals=(BinaryRelationalOperatorNode)
                        predicate.getAndNode().getLeftOperand();
                ColumnReference leftCR=(ColumnReference)equals.getLeftOperand();
                ColumnReference rightCR=(ColumnReference)equals.getRightOperand();
                ColumnReference otherCR;

                if(leftCR.getTableNumber()==tableNumber && leftCR.getColumnNumber()==colNumber){
                    otherCR=rightCR;
                }else if(rightCR.getTableNumber()==tableNumber && rightCR.getColumnNumber()==colNumber){
                    otherCR=leftCR;
                }else{
                    // this is not a matching equijoin clause
                    continue;
                }

        				/* At this point we've found a search clause and an equijoin that
			      	   * are candidates for adding a new search clause via transitive
				         * closure.  Look to see if a matching search clause already
				         * exists on the other table.  If not, then add one.
				         * NOTE: In either case we mark the join clause has having added
				         * a search clause of this type to short circuit any future searches
				         */
                predicate.setTransitiveSearchClauseAdded(ro);

                boolean match=false;
                ColumnReference searchCR2;
                RelationalOperator ro2;
                int scSize=searchClauses.size();
                for(int scIndex2=0;scIndex2<scSize;scIndex2++){
                    DataValueDescriptor currODV=null;
                    ro2=(RelationalOperator)searchClauses.elementAt(scIndex2).getAndNode().getLeftOperand();

                    // Find the ColumnReference in the search clause
                    if(ro2 instanceof UnaryComparisonOperatorNode){
                        searchCR2=(ColumnReference)((UnaryComparisonOperatorNode)ro2).getOperand();
                    }else{
                        searchCR2=(ColumnReference)((BinaryComparisonOperatorNode)ro2).getLeftOperand();
                        if(((BinaryComparisonOperatorNode)ro2).getRightOperand() instanceof ConstantNode){
                            ConstantNode currCN=(ConstantNode)((BinaryComparisonOperatorNode)ro2).getRightOperand();
                            currODV=currCN.getValue();
                        }else currODV=null;
                    }

            				/* Is this a match? A match is a search clause with
					           * the same operator on the same column with a comparison against
					           * the same value.
					           */
                    if(searchCR2.getTableNumber()==otherCR.getTableNumber() &&
                            searchCR2.getColumnNumber()==otherCR.getColumnNumber() &&
                            ((currODV!=null && searchODV!=null && currODV.compare(searchODV)==0) ||
                                    (currODV==null && searchODV==null)) &&
                            ro2.getOperator()==ro.getOperator() &&
                            ro2.getClass().getName().equals(ro.getClass().getName())){
                        match=true;
                        break;
                    }
                }

                // Add the new search clause if no match found
                if(!match){
                    // Build a new predicate
                    RelationalOperator roClone=ro.getTransitiveSearchClause((ColumnReference)otherCR.getClone());

           					/* Set type info for the operator node */
                    if(roClone instanceof BinaryComparisonOperatorNode){
                        ((BinaryComparisonOperatorNode)roClone).bindComparisonOperator();
                    }else{
                        ((UnaryComparisonOperatorNode)roClone).bindComparisonOperator();
                    }

         					  /* Create the AND */
                    ValueNode trueNode=(ValueNode)getNodeFactory().getNode(
                            C_NodeTypes.BOOLEAN_CONSTANT_NODE,
                            Boolean.TRUE,
                            getContextManager());
                    AndNode newAnd=(AndNode)getNodeFactory().getNode(
                            C_NodeTypes.AND_NODE,
                            roClone,
                            trueNode,
                            getContextManager());
                    newAnd.postBindFixup();
                    // Add a new predicate to both the search clauses and this list
                    JBitSet tableMap=new JBitSet(numTables);
                    newAnd.categorize(tableMap,false);
                    Predicate newPred=(Predicate)getNodeFactory().getNode(
                            C_NodeTypes.PREDICATE,
                            newAnd,
                            tableMap,
                            getContextManager());
                    addPredicate(newPred);
                    searchClauses.addElement(newPred);
                }
            }
        }

		    /* Finally, we eliminate any equijoin clauses made redundant by
         * transitive closure, but only if the user did not specify a hash join
         * in the current query block.
		     */
        if(hashJoinSpecified){
            return;
        }

        // If all equijoin predicates are candidate for transitive closure transformation,
        // don't do it, because otherwise all potentially fast hashable join strategies will be ruled out.
        boolean doTransform = false;
        for (int i = 0; i < equijoinClauses.size(); ++i) {
            Predicate predicate=equijoinClauses.elementAt(i);
            if (!predicate.transitiveSearchClauseAdded(equalsNode)) {
                doTransform = true;
                break;
            }
        }

        if (!doTransform) {
            return;
        }
    		/* Walk list backwards since we can delete while
	  	   * traversing the list.
	       */
        for(int index=size()-1;index>=0;index--){
            Predicate predicate=elementAt(index);

            if(predicate.transitiveSearchClauseAdded(equalsNode)){
                removeElementAt(index);
            }
        }
    }

    public void removeAllPredicates() throws StandardException {
        for (int i = size() - 1; i >= 0; i--) {
            removeOptPredicate(i);
        }
    }

    /**
     * Remove redundant predicates.  A redundant predicate has an equivalence
     * class (!= -1) and there are other predicates in the same equivalence
     * class after it in the list.  (Actually, we remove all of the predicates
     * in the same equivalence class that appear after this one.)
     */
    void removeRedundantPredicates(){
    		/* Walk backwards since we may remove 1 or more
	       * elements for each predicate in the outer pass.
		     */
        int outer=size()-1;
        while(outer>=0){
            Predicate predicate=elementAt(outer);
            int equivalenceClass=predicate.getEquivalenceClass();

            if(equivalenceClass==-1){
                outer--;
                continue;
            }

            // Walk the rest of the list backwards.
            for(int inner=outer-1;inner>=0;inner--){
                Predicate innerPredicate=elementAt(inner);
                if(innerPredicate.getEquivalenceClass()==equivalenceClass){
        					  /* Only 1 predicate per column can be marked as a start
				        	   * and/or a stop position.
					           * When removing a redundant predicate, we need to make sure
					           * that the outer predicate gets marked as a start and/or
					           * stop key if the inner predicate is a start and/or stop
					           * key.  In this case, we are not changing the number of
					           * start and/or stop positions, so we leave that count alone.
					           */
                    if(innerPredicate.isStartKey()){
                        predicate.markStartKey();
                    }
                    if(innerPredicate.isStopKey()){
                        predicate.markStopKey();
                    }
                    if(innerPredicate.isStartKey() || innerPredicate.isStopKey()){
                        if(innerPredicate.isQualifier()){
                            // Bug 5868 - Query returns duplicate rows. In order to fix this,
                            // If the inner predicate is a qualifer along with being a start and/or stop,
                            // then mark the outer predicate as a qualifer too(along with marking it as a start
                            // and/or stop) if it is not already marked as qualifer and increment the qualifiers counter
                            // The reason we do this is that a start and/or stop key is not equivalent to
                            // a qualifier. In the orderUsefulPredicates method in this class(comment on line 786),
                            // we mark a start/stop as qualifier if we have already seen a previous column in composite
                            // index whose RELOPS do not include '=' or IS NULL. And hence we should not disregard
                            // the qualifier flag of inner predicate
                            if(!predicate.isQualifier()){
                                predicate.markQualifier();
                                numberOfQualifiers++;
                            }
                        }
                    }
                    /*
				     * If the redundant predicate is a qualifier, then we must
				     * decrement the qualifier count.  (Remaining predicate is
				     * already marked correctly.)
				     */
                    if(innerPredicate.isQualifier()){
                        numberOfQualifiers--;
                    }
                    removeElementAt(inner);
                    outer--;
                }
            }

            outer--;
        }
    }

    @Override
    public void transferPredicates(OptimizablePredicateList otherList,
                                   JBitSet referencedTableMap,
                                   Optimizable table) throws StandardException{
        Predicate predicate;
        PredicateList theOtherList=(PredicateList)otherList;

    		/* Walk list backwards since we can delete while
	       * traversing the list.
		     */
        for(int index=size()-1;index>=0;index--){
            predicate=elementAt(index);

            if(SanityManager.DEBUG){
                if(referencedTableMap.size()!=predicate.getReferencedSet().size()){
                    SanityManager.THROWASSERT(
                            "referencedTableMap.size() ("+referencedTableMap.size()+
                                    ") does not equal predicate.getReferencedSet().size() ("+
                                    predicate.getReferencedSet().size());
                }
            }

            if(referencedTableMap.contains(predicate.getReferencedSet())){
                // We need to keep the counters up to date when removing a predicate
                if(predicate.isStartKey())
                    numberOfStartPredicates--;
                if(predicate.isStopKey())
                    numberOfStopPredicates--;
                if(predicate.isQualifier())
                    numberOfQualifiers--;

        				/* Clear all of the scan flags since they may be different
				         * due to the splitting of the list.
				         */
                predicate.clearScanFlags();
                // Do the actual xfer
                theOtherList.addPredicate(predicate);
                removeElementAt(index);
            }
        }

        // order the useful predicates on the other list
        AccessPath ap=table.getTrulyTheBestAccessPath();
        theOtherList.orderUsefulPredicates(
                table,
                ap.getConglomerateDescriptor(),
                false,
                ap.getNonMatchingIndexScan(),
                ap.getCoveringIndexScan(),
                true);

        // count the start/stop positions and qualifiers
        theOtherList.countScanFlags();
    }

    @Override
    public void transferAllPredicates(OptimizablePredicateList otherList) throws StandardException{
        PredicateList theOtherList=(PredicateList)otherList;

        int size=size();
        for(int index=0;index<size;index++){
            Predicate predicate=elementAt(index);

            /*
		     * Clear all of the scan flags since they may be different
		     * when the new list is re-classified
		     */
            predicate.clearScanFlags();

            // Add the predicate to the other list
            theOtherList.addPredicate(predicate);
        }

        // Remove all of the predicates from this list
        removeAllElements();

    	/*
		 * This list is now empty, so there are no start predicates,
	 	 * stop predicates, or qualifiers.
		 */
        numberOfStartPredicates=0;
        numberOfStopPredicates=0;
        numberOfQualifiers=0;
    }

    @Override
    public void copyPredicatesToOtherList(OptimizablePredicateList otherList) throws StandardException{
        for(int i=0;i<size();i++){
            otherList.addOptPredicate(getOptPredicate(i));
        }
    }

    @Override
    public boolean isRedundantPredicate(int predNum){
        Predicate pred=elementAt(predNum);
        if(pred.getEquivalenceClass()==-1){
            return false;
        }
        for(int index=0;index<predNum;index++){
            if(elementAt(index).getEquivalenceClass()==pred.getEquivalenceClass()){
                return true;
            }
        }
        return false;
    }

    @Override
    public void setPredicatesAndProperties(OptimizablePredicateList otherList) throws StandardException{
        PredicateList theOtherList=(PredicateList)otherList;

        theOtherList.removeAllElements();

        for(int i=0;i<size();i++){
            theOtherList.addOptPredicate(getOptPredicate(i));
        }

        theOtherList.numberOfStartPredicates=numberOfStartPredicates;
        theOtherList.numberOfStopPredicates=numberOfStopPredicates;
        theOtherList.numberOfQualifiers=numberOfQualifiers;
    }

    @Override
    public int startOperator(Optimizable optTable){

        /*
	     * This is the value we will use if there are no keys.  It doesn't
	     * matter what it is, as long as the operator is one of GT or GE
	     * (to match the openScan() interface).
	     */
        int startOperator=ScanController.GT;

	    /* beetle 4572. start operator should be the last start key column's
	     * start operator.  Note that all previous ones should be GE.
	     */
        int size=size();
        for(int index=size-1;index>=0;index--){
            Predicate pred=elementAt(index);

            if(!pred.isStartKey())
                continue;

            startOperator=pred.getStartOperator(optTable);
            break;
        }
        return startOperator;
    }

    @Override
    public void generateStopKey(ExpressionClassBuilderInterface acbi,
                                MethodBuilder mb,
                                Optimizable optTable) throws StandardException{
        ExpressionClassBuilder acb=(ExpressionClassBuilder)acbi;

		/*
		** To make the stop-key allocating function we cycle through
		** the Predicates and generate the function and initializer:
		**
		** private ExecIndexRow exprN()
		** { ExecIndexRow r = getExecutionFactory().getIndexableRow(# stop keys);
		**   for (pred = each predicate in list)
		**	 {
		**		if (pred.isStartKey())
		**		{
		**			pred.generateKey(acb);
		**		}
		**	 }
		** }
		**
		** If there are no start predicates, we do not generate anything.
		*/

        if(numberOfStopPredicates!=0){
    
            int countedStopPreds = 0;
            int size = size();
            int numberOfColumns = 0;
            for (int index = 0; index < size; index++) {
                Predicate pred = elementAt(index);
                if (!pred.isStopKey())
                    continue;
                countedStopPreds++;
                if (pred.isInListProbePredicate()) {
                    InListOperatorNode ilop = pred.getSourceInList(true);
                    if (ilop == null)
                        numberOfColumns++;
                    else
                        numberOfColumns += ilop.leftOperandList.size();
                } else
                    numberOfColumns++;
            }
            assert countedStopPreds == numberOfStopPredicates : "Number of stop predicates does not match";
    
            /* This sets up the method and the static field */
            MethodBuilder exprFun=acb.newExprFun();

		    /* Now we fill in the body of the method */
            LocalField rowField=generateIndexableRow(acb, numberOfColumns);

            int colNum=0;
            for(int index=0;index<size;index++){
                Predicate pred=elementAt(index);

                if(!pred.isStopKey())
                    continue;
    
                int forLim = 1;
                if (pred.isInListProbePredicate()) {
                    InListOperatorNode ilop = pred.getSourceInList(true);
                    if (ilop != null)
                        forLim = ilop.leftOperandList.size();
                }
                AndNode origAndNode = pred.getAndNode();
                for (int i = 0; i < forLim; i++) {
                    // Walk through each binary relational operator in the AND chain
                    // and generate a setColumn method call for each one.
                    generateSetColumn(acb, exprFun, colNum, pred, optTable, rowField, false);
                    if (forLim > 1 && i != (forLim - 1))
                        pred.setAndNode((AndNode) pred.getAndNode().getRightOperand());
                    colNum++;
                }
                if (forLim > 1)
                    pred.setAndNode(origAndNode);
            }

            assert colNum==numberOfColumns: "Number of stop predicates does not match";

            finishKey(acb,mb,exprFun,rowField);
            return;
        }

        mb.pushNull(ClassName.GeneratedMethod);
    }

    @Override
    public int stopOperator(Optimizable optTable){
        int stopOperator;

		/*
		** This is the value we will use if there are no keys.  It doesn't
		** matter what it is, as long as the operator is one of GT or GE
		** (to match the openScan() interface).
		*/
        stopOperator=ScanController.GT;

        int size=size();
	    /* beetle 4572. stop operator should be the last start key column's
	     * stop operator.  Note that all previous ones should be GT.
	     */
        for(int index=size-1;index>=0;index--){
            Predicate pred=elementAt(index);

            if(!pred.isStopKey())
                continue;

            stopOperator=pred.getStopOperator(optTable);
            break;
        }
        return stopOperator;
    }

    private void generateSingleQualifierCode(MethodBuilder consMB,
                                             Optimizable optTable,
                                             boolean absolute,
                                             ExpressionClassBuilder acb,
                                             RelationalOperator or_node,
                                             LocalField qualField,
                                             int array_idx_1,
                                             int array_idx_2) throws StandardException{
        consMB.getField(qualField); // first arg for setQualifier

        // get instance for getQualifier call
        consMB.pushThis();
        consMB.callMethod(VMOpcode.INVOKEVIRTUAL,acb.getBaseClassName(),"getExecutionFactory",ExecutionFactory.MODULE,0);

        // Column Id - first arg
        if(absolute)
            or_node.generateAbsoluteColumnId(consMB,optTable);
        else
            or_node.generateRelativeColumnId(consMB,optTable);

        // Operator - second arg
        or_node.generateOperator(consMB,optTable);

        // Method to evaluate qualifier -- third arg
        or_node.generateQualMethod(acb,consMB,optTable);

        // Receiver for above method - fourth arg
        acb.pushThisAsActivation(consMB);

        // Ordered Nulls? - fifth arg
        or_node.generateOrderedNulls(consMB);


        /*
        ** "Unknown" return value. For qualifiers,
        ** we never want to return rows where the
        ** result of a comparison is unknown.
        ** But we can't just generate false, because
        ** the comparison result could be negated.
        ** So, generate the same as the negation
        ** operand - that way, false will not be
        ** negated, and true will be negated to false.
        */
        or_node.generateNegate(consMB,optTable);

        /* Negate comparison result? */
        or_node.generateNegate(consMB,optTable);

        /* variantType for qualifier's orderable */
        consMB.push(or_node.getOrderableVariantType(optTable));

        int numArgs=10;
        /* Qualifier's string representation */
        consMB.push("TODOJL");

        consMB.callMethod(VMOpcode.INVOKEINTERFACE,ExecutionFactory.MODULE,"getQualifier",ClassName.Qualifier,numArgs);

        // result of getQualifier() is second arg for setQualifier

        consMB.push(array_idx_1);       // third  arg for setQualifier
        consMB.push(array_idx_2);       // fourth arg for setQualifier

        consMB.callMethod(VMOpcode.INVOKESTATIC,acb.getBaseClassName(),"setQualifier","void",4);
    }

    /**
     * generate ORed terms from inlist predicate
     */
    private void generateSingleQualifierCodeForInListElement(MethodBuilder consMB,
                                             ExpressionClassBuilder acb,
                                             int columnPosition,
                                             int storagePosition,
                                             ValueNode valNode,
                                             LocalField qualField,
                                             int array_idx_1,
                                             int array_idx_2) throws StandardException{
        consMB.getField(qualField); // first arg for setQualifier

        // get instance for getQualifier call
        consMB.pushThis();
        consMB.callMethod(VMOpcode.INVOKEVIRTUAL,acb.getBaseClassName(),"getExecutionFactory",ExecutionFactory.MODULE,0);

        // Column Id - first arg

        consMB.push(columnPosition);
        consMB.push(storagePosition);

        // Operator - second arg
        consMB.push(Orderable.ORDER_OP_EQUALS);

        // Method to evaluate qualifier -- third arg
        MethodBuilder qualMethod = acb.newUserExprFun();
        valNode.generateExpression(acb, qualMethod);
        qualMethod.methodReturn();
        qualMethod.complete();
        acb.pushMethodReference(consMB, qualMethod);

        // Receiver for above method - fourth arg
        acb.pushThisAsActivation(consMB);

        // Ordered Nulls? - fifth arg
        consMB.push(false);


        /*
        ** "Unknown" return value. For qualifiers,
        ** we never want to return rows where the
        ** result of a comparison is unknown.
        ** But we can't just generate false, because
        ** the comparison result could be negated.
        ** So, generate the same as the negation
        ** operand - that way, false will not be
        ** negated, and true will be negated to false.
        */
        consMB.push(false);

        /* Negate comparison result? */
        consMB.push(false);

        /* variantType for qualifier's orderable */
        consMB.push(valNode.getOrderableVariantType());

        int numArgs=10;
        /* Qualifier's string representation */
        consMB.push("TODOJL");

        consMB.callMethod(VMOpcode.INVOKEINTERFACE,ExecutionFactory.MODULE,"getQualifier",ClassName.Qualifier,numArgs);

        // result of getQualifier() is second arg for setQualifier

        consMB.push(array_idx_1);       // third  arg for setQualifier
        consMB.push(array_idx_2);       // fourth arg for setQualifier

        consMB.callMethod(VMOpcode.INVOKESTATIC,acb.getBaseClassName(),"setQualifier","void",4);
    }

    /**
     * If there is an IN-list probe predicate in this list then generate
     * the corresponding IN-list values as a DataValueDescriptor array,
     * to be used for probing at execution time.  Also generate a boolean
     * value indicating whether or not the values are already in sorted
     * order.
     * <p/>
     * Assumption is that by the time we get here there is at most one
     * IN-list probe predicate in this list.
     *
     * @param acb The ActivationClassBuilder for the class we're building
     * @param mb  The MethodBuilder for the method we're building
     */
    public void generateInListValues(ExpressionClassBuilder acb,MethodBuilder mb) throws StandardException{
        for(int index=size()-1;index>=0;index--){
            Predicate pred=elementAt(index);

            // Don't do anything if it's not an IN-list probe predicate.
            if(!pred.isInListProbePredicate())
                continue;

        		/* We're going to generate the relevant code for the probe
			       * predicate below, so we no longer need it to be in the
			       * list.  Remove it now.
			       */
            removeOptPredicate(pred);

            InListOperatorNode ilon=pred.getSourceInList();
            /* create a new method to get the probeValues*/
            MethodBuilder getProbeValuesMethod = acb.newExprFun();
            LocalField inlistArray = ilon.generateListAsArray(acb, getProbeValuesMethod);

            getProbeValuesMethod.getField(inlistArray);
            getProbeValuesMethod.methodReturn();
            getProbeValuesMethod.complete();
            acb.pushMethodReference(mb, getProbeValuesMethod);

            if(ilon.sortDescending())
                mb.push(RowOrdering.DESCENDING);
            else if(!ilon.isOrdered()){
                /* If there is no requirement to sort descending and the
			     * IN list values have not already been sorted, then we
		         * sort them in ascending order at execution time.
		         */
                mb.push(RowOrdering.ASCENDING);
            }else{
		        /* DONTCARE here means we don't have to sort the IN
		         * values at execution time because we already did
		         * it as part of compilation (esp. preprocessing).
		         * This can only be the case if all values in the IN
		         * list are literals (as opposed to parameters).
		         */
                mb.push(RowOrdering.DONTCARE);
            }

            /* With DB-6231's enhancmenet, inlist may not be the first column in the index or primary key,
               so push the column position of the inlist column in the index or primary key */
            int inlistPosition = pred.getIndexPosition();
            assert inlistPosition >= 0: "inlistPosition of " + inlistPosition + " for MultiProbeScan is not expected";
            mb.push(inlistPosition);

            /* genereate an array of type descriptors for the inlist columns */
            DataTypeDescriptor[] typeArray = new DataTypeDescriptor[ilon.getLeftOperandList().size()];
            for (int i = 0; i < ilon.getLeftOperandList().size(); i++) {
                typeArray[i] = ((ColumnReference)(ilon.getLeftOperandList().elementAt(i))).getTypeServices();
            }
            FormatableArrayHolder typeArrayHolder = new FormatableArrayHolder(typeArray);
            int typeArrayItem = acb.addItem(typeArrayHolder);
            mb.push(typeArrayItem);

            return;
        }

	    /* If we get here then we didn't find any probe predicates.  But
	     * if that's true then we shouldn't have made it to this method
	     * to begin with.
	     */
        if(SanityManager.DEBUG){
            SanityManager.THROWASSERT("Attempted to generate IN-list values"+
                    "for multi-probing but no probe predicates were found.");
        }
    }

    @Override
    public void generateQualifiers(ExpressionClassBuilderInterface acbi,
                                   MethodBuilder mb,
                                   Optimizable optTable,
                                   boolean absolute) throws StandardException{
        generateProbeQualifiers(acbi,mb,optTable,absolute);
    }

    public void generateProbeQualifiers(ExpressionClassBuilderInterface acbi,
                                        MethodBuilder mb,
                                        Optimizable optTable,
                                        boolean absolute) throws StandardException{
        ExpressionClassBuilder acb=(ExpressionClassBuilder)acbi;

        String retvalType=ClassName.Qualifier+"[][]";
        MethodBuilder consMB=acb.getConstructor();
        MethodBuilder executeMB=acb.getExecuteMethod();

	    /* Create and initialize the array of Qualifiers */
        LocalField qualField=acb.newFieldDeclaration(Modifier.PUBLIC,retvalType); // made it public... - JL
        mb.push("e"+(((ExpressionClassBuilder)acbi).getNextFieldNum()-1));

		/* 
		** Stick a reinitialize of the Qualifier array in execute().
		** Done because although we call Exec/Qualifier.clearOrderableCache() 
		** before each query, we only clear the cache for VARIANT and
		** SCAN_INVARIANT qualifiers.  However, each time the same
		** statement is executed, even the QUERY_INVARIANT qualifiers
		** need to be flushed.  For example:
		**	prepare select c1 from t where c1 = (select max(c1) from t) as p;
		**	execute p; -- we now have the materialized subquery result (1) 
		**			   -- in our predicate
		**	insert into t values 666;
		**	execute p; -- we need to clear out 1 and recache the subq result
		*/

        // generate code to reinitializeQualifiers(Qualifier[][] qualifiers)
        executeMB.getField(qualField); // first arg to reinitializeQualifiers()
        executeMB.callMethod(
                VMOpcode.INVOKESTATIC,
                acb.getBaseClassName(),"reinitializeQualifiers","void",1);

		/*
		** Initialize the Qualifier array to a new Qualifier[][] if
		** there are any qualifiers.  It is automatically initialized to
		** null if it isn't explicitly initialized.
		*/
        if(numberOfQualifiers!=0){
            if(SanityManager.DEBUG){
                if(numberOfQualifiers>size()){
                    SanityManager.THROWASSERT(
                            "numberOfQualifiers("+numberOfQualifiers+
                                    ") > size("+size()+")."+":"+this.hashCode());
                }
            }

            // Determine number of leading AND qualifiers, and subsequent
            // trailing OR qualifiers.
            int num_of_or_conjunctions=0;
            int numExtraInListColumns=0;
            for(int i=0;i<numberOfQualifiers;i++){
                if(elementAt(i).isOrList()){
                    num_of_or_conjunctions++;
                }
                else
                if (elementAt(i).isInListProbePredicate()) {
                    InListOperatorNode ilop = elementAt(i).getSourceInList(true);
                    if (ilop != null)
                        numExtraInListColumns += (ilop.leftOperandList.size()-1);
                }
            }

            
	        /* Assign the initializer to the Qualifier[] field */
            consMB.pushNewArray(ClassName.Qualifier+"[]",num_of_or_conjunctions+1);
            consMB.setField(qualField);

            // Allocate qualifiers[0] which is an entry for each of the leading
            // AND clauses.

            consMB.getField(qualField);             // 1st arg allocateQualArray
            consMB.push(0);                   // 2nd arg allocateQualArray
            consMB.push(numberOfQualifiers+numExtraInListColumns-num_of_or_conjunctions);  // 3rd arg allocateQualArray

            consMB.callMethod(VMOpcode.INVOKESTATIC,acb.getBaseClassName(),"allocateQualArray","void",3);
        }

		/* Sort the qualifiers by "selectivity" before generating.
		 * We want the qualifiers ordered by selectivity with the
		 * most selective ones first.  There are 3 groups of qualifiers:
		 * = and IS NULL are the most selective,
		 * <> and IS NOT NULL are the least selective and
		 * all of the other RELOPs are in between.
		 * We break the list into 4 parts (3 types of qualifiers and
		 * then everything else) and then rebuild the ordered list.
		 * RESOLVE - we will eventually want to order the qualifiers
		 * by (column #, selectivity) once the store does just in time
		 * instantiation.
		 */
        if(numberOfQualifiers>0){
            orderQualifiers();
        }

		    /* Generate each of the qualifiers, if any */

        // First generate the "leading" AND qualifiers.
        int qualNum=0;
        int size=size();
        boolean gotOrQualifier=false;

        for(int index=0;index<size;index++){
            Predicate pred=elementAt(index);

            if(pred.isQualifier()){
                if(pred.isOrList()){
                    gotOrQualifier=true;

                    // will generate the OR qualifiers below.
                    break;
                }else{
                    int forLim = 1;
                    
                    if (pred.isInListProbePredicate()) {
                        InListOperatorNode ilop = pred.getSourceInList(true);
                        if (ilop != null)
                            forLim = ilop.leftOperandList.size();
                    }
                    AndNode origAndNode = pred.getAndNode();
                    for (int i = 0; i < forLim; i++) {
                        // Walk through each binary relational operator in the AND chain
                        // and generate qualField for each one.
                        generateSingleQualifierCode(
                            consMB,
                            optTable,
                            absolute,
                            acb,
                            pred.getRelop(),
                            qualField,
                            0,
                            qualNum);
    
                        qualNum++;
                        if (forLim > 1 && i != (forLim - 1))
                            pred.setAndNode((AndNode) pred.getAndNode().getRightOperand());
                    }
                    if (forLim > 1)
                        pred.setAndNode(origAndNode);
                }
            }
        }

        if(gotOrQualifier){

            // process each set of or's into a list which are AND'd.  Each
            // predicate will become an array list in the qualifier array of
            // array's.
            //
            // The first list of And's went into qual[0][0...N]
            // Now each subquent predicate is actually a list of OR's so
            // will be passed as:
            //     1st OR predicate -> qual[1][0.. number of OR terms]
            //     2nd OR predicate -> qual[2][0.. number of OR terms]
            //     ...
            //
            int and_idx=1;

            // The remaining qualifiers must all be OR predicates, which
            // are pushed slightly differently than the leading AND qualifiers.

            for(int index=qualNum;index<size;index++,and_idx++){
                Predicate pred=elementAt(index);

                if(SanityManager.DEBUG){
                    SanityManager.ASSERT(pred.isOrList());
                }

                if (!(pred.getAndNode().getLeftOperand() instanceof InListOperatorNode)) {
                    // create an ArrayList of the OR nodes.  We need the count
                    // of Or's in order to first generate the allocateQualArray()
                    // call, then we walk the list assigning each of the OR's to
                    // entries in the array in generateSingleQualifierCode().
                    List<ValueNode> a_list = new ArrayList<>();

                    QueryTreeNode node = pred.getAndNode().getLeftOperand();

                    while (node instanceof OrNode) {
                        OrNode or_node = (OrNode) node;

                        // The left operand of OR node is one of the terms,
                        // (ie. A = 1)
                        if (or_node.getLeftOperand() instanceof RelationalOperator) {
                            a_list.add(or_node.getLeftOperand());
                        }

                        // The next OR node in the list if linked to the right.
                        node = or_node.getRightOperand();
                    }

                    // Allocate an array to hold each of the terms of this OR,
                    // clause.  ie. (a = 1 or b = 2), will allocate a 2 entry array.

                    consMB.getField(qualField);        // 1st arg allocateQualArray
                    consMB.push(and_idx);        // 2nd arg allocateQualArray
                    consMB.push(a_list.size());  // 3rd arg allocateQualArray

                    consMB.callMethod(VMOpcode.INVOKESTATIC, acb.getBaseClassName(), "allocateQualArray", "void", 3);


                    // finally transfer the nodes to the 2-d qualifier
                    for (int i = 0; i < a_list.size(); i++) {
                        generateSingleQualifierCode(
                                consMB,
                                optTable,
                                absolute,
                                acb,
                                (RelationalOperator) a_list.get(i),
                                qualField,
                                and_idx,
                                i);

                    }
                } else {
                    // handle inlist
                    // inlist getting here cannot be used for multi-probe scan and
                    // Scans.qualifyRecordFromRow() does not know how to evaluate it. So convert it back to the
                    // equivalent ORed.
                    InListOperatorNode inListNode = (InListOperatorNode)pred.getAndNode().getLeftOperand();

                    ValueNodeList inlistVals = inListNode.getRightOperandList();
                    consMB.getField(qualField);        // 1st arg allocateQualArray
                    consMB.push(and_idx);        // 2nd arg allocateQualArray
                    consMB.push(inlistVals.size());  // 3rd arg allocateQualArray

                    consMB.callMethod(VMOpcode.INVOKESTATIC, acb.getBaseClassName(), "allocateQualArray", "void", 3);

                    // get inlist column
                    // TODO-msirek Can't currently push an OR'ed multicolumn in list qualifier.
                    //  Need to handle all operands in leftOperandList if we support
                    //  this in the future.
                    ColumnReference cr = (ColumnReference)inListNode.getLeftOperand().getHashableJoinColumnReference().get(0);

                    ConglomerateDescriptor bestCD = optTable.getTrulyTheBestAccessPath().
                            getConglomerateDescriptor();
                    /*
		            ** Column positions are one-based, store is zero-based.
		            */
                    int columnPosition;
                    int storagePosition;

		            /*
		            ** If it's an index, find the base column position in the index
		            ** and translate it to an index column position.
		            */
                    if(bestCD!=null && bestCD.isIndex()){
                        columnPosition=cr.getSource().getColumnPosition();
                        columnPosition=bestCD.getIndexDescriptor().
                                getKeyColumnPosition(columnPosition);
                        storagePosition = columnPosition;

                        if(SanityManager.DEBUG){
                            SanityManager.ASSERT(columnPosition>0,
                                    "Base column not found in index");
                        }
                    } else {
                        columnPosition=cr.getSource().getColumnPosition();
                        storagePosition = cr.getSource().getStoragePosition();
                    }
                    // get 0-based posistion
                    columnPosition = columnPosition - 1;
                    storagePosition = storagePosition - 1;

                    if (!absolute) {
                        columnPosition = optTable.convertAbsoluteToRelativeColumnPosition(columnPosition);
                        storagePosition = optTable.convertAbsoluteToRelativeColumnPosition(storagePosition);
                    }

                    // transfer inlist to a bunch of "col=val" qualifiers
                    for (int i=0; i< inlistVals.size(); i++) {
                        generateSingleQualifierCodeForInListElement(consMB,
                                                                    acb,
                                                                    columnPosition,
                                                                    storagePosition,
                                                                    (ValueNode)inlistVals.elementAt(i),
                                                                    qualField,
                                                                    and_idx,
                                                                    i);
                    }
                }

                qualNum++;
            }

        }

        //assert qualNum==numberOfQualifiers: qualNum+" Qualifiers found, "+ numberOfQualifiers+" expected.";
    }


    /* Sort the qualifiers by "selectivity" before generating.
     * We want the qualifiers ordered by selectivity with the
     * most selective ones first.  There are 3 groups of qualifiers:
     * = and IS NULL are the most selective,
     * <> and IS NOT NULL are the least selective and
     * all of the other RELOPs are in between.
     * We break the list into 4 parts (3 types of qualifiers and
     * then everything else) and then rebuild the ordered list.
     * RESOLVE - we will eventually want to order the qualifiers
     * by (column #, selectivity) once the store does just in time
     * instantiation.
     */
    private static final int QUALIFIER_IN_LIST = 0;
    private static final int QUALIFIER_ORDER_EQUALS=1;
    private static final int QUALIFIER_ORDER_OTHER_RELOP=2;
    private static final int QUALIFIER_ORDER_NOT_EQUALS=3;
    private static final int QUALIFIER_ORDER_NON_QUAL=4;
    private static final int QUALIFIER_ORDER_OR_CLAUSE=5;
    private static final int QUALIFIER_NUM_CATEGORIES=6;

    private void orderQualifiers() throws StandardException{
        // Sort the predicates into buckets, sortList[0] is the most 
        // selective, while sortList[4] is the least restrictive.
        //
        //     sortList[0]:  "= and IS NULL"
        //     sortList[1]:  "a set of OR'd conjunctions"
        //     sortList[2]:  "all other relop's"
        //     sortList[3]:  "<> and IS NOT NULL"
        //     sortList[4]:  "everything else"
        PredicateList[] sortList=new PredicateList[QUALIFIER_NUM_CATEGORIES];

        for(int i=sortList.length-1;i>=0;i--)
            sortList[i]=new PredicateList();

        int predIndex;
        int size=size();
        for(predIndex=0;predIndex<size;predIndex++){
            Predicate pred=elementAt(predIndex);

            if(!pred.isQualifier()){
                sortList[QUALIFIER_ORDER_NON_QUAL].addElement(pred);
                continue;
            }

            AndNode node=pred.getAndNode();

            if(!(pred.isOrList())){
                RelationalOperator relop=(RelationalOperator)node.getLeftOperand();

                int op=relop.getOperator();

                switch(op){
                    case RelationalOperator.EQUALS_RELOP:
                        BinaryRelationalOperatorNode brelop = (BinaryRelationalOperatorNode)relop;

                        if (brelop.getInListOp() != null) {
                            sortList[QUALIFIER_IN_LIST].addElement(pred);
                        }
                        else {
                            sortList[QUALIFIER_ORDER_EQUALS].addElement(pred);
                        }
                        break;
                    case RelationalOperator.IS_NULL_RELOP:
                        sortList[QUALIFIER_ORDER_EQUALS].addElement(pred);
                        break;
                    case RelationalOperator.NOT_EQUALS_RELOP:
                    case RelationalOperator.IS_NOT_NULL_RELOP:
                        sortList[QUALIFIER_ORDER_NOT_EQUALS].addElement(pred);
                        break;
                    default:
                        sortList[QUALIFIER_ORDER_OTHER_RELOP].addElement(pred);
                }
            }else{
                sortList[QUALIFIER_ORDER_OR_CLAUSE].addElement(pred);
            }
        }

	    /* Rebuild the list */
        predIndex=0;

        for(int index=0;index<QUALIFIER_NUM_CATEGORIES;index++){
            for(int items=0;items<sortList[index].size();items++){
                setElementAt(sortList[index].elementAt(items),predIndex++);
            }
        }
    }

    @Override
    public void generateStartKey(ExpressionClassBuilderInterface acbi,
                                 MethodBuilder mb,
                                 Optimizable optTable) throws StandardException{
        ExpressionClassBuilder acb=(ExpressionClassBuilder)acbi;

		/*
		** To make the start-key allocating function we cycle through
		** the Predicates and generate the function and initializer:
		**
		** private Object exprN()
		** { ExecIndexRow r = getExecutionFactory().getIndexableRow(# start keys);
		**   for (pred = each predicate in list)
		**	 {
		**		if (pred.isStartKey())
		**		{
		**			pred.generateKey(acb);
		**		}
		**	 }
		** }
		**
		** If there are no start predicates, we do not generate anything.
		*/

		int countedStartPreds = 0;
        if(numberOfStartPredicates!=0){
	        /* This sets up the method and the static field */
            MethodBuilder exprFun=acb.newExprFun();
            
            int size = size();
            int numberOfColumns = 0;
            for (int index = 0; index < size; index++) {
                Predicate pred = elementAt(index);
    
                if (!pred.isStartKey())
                    continue;
    
                countedStartPreds++;
                if (pred.isInListProbePredicate()) {
                    InListOperatorNode ilop = pred.getSourceInList(true);
                    if (ilop == null)
                        numberOfColumns++;
                    else
                        numberOfColumns += ilop.leftOperandList.size();
                }
                else
                    numberOfColumns++;
            }
            assert countedStartPreds == numberOfStartPredicates : "Number of start predicates does not match";
            
		    /* Now we fill in the body of the method */
            LocalField rowField=generateIndexableRow(acb, numberOfColumns);

            int colNum=0;
            for(int index=0;index<size;index++){
                Predicate pred=elementAt(index);

                if(!pred.isStartKey())
                    continue;
    
                int forLim = 1;
                if (pred.isInListProbePredicate()) {
                    InListOperatorNode ilop = pred.getSourceInList(true);
                    if (ilop != null)
                        forLim = ilop.leftOperandList.size();
                }
                AndNode origAndNode = pred.getAndNode();
                for (int i = 0; i < forLim; i++) {
                    // Walk through each binary relational operator in the AND chain
                    // and generate a setColumn method call for each one.
                    generateSetColumn(acb, exprFun, colNum, pred, optTable, rowField, true);
                    if (forLim > 1 && i != (forLim-1))
                        pred.setAndNode((AndNode)pred.getAndNode().getRightOperand());
                    colNum++;
                }
                if (forLim > 1)
                    pred.setAndNode(origAndNode);
            }

            assert colNum== numberOfColumns: "Number of start predicates does not match";

            finishKey(acb,mb,exprFun,rowField);
            return;
        }

        mb.pushNull(ClassName.GeneratedMethod);
    }

    @Override
    public boolean sameStartStopPosition() throws StandardException{
		  /* We can only use the same row for both the
		   * start and stop positions if the number of
		   * start and stop predicates are the same.
		   */
        if(numberOfStartPredicates!=numberOfStopPredicates){
            return false;
        }

   		/* We can only use the same row for both the
	  	 * start and stop positions when a predicate is
	  	 * a start key iff it is a stop key.
	  	 */
        int size=size();
        for(int index=0;index<size;index++){
            Predicate pred=elementAt(index);

            if((pred.isStartKey() && (!pred.isStopKey())) ||
                    (pred.isStopKey() && (!pred.isStartKey()))){
                return false;
            }
            // "in"'s dynamic start and stop key are not the same, beetle 3858
            if(pred.getAndNode().getLeftOperand() instanceof InListOperatorNode)
                return false;
        }

        return true;
    }

    /**
     * Generate the indexable row for a start key or stop key.
     *
     * @param acb             The ActivationClassBuilder for the class we're building
     * @param numberOfColumns The number of columns in the key
     * @return The field that holds the indexable row
     */
    public static LocalField generateIndexableRow(ExpressionClassBuilder acb,int numberOfColumns){
        MethodBuilder mb=acb.getConstructor();
    		/*
	  	   * Generate a call to get an indexable row
	  	   * with the given number of columns
	  	   */
        acb.pushGetExecutionFactoryExpression(mb); // instance
        mb.push(numberOfColumns);
        mb.callMethod(VMOpcode.INVOKEINTERFACE,ClassName.ExecutionFactory,"getIndexableRow",ClassName.ExecIndexRow,1);

		    /*
		     * Assign the indexable row to a field, and put this assignment into
		     * the constructor for the activation class.  This way, we only have
		     * to get the row once.
		     */
        LocalField field=acb.newFieldDeclaration(Modifier.PRIVATE,ClassName.ExecIndexRow);

        mb.setField(field);

        return field;
    }
    
    public static void generateValueRowOnStack(ExpressionClassBuilder acb, int numberOfColumns) {
        /*
         * Generate a call to get an indexable row
         * with the given number of columns
         */
        MethodBuilder mb = acb.getConstructor();
        acb.pushGetExecutionFactoryExpression(mb); // instance
        mb.push(numberOfColumns);
        mb.callMethod(VMOpcode.INVOKEINTERFACE, ClassName.ExecutionFactory, "getValueRow", ClassName.ExecRow, 1);
    }
    
    public static LocalField generateValueRow(ExpressionClassBuilder acb, int numberOfColumns) {
        /*
         * Generate a call to get an indexable row
         * with the given number of columns
         */
        MethodBuilder mb = acb.getConstructor();
        acb.pushGetExecutionFactoryExpression(mb); // instance
        mb.push(numberOfColumns);
        mb.callMethod(VMOpcode.INVOKEINTERFACE, ClassName.ExecutionFactory, "getValueRow", ClassName.ExecRow, 1);
        LocalField field = acb.newFieldDeclaration(Modifier.PRIVATE, ClassName.ExecRow);
    
        mb.setField(field);
    
        return field;
    }
    
    
    /**
     * Generate a new ListDataType with "numberOfValues" values and assign it to a field
     *
     * @param acb            The ActivationClassBuilder for the class we're building
     * @param numberOfValues The number of values in grouped data type.
     * @return The field that holds the list data
     */
    public static LocalField generateListData(ExpressionClassBuilder acb, int numberOfValues) {
    
        MethodBuilder mb = acb.getConstructor();
        acb.pushGetExecutionFactoryExpression(mb); // instance
        mb.push(numberOfValues);
        mb.callMethod(VMOpcode.INVOKEINTERFACE, ClassName.ExecutionFactory, "getListData", ClassName.ListDataType, 1);
        /*
         * Assign the ListDataType to a field, and put this assignment into
         * the constructor for the activation class.  This way, we only have
         * to get the row once.
         */
        LocalField field = acb.newFieldDeclaration(Modifier.PRIVATE, ClassName.ListDataType);
    
        mb.setField(field);
    
        return field;
    }
    
    /**
     * Generate a new ListDataType with "numberOfValues" values and place it on the stack.
     *
     * @param acb            The ActivationClassBuilder for the class we're building
     * @param numberOfValues The number of values in grouped data type.
     * @return The field that holds the list data
     */
    public static void generateListDataOnStack(ExpressionClassBuilder acb, int numberOfValues) {
    
        MethodBuilder mb = acb.getConstructor();
        acb.pushGetExecutionFactoryExpression(mb); // instance
        mb.push(numberOfValues);
        mb.callMethod(VMOpcode.INVOKEINTERFACE, ClassName.ExecutionFactory, "getListData", ClassName.ListDataType, 1);
    }
    
    /**
     * Generate a new ListDataType with "numberOfValues" values and place it on the stack.
     *
     * @param acb            The ActivationClassBuilder for the class we're building
     * @param numberOfValues The number of values in grouped data type.
     * @param mb             The method build whose stack to place the ListDataType on.
     * @return The field that holds the list data
     */
    public static void generateListDataOnStack(ExpressionClassBuilder acb, int numberOfValues, MethodBuilder mb) {
        
        acb.pushGetExecutionFactoryExpression(mb); // instance
        mb.push(numberOfValues);
        mb.callMethod(VMOpcode.INVOKEINTERFACE, ClassName.ExecutionFactory, "getListData", ClassName.ListDataType, 1);
    }
    
    
    /**
     * Generate the code to set the value from a predicate in an index column.
     *
     * @param acb          The ActivationClassBuilder for the class we're building
     * @param exprFun      The MethodBuilder for the method we're building
     * @param columnNumber The position number of the column we're setting
     *                     the value in (zero-based)
     * @param pred         The Predicate with the value to put in the index column
     * @param optTable     The Optimizable table the column is in
     * @param rowField     The field that holds the indexable row
     * @param isStartKey   Are we generating start or stop key?  This information
     *                     is useful for "in"'s dynamic start/stop key, bug 3858
     * @throws StandardException Thrown on error
     */
    private void generateSetColumn(ExpressionClassBuilder acb,
                                   MethodBuilder exprFun,
                                   int columnNumber,
                                   Predicate pred,
                                   Optimizable optTable,
                                   LocalField rowField,
                                   boolean isStartKey) throws StandardException{
        MethodBuilder mb;
		
		    /* Code gets generated in constructor if comparison against
		     * a constant, otherwise gets generated in the current
		     * statement block.
		     */
        boolean withKnownConstant=false;
        if(pred.compareWithKnownConstant(optTable,false)){
            withKnownConstant=true;
            mb=acb.getConstructor();
        }else{
            mb=exprFun;
        }

        int[] baseColumns;
        boolean[] isAscending;
        if (pred.isRowId()) {
            baseColumns = new int[1];
            baseColumns[0] = 1;

            isAscending = new boolean[1];
            isAscending[0] = true;
        }
        else {
            baseColumns = optTable.getTrulyTheBestAccessPath()
                    .getConglomerateDescriptor()
                    .getIndexDescriptor().baseColumnPositions();
            isAscending = optTable.getTrulyTheBestAccessPath().
                    getConglomerateDescriptor().
                    getIndexDescriptor().isAscending();
        }
		/* If the predicate is an IN-list probe predicate then we are
		 * using it as a start/stop key "placeholder", to be over-ridden
		 * at execution time.  Put differently, we want to generate
		 * "column = ?" as a start/stop key and then use the "?" value
		 * as a placeholder into which we'll plug the various IN values
		 * at execution time.
		 *
		 * In that case "isIn" will be false here, which is fine: there's
		 * no need to generate dynamic start/stop keys like we do for
		 * "normal" IN lists because we're just using the key as a place-
		 * holder.  So by generating the probe predicate ("column = ?")
		 * as a normal one-sided start/stop key, we get our requisite
		 * execution-time placeholder and that's that.  For more on how
		 * we use this "placeholder", see MultiProbeTableScanResultSet.
		 *
		 * Note that we generate the corresponding IN-list values
		 * separately (see generateInListValues() in this class).
		 */
        boolean isIn=pred.getAndNode().getLeftOperand() instanceof InListOperatorNode;

    		/*
	       * Generate statements of the form
		     *
		     * r.setColumn(columnNumber, columnExpression);
		     *
		     * and put the generated statement in the allocator function.
		     */
        mb.getField(rowField);
        mb.push(columnNumber+1);

        // second arg
        if(isIn){
            pred.getSourceInList().generateStartStopKey(isAscending[columnNumber],isStartKey,acb,mb);
        }else
            pred.generateExpressionOperand(optTable,baseColumns[columnNumber],acb,mb);

        mb.upCast(ClassName.DataValueDescriptor);

        mb.callMethod(VMOpcode.INVOKEINTERFACE,ClassName.Row,"setColumn","void",2);

    		/* Also tell the row if this column uses ordered null semantics */
        if(!isIn){
            RelationalOperator relop=pred.getRelop();
            assert relop != null;
            boolean setOrderedNulls=relop.orderedNulls();

			      /* beetle 4464, performance work.  If index column is not nullable
             * (which is frequent), we should treat it as though nulls are 
             * ordered (indeed because they don't exist) so that we do not have
             * to check null at scan time for each row, each column.  This is 
             * an overload to "is null" operator, so that we have less overhead,
             * provided that they don't interfere.  It doesn't interfere if it 
             * doesn't overload if key is null.  If key is null, but operator
			       * is not orderedNull type (is null), skipScan will use this flag
             * (false) to skip scan.
			       */
            if((!setOrderedNulls) && !relop.getColumnOperand(optTable).getTypeServices().isNullable()){
                if(withKnownConstant)
                    setOrderedNulls=true;
                else{
                    ValueNode keyExp=
                            relop.getExpressionOperand(optTable.getTableNumber(),baseColumns[columnNumber],(FromTable)optTable);

                    if(keyExp instanceof ColumnReference)
                        setOrderedNulls=!keyExp.getTypeServices().isNullable();
                }
            }
            if(setOrderedNulls){
                mb.getField(rowField);
                mb.push(columnNumber);
                mb.callMethod(VMOpcode.INVOKEINTERFACE,ClassName.ExecIndexRow,"orderedNulls","void",1);
            }
        }
    }

    /**
     * Finish generating a start or stop key
     *
     * @param acb      The ActivationClassBuilder for the class we're building
     * @param exprFun  The MethodBuilder for the method we're building
     * @param rowField The name of the field that holds the indexable row
     */
    public static void finishKey(ExpressionClassBuilder acb, MethodBuilder mb, MethodBuilder exprFun, LocalField rowField){
		/* Generate return statement and add to exprFun */
        exprFun.getField(rowField);
        exprFun.methodReturn();
		/* We are done putting stuff in exprFun */
        exprFun.complete();

	    /*
	     * What we use is the access of the static field,
	     * i.e. the pointer to the method.
	     */
        acb.pushMethodReference(mb,exprFun);
    }

    boolean constantColumn(ColumnReference colRef){
        return constantColumn(colRef.getTableNumber(), colRef.getColumnNumber());
    }

    /* Class implementation */
    boolean constantColumn(int tableNum, int colNum){
        boolean retval=false;

		  /*
		   * Walk this list
		   */
        int size=size();
        for(int index=0;index<size;index++){
            Predicate pred=elementAt(index);
            RelationalOperator relop=pred.getRelop();

            if(pred.isRelationalOpPredicate()){
                assert relop != null;
                if(relop.getOperator()==RelationalOperator.EQUALS_RELOP){
                    ValueNode exprOp=relop.getOperand(tableNum, colNum, pred.getReferencedSet().size(), true );

                    if(exprOp!=null){
                        if(exprOp.isConstantExpression()){
                            retval=true;
                            break;
                        }
                    }
                }else if(relop.getOperator()==RelationalOperator.IS_NULL_RELOP){
                    ColumnReference columnOp=
                            (ColumnReference)relop.getOperand(tableNum, colNum, pred.getReferencedSet().size(), false );

                    if(columnOp!=null){
                        retval=true;
                    }
                }
            }
        }

        return retval;
    }
    /**
     * @see OptimizablePredicateList#adjustForSortElimination
     * <p/>
     * Currently this method only accounts for IN list multi-probing
     * predicates (DERBY-3279).
     */
    @SuppressWarnings("ConstantConditions")
    @Override
    public void adjustForSortElimination(RequiredRowOrdering ordering) throws StandardException{
        // Nothing to do if there's no required ordering.
        if(ordering==null)
            return;

		/* Walk through the predicate list and search for any
		 * multi-probing predicates.  If we find any which
		 * operate on a column that is part of the received
		 * ORDER BY, then check to see if the ORDER BY requires
		 * a DESCENDING sort.  If so, then we must take note
		 * of this requirement so that the IN list values for
		 * the probe predicate are sorted in DESCENDING order
		 * at execution time.
		 */
        int size=size();
        OrderByList orderBy=(OrderByList)ordering;
        for(int index=0;index<size;index++){
            Predicate pred=elementAt(index);
            if(!pred.isInListProbePredicate())
                continue;

            BinaryRelationalOperatorNode bron=(BinaryRelationalOperatorNode)pred.getRelop();

            assert bron != null;
            if(orderBy.requiresDescending((ColumnReference)bron.getLeftOperand(),pred.getReferencedSet().size())){
                pred.getSourceInList(true).markSortDescending();
            }
        }
    }

    @Override
    public String toString() {
        return OperatorToString.toString(this);
    }

    /* assign a weight to each predicate-- the maximum weight that a predicate
     * can have is numUsefulPredicates. If a predicate corresponds to the first
     * index position then its weight is numUsefulPredicates. The weight of a
     * pwlist is the sum of the weights of individual predicates.
     */
    private void calculateWeight(PredicateWrapperList[] pwList,int numUsefulPredicates){
        int[] s=new int[numUsefulPredicates];

        for(PredicateWrapperList pwL : pwList){
            if(pwL==null)
                continue;

            for(int j=0;j<pwL.size();j++){
                s[pwL.elementAt(j).getPredicateID()]+=(numUsefulPredicates-j);
            }
        }

        for(PredicateWrapperList pwL : pwList){
            int w=0;

            if(pwL==null)
                continue;

            for(int j=0;j<pwL.size();j++){
                w+=s[pwL.elementAt(j).getPredicateID()];
            }
            pwL.setWeight(w);
        }
    }

    /**
     * choose the statistic which has the maximum match with the predicates.
     * value is returned in ret.
     */
    private int chooseLongestMatch(PredicateWrapperList[] predArray,List<Predicate> ret,int numWorkingPredicates){
        int max=0, maxWeight=0;
        int position=-1;

        for(int i=0;i<predArray.length;i++){
            if(predArray[i]==null)
                continue;

            if(predArray[i].uniqueSize()==0)
                continue;

            if(predArray[i].uniqueSize()>max){
                max=predArray[i].uniqueSize();
                position=i;
                maxWeight=predArray[i].getWeight();
            }

            if(predArray[i].uniqueSize()==max){
        		/* if the matching length is the same, choose the one with the
		         * lower weight.
		         */
                if(predArray[i].getWeight()>maxWeight)
                    continue;

                position=i;
                max=predArray[i].uniqueSize();
                maxWeight=predArray[i].getWeight();
            }
        }

        if(position==-1)
            return -1;

		/* the problem here is that I might have more than one predicate
		 * matching on the same index position; i.e
		 * col_1 = .. [p1] AND col_1 = .. [p2] AND col_2 = ..[p3];
		 * In that case that maximum matching predicate is [p1] and [p3] but
		 * [p2] shuld be considered again later.
		*/
        PredicateWrapperList pwl=predArray[position];
        List<PredicateWrapper> uniquepreds=pwl.createLeadingUnique();
		
    	/* uniqueprds is a vector of predicate (along with wrapper) that I'm
	 	   going  to use to get statistics from-- we now have to delete these
	 	   predicates from all the predicateWrapperLists!
		 */
        for(PredicateWrapper uniquepred : uniquepreds){
            Predicate p=uniquepred.getPredicate();
            ret.add(p);
            for(PredicateWrapperList predList : predArray){
                if(predList==null)
                    continue;

                pwl=predList;
                /* note that we use object identity with the predicate and not
                 * the predicatewrapper to remove it from the prediate wrapper
			     * lists.
			     */
                pwl.removeElement(p);
            }
        }

	    /* if removing this predicate caused a gap, get rid of everything after
		 * the gaps.
		 */
        for(PredicateWrapperList predList : predArray){
            if(predList==null)
                continue;
            predList.retainLeadingContiguous();
        }

		/* recalculate the weights of the pwlists... */
        calculateWeight(predArray,numWorkingPredicates);
        return position;
    }

    /**
     * Compute selectivity the old fashioned way.
     */
    private double selectivityNoStatistics(Optimizable optTable) throws StandardException{
        double selectivity=1.0;

        for(int i=0;i<size();i++){
            OptimizablePredicate pred=elementAt(i);
            selectivity*=pred.selectivity(optTable);
        }

        return selectivity;
    }

    /**
     * Inner class which helps statistics routines do their work.
     * We need to keep track of the index position for each predicate for each
     * index while we're manipulating predicates and statistics. Each predicate
     * does have internal state for indexPosition, but this is a more permanent
     * sort of indexPosition, which keeps track of the position for the index
     * being considered in estimateCost. For us, each predicate can have
     * different index positions for different indices.
     */
    private class PredicateWrapper{
        int indexPosition;
        Predicate pred;
        int predicateID;

        PredicateWrapper(int ip,Predicate p,int predicateID){
            this.indexPosition=ip;
            this.pred=p;
            this.predicateID=predicateID;
        }

        int getIndexPosition(){
            return indexPosition;
        }

        Predicate getPredicate(){
            return pred;
        }

        int getPredicateID(){
            return predicateID;
        }

        /* a predicatewrapper is BEFORE another predicate wrapper iff its index
         * position is less than the index position of the other.
         */
        boolean before(PredicateWrapper other){
            return (indexPosition<other.getIndexPosition());
        }

        /* for our purposes two predicates at the same index
          position are contiguous. (have i spelled this right?)
        */
        boolean contiguous(PredicateWrapper other){
            int otherIP=other.getIndexPosition();
            return ((indexPosition==otherIP) || (indexPosition-otherIP==1)
                    || (indexPosition-otherIP==-1));
        }
    }

    /**
     * Another inner class which is basically a List of Predicate Wrappers.
     */
    private class PredicateWrapperList{
        List<PredicateWrapper> pwList;
        int numPreds;
        int numDuplicates;
        int weight;

        PredicateWrapperList(int maxValue){
            pwList=new ArrayList<>(maxValue);
        }

        void removeElement(Predicate p){
            for(int i=numPreds-1;i>=0;i--){
                Predicate predOnList=elementAt(i).getPredicate();
                if(predOnList==p) // object equality is what we need
                    removeElementAt(i);
            }
        }

        void removeElementAt(int index){
            if(index<numPreds-1){
                PredicateWrapper nextPW=elementAt(index+1);
                if(nextPW.getIndexPosition()==index)
                    numDuplicates--;
            }
            pwList.remove(index);
            numPreds--;
        }

        PredicateWrapper elementAt(int i){
            return pwList.get(i);
        }

        void insert(PredicateWrapper pw){
            int i;
            for(i=0;i<pwList.size();i++){
                if(pw.getIndexPosition()==elementAt(i).getIndexPosition())
                    numDuplicates++;

                if(pw.before(elementAt(i)))
                    break;
            }
            numPreds++;
            pwList.add(i,pw);
        }

        int size(){
            return numPreds;
        }

        /* number of unique references to a column; i.e two predicates which
         * refer to the same column in the table will give rise to duplicates.
         */
        int uniqueSize(){
            if(numPreds>0)
                return numPreds-numDuplicates;
            return 0;
        }

        /* From a list of PredicateWrappers, retain only contiguous ones. Get
         * rid of everything after the first gap.
         */
        void retainLeadingContiguous(){
            if(pwList==null)
                return;

            if(pwList.isEmpty())
                return;

            if(elementAt(0).getIndexPosition()!=0){
                pwList.clear();
                numPreds=numDuplicates=0;
                return;
            }

            int j;
            for(j=0;j<numPreds-1;j++){
                if(!(elementAt(j).contiguous(elementAt(j+1))))
                    break;
            }
			
			/* j points to the last good one; i.e 
			 * 		0 1 1 2 4 4
			 * j points to 2. remove 4 and everything after it.
			 * Beetle 4321 - need to remove from back to front
			 * to prevent array out of bounds problems
			 *
			 */

            for(int k=numPreds-1;k>j;k--){
                if(elementAt(k).getIndexPosition()==
                        elementAt(k-1).getIndexPosition())
                    numDuplicates--;
                pwList.remove(k);
            }
            numPreds=j+1;
        }

        /* From a list of PW, extract the leading unique predicates; i.e if the
         * PW's with index positions are strung together like this:
         * 0 0 1 2 3 3
         * I need to extract out 0 1 2 3.
         * leaving 0 2 3 in there.
         */
        List<PredicateWrapper> createLeadingUnique(){
            List<PredicateWrapper> scratch=new ArrayList<>();

            if(numPreds==0)
                return null;

            int lastIndexPosition=elementAt(0).getIndexPosition();

            if(lastIndexPosition!=0)
                return null;

            scratch.add(elementAt(0));    // always add 0.

            for(int i=1;i<numPreds;i++){
                if(elementAt(i).getIndexPosition()==lastIndexPosition)
                    continue;
                lastIndexPosition=elementAt(i).getIndexPosition();
                scratch.add(elementAt(i));
            }
            return scratch;
        }

        void setWeight(int weight){
            this.weight=weight;
        }

        int getWeight(){
            return weight;
        }
    }

    public boolean isUnsatisfiable() {
        for (int i=0; i<size(); i++)
            if (elementAt(i).isUnsatisfiable())
                return true;
        return false;
    }
    @Override
    public boolean canSupportIndexExcludedNulls(int tableNumber, ConglomerateDescriptor cd, TableDescriptor td) throws StandardException {
        int size = size();
        if (size == 0)
            return false;

        // we need to have at least one null filtering condition involving this table to qualify the Index
        int[] baseColumnPositions = cd.getIndexDescriptor().baseColumnPositions();
        ExcludedNullIndexColumnVisitor excludedIndexColumnVisitor = new ExcludedNullIndexColumnVisitor(tableNumber,baseColumnPositions[0]);
        boolean canSupportExcludedColumns = false;
        for(int index=0;index<size;index++){
            Predicate pred=elementAt(index);

            // skip OrList
            if(pred.isOrList()){
                continue;
            } else
            {
                pred.accept(excludedIndexColumnVisitor);
                if (excludedIndexColumnVisitor.isValid) {
                    canSupportExcludedColumns = true;
                    break;
                }
            }
        }
        return canSupportExcludedColumns;
    }

    @Override
    public boolean canSupportIndexExcludedDefaults(int tableNumber, ConglomerateDescriptor cd, TableDescriptor td) throws StandardException {
        int size = size();
        if (size == 0)
            return false;
        int[] baseColumnPositions = cd.getIndexDescriptor().baseColumnPositions();
        int columnNumber = baseColumnPositions[0];

        DataValueDescriptor defaultValue = td.getDefaultValue(cd.getIndexDescriptor().baseColumnPositions()[0]);

        if (defaultValue.isNull())
            return canSupportIndexExcludedNulls(tableNumber, cd, td);

        boolean canSupportExcludedColumns = false;
        for(int index=0;index<size;index++){
            Predicate pred=elementAt(index);

            // we can support only simple predicate of the following forms without expression on the column:
            // 1. col relop constant, where relop could be ">,>=, =,<=,<, <>"; or
            // 2. col is null
            // 3. col in (val1, val2, ..., val3)
            if(pred.isOrList()){
                continue;
            }

            if (pred.andNode.getLeftOperand() instanceof IsNullNode) {
                //check default value is null or not
                if (((IsNullNode)(pred.andNode.getLeftOperand())).getNodeType() == C_NodeTypes.IS_NULL_NODE) {
                    canSupportExcludedColumns = true;
                    break;
                }
            }

            //check inlist predicate
            InListOperatorNode inNode = pred.getSourceInList();
            if (inNode != null) {
                for (int j = 0; j < inNode.leftOperandList.size(); j++) {
                    if (inNode.leftOperandList.elementAt(j) instanceof ColumnReference) {
                        ColumnReference col = (ColumnReference) inNode.leftOperandList.elementAt(j);
                        if (col.getSource() != null &&
                            col.getTableNumber() == tableNumber &&
                            col.getColumnNumber() == columnNumber) {
                            //inlist values should not include the default value
                            int i = 0;
                            for (; i < inNode.getRightOperandList().size(); i++) {
                                DataValueDescriptor compare = ((ConstantNode) (inNode.getRightOperandList().elementAt(i))).getValue();
                                if (compare instanceof ListDataType)
                                    compare = ((ListDataType)compare).getDVD(j);
                                if (compare != null && compare.equals(defaultValue)) {
                                    canSupportExcludedColumns = false;
                                    break;
                                }
                            }
                            if (i == inNode.getRightOperandList().size()) {
                                canSupportExcludedColumns = true;
                                break;
                            }
                        }
                    }
                }
                continue;
            }

            // check binary relational operator
            RelationalOperator relop = pred.getRelop();
            if (relop != null && relop instanceof BinaryRelationalOperatorNode) {
                BinaryRelationalOperatorNode bro = (BinaryRelationalOperatorNode) relop;
                if (bro.getLeftOperand() instanceof ColumnReference){
                    ColumnReference col= (ColumnReference)bro.getLeftOperand();
                    if (col.getSource() != null &&
                        col.getTableNumber() == tableNumber &&
                        col.getColumnNumber() == columnNumber) {
                        // check whether right side is constant value and whether the value range exclude the
                        // default value
                        if (bro.getRightOperand() instanceof ConstantNode) {
                            DataValueDescriptor compare = ((ConstantNode)bro.getRightOperand()).getValue();

                            switch (bro.getOperator()) {
                                case RelationalOperator.EQUALS_RELOP:
                                    if (compare == null || !compare.equals(defaultValue))
                                        canSupportExcludedColumns = true;
                                    break;
                                case RelationalOperator.GREATER_EQUALS_RELOP:
                                    if (compare == null || compare.compare(ORDER_OP_GREATERTHAN, defaultValue, false, false))
                                        canSupportExcludedColumns = true;
                                    break;
                                case RelationalOperator.GREATER_THAN_RELOP:
                                    if (compare == null || compare.compare(ORDER_OP_GREATEROREQUALS, defaultValue, false, false))
                                        canSupportExcludedColumns = true;
                                    break;
                                case RelationalOperator.LESS_EQUALS_RELOP:
                                    if (compare == null || compare.compare(ORDER_OP_LESSTHAN, defaultValue, false, false))
                                        canSupportExcludedColumns = true;
                                    break;
                                case RelationalOperator.LESS_THAN_RELOP:
                                    if (compare == null || compare.compare(ORDER_OP_LESSOREQUALS, defaultValue, false, false))
                                        canSupportExcludedColumns = true;
                                    break;
                                case RelationalOperator.NOT_EQUALS_RELOP:
                                    if (compare == null || compare.equals(defaultValue))
                                        canSupportExcludedColumns = true;
                                    break;
                                default:
                                    break;
                            }
                        }
                        if (canSupportExcludedColumns)
                            break;
                    }
                }
            }
        }
        return canSupportExcludedColumns;
    }



}
