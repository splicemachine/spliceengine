/*

   Derby - Class org.apache.derby.impl.sql.compile.BaseJoinStrategy

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.util.PropertyUtil;

import java.util.BitSet;

public abstract class BaseJoinStrategy implements JoinStrategy{
    public BaseJoinStrategy(){
    }

    /**
     * @see JoinStrategy#bulkFetchOK
     */
    public boolean bulkFetchOK(){
        return true;
    }

    /**
     * @see JoinStrategy#ignoreBulkFetch
     */
    public boolean ignoreBulkFetch(){
        return false;
    }

    /**
     * Push the first set of common arguments for obtaining a scan ResultSet from
     * ResultSetFactory.
     * The first 11 arguments are common for these ResultSet getters
     * <UL>
     * <LI> ResultSetFactory.getBulkTableScanResultSet
     * <LI> ResultSetFactory.getHashScanResultSet
     * <LI> ResultSetFactory.getTableScanResultSet
     * <LI> ResultSetFactory.getRaDependentTableScanResultSet
     * </UL>
     *
     * @param tc
     * @param mb
     * @param innerTable
     * @param predList
     * @param acbi
     * @param resultRowAllocator
     * @throws StandardException
     */
    public void fillInScanArgs1(
            TransactionController tc,
            MethodBuilder mb,
            Optimizable innerTable,
            OptimizablePredicateList predList,
            ExpressionClassBuilderInterface acbi,
            MethodBuilder resultRowAllocator
    )
            throws StandardException{
        boolean sameStartStopPosition=predList.sameStartStopPosition();
        ExpressionClassBuilder acb=(ExpressionClassBuilder)acbi;
        long conglomNumber=
                innerTable.getTrulyTheBestAccessPath().
                        getConglomerateDescriptor().
                        getConglomerateNumber();
        StaticCompiledOpenConglomInfo scoci=tc.getStaticCompiledConglomInfo(conglomNumber);

        acb.pushThisAsActivation(mb);
        mb.push(conglomNumber);
        mb.push(acb.addItem(scoci));

        acb.pushMethodReference(mb,resultRowAllocator);
        mb.push(innerTable.getResultSetNumber());

        predList.generateStartKey(acb,mb,innerTable);
        mb.push(predList.startOperator(innerTable));

        if(!sameStartStopPosition){
            predList.generateStopKey(acb,mb,innerTable);
        }else{
            mb.pushNull(ClassName.GeneratedMethod);
        }

        mb.push(predList.stopOperator(innerTable));
        mb.push(sameStartStopPosition);
        mb.push(predList.isRowIdScan());
        predList.generateQualifiers(acb,mb,innerTable,true);
        //mb.upCast(ClassName.Qualifier + "[][]");
    }

    public final void fillInScanArgs2(MethodBuilder mb,
                                      Optimizable innerTable,
                                      int bulkFetch,
                                      int colRefItem,
                                      int indexColItem,
                                      int lockMode,
                                      boolean tableLocked,
                                      int isolationLevel)
            throws StandardException{
        mb.push(innerTable.getBaseTableName());
        //User may have supplied optimizer overrides in the sql
        //Pass them onto execute phase so it can be shown in
        //run time statistics.
        if(innerTable.getProperties()!=null)
            mb.push(PropertyUtil.sortProperties(innerTable.getProperties()));
        else
            mb.pushNull("java.lang.String");

        ConglomerateDescriptor cd=
                innerTable.getTrulyTheBestAccessPath().getConglomerateDescriptor();
        if(cd.isConstraint()){
            DataDictionary dd=innerTable.getDataDictionary();
            TableDescriptor td=innerTable.getTableDescriptor();
            ConstraintDescriptor constraintDesc=dd.getConstraintDescriptor(
                    td,cd.getUUID());
            mb.push(constraintDesc.getConstraintName());
        }else if(cd.isIndex()){
            mb.push(cd.getConglomerateName());
        }else{
            mb.pushNull("java.lang.String");
        }

        // Whether or not the conglomerate is the backing index for a constraint
        mb.push(cd.isConstraint());

        // tell it whether it's to open for update, which we should do if
        // it's an update or delete statement, or if it's the target
        // table of an updatable cursor.
        mb.push(innerTable.forUpdate());

        mb.push(colRefItem);

        mb.push(indexColItem);

        mb.push(lockMode);

        mb.push(tableLocked);

        mb.push(isolationLevel);

        if(bulkFetch>0){
            mb.push(bulkFetch);
            // If the table references LOBs, we want to disable bulk fetching
            // when the cursor is holdable. Otherwise, a commit could close
            // LOBs before they have been returned to the user.
            mb.push(innerTable.hasLargeObjectColumns());
        }

		/* 1 row scans (avoiding 2nd next()) are
          * only meaningful for some join strategies.
		 * (Only an issue for outer table, which currently
		 * can only be nested loop, as avoidance of 2nd next
		 * on inner table already factored in to join node.)
		 */
        if(validForOutermostTable()){
            mb.push(innerTable.isOneRowScan());
        }

        mb.push(
                innerTable.getTrulyTheBestAccessPath().
                        getCostEstimate().rowCount());

        mb.push(
                innerTable.getTrulyTheBestAccessPath().
                        getCostEstimate().getEstimatedCost());
    }

    /**
     * @see JoinStrategy#isHashJoin
     */
    public boolean isHashJoin(){
        return false;
    }

    @Override
    public boolean allowsJoinPredicatePushdown(){
        return false;
    }

    @Override
    public void setRowOrdering(CostEstimate costEstimate){
        /*
         * By default, we do nothing here, because more join strategies inherit the sort order
         * of the outer table (which is the default behavior).
         */
    }

    /**
     * Can this join strategy be used on the
     * outermost table of a join.
     *
     * @return Whether or not this join strategy
     * can be used on the outermose table of a join.
     */
    protected boolean validForOutermostTable(){
        return false;
    }

    protected double estimateJoinSelectivity(Optimizable innerTable,
                                             ConglomerateDescriptor cd,
                                             OptimizablePredicateList predList,
                                             double innerRowCount) throws StandardException{
        IndexRowGenerator irg = null;
        if (cd != null) {
            irg = cd.getIndexDescriptor();
        }
        if(cd == null || irg==null||irg.getIndexDescriptor()==null){
            return estimateNonKeyedSelectivity(innerTable,cd,predList);
        }else{
            return estimateKeyedSelectivity(innerTable,cd,predList,irg,innerRowCount);
        }
    }

    private double estimateKeyedSelectivity(Optimizable innerTable,
                                            ConglomerateDescriptor cd,
                                            OptimizablePredicateList predList,
                                            IndexRowGenerator irg,
                                            double innerRowCount) throws StandardException{
        BitSet setKeyColumns = new BitSet(irg.numberOfOrderedColumns());
        if (predList != null) {
            for (int i = 0; i < predList.size(); i++) {
                Predicate pred = (Predicate) predList.getOptPredicate(i);
                if (!pred.isJoinPredicate()) continue;
                //we can only check equals predicates to know what the selectivity will be precisely
                RelationalOperator relop = pred.getRelop();
                if (relop.getOperator() == RelationalOperator.EQUALS_RELOP) {
                    ColumnReference column = relop.getColumnOperand(innerTable);
                    int keyColPos = irg.getKeyColumnPosition(column.getColumnNumber());
                    if (keyColPos > 0) {
                        setKeyColumns.set(keyColPos - 1);
                    }
                }
            }
        }
        if(setKeyColumns.cardinality()==irg.numberOfOrderedColumns()){
            /*
             * If we have a equality predicate on every join column, then we know that we will
             * basically perform a lookup for every row, so we should reduce innerCost's rowCount
             * to 1 with our selectivity. Because we do this, we do(!includeStart || !includeStop)) return 0l; //empty interval has no data so don't bother adding in any additional
             * selectivity
             */
            return 1d/innerRowCount;
        }else{
            /*
             * We don't have a complete set of predicates on the key columns of the inner table,
             * so we really don't have much choice besides going with the base selectivity
             */
            return estimateNonKeyedSelectivity(innerTable,cd,predList);
        }
    }

    private double estimateNonKeyedSelectivity(Optimizable innerTable,
                                               ConglomerateDescriptor cd,
                                               OptimizablePredicateList predList) throws StandardException{
        double selectivity = 1.0d;
        if (predList != null) {
            for (int i = 0; i < predList.size(); i++) {
                Predicate p = (Predicate) predList.getOptPredicate(i);
                if (!p.isJoinPredicate()) continue;
                selectivity = Math.min(selectivity, p.selectivity(innerTable, cd));
            }
        }
        return selectivity;
    }
}
