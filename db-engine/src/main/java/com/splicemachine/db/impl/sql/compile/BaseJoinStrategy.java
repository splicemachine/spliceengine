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
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
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
                                      int isolationLevel, String tableVersion,
                                      boolean pin,
                                      int splits,
                                      String delimited,
                                      String escaped,
                                      String lines,
                                      String storedAs,
                                      String location,
                                      int partitionReferenceItem
                                      )
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
                        getCostEstimate().getBase().rowCount());
        mb.push(
                innerTable.getTrulyTheBestAccessPath().
                        getCostEstimate().getBase().getEstimatedCost());
        mb.push(tableVersion);
        mb.push(innerTable instanceof ResultSetNode ? ((ResultSetNode)innerTable).printExplainInformationForActivation() : "");
        mb.push(pin);
        mb.push(splits);
        pushNullableString(mb,delimited);
        pushNullableString(mb,escaped);
        pushNullableString(mb,lines);
        pushNullableString(mb,storedAs);
        pushNullableString(mb,location);
        mb.push(partitionReferenceItem);
    }

    /**
     * @see JoinStrategy#isHashJoin
     */
    public boolean isHashJoin(){
        return false;
    }

    /*
    * Defers to the join strategy type to determine if the predicate costing should be pushed down to the right side.
    * Currently this is focused on NLJ.
            *
            * @return
     */
    @Override
    public boolean allowsJoinPredicatePushdown(){
        return getJoinStrategyType().isAllowsJoinPredicatePushdown();
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

    /**
     * @see JoinStrategy#resultSetMethodName
     */
    public String resultSetMethodName(boolean multiprobe) {
        if (multiprobe)
            return "getMultiProbeTableScanResultSet";
        else
            return "getTableScanResultSet";
    }

    public static void pushNullableString(MethodBuilder mb, String pushable) {
        if (pushable != null)
            mb.push(pushable);
        else
            mb.pushNull("java.lang.String");
    }

    @Override
    public boolean isMemoryUsageUnderLimit(double totalMemoryConsumed) {
        return true;
    }

}
