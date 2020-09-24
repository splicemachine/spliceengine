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

import com.splicemachine.db.catalog.IndexDescriptor;
import com.splicemachine.db.catalog.types.ReferencedColumnsDescriptorImpl;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.classfile.VMOpcode;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.io.FormatableArrayHolder;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.io.FormatableIntHolder;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.conn.SessionProperties;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionContext;
import com.splicemachine.db.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.splicemachine.db.iapi.store.access.StoreCostController;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.util.JBitSet;
import com.splicemachine.db.iapi.util.ReuseFactory;
import com.splicemachine.db.iapi.util.StringUtil;
import com.splicemachine.db.impl.ast.CollectingVisitorBuilder;
import com.splicemachine.db.impl.ast.PredicateUtils;
import com.splicemachine.db.impl.ast.RSUtils;
import com.splicemachine.db.impl.sql.catalog.SYSTOKENSRowFactory;
import com.splicemachine.db.impl.sql.catalog.SYSUSERSRowFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import splice.com.google.common.base.Joiner;
import splice.com.google.common.base.Predicates;
import splice.com.google.common.collect.Lists;

import java.lang.reflect.Modifier;
import java.util.*;

import static com.splicemachine.db.impl.ast.RSUtils.isRSN;

// Temporary until user override for disposable stats has been removed.

/**
 * A FromBaseTable represents a table in the FROM list of a DML statement,
 * as distinguished from a FromSubquery, which represents a subquery in the
 * FROM list. A FromBaseTable may actually represent a view.  During parsing,
 * we can't distinguish views from base tables. During binding, when we
 * find FromBaseTables that represent views, we replace them with FromSubqueries.
 * By the time we get to code generation, all FromSubqueries have been eliminated,
 * and all FromBaseTables will represent only true base tables.
 * <p/>
 * <B>Positioned Update</B>: Currently, all columns of an updatable cursor
 * are selected to deal with a positioned update.  This is because we don't
 * know what columns will ultimately be needed from the UpdateNode above
 * us.  For example, consider:<pre><i>
 * <p/>
 *     get c as 'select cint from t for update of ctinyint'
 *  update t set ctinyint = csmallint
 * <p/>
 * </pre></i> Ideally, the cursor only selects cint.  Then,
 * something akin to an IndexRowToBaseRow is generated to
 * take the CursorResultSet and get the appropriate columns
 * out of the base table from the RowLocation retunrned by the
 * cursor.  Then the update node can generate the appropriate
 * NormalizeResultSet (or whatever else it might need) to
 * get things into the correct format for the UpdateResultSet.
 * See CurrentOfNode for more information.
 */

public class FromBaseTable extends FromTable {
    static final int UNSET=-1;
    /**
     * Whether or not we have checked the index statistics for staleness.
     * Used to avoid performing the check multiple times per compilation.
     */
//    private boolean hasCheckedIndexStats;

    TableName tableName;
    TableDescriptor tableDescriptor;

    ConglomerateDescriptor baseConglomerateDescriptor;
    ConglomerateDescriptor[] conglomDescs;
    private boolean pin;
    int updateOrDelete;
    boolean skipStats;
    boolean useRealTableStats;
    HashSet<Integer> usedNoStatsColumnIds;
    int splits;
    long defaultRowCount;
    double defaultSelectivityFactor = -1d;

    /*
    ** The number of rows to bulkFetch.
    ** Initially it is unset.  If the user
    ** uses the bulkFetch table property,
    ** it is set to that.  Otherwise, it
    ** may be turned on if it isn't an updatable
    ** cursor and it is the right type of
    ** result set (more than 1 row expected to
    ** be returned, and not hash, which does its
    ** own bulk fetch, and subquery).
    */
    int bulkFetch=UNSET;

    /* We may turn off bulk fetch for a variety of reasons,
     * including because of the min optimization.
     * bulkFetchTurnedOff is set to true in those cases.
     */
    boolean bulkFetchTurnedOff;

    /* Whether or not we are going to do execution time "multi-probing"
     * on the table scan for this FromBaseTable.
     */
    boolean multiProbing=false;

    private double singleScanRowCount;

    private FormatableBitSet heapReferencedCols;
    private FormatableBitSet referencedCols;
    private ResultColumnList templateColumns;


    /* A 0-based array of column names for this table used
     * for optimizer trace.
     */
    private String[] columnNames;

    // true if we are to do a special scan to retrieve the last value
    // in the index
    private boolean specialMaxScan;

    // true if we are to do a distinct scan
    private boolean distinctScan;

    /**
     * Information for dependent table scan for Referential Actions
     */
    private String raParentResultSetId;
    private long fkIndexConglomId;
    private int[] fkColArray;

    /**
     * Restriction as a PredicateList
     */
    PredicateList baseTableRestrictionList;
    PredicateList nonBaseTableRestrictionList;
    PredicateList restrictionList;
    public PredicateList storeRestrictionList;
    PredicateList nonStoreRestrictionList;
    PredicateList requalificationRestrictionList;

    public static final int UPDATE=1;
    public static final int DELETE=2;

    private boolean getUpdateLocks;

    // true if we are running with sql authorization and this is the SYSUSERS table
    private boolean authorizeSYSUSERS;

    private ResultColumn rowIdColumn;

    private boolean isAntiJoin;

    private AggregateNode aggrForSpecialMaxScan;

    private boolean isBulkDelete = false;

    private ValueNode pastTxIdExpression = null;

    @Override
    public boolean isParallelizable(){
        return false;
    }

    /**
     * Initializer for a table in a FROM list.
     * @param tableName The name of the table
     * @param correlationName The correlation name
     * @param rclOrUD update/delete flag or result column list
     * @param propsOrRcl properties or result column list
     * @param isBulkDelete bulk delete flag or past tx id.
     * @param pastTxIdExpr the past transaction expression.
     */
    @Override
    public void init(Object tableName,Object correlationName,Object rclOrUD,Object propsOrRcl, Object isBulkDelete, Object pastTxIdExpr){
        this.isBulkDelete = (Boolean) isBulkDelete;
        if(pastTxIdExpr != null) {
            this.pastTxIdExpression = (ValueNode) pastTxIdExpr;
        }
        init(tableName, correlationName, rclOrUD, propsOrRcl);
    }

    @Override
    public void init(Object tableName,Object correlationName,Object rclOrUD,Object propsOrRcl){
        if(rclOrUD instanceof Integer){
            init(correlationName,null);
            this.tableName=(TableName)tableName;
            this.updateOrDelete=(Integer)rclOrUD;
            resultColumns=(ResultColumnList)propsOrRcl;
        }else{
            init(correlationName,propsOrRcl);
            this.tableName=(TableName)tableName;
            resultColumns=(ResultColumnList)rclOrUD;
        }

        setOrigTableName(this.tableName);
        templateColumns=resultColumns;
        usedNoStatsColumnIds=new HashSet<>();
    }

    /**
     * no LOJ reordering for base table.
     */
    @Override
    public boolean LOJ_reorderable(int numTables) throws StandardException{
        return false;
    }

    @Override
    public JBitSet LOJgetReferencedTables(int numTables) throws StandardException{
        JBitSet map=new JBitSet(numTables);
        fillInReferencedTableMap(map);
        return map;
    }

    @SuppressWarnings("unused")
    public boolean isMultiProbing(){  return multiProbing; } //used in Splice engine code

    private boolean isIndexEligible(ConglomerateDescriptor currentConglomerateDescriptor,
                                    OptimizablePredicateList predList) throws StandardException {
        if (currentConglomerateDescriptor !=null && currentConglomerateDescriptor.isIndex()) {
            if (currentConglomerateDescriptor.getIndexDescriptor().excludeNulls()
                    && (predList == null || !predList.canSupportIndexExcludedNulls(tableNumber,currentConglomerateDescriptor, tableDescriptor))) {
                return false;
            }
            else if (currentConglomerateDescriptor.getIndexDescriptor().excludeDefaults()
                    && (predList == null || !predList.canSupportIndexExcludedDefaults(tableNumber,currentConglomerateDescriptor, tableDescriptor))) {
                return false;
            }
            return currentConglomerateDescriptor.getIndexDescriptor().getExprBytecode().length <= 0;
        }
        return true;
    }

    /*
     * Optimizable interface.
     */

    @Override
    public boolean nextAccessPath(Optimizer optimizer,
                                  OptimizablePredicateList predList,
                                  RowOrdering rowOrdering) throws StandardException{
        String userSpecifiedIndexName=getUserSpecifiedIndexName();
        AccessPath ap=getCurrentAccessPath();
        ConglomerateDescriptor currentConglomerateDescriptor=ap.getConglomerateDescriptor();

        OptimizerTrace tracer=optimizer.tracer();
        tracer.trace(OptimizerFlag.CALLING_NEXT_ACCESS_PATH,((predList==null)?0:predList.size()),0,0.0,getExposedName(), predList);

        /*
         ** Remove the ordering of the current conglomerate descriptor,
         ** if any.
         */
        rowOrdering.removeOptimizable(getTableNumber());

        // RESOLVE: This will have to be modified to step through the
        // join strategies as well as the conglomerates.

        if(userSpecifiedIndexName!=null){
            /*
             ** User specified an index name, so we should look at only one
             ** index.  If there is a current conglomerate descriptor, and there
             ** are no more join strategies, we've already looked at the index,
             ** so go back to null.
             */
            if(currentConglomerateDescriptor!=null){
                if(!super.nextAccessPath(optimizer,predList,rowOrdering)){
                    currentConglomerateDescriptor=null;
                }
            }else{
                tracer.trace(OptimizerFlag.LOOKING_FOR_SPECIFIED_INDEX,tableNumber,0,0.0,userSpecifiedIndexName,
                             correlationName);

                if(StringUtil.SQLToUpperCase(userSpecifiedIndexName).equals("NULL")){
                    /* Special case - user-specified table scan */
                    currentConglomerateDescriptor=tableDescriptor.getConglomerateDescriptor(
                            tableDescriptor.getHeapConglomerateId());
                }else{
                    /* User-specified index name */
                    getConglomDescs();

                    for(ConglomerateDescriptor conglomDesc : conglomDescs){
                        currentConglomerateDescriptor=conglomDesc;
                        String conglomerateName=currentConglomerateDescriptor.getConglomerateName();
                        if(conglomerateName!=null){
                            /* Have we found the desired index? */
                            if(conglomerateName.equals(userSpecifiedIndexName)){
                                break;
                            }
                        }
                    }

                    /* We should always find a match */
                    assert currentConglomerateDescriptor!=null:"Expected to find match for forced index "+userSpecifiedIndexName;
                    if (!isIndexEligible(currentConglomerateDescriptor, predList))
                        currentConglomerateDescriptor = null;
                }

                if(currentConglomerateDescriptor != null) {
                    if (!super.nextAccessPath(optimizer, predList, rowOrdering)) {
                        if (SanityManager.DEBUG) {
                            SanityManager.THROWASSERT("No join strategy found");
                        }
                    }
                }
            }
        }else{
            if(currentConglomerateDescriptor!=null){
                /*
                 ** Once we have a conglomerate descriptor, cycle through
                 ** the join strategies (done in parent).
                 */
                if(!super.nextAccessPath(optimizer,predList,rowOrdering)){
                    /*
                     ** When we're out of join strategies, go to the next
                     ** conglomerate descriptor.
                     */
                    currentConglomerateDescriptor=getNextConglom(currentConglomerateDescriptor, predList, optimizer);

                    if (currentConglomerateDescriptor != null) {
                    /*
                     ** New conglomerate, so step through join strategies
                     ** again.
                     */
                        resetJoinStrategies();

                        if (!super.nextAccessPath(optimizer, predList, rowOrdering)) {
                            if (SanityManager.DEBUG) {
                                SanityManager.THROWASSERT("No join strategy found");
                            }
                        }
                    }
                }
            }else{
                /* Get the first conglomerate descriptor */
                currentConglomerateDescriptor=getFirstConglom(predList, optimizer);

                if (currentConglomerateDescriptor != null) {
                    if (!super.nextAccessPath(optimizer, predList, rowOrdering)) {
                        if (SanityManager.DEBUG) {
                            SanityManager.THROWASSERT("No join strategy found");
                        }
                    }
                }
            }
        }

        if(currentConglomerateDescriptor==null){
            tracer.trace(OptimizerFlag.NO_MORE_CONGLOMERATES,tableNumber,0,0.0,correlationName);
        }else{
            currentConglomerateDescriptor.setColumnNames(columnNames);
            tracer.trace(OptimizerFlag.CONSIDERING_CONGLOMERATE,tableNumber,0,0.0,currentConglomerateDescriptor,
                         correlationName);
        }

        /*
         * Tell the rowOrdering that what the ordering of this conglomerate is
         */
        IndexRowGenerator irg=null;
        if(currentConglomerateDescriptor!=null){
            if(currentConglomerateDescriptor.isIndex()){ // Index
                irg=currentConglomerateDescriptor.getIndexDescriptor();
            }else{ // Primary Key
                if(conglomDescs==null)
                    getConglomDescs();
                for(ConglomerateDescriptor primaryKeyCheck : conglomDescs){
                    IndexDescriptor indexDec=primaryKeyCheck.getIndexDescriptor();
                    if(indexDec!=null){
                        String indexType=indexDec.indexType();
                        if(indexType!=null && indexType.contains("PRIMARY")){
                            irg=currentConglomerateDescriptor.getIndexDescriptor();
                        }
                    }
                }
            }
            if(irg==null){ // We are scanning a table without a primary key
                /* If we are scanning the heap, but there
                 * is a full match on a unique key, then
                 * we can say that the table IS NOT unordered.
                 * (We can't currently say what the ordering is
                 * though.)
                 */
                if(!isOneRowResultSet(predList)){
                    tracer.trace(OptimizerFlag.ADDING_UNORDERED_OPTIMIZABLE,((predList==null)?0:predList.size()),0,0.0,predList);

                    rowOrdering.addUnorderedOptimizable(this);
                }else{
                    tracer.trace(OptimizerFlag.SCANNING_HEAP_FULL_MATCH_ON_UNIQUE_KEY,0,0,0.0,null);
                }
            }else{
                int[] baseColumnPositions=irg.baseColumnPositions();
                boolean[] isAscending=irg.isAscending();

                for(int i=0;i<baseColumnPositions.length;i++){
                    int rowOrderDirection=isAscending[i]?RowOrdering.ASCENDING:RowOrdering.DESCENDING;
                    int pos = rowOrdering.orderedPositionForColumn(rowOrderDirection,getTableNumber(),baseColumnPositions[i]);
                    if (pos == -1) {
                        rowOrdering.nextOrderPosition(rowOrderDirection);
                        pos = rowOrdering.addOrderedColumn(rowOrderDirection,getTableNumber(),baseColumnPositions[i]);
                    }
                    // check if the column has a constant predicate like "col=constant" defined on it,
                    // if so, we can treat it as sorted as it has only one value
                    if (pos >=0 &&    /* a column ordering is added or exists */
                        hasConstantPredicate(getTableNumber(), baseColumnPositions[i], predList)) {
                        ColumnOrdering co = rowOrdering.getOrderedColumn(pos);
                        co.setBoundByConstant(true);
                    }
                }
            }
        }
        ap.setConglomerateDescriptor(currentConglomerateDescriptor);
        return currentConglomerateDescriptor!=null;
    }

    @Override
    protected boolean canBeOrdered(){
        /** Tell super-class that this Optimizable can be ordered */
        return true;
    }

    @Override
    public CostEstimate optimizeIt(Optimizer optimizer,
                                   OptimizablePredicateList predList,
                                   CostEstimate outerCost,
                                   RowOrdering rowOrdering) throws StandardException{
        optimizer.costOptimizable(this,
                tableDescriptor,
                getCurrentAccessPath().getConglomerateDescriptor(),
                predList,
                outerCost);

        // The cost that we found from the above call is now stored in the
        // cost field of this FBT's current access path.  So that's the
        // cost we want to return here.
        //        costEstimate.setRowOrdering(rowOrdering);
        return getCurrentAccessPath().getCostEstimate();
    }

    @Override
    public TableDescriptor getTableDescriptor(){
        return tableDescriptor;
    }

    @Override
    public boolean isMaterializable() throws StandardException{
            /* base tables are always materializable */
        return true;
    }

    @Override
    public boolean pushOptPredicate(OptimizablePredicate optimizablePredicate) throws StandardException{
        assert optimizablePredicate instanceof Predicate: "optimizablePredicate expected to be instanceof Predicate";

        /* Add the matching predicate to the restrictionList */
        restrictionList.addPredicate((Predicate)optimizablePredicate);

        return true;
    }

    @Override
    public void pullOptPredicates(OptimizablePredicateList optimizablePredicates) throws StandardException{
        for(int i=restrictionList.size()-1;i>=0;i--){
            optimizablePredicates.addOptPredicate(restrictionList.getOptPredicate(i));
            restrictionList.removeOptPredicate(i);
        }
    }

    @Override
    public boolean isCoveringIndex(ConglomerateDescriptor cd) throws StandardException{
            /* You can only be a covering index if you're an index */
        if(!cd.isIndex())
            return false;

        IndexRowGenerator irg=cd.getIndexDescriptor();
        int[] baseCols=irg.baseColumnPositions();

        int rclSize=resultColumns.size();
        boolean coveringIndex=true;
        int colPos;
        for(int index=0;index<rclSize;index++){
            ResultColumn rc=resultColumns.elementAt(index);

            /* Ignore unreferenced columns */
            if(!rc.isReferenced()){
                continue;
            }

            /* Ignore constants - this can happen if all of the columns
             * were projected out and we ended up just generating
             * a "1" in RCL.doProject().
             */
            if(rc.getExpression() instanceof ConstantNode){
                continue;
            }

            coveringIndex=false;

            colPos=rc.getColumnPosition();

            /* Is this column in the index? */
            for(int baseCol : baseCols){
                if(colPos==baseCol){
                    coveringIndex=true;
                    break;
                }
            }

            /* No need to continue if the column was not in the index */
            if(!coveringIndex){
                break;
            }
        }
        return coveringIndex;
    }

    @Override
    public void verifyProperties(DataDictionary dDictionary) throws StandardException{
        if (tableDescriptor.getStoredAs()!=null) {
            dataSetProcessorType = dataSetProcessorType.combine(DataSetProcessorType.FORCED_SPARK);
        }

        /* check if we need to inherent some property from the connection */
        Boolean skipStatsObj = (Boolean)getLanguageConnectionContext().getSessionProperties().getProperty(SessionProperties.PROPERTYNAME.SKIPSTATS);
        skipStats = skipStatsObj==null?false:skipStatsObj.booleanValue();
        Double defaultSelectivityFactorObj = (Double)getLanguageConnectionContext().getSessionProperties().getProperty(SessionProperties.PROPERTYNAME.DEFAULTSELECTIVITYFACTOR);
        defaultSelectivityFactor = defaultSelectivityFactorObj==null?-1d:defaultSelectivityFactorObj.doubleValue();
        Boolean useSparkObj = (Boolean)getLanguageConnectionContext().getSessionProperties().getProperty(SessionProperties.PROPERTYNAME.USEOLAP);
        if (useSparkObj != null)
            dataSetProcessorType = dataSetProcessorType.combine(useSparkObj ?
                    DataSetProcessorType.SESSION_HINTED_SPARK:
                    DataSetProcessorType.SESSION_HINTED_CONTROL);

        if (defaultSelectivityFactor > 0)
            skipStats = true;

        if(tableProperties==null){
            return;
        }
        /* Check here for:
         *        invalid properties key
         *        index and constraint properties
         *        non-existent index
         *        non-existent constraint
         *        invalid joinStrategy
         *        invalid value for hashInitialCapacity
         *        invalid value for hashLoadFactor
         *        invalid value for hashMaxCapacity
         */
        boolean indexSpecified=false;
        boolean constraintSpecified=false;
        ConstraintDescriptor consDesc=null;

        Enumeration e=tableProperties.keys();
        while(e.hasMoreElements()){
            String key=(String)e.nextElement();
            String value=(String)tableProperties.get(key);


            //noinspection IfCanBeSwitch
            if(key.equals("index")){
                // User only allowed to specify 1 of index and constraint, not both
                if(constraintSpecified){
                    throw StandardException.newException(SQLState.LANG_BOTH_FORCE_INDEX_AND_CONSTRAINT_SPECIFIED,
                            getBaseTableName());
                }
                indexSpecified=true;

                /* Validate index name - NULL means table scan */
                if(!StringUtil.SQLToUpperCase(value).equals("NULL")){
                    ConglomerateDescriptor[] cds=tableDescriptor.getConglomerateDescriptors();

                    ConglomerateDescriptor cd=null;
                    for(ConglomerateDescriptor toFind : cds){
                        cd=toFind;
                        String conglomerateName=cd.getConglomerateName();
                        if(conglomerateName!=null){
                            if(conglomerateName.equals(value)){
                                break;
                            }
                        }
                        // Not a match, clear cd
                        cd=null;
                    }

                    // Throw exception if user specified index not found
                    if(cd==null){
                        throw StandardException.newException(SQLState.LANG_INVALID_FORCED_INDEX1, value,getBaseTableName());
                    }
                                  /* Query is dependent on the ConglomerateDescriptor */
                    getCompilerContext().createDependency(cd);
                }
            } else if(key.equals("constraint")){
                // User only allowed to specify 1 of index and constraint, not both
                if(indexSpecified){
                    throw StandardException.newException(SQLState.LANG_BOTH_FORCE_INDEX_AND_CONSTRAINT_SPECIFIED,
                            getBaseTableName());
                }
                constraintSpecified=true;

             if(!StringUtil.SQLToUpperCase(value).equals("NULL")){
                 consDesc=dDictionary.getConstraintDescriptorByName(tableDescriptor,null,value,false);

                    /* Throw exception if user specified constraint not found
                     * or if it does not have a backing index.
                     */
                    if((consDesc==null) || !consDesc.hasBackingIndex()){
                        throw StandardException.newException(SQLState.LANG_INVALID_FORCED_INDEX2,value,getBaseTableName());
                    }

                    /* Query is dependent on the ConstraintDescriptor */
                    getCompilerContext().createDependency(consDesc);
                }
            }else if(key.equals("joinStrategy")){
                userSpecifiedJoinStrategy=StringUtil.SQLToUpperCase(value);
                if (userSpecifiedJoinStrategy.equals("CROSS")) {
                    dataSetProcessorType = dataSetProcessorType.combine(DataSetProcessorType.FORCED_SPARK);
                }
            }
            else if (key.equals("useSpark") || key.equals("useOLAP")) {
                try {
                    dataSetProcessorType = dataSetProcessorType.combine(
                            Boolean.parseBoolean(StringUtil.SQLToUpperCase(value))?
                                    DataSetProcessorType.QUERY_HINTED_SPARK:
                                    DataSetProcessorType.QUERY_HINTED_CONTROL);
                } catch (Exception sparkE) {
                    throw StandardException.newException(SQLState.LANG_INVALID_FORCED_SPARK, key, value);
                }
            }
            else if (key.equals("pin")) {
                try {
                    pin = Boolean.parseBoolean(StringUtil.SQLToUpperCase(value));
                    dataSetProcessorType = dataSetProcessorType.combine(DataSetProcessorType.FORCED_SPARK);
                    tableProperties.setProperty("index","null");
                } catch (Exception pinE) {
                    throw StandardException.newException(SQLState.LANG_INVALID_FORCED_SPARK, key, value); // TODO Fix Error message - JL
                }
            }
            else if (key.equals("skipStats")) {
                try {
                    boolean bValue = Boolean.parseBoolean(StringUtil.SQLToUpperCase(value));
                    // skipStats may have been set to true by the other hint useDefaultRowCount or defaultSelectivityFactor
                    // in that case, useDefaultRowCount and defaultSelectivityFactor take precedence and we don't want to override that decision
                    if (!skipStats)
                        skipStats = bValue;
                } catch (Exception skipStatsE) {
                    throw StandardException.newException(SQLState.LANG_INVALID_FORCED_SKIPSTATS, value);
                }
            } else if (key.equals("splits")) {
                try {
                    splits = Integer.parseInt(value);
                    if (splits <= 0)
                        throw StandardException.newException(SQLState.LANG_INVALID_SPLITS, value);
                } catch (NumberFormatException skipStatsE) {
                    throw StandardException.newException(SQLState.LANG_INVALID_SPLITS, value);
                }
            } else if (key.equals("useDefaultRowCount")) {
                try {
                    skipStats = true;
                    defaultRowCount = Long.parseLong(value);
                } catch (NumberFormatException parseLongE) {
                    throw StandardException.newException(SQLState.LANG_INVALID_ROWCOUNT, value);
                }
                if (defaultRowCount <= 0) {
                    throw StandardException.newException(SQLState.LANG_INVALID_ROWCOUNT, value);
                }
            } else if (key.equals("defaultSelectivityFactor")) {
                try {
                    skipStats = true;
                    defaultSelectivityFactor = Double.parseDouble(value);
                } catch (NumberFormatException parseDoubleE) {
                    throw StandardException.newException(SQLState.LANG_INVALID_SELECTIVITY, value);
                }
                if (defaultSelectivityFactor <= 0 || defaultSelectivityFactor > 1.0)
                    throw StandardException.newException(SQLState.LANG_INVALID_SELECTIVITY, value);
            } else if (key.equals("broadcastCrossRight")) {
                // no op since parseBoolean never throw
            }else {
                // No other "legal" values at this time
                throw StandardException.newException(SQLState.LANG_INVALID_FROM_TABLE_PROPERTY,key,
                        "index, constraint, joinStrategy, useSpark, useOLAP, pin, skipStats, splits, " +
                                "useDefaultRowcount, defaultSelectivityFactor");
            }


            /* If user specified a non-null constraint name(DERBY-1707), then
             * replace it in the properties list with the underlying index name to
             * simplify the code in the optimizer.
             * NOTE: The code to get from the constraint name, for a constraint
             * with a backing index, to the index name is convoluted.  Given
             * the constraint name, we can get the conglomerate id from the
             * ConstraintDescriptor.  We then use the conglomerate id to get
             * the ConglomerateDescriptor from the DataDictionary and, finally,
             * we get the index name (conglomerate name) from the ConglomerateDescriptor.
             */
            if(constraintSpecified && consDesc!=null){
                ConglomerateDescriptor cd=dDictionary.getConglomerateDescriptor(consDesc.getConglomerateId());
                String indexName=cd.getConglomerateName();

                tableProperties.remove("constraint");
                tableProperties.put("index",indexName);
            }
        }
    }

    @Override
    public String getBaseTableName(){
        return tableName.getTableName();
    }

    @Override
    public void startOptimizing(Optimizer optimizer,RowOrdering rowOrdering){
        AccessPath ap=getCurrentAccessPath();
        AccessPath bestAp=getBestAccessPath();
        AccessPath bestSortAp=getBestSortAvoidancePath();

        ap.setConglomerateDescriptor(null);
        bestAp.setConglomerateDescriptor(null);
        bestSortAp.setConglomerateDescriptor(null);
        ap.setCoveringIndexScan(false);
        bestAp.setCoveringIndexScan(false);
        bestSortAp.setCoveringIndexScan(false);
        ap.setSpecialMaxScan(false);
        bestAp.setSpecialMaxScan(false);
        bestSortAp.setSpecialMaxScan(false);
        ap.setLockMode(0);
        bestAp.setLockMode(0);
        bestSortAp.setLockMode(0);
        ap.setMissingHashKeyOK(false);
        bestAp.setMissingHashKeyOK(false);
        bestSortAp.setMissingHashKeyOK(false);

        /*
         ** Only need to do this for current access path, because the
         ** costEstimate will be copied to the best access paths as
         ** necessary.
         */
        CostEstimate costEstimate=getCostEstimate(optimizer);
        ap.setCostEstimate(costEstimate);

        /*
         ** This is the initial cost of this optimizable.  Initialize it
         ** to the maximum cost so that the optimizer will think that
         ** any access path is better than none.
         */
        costEstimate.setCost(Double.MAX_VALUE,Double.MAX_VALUE,Double.MAX_VALUE);

        super.startOptimizing(optimizer,rowOrdering);
    }

    @Override
    public int convertAbsoluteToRelativeColumnPosition(int absolutePosition){
        return mapAbsoluteToRelativeColumnPosition(absolutePosition);
    }

    @Override
    public CostEstimate estimateCost(OptimizablePredicateList predList,
                                     ConglomerateDescriptor cd,
                                     CostEstimate outerCost,
                                     Optimizer optimizer,
                                     RowOrdering rowOrdering) throws StandardException{

        AccessPath currentAccessPath=getCurrentAccessPath();
        JoinStrategy currentJoinStrategy=currentAccessPath.getJoinStrategy();
        OptimizerTrace tracer =optimizer.tracer();
        tracer.trace(OptimizerFlag.ESTIMATING_COST_OF_CONGLOMERATE,tableNumber,0,0.0,cd,correlationName);
        /* Get the uniqueness factory for later use (see below) */
        /* Get the predicates that can be used for scanning the base table */
        baseTableRestrictionList.removeAllElements();

        currentJoinStrategy.getBasePredicates(predList,baseTableRestrictionList,this);
        /* RESOLVE: Need to figure out how to cache the StoreCostController */
        StoreCostController scc=getStoreCostController(tableDescriptor,cd);
        useRealTableStats=scc.useRealTableStatistics();
        CostEstimate costEstimate=getScratchCostEstimate(optimizer);
        costEstimate.setRowOrdering(rowOrdering);
        costEstimate.setPredicateList(baseTableRestrictionList);

        //get a BitSet representing the column positions of interest
        BitSet scanColumnList = null;
        BitSet indexLookupList = null;
        int[] baseColumnPositions = null;
        ResultColumnList rcl = templateColumns;
        if(rcl!=null&& !rcl.isEmpty()){
            scanColumnList = new BitSet(rcl.size());
            for(ResultColumn rc:rcl){
                int columnPosition=rc.getColumnPosition();
                scanColumnList.set(columnPosition);
            }
            /*
             * It's possible that we are scanning an index. In that case, we don't necessarily
             * have access to all the columns in the scanColumnList. Thus, we go through
             * the index baseColumnPositions, and remove everything else
             */

//            if (cd.isIndex() || cd.isPrimaryKey()) {
            if (cd.isIndex() || cd.isPrimaryKey()) {
                baseColumnPositions = cd.getIndexDescriptor().baseColumnPositions();
               if (!isCoveringIndex(cd) && !cd.isPrimaryKey()) {
                    indexLookupList = new BitSet();
                    for (int i = scanColumnList.nextSetBit(0); i >= 0; i = scanColumnList.nextSetBit(i + 1)) {
                        boolean found = false;
                        for (int baseColumnPosition : baseColumnPositions) {
                            if (i == baseColumnPosition) {
                                found = true;
                                break;
                            }
                        }
                        if (!found) {
                            indexLookupList.set(i);
                            scanColumnList.clear(i);
                        }
                    }
                }
            }
        }else{
            scanColumnList = new BitSet();
        }
        DataValueDescriptor[] rowTemplate=getRowTemplate(cd,getBaseCostController());
        ScanCostFunction scf = new ScanCostFunction(scanColumnList,
                indexLookupList,
                this,
                scc,
                costEstimate,
                rowTemplate,
                baseColumnPositions,
                forUpdate(),
                resultColumns,
                usedNoStatsColumnIds);

        // check if specialMaxScan is applicable
        currentAccessPath.setSpecialMaxScan(false);
        if (!optimizer.isForSpark() && aggrForSpecialMaxScan != null) {
            ColumnReference cr = (ColumnReference)aggrForSpecialMaxScan.getOperand();
            boolean isMax = ((MaxMinAggregateDefinition)aggrForSpecialMaxScan.getAggregateDefinition()).isMax();
            if (cd.isIndex() || cd.isPrimaryKey()) {
                IndexDescriptor id = cd.getIndexDescriptor();
                baseColumnPositions = id.baseColumnPositions();
                if (baseColumnPositions[0] == cr.getColumnNumber()) {
                    if (id.isAscending()[0] && isMax)
                        currentAccessPath.setSpecialMaxScan(true);
                    else if (!id.isAscending()[0] && !isMax)
                        currentAccessPath.setSpecialMaxScan(true);
                }
            }
        }
        if(currentJoinStrategy.allowsJoinPredicatePushdown() && isOneRowResultSet(cd,baseTableRestrictionList) || currentAccessPath.getSpecialMaxScan()){ // Retrieving only one row...
            singleScanRowCount=1.0;
            scf.generateOneRowCost();
        }
        else {
            int predListSize = predList!=null?baseTableRestrictionList.size():0;
            for(int i=0;i<predListSize;i++){
                Predicate p = (Predicate)baseTableRestrictionList.getOptPredicate(i);
                if(!p.isStartKey()&&!p.isStopKey()){
                    if(baseTableRestrictionList.isRedundantPredicate(i)) continue;
                }

                if(!p.isJoinPredicate()&& !p.isFullJoinPredicate() || currentJoinStrategy.allowsJoinPredicatePushdown()) //skip join predicates unless they support predicate pushdown
                    scf.addPredicate(p, defaultSelectivityFactor);
            }
            scf.generateCost();
            singleScanRowCount=costEstimate.singleScanRowCount();
        }
        tracer.trace(OptimizerFlag.COST_OF_CONGLOMERATE_SCAN1,tableNumber,0,0.0,cd, correlationName, costEstimate);

        costEstimate.setSingleScanRowCount(singleScanRowCount);

        // Clamp to 1.0d anything under 1.0d
        if (costEstimate.singleScanRowCount() < 1.0d || costEstimate.getEstimatedRowCount() < 1.0d) {
            costEstimate.setSingleScanRowCount(1.0d);
            costEstimate.setRowCount(1.0d);
        }

        /*
         * Now compute the joinStrategy costs.
         */
        boolean oldIsAntiJoin = outerCost.isAntiJoin();
        outerCost.setAntiJoin(isAntiJoin);
        currentJoinStrategy.estimateCost(this, baseTableRestrictionList, cd, outerCost, optimizer, costEstimate);
        outerCost.setAntiJoin(oldIsAntiJoin);
        tracer.trace(OptimizerFlag.COST_OF_N_SCANS,tableNumber,0,outerCost.rowCount(),costEstimate, correlationName);

        /* Put the base predicates back in the predicate list */
        currentJoinStrategy.putBasePredicates(predList, baseTableRestrictionList);

        return costEstimate;
    }

    @Override
    public boolean isBaseTable(){
        return true;
    }

    @Override
    public boolean forUpdate(){
        /* This table is updatable if it is the
         * target table of an update or delete,
         * or it is (or was) the target table of an
         * updatable cursor.
         */
        return (updateOrDelete!=0) || cursorTargetTable || getUpdateLocks;
    }

    @Override public boolean isTargetTable(){ return (updateOrDelete!=0); }

    public double uniqueJoin(OptimizablePredicateList predList) throws StandardException{
        double retval=-1.0;
        PredicateList pl=(PredicateList)predList;
        int numColumns=getTableDescriptor().getNumberOfColumns();
        int tableNumber=getTableNumber();

        // This is supposed to be an array of table numbers for the current
        // query block. It is used to determine whether a join is with a
        // correlation column, to fill in eqOuterCols properly. We don't care
        // about eqOuterCols, so just create a zero-length array, pretending
        // that all columns are correlation columns.
        int[] tableNumbers=new int[0];
        JBitSet[] tableColMap=new JBitSet[1];
        tableColMap[0]=new JBitSet(numColumns+1);

        pl.checkTopPredicatesForEqualsConditions(tableNumber,
                null,
                tableNumbers,
                tableColMap,
                false);

        if(supersetOfUniqueIndex(tableColMap)){
            retval= getBestAccessPath().getCostEstimate().singleScanRowCount();
        }

        return retval;
    }

    @Override
    public boolean isOneRowScan() throws StandardException{
        /* EXISTS FBT will never be a 1 row scan.
         * Otherwise call method in super class.
         */
        return !existsTable && super.isOneRowScan();
    }

    @Override
    public boolean legalJoinOrder(JBitSet assignedTableMap){
        /* Have all of our dependencies been satisfied? */
        if (dependencyMap != null) {
            if (existsTable || fromSSQ)
                // the check of getFirstSetBit()!= -1 ensures that exists table or table converted from SSQ won't be the leftmost table
                return (assignedTableMap.getFirstSetBit()!= -1) && assignedTableMap.contains(dependencyMap);
            else
                return assignedTableMap.contains(dependencyMap);
        }
        return true;
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */
    @Override
    public String toString(){
        if(SanityManager.DEBUG){
            return "tableName: "+
                    (tableName!=null?tableName.toString():"null")+"\n"+
                    "tableDescriptor: "+tableDescriptor+"\n"+
                    "updateOrDelete: "+updateOrDelete+"\n"+
                    (tableProperties!=null?
                            tableProperties.toString():"null")+"\n"+
                    "existsTable: "+existsTable+"\n"+
                    "dependencyMap: "+
                    (dependencyMap!=null
                            ?dependencyMap.toString()
                            :"null")+"\n"+
                    super.toString();
        }else{
            return "";
        }
    }


    /**
     * Set the table properties for this table.
     *
     * @param tableProperties The new table properties.
     */
    public void setTableProperties(Properties tableProperties){ this.tableProperties=tableProperties; }

    /**
     * Bind the table in this FromBaseTable.
     * This is where view resolution occurs
     *
     * @param dataDictionary The DataDictionary to use for binding
     * @param fromListParam  FromList to use/append to.
     * @throws StandardException Thrown on error
     * @return ResultSetNode    The FromTable for the table or resolved view.
     */
    @Override
    public ResultSetNode bindNonVTITables(DataDictionary dataDictionary,
                                          FromList fromListParam)throws StandardException{
        TableDescriptor tableDescriptor=bindTableDescriptor();

        int tableType = tableDescriptor.getTableType();
        if(pastTxIdExpression != null)
        {
            if(tableType==TableDescriptor.VIEW_TYPE) {
                throw StandardException.newException(SQLState.LANG_ILLEGAL_TIME_TRAVEL, "views");
            }
            else if(tableType==TableDescriptor.EXTERNAL_TYPE) {
                throw StandardException.newException(SQLState.LANG_ILLEGAL_TIME_TRAVEL, "external tables");
            }
            else if(tableType==TableDescriptor.WITH_TYPE) {
                throw StandardException.newException(SQLState.LANG_ILLEGAL_TIME_TRAVEL, "common table expressions");
            }
        }

        if(tableDescriptor.getTableType()==TableDescriptor.VTI_TYPE){
            ResultSetNode vtiNode=mapTableAsVTI(
                    tableDescriptor,
                    getCorrelationName(),
                    resultColumns,
                    getProperties(),
                    getContextManager());
            return vtiNode.bindNonVTITables(dataDictionary,fromListParam);
        }

        ResultColumnList derivedRCL=resultColumns;

        // make sure there's a restriction list
        restrictionList=(PredicateList)getNodeFactory().getNode(C_NodeTypes.PREDICATE_LIST, getContextManager());
        baseTableRestrictionList=(PredicateList)getNodeFactory().getNode(C_NodeTypes.PREDICATE_LIST, getContextManager());

        CompilerContext compilerContext=getCompilerContext();

        /* Generate the ResultColumnList */
        resultColumns=genResultColList();
        templateColumns=resultColumns;

        /* Resolve the view, if this is a view */
        if(tableDescriptor.getTableType()==TableDescriptor.VIEW_TYPE || tableDescriptor.getTableType()==TableDescriptor.WITH_TYPE){
            FromSubquery fsq;
            ResultSetNode rsn;
            ViewDescriptor vd;
            CreateViewNode cvn;
            SchemaDescriptor compSchema;

            /* Get the associated ViewDescriptor so that we can get
             * the view definition text.
             */
            vd=dataDictionary.getViewDescriptor(tableDescriptor);

            /*
            ** Set the default compilation schema to be whatever
            ** this schema this view was originally compiled against.
            ** That way we pick up the same tables no matter what
            ** schema we are running against.
            */
            compSchema=dataDictionary.getSchemaDescriptor(vd.getCompSchemaId(),null);

            compilerContext.pushCompilationSchema(compSchema);

            try{
                /* This represents a view - query is dependent on the ViewDescriptor */
                // Removes with clause dependency creation.
                // No reason to create dependency.
                if (vd.getUUID() != null)
                    compilerContext.createDependency(vd);

                if(SanityManager.DEBUG){
                    //noinspection ConstantConditions
                    SanityManager.ASSERT(vd!=null,"vd not expected to be null for "+tableName);
                }

                cvn=(CreateViewNode)parseStatement(vd.getViewText(),false);

                if (cvn.isRecursive()) {
                    cvn.replaceSelfReferenceForRecursiveView(tableDescriptor);
                }

                rsn=cvn.getParsedQueryExpression();


                /* If the view contains a '*' then we mark the views derived column list
                 * so that the view will still work, and return the expected results,
                 * if any of the tables referenced in the view have columns added to
                 * them via ALTER TABLE.  The expected results means that the view
                 * will always return the same # of columns.
                 */
                if(rsn.getResultColumns().containsAllResultColumn()){
                    resultColumns.setCountMismatchAllowed(true);
                }
                //Views execute with definer's privileges and if any one of
                //those privileges' are revoked from the definer, the view gets
                //dropped. So, a view can exist in Derby only if it's owner has
                //all the privileges needed to create one. In order to do a
                //select from a view, a user only needs select privilege on the
                //view and doesn't need any privilege for objects accessed by
                //the view. Hence, when collecting privilege requirement for a
                //sql accessing a view, we only need to look for select privilege
                //on the actual view and that is what the following code is
                //checking.
                // we only need to collect privilege requirement if the current
                // view's isPrivilegeCollectionRequired is set
                if(isPrivilegeCollectionRequired()) {
                    for (ResultColumn rc : resultColumns) {
                        compilerContext.addRequiredColumnPriv(rc.getTableColumnDescriptor());
                    }
                }

                fsq=(FromSubquery)getNodeFactory().getNode(
                        C_NodeTypes.FROM_SUBQUERY,
                        rsn,
                        cvn.getOrderByList(),
                        cvn.getOffset(),
                        cvn.getFetchFirst(),
                        cvn.hasJDBClimitClause(),
                        (correlationName!=null)?
                                correlationName:getOrigTableName().getTableName(),
                        resultColumns,
                        tableProperties,
                        getContextManager());
                // Transfer the nesting level to the new FromSubquery
                fsq.setLevel(level);
                //We are getting ready to bind the query underneath the view. Since
                //that query is going to run with definer's privileges, we do not
                //need to collect any privilege requirement for that query.
                //Following call is marking the query to run with definer
                //privileges. This marking will make sure that we do not collect
                //any privilege requirement for it.
                CollectNodesVisitor cnv=
                        new CollectNodesVisitor(QueryTreeNode.class,null);

                fsq.accept(cnv, null);

                for(Object o : cnv.getList()){
                    ((QueryTreeNode)o).disablePrivilegeCollection();
                }

                fsq.setOrigTableName(this.getOrigTableName());

                // since we reset the compilation schema when we return, we
                // need to save it for use when we bind expressions:
                fsq.setOrigCompilationSchema(compSchema);
                ResultSetNode fsqBound= fsq.bindNonVTITables(dataDictionary,fromListParam);

                /* Do error checking on derived column list and update "exposed"
                 * column names if valid.
                 */
                if(derivedRCL!=null){
                    fsqBound.getResultColumns().propagateDCLInfo(derivedRCL, origTableName.getFullTableName());
                }

                return fsqBound;
            }finally{
                compilerContext.popCompilationSchema();
            }
        }else{
            /* This represents a table - query is dependent on the TableDescriptor */
            compilerContext.createDependency(tableDescriptor);

            long heapConglomerateId = tableDescriptor.getHeapConglomerateId();
            /* Get the base conglomerate descriptor */
            baseConglomerateDescriptor= tableDescriptor.getConglomerateDescriptor(heapConglomerateId);

            // Bail out if the descriptor couldn't be found. The conglomerate
            // probably doesn't exist anymore.
            if(baseConglomerateDescriptor==null){
                //noinspection UnnecessaryBoxing
                throw StandardException.newException(SQLState.STORE_CONGLOMERATE_DOES_NOT_EXIST,Long.valueOf(heapConglomerateId));
            }

            /* Build the 0-based array of base column names. */
            columnNames=resultColumns.getColumnNames();

            /* Do error checking on derived column list and update "exposed"
             * column names if valid.
             */
            if(derivedRCL!=null){
                resultColumns.propagateDCLInfo(derivedRCL,origTableName.getFullTableName());
            }

            /* Assign the tableNumber */
            if(tableNumber==-1)  // allow re-bind, in which case use old number
                tableNumber=compilerContext.getNextTableNumber();
        }

        //
        // Only the DBO can select from SYS.SYSUSERS or SYS.SYSTOKENS
        //
        authorizeSYSUSERS= dataDictionary.usesSqlAuthorization() &&
                tableDescriptor.getUUID().toString().equals(SYSUSERSRowFactory.SYSUSERS_UUID);
        boolean authorizeSYSTOKENS= dataDictionary.usesSqlAuthorization() &&
                tableDescriptor.getUUID().toString().equals(SYSTOKENSRowFactory.SYSTOKENS_UUID);
        if(authorizeSYSUSERS || authorizeSYSTOKENS){
            String databaseOwner=dataDictionary.getAuthorizationDatabaseOwner();
            String currentUser=getLanguageConnectionContext().getStatementContext().getSQLSessionContext().getCurrentUser();
            List<String> groupuserlist = getLanguageConnectionContext().getStatementContext().getSQLSessionContext().getCurrentGroupUser();

            if(! (databaseOwner.equals(currentUser) || (groupuserlist != null && groupuserlist.contains(databaseOwner)))){
                throw StandardException.newException(SQLState.DBO_ONLY);
            }
        }

        return this;
    }

    /**
     * Return a node that represents invocation of the virtual table for the
     * given table descriptor. The mapping of the table descriptor to a specific
     * VTI class name will occur as part of the "init" phase for the
     * NewInvocationNode that we create here.
     * <p/>
     * Currently only handles no argument VTIs corresponding to a subset of the
     * diagnostic tables. (e.g. lock_table). The node returned is a FROM_VTI
     * node with a passed in NEW_INVOCATION_NODE representing the class, with no
     * arguments. Other attributes of the original FROM_TABLE node (such as
     * resultColumns) are passed into the FROM_VTI node.
     */
    private ResultSetNode mapTableAsVTI(
            TableDescriptor td,
            String correlationName,
            ResultColumnList resultColumns,
            Properties tableProperties,
            ContextManager cm)
            throws StandardException{


        // The fact that we pass a non-null table descriptor to the following
        // call is an indication that we are mapping to a no-argument VTI. Since
        // we have the table descriptor we do not need to pass in a TableName.
        // See NewInvocationNode for more.
        QueryTreeNode newNode=(QueryTreeNode)getNodeFactory().getNode(
                C_NodeTypes.NEW_INVOCATION_NODE,
                null, // TableName
                td, // TableDescriptor
                Collections.EMPTY_LIST,
                Boolean.FALSE,
                cm);

        QueryTreeNode vtiNode;

        if(correlationName!=null){
            vtiNode=(QueryTreeNode)getNodeFactory().getNode(
                    C_NodeTypes.FROM_VTI,
                    newNode,
                    correlationName,
                    resultColumns,
                    tableProperties,
                    cm);
        }else{
            TableName exposedName=newNode.makeTableName(td.getSchemaName(),
                    td.getDescriptorName());

            vtiNode=(QueryTreeNode)getNodeFactory().getNode(
                    C_NodeTypes.FROM_VTI,
                    newNode,
                    null,
                    resultColumns,
                    tableProperties,
                    exposedName,
                    cm);
        }

        return (ResultSetNode)vtiNode;
    }

    /**
     * Determine whether or not the specified name is an exposed name in
     * the current query block.
     *
     * @param name       The specified name to search for as an exposed name.
     * @param schemaName Schema name, if non-null.
     * @param exactMatch Whether or not we need an exact match on specified schema and table
     *                   names or match on table id.
     * @return The FromTable, if any, with the exposed name.
     * @throws StandardException Thrown on error
     */
    @Override
    protected FromTable getFromTableByName(String name,String schemaName,boolean exactMatch) throws StandardException{
        // ourSchemaName can be null if correlation name is specified.
        String ourSchemaName=getOrigTableName().getSchemaName();
        String fullName=(schemaName!=null)?(schemaName+'.'+name):name;

        /* If an exact string match is required then:
         *    o  If schema name specified on 1 but not both then no match.
         *  o  If schema name not specified on either, compare exposed names.
         *  o  If schema name specified on both, compare schema and exposed names.
         */
        if(exactMatch){

            if((schemaName!=null && ourSchemaName==null) ||
                    (schemaName==null && ourSchemaName!=null)){
                return null;
            }

            if(getExposedName().equals(fullName)){
                return this;
            }

            return null;
        }

        /* If an exact string match is not required then:
         *  o  If schema name specified on both, compare schema and exposed names.
         *  o  If schema name not specified on either, compare exposed names.
         *    o  If schema name specified on column but not table, then compare
         *       the column's schema name against the schema name from the TableDescriptor.
         *       If they agree, then the column's table name must match the exposed name
         *       from the table, which must also be the base table name, since a correlation
         *       name does not belong to a schema.
         *  o  If schema name not specified on column then just match the exposed names.
         */
        // Both or neither schema name specified
        if(getExposedName().equals(fullName)){
            return this;
        }else if((schemaName!=null && ourSchemaName!=null) ||
                (schemaName==null && ourSchemaName==null)){
            return null;
        }

        // Schema name only on column
        // e.g.:  select w1.i from t1 w1 order by test2.w1.i;  (incorrect)
        if(schemaName!=null){
            // Compare column's schema name with table descriptor's if it is
            // not a synonym since a synonym can be declared in a different
            // schema.
            if(tableName.equals(origTableName) &&
                    !schemaName.equals(tableDescriptor.getSchemaDescriptor().getSchemaName())){
                return null;
            }

            // Compare exposed name with column's table name
            if(!getExposedName().equals(name)){
                return null;
            }

            // Make sure exposed name is not a correlation name
            if(!getExposedName().equals(getOrigTableName().getTableName())){
                return null;
            }

            return this;
        }

        /* Schema name only specified on table. Compare full exposed name
         * against table's schema name || "." || column's table name.
         */
        if(!getExposedName().equals(getOrigTableName().getSchemaName()+"."+name)){
            return null;
        }

        return this;
    }


    /**
     * Bind the table descriptor for this table.
     * <p/>
     * If the tableName is a synonym, it will be resolved here.
     * The original table name is retained in origTableName.
     *
     * @throws StandardException Thrown on error
     */
    private TableDescriptor bindTableDescriptor()
            throws StandardException{
        String schemaName=tableName.getSchemaName();
        SchemaDescriptor sd=getSchemaDescriptor(schemaName);

        tableDescriptor=getTableDescriptor(tableName.getTableName(),sd);

        // Find With Descriptors
        if (tableDescriptor==null && getLanguageConnectionContext().getWithDescriptor(tableName.tableName) !=null) {
            tableDescriptor = getLanguageConnectionContext().getWithDescriptor(tableName.tableName);
        }

        if(tableDescriptor==null){
            // Check if the reference is for a synonym.
            TableName synonymTab=resolveTableToSynonym(tableName);
            if(synonymTab==null)
                throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND,tableName.toString());

            tableName=synonymTab;
            sd=getSchemaDescriptor(tableName.getSchemaName());

            tableDescriptor=getTableDescriptor(synonymTab.getTableName(),sd);
            if(tableDescriptor==null)
                throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND,tableName.toString());
        }

        return tableDescriptor;
    }


    /**
     * Bind the expressions in this FromBaseTable.  This means binding the
     * sub-expressions, as well as figuring out what the return type is for
     * each expression.
     *
     * @param fromListParam FromList to use/append to.
     * @throws StandardException Thrown on error
     */
    @Override
    public void bindExpressions(FromList fromListParam) throws StandardException{
        if(pastTxIdExpression != null)
        {
            // not sure if this is necessary
            ValueNode result = pastTxIdExpression.bindExpression(fromListParam, null, null);
            // the result of the expression should either be:
            // a. timestamp (then we have to map it to closest tx id).
            // b. numeric (representing the tx id itself).
            TypeId typeId = result.getTypeId();
            if(!typeId.isDateTimeTimeStampTypeID() && !typeId.isNumericTypeId())
            {
                throw StandardException.newException(SQLState.DATA_TYPE_NOT_SUPPORTED, typeId.getSQLTypeName());
            }
        }
    }

    /**
     * Bind the result columns of this ResultSetNode when there is no
     * base table to bind them to.  This is useful for SELECT statements,
     * where the result columns get their types from the expressions that
     * live under them.
     *
     * @param fromListParam FromList to use/append to.
     * @throws StandardException Thrown on error
     */

    public void bindResultColumns(FromList fromListParam)
            throws StandardException{
        /* Nothing to do, since RCL bound in bindNonVTITables() */
    }

    /**
     * Try to find a ResultColumn in the table represented by this FromBaseTable
     * that matches the name in the given ColumnReference.
     *
     * @param columnReference The columnReference whose name we're looking
     *                        for in the given table.
     * @throws StandardException Thrown on error
     * @return A ResultColumn whose expression is the ColumnNode
     * that matches the ColumnReference.
     * Returns null if there is no match.
     */

    public ResultColumn getMatchingColumn(ColumnReference columnReference) throws StandardException{
        ResultColumn resultColumn=null;
        TableName columnsTableName;
        TableName exposedTableName;

        columnsTableName=columnReference.getTableNameNode();

        if(columnsTableName!=null){
            if(columnsTableName.getSchemaName()==null && correlationName==null)
                columnsTableName.bind(this.getDataDictionary());
        }
        /*
        ** If there is a correlation name, use that instead of the
        ** table name.
        */
        exposedTableName=getExposedTableName();

        if(exposedTableName.getSchemaName()==null && correlationName==null)
            exposedTableName.bind(this.getDataDictionary());
        /*
        ** If the column did not specify a name, or the specified name
        ** matches the table we're looking at, see whether the column
        ** is in this table.
        */
        if(columnsTableName==null || columnsTableName.equals(exposedTableName)){
            resultColumn=resultColumns.getResultColumn(columnReference.getColumnName());
            /* Did we find a match? */
            if(resultColumn!=null){
                columnReference.setTableNumber(tableNumber);
                columnReference.setColumnNumber(
                        resultColumn.getColumnPosition());

                if(tableDescriptor!=null){
                    FormatableBitSet referencedColumnMap=tableDescriptor.getReferencedColumnMap();
                    if(referencedColumnMap==null)
                        referencedColumnMap=new FormatableBitSet(
                                tableDescriptor.getMaxColumnID()+1);
                    referencedColumnMap.set(resultColumn.getColumnPosition());
                    tableDescriptor.setReferencedColumnMap(referencedColumnMap);
                }
            }else if(columnReference.columnName.compareTo("ROWID")==0){
                if(rowIdColumn==null){
                    ValueNode rowLocationNode=(ValueNode)getNodeFactory().getNode(
                            C_NodeTypes.CURRENT_ROW_LOCATION_NODE,
                            getContextManager());

                    rowLocationNode.setType(new DataTypeDescriptor(TypeId.getBuiltInTypeId(TypeId.REF_NAME),
                                    false        /* Not nullable */
                            )
                    );

                    rowIdColumn=(ResultColumn)getNodeFactory().getNode(
                            C_NodeTypes.RESULT_COLUMN,
                            columnReference.columnName,
                            rowLocationNode,
                            getContextManager());
                }
                columnReference.setTableNumber(tableNumber);
                resultColumn=rowIdColumn;
            }
        }

        return resultColumn;
    }

    /**
     * Preprocess a ResultSetNode - this currently means:
     * o  Generating a referenced table map for each ResultSetNode.
     * o  Putting the WHERE and HAVING clauses in conjunctive normal form (CNF).
     * o  Converting the WHERE and HAVING clauses into PredicateLists and
     * classifying them.
     * o  Ensuring that a ProjectRestrictNode is generated on top of every
     * FromBaseTable and generated in place of every FromSubquery.
     * o  Pushing single table predicates down to the new ProjectRestrictNodes.
     *
     * @param numTables The number of tables in the DML Statement
     * @param gbl       The group by list, if any
     * @param fromList  The from list, if any
     * @return ResultSetNode at top of preprocessed tree.
     * @throws StandardException Thrown on error
     */

    public ResultSetNode preprocess(int numTables,
                                    GroupByList gbl,
                                    FromList fromList)
            throws StandardException{
        /* Generate the referenced table map */
        referencedTableMap=new JBitSet(numTables);
        referencedTableMap.set(tableNumber);

        return genProjectRestrict(numTables);
    }

    /**
     * Put a ProjectRestrictNode on top of each FromTable in the FromList.
     * ColumnReferences must continue to point to the same ResultColumn, so
     * that ResultColumn must percolate up to the new PRN.  However,
     * that ResultColumn will point to a new expression, a VirtualColumnNode,
     * which points to the FromTable and the ResultColumn that is the source for
     * the ColumnReference.
     * (The new PRN will have the original of the ResultColumnList and
     * the ResultColumns from that list.  The FromTable will get shallow copies
     * of the ResultColumnList and its ResultColumns.  ResultColumn.expression
     * will remain at the FromTable, with the PRN getting a new
     * VirtualColumnNode for each ResultColumn.expression.)
     * We then project out the non-referenced columns.  If there are no referenced
     * columns, then the PRN's ResultColumnList will consist of a single ResultColumn
     * whose expression is 1.
     *
     * @param numTables Number of tables in the DML Statement
     * @return The generated ProjectRestrictNode atop the original FromTable.
     * @throws StandardException Thrown on error
     */

    protected ResultSetNode genProjectRestrict(int numTables)
            throws StandardException{
        /* We get a shallow copy of the ResultColumnList and its
         * ResultColumns.  (Copy maintains ResultColumn.expression for now.)
         */
        ResultColumnList prRCList=resultColumns;
        resultColumns=resultColumns.copyListAndObjects();

        /* Replace ResultColumn.expression with new VirtualColumnNodes
         * in the ProjectRestrictNode's ResultColumnList.  (VirtualColumnNodes include
         * pointers to source ResultSetNode, this, and source ResultColumn.)
         * NOTE: We don't want to mark the underlying RCs as referenced, otherwise
         * we won't be able to project out any of them.
         */
        prRCList.genVirtualColumnNodes(this,resultColumns,false);

        /* Project out any unreferenced columns.  If there are no referenced
         * columns, generate and bind a single ResultColumn whose expression is 1.
         */
        prRCList.doProjection(true);

        // Add rowId column to prRCList
        if(rowIdColumn!=null){
            prRCList.addResultColumn(rowIdColumn);
        }

        /* Finally, we create the new ProjectRestrictNode */
        return (ResultSetNode)getNodeFactory().getNode(
                C_NodeTypes.PROJECT_RESTRICT_NODE,
                this,
                prRCList,
                null,    /* Restriction */
                null,   /* Restriction as PredicateList */
                null,    /* Project subquery list */
                null,    /* Restrict subquery list */
                null,
                getContextManager());
    }

    /**
     * @throws StandardException Thrown on error
     * @see ResultSetNode#changeAccessPath
     */
    public ResultSetNode changeAccessPath(JBitSet joinedTableSet) throws StandardException{
        ResultSetNode retval;
        AccessPath ap=getTrulyTheBestAccessPath();
        ConglomerateDescriptor trulyTheBestConglomerateDescriptor= ap.getConglomerateDescriptor();
        JoinStrategy trulyTheBestJoinStrategy=ap.getJoinStrategy();
        Optimizer optimizer=ap.getOptimizer();

        optimizer.tracer().trace(OptimizerFlag.CHANGING_ACCESS_PATH_FOR_TABLE, tableNumber,0,0.0,correlationName);

        if(SanityManager.DEBUG){
            SanityManager.ASSERT(trulyTheBestConglomerateDescriptor!=null,
                    "Should only modify access path after conglomerate has been chosen.");
        }

        /*
        ** Make sure user-specified bulk fetch is OK with the chosen join
        ** strategy.
        */
        if(bulkFetch!=UNSET){
            if(!trulyTheBestJoinStrategy.bulkFetchOK()){
                throw StandardException.newException(SQLState.LANG_INVALID_BULK_FETCH_WITH_JOIN_TYPE,
                        trulyTheBestJoinStrategy.getName());
            }
            // bulkFetch has no meaning for hash join, just ignore it
            else if(trulyTheBestJoinStrategy.ignoreBulkFetch()){
                disableBulkFetch();
            }
            // bug 4431 - ignore bulkfetch property if it's 1 row resultset
            else if(isOneRowResultSet()){
                disableBulkFetch();
            }
        }

        // bulkFetch = 1 is the same as no bulk fetch
        if(bulkFetch==1){
            disableBulkFetch();
        }

        /* Remove any redundant join clauses.  A redundant join clause is one
         * where there are other join clauses in the same equivalence class
         * after it in the PredicateList.
         */
        restrictionList.removeRedundantPredicates();

        /*
        ** Divide up the predicates for different processing phases of the
        ** best join strategy.
        */
        ContextManager ctxMgr=getContextManager();
        storeRestrictionList=(PredicateList)getNodeFactory().getNode(C_NodeTypes.PREDICATE_LIST,ctxMgr);
        nonStoreRestrictionList=(PredicateList)getNodeFactory().getNode(C_NodeTypes.PREDICATE_LIST,ctxMgr);
        requalificationRestrictionList=(PredicateList)getNodeFactory().getNode(C_NodeTypes.PREDICATE_LIST,ctxMgr);
        trulyTheBestJoinStrategy.divideUpPredicateLists(
                this,
                joinedTableSet,
                restrictionList,
                storeRestrictionList,
                nonStoreRestrictionList,
                requalificationRestrictionList,
                getDataDictionary());

        /* Check to see if we are going to do execution-time probing
         * of an index using IN-list values.  We can tell by looking
         * at the restriction list: if there is an IN-list probe
         * predicate that is also a start/stop key then we know that
         * we're going to do execution-time probing.  In that case
         * we disable bulk fetching to minimize the number of non-
         * matching rows that we read from disk.  RESOLVE: Do we
         * really need to completely disable bulk fetching here,
         * or can we do something else?
         */

        /* Derby originally only looked in the restrictionList for InList predicates when considering whether or
         * not to do multiProbing. Some implementations of JoinStrategy.divideUpPredicateLists() (but not all)
         * will *move* InList predicates referencing this table from restrictionList to storeRestrictionList, other
         * implementations *copy* the original predicates to restrictionList. To always multiProve when possible
         * Splice changed the below loop to iterate over storeRestrictionList instead of restrictionList */
        for(int i=0;i<storeRestrictionList.size();i++){
            Predicate pred=storeRestrictionList.elementAt(i);
            if(pred.isInListProbePredicate() && pred.isStartKey()){
                disableBulkFetch();
                multiProbing=true;
                break;
            }
        }

        /*
        ** Consider turning on bulkFetch if it is turned
        ** off.  Only turn it on if it is a not an updatable
        ** scan and if it isn't a oneRowResultSet, and
        ** not a subquery, and it is OK to use bulk fetch
        ** with the chosen join strategy.  NOTE: the subquery logic
        ** could be more sophisticated -- we are taking
        ** the safe route in avoiding reading extra
        ** data for something like:
        **
        **    select x from t where x in (select y from t)
         **
        ** In this case we want to stop the subquery
        ** evaluation as soon as something matches.
        */
        if(trulyTheBestJoinStrategy.bulkFetchOK() &&
                !(trulyTheBestJoinStrategy.ignoreBulkFetch()) &&
                !bulkFetchTurnedOff &&
                (bulkFetch==UNSET) &&
                (!forUpdate() || isBulkDelete)&&
                !isOneRowResultSet() &&
                getLevel()==0){
            bulkFetch=getDefaultBulkFetch();
        }

        /* Statement is dependent on the chosen conglomerate. */
        getCompilerContext().createDependency(trulyTheBestConglomerateDescriptor);

        /* No need to modify access path if conglomerate is the heap */
        //noinspection ConstantConditions
        if(!trulyTheBestConglomerateDescriptor.isIndex()){
            /*
            ** We need a little special logic for SYSSTATEMENTS
            ** here.  SYSSTATEMENTS has a hidden column at the
            ** end.  When someone does a select * we don't want
            ** to get that column from the store.  So we'll always
            ** generate a partial read bitSet if we are scanning
            ** SYSSTATEMENTS to ensure we don't get the hidden
            ** column.
            */
            boolean isSysstatements=tableName.equals("SYS","SYSSTATEMENTS");
            /* Template must reflect full row.
             * Compact RCL down to partial row.
             */

            templateColumns=resultColumns;
            referencedCols=resultColumns.getReferencedFormatableBitSet(cursorTargetTable,isSysstatements,false,false);
            resultColumns=resultColumns.compactColumns(cursorTargetTable,isSysstatements);
            return this;
        }

        /* No need to go to the data page if this is a covering index */
        /* Derby-1087: use data page when returning an updatable resultset */
        if(ap.getCoveringIndexScan() && (!cursorTargetTable())){
            /* Massage resultColumns so that it matches the index. */
            resultColumns=newResultColumns(resultColumns,
                    trulyTheBestConglomerateDescriptor,
                    baseConglomerateDescriptor,
                    false);

            /* We are going against the index.  The template row must be the full index row.
             * The template row will have the RID but the result row will not
             * since there is no need to go to the data page.
             */
            templateColumns=newResultColumns(resultColumns,
                    trulyTheBestConglomerateDescriptor,
                    baseConglomerateDescriptor,
                    false);
            templateColumns.addRCForRID();

            // If this is for update then we need to get the RID in the result row
            if(forUpdate()){
                resultColumns.addRCForRID();
            }

            /* Compact RCL down to the partial row.  We always want a new
             * RCL and FormatableBitSet because this is a covering index.  (This is
             * because we don't want the RID in the partial row returned
             * by the store.)
             */
            referencedCols=resultColumns.getReferencedFormatableBitSet(cursorTargetTable,true,false,true);
                        resultColumns=resultColumns.compactColumns(cursorTargetTable,true);

            resultColumns.setIndexRow(
                    baseConglomerateDescriptor.getConglomerateNumber(),
                    forUpdate());

            return this;
        }

        /* Statement is dependent on the base conglomerate if this is
         * a non-covering index.
         */
        getCompilerContext().createDependency(baseConglomerateDescriptor);

        /*
        ** On bulkFetch, we need to add the restrictions from
        ** the TableScan and reapply them  here.
        */
        if(bulkFetch!=UNSET){
            restrictionList.copyPredicatesToOtherList(
                    requalificationRestrictionList);
        }

        /*
        ** We know the chosen conglomerate is an index.  We need to allocate
        ** an IndexToBaseRowNode above us, and to change the result column
        ** list for this FromBaseTable to reflect the columns in the index.
        ** We also need to shift "cursor target table" status from this
        ** FromBaseTable to the new IndexToBaseRowNow (because that's where
        ** a cursor can fetch the current row).
        */
        ResultColumnList newResultColumns= newResultColumns(resultColumns,
                trulyTheBestConglomerateDescriptor,
                baseConglomerateDescriptor,
                true);

        /* Compact the RCL for the IndexToBaseRowNode down to
         * the partial row for the heap.  The referenced BitSet
         * will reflect only those columns coming from the heap.
         * (ie, it won't reflect columns coming from the index.)
         * NOTE: We need to re-get all of the columns from the heap
         * when doing a bulk fetch because we will be requalifying
         * the row in the IndexRowToBaseRow.
         */
        // Get the BitSet for all of the referenced columns
        FormatableBitSet indexReferencedCols=null;
        FormatableBitSet indexReferencedStorageCols=null;

        //FormatableBitSet heapReferencedCols;
        if((bulkFetch==UNSET) && (requalificationRestrictionList==null || requalificationRestrictionList.isEmpty())){
            /* No BULK FETCH or requalification, XOR off the columns coming from the heap
             * to get the columns coming from the index.
             */
            indexReferencedCols=resultColumns.getReferencedFormatableBitSet(cursorTargetTable,true,false,false);
            heapReferencedCols=resultColumns.getReferencedFormatableBitSet(cursorTargetTable,true,true,false);
            if(heapReferencedCols!=null){
                indexReferencedCols.xor(heapReferencedCols);
            }
        }else{
            // BULK FETCH or requalification - re-get all referenced columns from the heap
            heapReferencedCols=resultColumns.getReferencedFormatableBitSet(cursorTargetTable,true,false,false);
        }
        ResultColumnList heapRCL=resultColumns.compactColumns(cursorTargetTable,false);
        retval=(ResultSetNode)getNodeFactory().getNode(
                C_NodeTypes.INDEX_TO_BASE_ROW_NODE,
                this,
                baseConglomerateDescriptor,
                heapRCL,
                cursorTargetTable,
                heapReferencedCols,
                indexReferencedCols,
                requalificationRestrictionList,
                forUpdate(),
                tableProperties,
                ctxMgr);

        /*
        ** The template row is all the columns.  The
        ** result set is the compacted column list.
        */
        resultColumns=newResultColumns;

        templateColumns=newResultColumns(resultColumns,trulyTheBestConglomerateDescriptor,baseConglomerateDescriptor,false);
        /* Since we are doing a non-covered index scan, if bulkFetch is on, then
         * the only columns that we need to get are those columns referenced in the start and stop positions
         * and the qualifiers (and the RID) because we will need to re-get all of the other
         * columns from the heap anyway.
         * At this point in time, columns referenced anywhere in the column tree are
         * marked as being referenced.  So, we clear all of the references, walk the
         * predicate list and remark the columns referenced from there and then add
         * the RID before compacting the columns.
         */
        if(bulkFetch!=UNSET){
            resultColumns.markAllUnreferenced();
            storeRestrictionList.markReferencedColumns();
            if(nonStoreRestrictionList!=null){
                nonStoreRestrictionList.markReferencedColumns();
            }
        }
        resultColumns.addRCForRID();
        templateColumns.addRCForRID();

        // Compact the RCL for the index scan down to the partial row.
        referencedCols=resultColumns.getReferencedFormatableBitSet(cursorTargetTable,false,false,true);
        resultColumns=resultColumns.compactColumns(cursorTargetTable,false);
        resultColumns.setIndexRow(
                baseConglomerateDescriptor.getConglomerateNumber(),
                forUpdate());

        /* We must remember if this was the cursorTargetTable
          * in order to get the right locking on the scan.
         */
        getUpdateLocks=cursorTargetTable;
        cursorTargetTable=false;

        return retval;
    }

    /*
     * Create a new ResultColumnList to reflect the columns in the
     * index described by the given ConglomerateDescriptor.  The columns
     * in the new ResultColumnList are based on the columns in the given
     * ResultColumnList, which reflects the columns in the base table.
     *
     * @param oldColumns The original list of columns, which reflects
     *                   the columns in the base table.
     * @param idxCD      The ConglomerateDescriptor, which describes
     *                   the index that the new ResultColumnList will
     *                   reflect.
     * @param heapCD     The ConglomerateDescriptor for the base heap
     * @param cloneRCs   Whether or not to clone the RCs
     * @throws StandardException Thrown on error
     * @return A new ResultColumnList that reflects the columns in the index.
     */
    private ResultColumnList newResultColumns(
            ResultColumnList oldColumns,
            ConglomerateDescriptor idxCD,
            ConglomerateDescriptor heapCD,
            boolean cloneRCs)
            throws StandardException{
        IndexRowGenerator irg=idxCD.getIndexDescriptor();
        int[] baseCols=irg.baseColumnPositions();
        ResultColumnList newCols=
                (ResultColumnList)getNodeFactory().getNode(
                        C_NodeTypes.RESULT_COLUMN_LIST,
                        getContextManager());

        for(int basePosition : baseCols){
            ResultColumn oldCol=oldColumns.getResultColumn(basePosition);
            ResultColumn newCol;

            if(SanityManager.DEBUG){
                SanityManager.ASSERT(oldCol!=null,
                        "Couldn't find base column "+basePosition+
                                "\n.  RCL is\n"+oldColumns);
            }

            /* If we're cloning the RCs its because we are
             * building an RCL for the index when doing
             * a non-covering index scan.  Set the expression
             * for the old RC to be a VCN pointing to the
             * new RC.
             */
            if(cloneRCs){
                //noinspection ConstantConditions
                newCol=oldCol.cloneMe();
                oldCol.setExpression(
                        (ValueNode)getNodeFactory().getNode(
                                C_NodeTypes.VIRTUAL_COLUMN_NODE,
                                this,
                                newCol,
                                ReuseFactory.getInteger(oldCol.getVirtualColumnId()),
                                getContextManager()));
            }else{
                newCol=oldCol;
            }

            newCols.addResultColumn(newCol);
        }

        /*
        ** The conglomerate is an index, so we need to generate a RowLocation
        ** as the last column of the result set.  Notify the ResultColumnList
        ** that it needs to do this.  Also tell the RCL whether this is
        ** the target of an update, so it can tell the conglomerate controller
        ** when it is getting the RowLocation template.
        */
        newCols.setIndexRow(heapCD.getConglomerateNumber(),forUpdate());

        return newCols;
    }

    /**
     * Generation on a FromBaseTable creates a scan on the
     * optimizer-selected conglomerate.
     *
     * @param acb The ActivationClassBuilder for the class being built
     * @param mb  the execute() method to be built
     * @throws StandardException Thrown on error
     */
    public void generate(ActivationClassBuilder acb,
                         MethodBuilder mb)
            throws StandardException{
        //
        // By now the map of referenced columns has been filled in.
        // We check to see if SYSUSERS.PASSWORD is referenced.
        // Even the DBO is not allowed to SELECT that column.
        // This is to prevent us from instantiating the password as a
        // String. The char[] inside the String can hang around, unzeroed
        // and be read by a memory-sniffer. See DERBY-866.
        //
        if(authorizeSYSUSERS){
            int passwordColNum=SYSUSERSRowFactory.PASSWORD_COL_NUM;

            //if we are using index we still must not be able to access PASSWORD column
            //so use heapReferencedColumns to check original columns referenced
            if((referencedCols==null) || // select * from sys.sysusers results in a null referecedCols
                    ((referencedCols.getLength()>=passwordColNum) && referencedCols.isSet(passwordColNum-1)) ||
                    ((heapReferencedCols != null) &&
                            ((heapReferencedCols.getLength()>=passwordColNum) && heapReferencedCols.isSet(passwordColNum-1))))
            {
                throw StandardException.newException
                    (SQLState.HIDDEN_COLUMN,SYSUSERSRowFactory.TABLE_NAME,SYSUSERSRowFactory.PASSWORD_COL_NAME);
            }
        }

        generateResultSet(acb,mb);

        /*
        ** Remember if this base table is the cursor target table, so we can
        ** know which table to use when doing positioned update and delete
        */
        if(cursorTargetTable){
            acb.rememberCursorTarget(mb);
        }
    }

    /**
     * Generation on a FromBaseTable for a SELECT. This logic was separated
     * out so that it could be shared with PREPARE SELECT FILTER.
     *
     * @param acb The ExpressionClassBuilder for the class being built
     * @param mb  The execute() method to be built
     * @throws StandardException Thrown on error
     */
    public void generateResultSet(ExpressionClassBuilder acb,
                                  MethodBuilder mb)
            throws StandardException{
        /* We must have been a best conglomerate descriptor here */
        if(SanityManager.DEBUG)
            SanityManager.ASSERT(
                    getTrulyTheBestAccessPath().getConglomerateDescriptor()!=null);

        /* Get the next ResultSet #, so that we can number this ResultSetNode, its
         * ResultColumnList and ResultSet.
         */
        assignResultSetNumber();

        /*
        ** If we are doing a special scan to get the last row
        ** of an index, generate it separately.
        */
        if(specialMaxScan){
            generateMaxSpecialResultSet(acb,mb);
            return;
        }

        /*
        ** If we are doing a special distinct scan, generate
        ** it separately.
        */
        if(distinctScan){
            generateDistinctScan(acb,mb);
            return;
        }

        JoinStrategy trulyTheBestJoinStrategy=
                getTrulyTheBestAccessPath().getJoinStrategy();

        // the table scan generator is what we return
        acb.pushGetResultSetFactoryExpression(mb);

        int nargs=getScanArguments(acb,mb);

        mb.callMethod(VMOpcode.INVOKEINTERFACE,null,
                trulyTheBestJoinStrategy.resultSetMethodName(multiProbing),
                ClassName.NoPutResultSet,nargs);

        if (rowIdColumn != null) {
            String type = ClassName.CursorResultSet;
            String name = acb.newRowLocationScanResultSetName();
            if (!acb.cb.existsField(type, name)) {
                acb.newFieldDeclaration(Modifier.PRIVATE, type, name);
            }
        }
        /* If this table is the target of an update or a delete, then we must
         * wrap the Expression up in an assignment expression before
         * returning.
         * NOTE - scanExpress is a ResultSet.  We will need to cast it to the
         * appropriate subclass.
         * For example, for a DELETE, instead of returning a call to the
         * ResultSetFactory, we will generate and return:
         *        this.SCANRESULTSET = (cast to appropriate ResultSet type)
         * The outer cast back to ResultSet is needed so that
         * we invoke the appropriate method.
         *                                        (call to the ResultSetFactory)
         */
        if((updateOrDelete==UPDATE) || (updateOrDelete==DELETE) || rowIdColumn!=null){
            mb.cast(ClassName.CursorResultSet);
            mb.putField(acb.getRowLocationScanResultSetName(),ClassName.CursorResultSet);
            mb.cast(ClassName.NoPutResultSet);
        }
    }

    /**
     * Get the final CostEstimate for this ResultSetNode.
     *
     * @return The final CostEstimate for this ResultSetNode.
     */
    public CostEstimate getFinalCostEstimate(boolean useSelf){
        return getTrulyTheBestAccessPath().getCostEstimate();
    }

    /* helper method used by generateMaxSpecialResultSet and
     * generateDistinctScan to return the name of the index if the
     * conglomerate is an index.
     * @param cd   Conglomerate for which we need to push the index name
     * @param mb   Associated MethodBuilder
     * @throws StandardException
     */
    private void pushIndexName(ConglomerateDescriptor cd,MethodBuilder mb)
            throws StandardException{
        if(cd.isConstraint()){
            DataDictionary dd=getDataDictionary();
            ConstraintDescriptor constraintDesc=
                    dd.getConstraintDescriptor(tableDescriptor,cd.getUUID());
            mb.push(constraintDesc.getConstraintName());
        }else if(cd.isIndex()){
            mb.push(cd.getConglomerateName());
        }else{
            // If the conglomerate is the base table itself, make sure we push null.
            //  Before the fix for DERBY-578, we would push the base table name
            //  and  this was just plain wrong and would cause statistics information to be incorrect.
            mb.pushNull("java.lang.String");
        }
    }
        /**
        ** getLastIndexKeyResultSet
        ** (
        **        activation,
        **        resultSetNumber,
        **        resultRowAllocator,
        **        conglomereNumber,
        **        tableName,
        **        optimizeroverride
        **        indexName,
        **        colRefItem,
        **        lockMode,
        **        tableLocked,
        **        isolationLevel,
        **        optimizerEstimatedRowCount,
        **        optimizerEstimatedRowCost,
        **    );
        */
    private void generateMaxSpecialResultSet ( ExpressionClassBuilder acb, MethodBuilder mb ) throws StandardException{
        ConglomerateDescriptor cd=getTrulyTheBestAccessPath().getConglomerateDescriptor();
        CostEstimate costEstimate=getFinalCostEstimate(false);
        int colRefItem=(referencedCols==null)?
                -1:
                acb.addItem(referencedCols);
        boolean tableLockGranularity=tableDescriptor.getLockGranularity()==TableDescriptor.TABLE_LOCK_GRANULARITY;


        acb.pushGetResultSetFactoryExpression(mb);

        acb.pushThisAsActivation(mb);
        mb.push(getResultSetNumber());
        resultColumns.generateHolder(acb,mb,referencedCols,null);
        mb.push(cd.getConglomerateNumber());
        mb.push(tableDescriptor.getName());
        //User may have supplied optimizer overrides in the sql
        //Pass them onto execute phase so it can be shown in
        //run time statistics.
        if(tableProperties!=null)
            mb.push(com.splicemachine.db.iapi.util.PropertyUtil.sortProperties(tableProperties));
        else
            mb.pushNull("java.lang.String");
        pushIndexName(cd,mb);
        mb.push(colRefItem);
        mb.push(getTrulyTheBestAccessPath().getLockMode());
        mb.push(tableLockGranularity);
        mb.push(getCompilerContext().getScanIsolationLevel());
        mb.push(costEstimate.singleScanRowCount());
        mb.push(costEstimate.getEstimatedCost());
        mb.push(tableDescriptor.getVersion());
        mb.push(printExplainInformationForActivation());
        mb.callMethod(VMOpcode.INVOKEINTERFACE,null,"getLastIndexKeyResultSet", ClassName.NoPutResultSet,15);

    }

    private void generateDistinctScan ( ExpressionClassBuilder acb, MethodBuilder mb ) throws StandardException{
        ConglomerateDescriptor cd=getTrulyTheBestAccessPath().getConglomerateDescriptor();
        CostEstimate costEstimate=getFinalCostEstimate(false);
        int colRefItem=(referencedCols==null)? -1: acb.addItem(referencedCols);
        boolean tableLockGranularity=tableDescriptor.getLockGranularity()==TableDescriptor.TABLE_LOCK_GRANULARITY;

        /* Get the hash key columns and wrap them in a formattable */
        int[] hashKeyColumns;

        hashKeyColumns=new int[resultColumns.size()];
        if(referencedCols==null){
            for(int index=0;index<hashKeyColumns.length;index++){
                hashKeyColumns[index]=index;
            }
        }else{
            int index=0;
            for(int colNum=referencedCols.anySetBit();
                colNum!=-1;
                colNum=referencedCols.anySetBit(colNum)){
                hashKeyColumns[index++]=colNum;
            }
        }

                /* Generate Partitions if Applicable */
        int partitionReferenceItem = -1;
        int[] partitionBy = tableDescriptor.getPartitionBy();
        if (partitionBy.length != 0)
            partitionReferenceItem=acb.addItem(new ReferencedColumnsDescriptorImpl(partitionBy));




        FormatableIntHolder[] fihArray=
                FormatableIntHolder.getFormatableIntHolders(hashKeyColumns);
        FormatableArrayHolder hashKeyHolder=new FormatableArrayHolder(fihArray);
        int hashKeyItem=acb.addItem(hashKeyHolder);
        long conglomNumber=cd.getConglomerateNumber();
        StaticCompiledOpenConglomInfo scoci=getLanguageConnectionContext().
                getTransactionCompile().
                getStaticCompiledConglomInfo(conglomNumber);

        acb.pushGetResultSetFactoryExpression(mb);

        acb.pushThisAsActivation(mb);
        mb.push(conglomNumber);
        mb.push(acb.addItem(scoci));
        resultColumns.generateHolder(acb,mb,referencedCols,null);
        mb.push(getResultSetNumber());
        mb.push(hashKeyItem);
        mb.push(tableDescriptor.getName());
        //User may have supplied optimizer overrides in the sql
        //Pass them onto execute phase so it can be shown in
        //run time statistics.
        if(tableProperties!=null)
            mb.push(com.splicemachine.db.iapi.util.PropertyUtil.sortProperties(tableProperties));
        else
            mb.pushNull("java.lang.String");
        pushIndexName(cd,mb);
        mb.push(cd.isConstraint());
        mb.push(colRefItem);
        mb.push(getTrulyTheBestAccessPath().getLockMode());
        mb.push(tableLockGranularity);
        mb.push(getCompilerContext().getScanIsolationLevel());
        mb.push(costEstimate.singleScanRowCount());
        mb.push(costEstimate.getEstimatedCost());
        mb.push(tableDescriptor.getVersion());
        mb.push(printExplainInformationForActivation());
        mb.push(pin);
        mb.push(splits);
        BaseJoinStrategy.pushNullableString(mb,tableDescriptor.getDelimited());
        BaseJoinStrategy.pushNullableString(mb,tableDescriptor.getEscaped());
        BaseJoinStrategy.pushNullableString(mb,tableDescriptor.getLines());
        BaseJoinStrategy.pushNullableString(mb,tableDescriptor.getStoredAs());
        BaseJoinStrategy.pushNullableString(mb,tableDescriptor.getLocation());
        mb.push(partitionReferenceItem);
        generateDefaultRow((ActivationClassBuilder)acb, mb);
        mb.callMethod(VMOpcode.INVOKEINTERFACE,null,"getDistinctScanResultSet",
                ClassName.NoPutResultSet,28);
    }

    private void generatePastTxFunc(ExpressionClassBuilder acb, MethodBuilder mb) throws StandardException {
        if(pastTxIdExpression != null) {
            MethodBuilder pastTxExpr = acb.newUserExprFun();
            pastTxIdExpression.generateExpression(acb, pastTxExpr);
            pastTxExpr.methodReturn();
            pastTxExpr.complete();
            acb.pushMethodReference(mb, pastTxExpr);
        }
        else
        {
            mb.pushNull(ClassName.GeneratedMethod);
        }
    }

    private int getScanArguments(ExpressionClassBuilder acb, MethodBuilder mb) throws StandardException{
        // get a function to allocate scan rows of the right shape and size
        MethodBuilder resultRowAllocator= resultColumns.generateHolderMethod(acb, referencedCols, null);

        // pass in the referenced columns on the saved objects
        // chain
        int colRefItem=-1;
        if(referencedCols!=null){
            colRefItem=acb.addItem(referencedCols);
        }

        //
        int indexColItem=-1;
        ConglomerateDescriptor cd=getTrulyTheBestAccessPath().getConglomerateDescriptor();
        if(cd.isIndex()){
            int [] baseColumnPositions = cd.getIndexDescriptor().baseColumnPositions();
            FormatableIntHolder[] fihArrayIndex = new FormatableIntHolder[baseColumnPositions.length];
            for (int index = 0; index < baseColumnPositions.length; index++) {
                fihArrayIndex[index] = new FormatableIntHolder(tableDescriptor.getColumnDescriptor(baseColumnPositions[index]).getStoragePosition());
            }
            FormatableArrayHolder hashKeyHolder=new FormatableArrayHolder(fihArrayIndex);
            indexColItem=acb.addItem(hashKeyHolder);
        }
        /*
        // beetle entry 3865: updateable cursor using index
        int indexColItem = -1;
        if (cursorTargetTable || getUpdateLocks)
        {
            cd = getTrulyTheBestAccessPath().getConglomerateDescriptor();
            if (cd.isIndex())
            {
                int[] baseColPos = cd.getIndexDescriptor().baseColumnPositions();
                boolean[] isAscending = cd.getIndexDescriptor().isAscending();
                int[] indexCols = new int[baseColPos.length];
                for (int i = 0; i < indexCols.length; i++)
                    indexCols[i] = isAscending[i] ? baseColPos[i] : -baseColPos[i];
                indexColItem = acb.addItem(indexCols);
            }
        }
        */

        int partitionReferenceItem = -1;
        int[] partitionBy = tableDescriptor.getPartitionBy();
        if (partitionBy.length != 0)
            partitionReferenceItem=acb.addItem(new ReferencedColumnsDescriptorImpl(partitionBy));


        AccessPath ap=getTrulyTheBestAccessPath();
        JoinStrategy trulyTheBestJoinStrategy=ap.getJoinStrategy();

        /*
        ** We can only do bulkFetch on NESTEDLOOP
        */
        if(SanityManager.DEBUG){
            if((!trulyTheBestJoinStrategy.bulkFetchOK()) &&
                    (bulkFetch!=UNSET)){
                SanityManager.THROWASSERT("bulkFetch should not be set "+
                        "for the join strategy "+
                        trulyTheBestJoinStrategy.getName());
            }
        }

        int numArgs = trulyTheBestJoinStrategy.getScanArgs(
                getLanguageConnectionContext().getTransactionCompile(),
                mb,
                this,
                storeRestrictionList,
                nonStoreRestrictionList,
                acb,
                bulkFetch,
                resultRowAllocator,
                colRefItem,
                indexColItem,
                getTrulyTheBestAccessPath().
                getLockMode(),
                (tableDescriptor.getLockGranularity()==TableDescriptor.TABLE_LOCK_GRANULARITY),
                getCompilerContext().getScanIsolationLevel(),
                ap.getOptimizer().getMaxMemoryPerTable(),
                multiProbing,
                tableDescriptor.getVersion(),
                pin,
                splits,
                tableDescriptor.getDelimited(),
                tableDescriptor.getEscaped(),
                tableDescriptor.getLines(),
                tableDescriptor.getStoredAs(),
                tableDescriptor.getLocation(),
                partitionReferenceItem
        );

        // compute the default row
        numArgs += generateDefaultRow((ActivationClassBuilder)acb, mb);

        // also add the past transaction id functor
        generatePastTxFunc(acb, mb);
        numArgs++;

        return numArgs;
    }

    /**
     * Convert an absolute to a relative 0-based column position.
     *
     * @param absolutePosition The absolute 0-based column position.
     * @return The relative 0-based column position.
     */
    private int mapAbsoluteToRelativeColumnPosition(int absolutePosition){
        if(referencedCols==null){
            return absolutePosition;
        }

        /* setBitCtr counts the # of columns in the row,
         * from the leftmost to the absolutePosition, that will be
         * in the partial row returned by the store.  This becomes
         * the new value for column position.
         */
        int setBitCtr=0;
        int bitCtr=0;
        for(;
            bitCtr<referencedCols.size() && bitCtr<absolutePosition;
            bitCtr++){
            if(referencedCols.get(bitCtr)){
                setBitCtr++;
            }
        }
        return setBitCtr;
    }

    /**
     * Get the exposed name for this table, which is the name that can
     * be used to refer to it in the rest of the query.
     *
     * @return The exposed name of this table.
     */
    public String getExposedName(){
        if(correlationName!=null)
            return correlationName;
        else
            return getOrigTableName().getFullTableName();
    }

    /**
     * Get the exposed table name for this table, which is the name that can
     * be used to refer to it in the rest of the query.
     *
     * @throws StandardException Thrown on error
     * @return TableName The exposed name of this table.
     */
    private TableName getExposedTableName() throws StandardException{
        if(correlationName!=null)
            return makeTableName(null,correlationName);
        else
            return getOrigTableName();
    }

    /**
     * Return the table name for this table.
     *
     * @return The table name for this table.
     */
    public TableName getTableNameField(){
        return tableName;
    }

    /**
     * Return a ResultColumnList with all of the columns in this table.
     * (Used in expanding '*'s.)
     * NOTE: Since this method is for expanding a "*" in the SELECT list,
     * ResultColumn.expression will be a ColumnReference.
     *
     * @param allTableName The qualifier on the "*"
     * @return ResultColumnList    List of result columns from this table.
     * @throws StandardException Thrown on error
     */
    @Override
    public ResultColumnList getAllResultColumns(TableName allTableName) throws StandardException{
        return getResultColumnsForList(allTableName,resultColumns,
                getOrigTableName());
    }

    /**
     * Build a ResultColumnList based on all of the columns in this FromBaseTable.
     * NOTE - Since the ResultColumnList generated is for the FromBaseTable,
     * ResultColumn.expression will be a BaseColumnNode.
     *
     * @return ResultColumnList representing all referenced columns
     * @throws StandardException Thrown on error
     */
    public ResultColumnList genResultColList() throws StandardException{
        ResultColumnList rcList;
        ResultColumn resultColumn;
        ValueNode valueNode;
        ColumnDescriptor colDesc;
        TableName exposedName;

        /* Cache exposed name for this table.
         * The exposed name becomes the qualifier for each column
         * in the expanded list.
         */
        exposedName=getExposedTableName();

        /* Add all of the columns in the table */
        rcList=(ResultColumnList)getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN_LIST,
                getContextManager());
        ColumnDescriptorList cdl=tableDescriptor.getColumnDescriptorList();
        int cdlSize=cdl.size();

        for(int index=0;index<cdlSize;index++){
            /* Build a ResultColumn/BaseColumnNode pair for the column */
            colDesc=cdl.elementAt(index);
            //A ColumnDescriptor instantiated through SYSCOLUMNSRowFactory only has
            //the uuid set on it and no table descriptor set on it. Since we know here
            //that this columnDescriptor is tied to tableDescriptor, set it so using
            //setTableDescriptor method. ColumnDescriptor's table descriptor is used
            //to get ResultSetMetaData.getTableName & ResultSetMetaData.getSchemaName
            colDesc.setTableDescriptor(tableDescriptor);

            valueNode=(ValueNode)getNodeFactory().getNode(
                    C_NodeTypes.BASE_COLUMN_NODE,
                    colDesc.getColumnName(),
                    exposedName,
                    colDesc.getType(),
                    getContextManager());
            resultColumn=(ResultColumn)getNodeFactory().getNode(
                    C_NodeTypes.RESULT_COLUMN,
                    colDesc,
                    valueNode,
                    getContextManager());

            /* Build the ResultColumnList to return */
            rcList.addResultColumn(resultColumn);
        }

        return rcList;
    }

    /**
     * Augment the RCL to include the columns in the FormatableBitSet.
     * If the column is already there, don't add it twice.
     * Column is added as a ResultColumn pointing to a
     * ColumnReference.
     *
     * @param inputRcl   The original list
     * @param colsWeWant bit set of cols we want
     * @return ResultColumnList the rcl
     * @throws StandardException Thrown on error
     */
    public ResultColumnList addColsToList
    (
            ResultColumnList inputRcl,
            FormatableBitSet colsWeWant
    )
            throws StandardException{
        ResultColumn resultColumn;
        ValueNode valueNode;
        ColumnDescriptor cd;
        TableName exposedName;

        /* Cache exposed name for this table.
         * The exposed name becomes the qualifier for each column
         * in the expanded list.
         */
        exposedName=getExposedTableName();

        /* Add all of the columns in the table */
        ResultColumnList newRcl=(ResultColumnList)getNodeFactory().getNode(
                C_NodeTypes.RESULT_COLUMN_LIST,
                getContextManager());
        ColumnDescriptorList cdl=tableDescriptor.getColumnDescriptorList();
        int cdlSize=cdl.size();

        for(int index=0;index<cdlSize;index++){
            /* Build a ResultColumn/BaseColumnNode pair for the column */
            cd=cdl.elementAt(index);
            int position=cd.getPosition();

            if(!colsWeWant.get(position)){
                continue;
            }

            if((resultColumn=inputRcl.getResultColumn(position))==null){
                valueNode=(ValueNode)getNodeFactory().getNode(
                        C_NodeTypes.COLUMN_REFERENCE,
                        cd.getColumnName(),
                        exposedName,
                        getContextManager());
                resultColumn=(ResultColumn)getNodeFactory().
                        getNode(
                                C_NodeTypes.RESULT_COLUMN,
                                cd,
                                valueNode,
                                getContextManager());
            }

            /* Build the ResultColumnList to return */
            newRcl.addResultColumn(resultColumn);
        }

        return newRcl;
    }

    /**
     * Return a TableName node representing this FromTable.
     *
     * @return a TableName node representing this FromTable.
     * @throws StandardException Thrown on error
     */
    @Override
    public TableName getTableName()
            throws StandardException{
        TableName tn;

        tn=super.getTableName();

        if(tn!=null){
            if(tn.getSchemaName()==null && correlationName==null)
                tn.bind(this.getDataDictionary());
        }

        return (tn!=null?tn:tableName);
    }

    /**
     * Mark this ResultSetNode as the target table of an updatable
     * cursor.
     */
    @Override
    public boolean markAsCursorTargetTable(){
        cursorTargetTable=true;
        return true;
    }

    /**
     * Is this a table that has a FOR UPDATE
     * clause?
     *
     * @return true/false
     */
    @Override
    protected boolean cursorTargetTable(){
        return cursorTargetTable;
    }

    /*
     * Mark as updatable all the columns in the result column list of this
     * FromBaseTable that match the columns in the given update column list.
     *
     * @param updateColumns A ResultColumnList representing the columns
     *                      to be updated.
     */
    void markUpdated(ResultColumnList updateColumns){
        resultColumns.markUpdated(updateColumns);
    }

    /**
     * Search to see if a query references the specifed table name.
     *
     * @param name      Table name (String) to search for.
     * @param baseTable Whether or not name is for a base table
     * @throws StandardException Thrown on error
     * @return true if found, else false
     */
    @Override
    public boolean referencesTarget(String name,boolean baseTable) throws StandardException{
        return baseTable && name.equals(getBaseTableName());
    }

    /**
     * Return true if the node references SESSION schema tables (temporary or permanent)
     *
     * @throws StandardException Thrown on error
     * @return true if references SESSION schema tables, else false
     */
    @Override
    public boolean referencesSessionSchema() throws StandardException{
        //If base table is a SESSION schema table, then return true.
        return isSessionSchema(tableDescriptor.getSchemaDescriptor());
    }


    /**
     * Return whether or not the underlying ResultSet tree will return
     * a single row, at most.  This method is intended to be used during
     * generation, after the "truly" best conglomerate has been chosen.
     * This is important for join nodes where we can save the extra next
     * on the right side if we know that it will return at most 1 row.
     *
     * @return Whether or not the underlying ResultSet tree will return a single row.
     * @throws StandardException Thrown on error
     */
    @Override
    public boolean isOneRowResultSet() throws StandardException{
        // EXISTS FBT will only return a single row
        if (matchRowId) {
            return false;
        }
        if(existsTable ){
            return true;
        }

        /* For hash join, we need to consider both the qualification
         * and hash join predicates and we consider them against all
         * conglomerates since we are looking for any uniqueness
         * condition that holds on the columns in the hash table,
         * otherwise we just consider the predicates in the
         * restriction list and the conglomerate being scanned.

         */
        AccessPath ap=getTrulyTheBestAccessPath();
        JoinStrategy trulyTheBestJoinStrategy=ap.getJoinStrategy();
        PredicateList pl;
        if (trulyTheBestJoinStrategy==null)
            return false;

        if(trulyTheBestJoinStrategy.isHashJoin()){
            pl=(PredicateList)getNodeFactory().getNode(
                    C_NodeTypes.PREDICATE_LIST,
                    getContextManager());
            if(storeRestrictionList!=null){
                pl.nondestructiveAppend(storeRestrictionList);
            }
            if(nonStoreRestrictionList!=null){
                pl.nondestructiveAppend(nonStoreRestrictionList);
            }
            return isOneRowResultSet(pl);
        }else{
            AccessPath trulyTheBestAccessPath=getTrulyTheBestAccessPath();
            return isOneRowResultSet(trulyTheBestAccessPath.getConglomerateDescriptor(),restrictionList);
        }
    }

    /**
     * Return whether or not this is actually a EBT for NOT EXISTS.
     */
    @Override
    public boolean isNotExists(){
        return isNotExists;
    }

    public boolean isOneRowResultSet(OptimizablePredicateList predList) throws StandardException{
        ConglomerateDescriptor[] cds=tableDescriptor.getConglomerateDescriptors();

        for(ConglomerateDescriptor cd : cds){
            if(isOneRowResultSet(cd,predList)){
                return true;
            }
        }

        return false;
    }

    /**
     * Determine whether or not the columns marked as true in
     * the passed in array are a superset of any unique index
     * on this table.
     * This is useful for subquery flattening and distinct elimination
     * based on a uniqueness condition.
     *
     * @param eqCols The columns to consider
     * @return Whether or not the columns marked as true are a superset
     */
    protected boolean supersetOfUniqueIndex(boolean[] eqCols) throws StandardException{
        ConglomerateDescriptor[] cds=tableDescriptor.getConglomerateDescriptors();

        /* Cycle through the ConglomerateDescriptors */
        IndexDescriptor id;
        for(ConglomerateDescriptor cd : cds){
            if(!cd.isIndex()) {
                if (!cd.isPrimaryKey())
                    continue;
                else
                    id = cd.getIndexDescriptor();
            } else {
                id=cd.getIndexDescriptor();
                if(!id.isUnique()){
                    continue;
                }
            }

            int[] keyColumns=id.baseColumnPositions();

            int inner=0;
            for(;inner<keyColumns.length;inner++){
                if(!eqCols[keyColumns[inner]]){
                    break;
                }
            }

            /* Did we get a full match? */
            if(inner==keyColumns.length){
                return true;
            }
        }

        return false;
    }

    /**
     * Determine whether or not the columns marked as true in
     * the passed in join table matrix are a superset of any single column unique index
     * on this table.
     * This is useful for distinct elimination
     * based on a uniqueness condition.
     *
     * @param tableColMap The columns to consider
     * @return Whether or not the columns marked as true for one at least
     * one table are a superset
     */
    protected boolean supersetOfUniqueIndex(JBitSet[] tableColMap)
            throws StandardException{
        ConglomerateDescriptor[] cds=tableDescriptor.getConglomerateDescriptors();

        /* Cycle through the ConglomerateDescriptors */
        IndexDescriptor id;
        for(ConglomerateDescriptor cd : cds){
            if(!cd.isIndex()) {
                if (!cd.isPrimaryKey())
                    continue;
                else
                    id = cd.getIndexDescriptor();
            } else {
                id=cd.getIndexDescriptor();
                if(!id.isUnique()){
                    continue;
                }
            }

            int[] keyColumns=id.baseColumnPositions();
            int numBits=tableColMap[0].size();
            JBitSet keyMap=new JBitSet(numBits);
            JBitSet resMap=new JBitSet(numBits);

            int inner=0;
            for(;inner<keyColumns.length;inner++){
                keyMap.set(keyColumns[inner]);
            }
            int table=0;
            for(;table<tableColMap.length;table++){
                resMap.setTo(tableColMap[table]);
                resMap.and(keyMap);
                if(keyMap.equals(resMap)){
                    tableColMap[table].set(0);
                    return true;
                }
            }

        }

        return false;
    }

    /**
     * Get the lock mode for the target table heap of an update or delete
     * statement.  It is not always MODE_RECORD.  We want the lock on the
     * heap to be consistent with optimizer and eventually system's decision.
     * This is to avoid deadlock (beetle 4318).  During update/delete's
     * execution, it will first use this lock mode we return to lock heap to
     * open a RowChanger, then use the lock mode that is the optimizer and
     * system's combined decision to open the actual source conglomerate.
     * We've got to make sure they are consistent.  This is the lock chart (for
     * detail reason, see comments below):
     * BEST ACCESS PATH            LOCK MODE ON HEAP
     * ----------------------        -----------------------------------------
     * index                      row lock
     * <p/>
     * heap                      row lock if READ_COMMITTED,
     * REPEATBLE_READ, or READ_UNCOMMITTED &&
     * not specified table lock otherwise,
     * use optimizer decided best acess
     * path's lock mode
     *
     * @return The lock mode
     */
    @Override
    public int updateTargetLockMode(){
        /* if best access path is index scan, we always use row lock on heap,
         * consistent with IndexRowToBaseRowResultSet's openCore().  We don't
         * need to worry about the correctness of serializable isolation level
         * because index will have previous key locking if it uses row locking
         * as well.
         */
        if(getTrulyTheBestAccessPath().getConglomerateDescriptor().isIndex())
            return TransactionController.MODE_RECORD;

        /* we override optimizer's decision of the lock mode on heap, and
         * always use row lock if we are read committed/uncommitted or
         * repeatable read isolation level, and no forced table lock.  
         *
         * This is also reflected in TableScanResultSet's constructor, 
         * KEEP THEM CONSISTENT!  
         *
         * This is to improve concurrency, while maintaining correctness with 
         * serializable level.  Since the isolation level can change between 
         * compilation and execution if the statement is cached or stored, we 
         * encode both the SERIALIZABLE lock mode and the non-SERIALIZABLE
         * lock mode in the returned lock mode if they are different.
         */
        int isolationLevel= getLanguageConnectionContext().getCurrentIsolationLevel();


        if((isolationLevel!=ExecutionContext.SERIALIZABLE_ISOLATION_LEVEL) &&
                (tableDescriptor.getLockGranularity()!=
                        TableDescriptor.TABLE_LOCK_GRANULARITY)){
            int lockMode=getTrulyTheBestAccessPath().getLockMode();
            if(lockMode!=TransactionController.MODE_RECORD)
                lockMode=(lockMode&0xff)<<16;
            else
                lockMode=0;
            lockMode+=TransactionController.MODE_RECORD;

            return lockMode;
        }

        /* if above don't apply, use optimizer's decision on heap's lock
         */
        return getTrulyTheBestAccessPath().getLockMode();
    }

    /**
     * Return whether or not the underlying ResultSet tree
     * is ordered on the specified columns.
     * RESOLVE - This method currently only considers the outermost table
     * of the query block.
     * RESOLVE - We do not currently push method calls down, so we don't
     * worry about whether the equals comparisons can be against a variant method.
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
        /* The following conditions must be met, regardless of the value of permuteOrdering,
         * in order for the table to be ordered on the specified columns:
         *    o  Each column is from this table. (RESOLVE - handle joins later)
         *    o  The access path for this table is an index.
         */
        // Verify that all CRs are from this table
        for(ColumnReference cr : crs){
            if(cr.getTableNumber()!=tableNumber){
                return false;
            }
        }
        // Verify access path is an index or table with primary key constraint
        ConglomerateDescriptor cd=getTrulyTheBestAccessPath().getConglomerateDescriptor();
        if(cd.getIndexDescriptor()==null || cd.getIndexDescriptor().indexType()==null){
            return false;
        }

        // Now consider whether or not the CRs can be permuted
        boolean isOrdered;
        if(permuteOrdering){
            isOrdered=isOrdered(crs,cd);
        }else{
            isOrdered=isStrictlyOrdered(crs,cd);
        }

        if(fbtVector!=null){
            //noinspection unchecked
            fbtVector.add(this);
        }

        return isOrdered;
    }

    /**
     * Turn off bulk fetch
     */
    void disableBulkFetch(){
        bulkFetchTurnedOff=true;
        bulkFetch=UNSET;
    }

    /**
     * Do a special scan for max.
     */
    void doSpecialMaxScan(){
        if(SanityManager.DEBUG){
            if((!restrictionList.isEmpty()) ||
                    (!storeRestrictionList.isEmpty()) ||
                    (!nonStoreRestrictionList.isEmpty())){
                SanityManager.THROWASSERT("shouldn't be setting max special scan because there is a restriction");
            }
        }
        specialMaxScan=true;
    }

    /**
     * Is it possible to do a distinct scan on this ResultSet tree.
     * (See SelectNode for the criteria.)
     *
     * @param distinctColumns the set of distinct columns
     * @return Whether or not it is possible to do a distinct scan on this ResultSet tree.
     */
    boolean isPossibleDistinctScan(Set<BaseColumnNode> distinctColumns){
        if((restrictionList!=null && !restrictionList.isEmpty())){
            return false;
        }

        HashSet columns=new HashSet();
        for(int i=0;i<resultColumns.size();i++){
            ResultColumn rc=resultColumns.elementAt(i);
            //noinspection unchecked
            columns.add(rc.getExpression());
        }

        return columns.equals(distinctColumns);
    }

    /**
     * Mark the underlying scan as a distinct scan.
     */
    @Override
    void markForDistinctScan() throws StandardException {
        distinctScan=true;
        resultColumns.computeDistinctCardinality(getFinalCostEstimate(false));
    }


    /**
     * @see ResultSetNode#adjustForSortElimination
     */
    @Override
    void adjustForSortElimination(){
        /* NOTE: IRTBR will use a different method to tell us that
         * it cannot do a bulk fetch as the ordering issues are
         * specific to a FBT being under an IRTBR as opposed to a
         * FBT being under a PRN, etc.
         * So, we just ignore this call for now.
         */
    }

    /**
     * @see ResultSetNode#adjustForSortElimination
     */
    @Override
    void adjustForSortElimination(RequiredRowOrdering rowOrdering) throws StandardException{
        /* We may have eliminated a sort with the assumption that
         * the rows from this base table will naturally come back
         * in the correct ORDER BY order. But in the case of IN
         * list probing predicates (see DERBY-47) the predicate
         * itself may affect the order of the rows.  In that case
         * we need to notify the predicate so that it does the
         * right thing--i.e. so that it preserves the natural
         * ordering of the rows as expected from this base table.
         * DERBY-3279.
         */
        if(restrictionList!=null)
            restrictionList.adjustForSortElimination(rowOrdering);
    }

    /**
     * Return whether or not this index is ordered on a permutation of the specified columns.
     *
     * @throws StandardException Thrown on error
     * @param    crs        The specified ColumnReference[]
     * @param    cd        The ConglomerateDescriptor for the chosen index.
     * @return Whether or not this index is ordered exactly on the specified columns.
     */
    private boolean isOrdered(ColumnReference[] crs,ConglomerateDescriptor cd) throws StandardException{
        /* This table is ordered on a permutation of the specified columns if:
         *  o  For each key column, until a match has been found for all of the
         *       ColumnReferences, it is either in the array of ColumnReferences
         *       or there is an equality predicate on it.
         *       (NOTE: It is okay to exhaust the key columns before the ColumnReferences
         *       if the index is unique.  In other words if we have CRs left over after
         *       matching all of the columns in the key then the table is considered ordered
         *       iff the index is unique. For example:
         *        i1 on (c1, c2), unique
         *        select distinct c3 from t1 where c1 = 1 and c2 = ?;
         *       is ordered on c3 since there will be at most 1 qualifying row.)
         */
        boolean[] matchedCRs=new boolean[crs.length];

        int nextKeyColumn=0;
        int[] keyColumns=cd.getIndexDescriptor().baseColumnPositions();

        // Walk through the key columns
        for(;nextKeyColumn<keyColumns.length;nextKeyColumn++){
            boolean currMatch=false;
            // See if the key column is in crs
            for(int nextCR=0;nextCR<crs.length;nextCR++){
                if(crs[nextCR].getColumnNumber()==keyColumns[nextKeyColumn]){
                    matchedCRs[nextCR]=true;
                    currMatch=true;
                    break;
                }
            }

            // Advance to next key column if we found a match on this one
            if(currMatch){
                continue;
            }

            // Stop search if there is no equality predicate on this key column
            if(!storeRestrictionList.hasOptimizableEqualityPredicate(this,keyColumns[nextKeyColumn],true)){
                break;
            }
        }

        /* Count the number of matched CRs. The table is ordered if we matched all of them. */
        int numCRsMatched=0;
        for(boolean matchedCR : matchedCRs){
            if(matchedCR){
                numCRsMatched++;
            }
        }

        if(numCRsMatched==matchedCRs.length){
            return true;
        }

        /* We didn't match all of the CRs, but if
         * we matched all of the key columns then
         * we need to check if the index is unique.
         */
        return nextKeyColumn==keyColumns.length && cd.getIndexDescriptor().isUnique();
    }

    /**
     * Return whether or not this index is ordered on a permutation of the specified columns.
     *
     * @throws StandardException Thrown on error
     * @param    crs        The specified ColumnReference[]
     * @param    cd        The ConglomerateDescriptor for the chosen index.
     * @return Whether or not this index is ordered exactly on the specified columns.
     */
    private boolean isStrictlyOrdered(ColumnReference[] crs,ConglomerateDescriptor cd)
            throws StandardException{
        /* This table is ordered on the specified columns in the specified order if:
         *  o  For each ColumnReference, it is either the next key column or there
         *       is an equality predicate on all key columns prior to the ColumnReference.
         *       (NOTE: If the index is unique, then it is okay to have a suffix of
         *       unmatched ColumnReferences because the set is known to be ordered. For example:
         *        i1 on (c1, c2), unique
         *        select distinct c3 from t1 where c1 = 1 and c2 = ?;
         *       is ordered on c3 since there will be at most 1 qualifying row.)
         */
        int nextCR=0;
        int nextKeyColumn=0;
        int[] keyColumns=cd.getIndexDescriptor().baseColumnPositions();

        // Walk through the CRs
        for(;nextCR<crs.length;nextCR++){
            /* If we've walked through all of the key columns then
             * we need to check if the index is unique.
             * Beetle 4402
             */
            if(nextKeyColumn==keyColumns.length){
                if(cd.getIndexDescriptor().isUnique()){
                    break;
                }else{
                    return false;
                }
            }
            while(crs[nextCR].getColumnNumber()!=keyColumns[nextKeyColumn]){
                // Stop if there is no equality predicate on this key column
                if(!storeRestrictionList.hasOptimizableEqualityPredicate(this,keyColumns[nextKeyColumn],true)){
                    return false;
                }

                // Advance to the next key column
                nextKeyColumn++;

                /* If we've walked through all of the key columns then
                 * we need to check if the index is unique.
                 */
                if(nextKeyColumn==keyColumns.length){
                    if(cd.getIndexDescriptor().isUnique()){
                        break;
                    }else{
                        return false;
                    }
                }
            }
            nextKeyColumn++;
        }
        return true;
    }

    /**
     * Is this a one-row result set with the given conglomerate descriptor?
     */
    public boolean isOneRowResultSet(ConglomerateDescriptor cd,
                                      OptimizablePredicateList predList) throws StandardException{
        if(predList==null){
            return false;
        }

        assert predList instanceof PredicateList;

        @SuppressWarnings("ConstantConditions") PredicateList restrictionList=(PredicateList)predList;

        if(!cd.isIndex() && !cd.isPrimaryKey())
            return false;
        IndexRowGenerator irg= cd.getIndexDescriptor();

        // is this a unique index
        if(!irg.isUnique())
            return false;

        int[] baseColumnPositions=irg.baseColumnPositions();

        // Do we have an exact match on the full key

        for(int curCol : baseColumnPositions){
            // get the column number at this position
            /* Is there a pushable equality predicate on this key column?
             * (IS NULL is also acceptable)
             */
            List<Predicate> optimizableEqualityPredicateList =
                    restrictionList.getOptimizableEqualityPredicateList(this,curCol,true);

            // No equality predicate for this column, so this is not a one row result set
            if (optimizableEqualityPredicateList == null)
                return false;

            // Look for equality predicate that is not a join predicate
            boolean existsNonjoinPredicate = false;
            for (Predicate predicate : optimizableEqualityPredicateList) {
                if (!predicate.isJoinPredicate() && !predicate.isFullJoinPredicate()) {
                    existsNonjoinPredicate = true;
                    break;
                }
            }
            // If all equality predicates are join predicates, then this is NOT a one row result set
            if (!existsNonjoinPredicate)
                return false;
        }

        return true;
    }

    private int getDefaultBulkFetch() throws StandardException{
        return 16; // Stop Going to the database for parsing this number.  JL
    }

    private String getUserSpecifiedIndexName(){
        String retval=null;

        if(tableProperties!=null){
            retval=tableProperties.getProperty("index");
        }

        return retval;
    }

    /*
    ** RESOLVE: This whole thing should probably be moved somewhere else,
    ** like the optimizer or the data dictionary.
    */
    private StoreCostController getStoreCostController(TableDescriptor td, ConglomerateDescriptor cd) throws StandardException{
        if (skipStats) {
            if (!getCompilerContext().skipStats(this.getTableNumber()))
                getCompilerContext().getSkipStatsTableList().add(this.getTableNumber());
            return getCompilerContext().getStoreCostController(td, cd, true, defaultRowCount);
        }
        else {
            return getCompilerContext().getStoreCostController(td, cd,
                    getCompilerContext().skipStats(this.getTableNumber()), 0);
        }
    }

    private StoreCostController getBaseCostController() throws StandardException{
        return getStoreCostController(this.tableDescriptor,baseConglomerateDescriptor);
    }

    private boolean gotRowCount=false;
    private long rowCount=0;

    private long baseRowCount() throws StandardException{
        if(!gotRowCount){
            StoreCostController scc=getBaseCostController();
            rowCount=scc.getEstimatedRowCount();
            gotRowCount=true;
        }

        return rowCount;
    }

    private DataValueDescriptor[] getRowTemplate(ConglomerateDescriptor cd,
                                                 StoreCostController scc) throws StandardException{
        /*
        ** If it's for a heap scan, just get all the columns in the
        ** table.
        */
        if(!cd.isIndex())
            return templateColumns.buildEmptyRow().getRowArray();

        /* It's an index scan, so get all the columns in the index */
        ExecRow emptyIndexRow=templateColumns.buildEmptyIndexRow(
                tableDescriptor,
                cd,
                scc
        );

        return emptyIndexRow.getRowArray();
    }

    private ConglomerateDescriptor getFirstConglom(OptimizablePredicateList predList,
                                                   Optimizer optimizer) throws StandardException{
        getConglomDescs();
        return getNextConglom(null, predList, optimizer);
    }

    private ConglomerateDescriptor getNextConglom(ConglomerateDescriptor currCD,
                                                  OptimizablePredicateList predList,
                                                  Optimizer optimizer) throws StandardException {
        OptimizerTrace tracer=optimizer.tracer();
        int index=-1;

        if (currCD != null){
            for (index=0; index < conglomDescs.length; index++) {
                if (currCD == conglomDescs[index]) {
                    break;
                }
            }
        }

        index ++;
        while (index<conglomDescs.length) {
            if (isIndexEligible(conglomDescs[index], predList))
                return conglomDescs[index];
            tracer.trace(OptimizerFlag.SPARSE_INDEX_NOT_ELIGIBLE,0,0,0.0,conglomDescs[index]);
            index ++;
        }

        return null;
    }

    private void getConglomDescs() throws StandardException{
        if(conglomDescs==null){
            conglomDescs=tableDescriptor.getConglomerateDescriptors();
        }
    }

    /**
     * set the Information gathered from the parent table that is
     * required to peform a referential action on dependent table.
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    @Override
    public void setRefActionInfo(long fkIndexConglomId,
                                 int[] fkColArray,
                                 String parentResultSetId,
                                 boolean dependentScan){


        this.fkIndexConglomId=fkIndexConglomId;
        this.fkColArray=fkColArray;
        this.raParentResultSetId=parentResultSetId;
        if (dependentScan)
            throw new RuntimeException("Not implemented");
    }

    /**
     * Accept the visitor for all visitable children of this node.
     *
     * @param v the visitor
     */
    @Override
    public void acceptChildren(Visitor v)  throws StandardException{
        super.acceptChildren(v);

        if(nonStoreRestrictionList!=null){
            nonStoreRestrictionList.accept(v, this);
        }

        if(restrictionList!=null){
            restrictionList.accept(v, this);
        }

        if(nonBaseTableRestrictionList!=null){
            nonBaseTableRestrictionList.accept(v, this);
        }

        if(requalificationRestrictionList!=null){
            requalificationRestrictionList.accept(v, this);
        }
    }

    @Override
    public ResultColumn getRowIdColumn(){
        return rowIdColumn;
    }

    @Override
    public void setRowIdColumn(ResultColumn rc) {
        rowIdColumn = rc;
    }

    /**
     *
     * Is this FromBaseTable node a distinct scan (effects cost estimate)
     *
     * @return
     */
    public boolean isDistinctScan() {
        return distinctScan;
    }

    public void setAntiJoin (boolean isAntiJoin) {
        this.isAntiJoin = isAntiJoin;
    }

    public boolean isAntiJoin() {
        return this.isAntiJoin;
    }

    @Override
    public String printRuntimeInformation() throws StandardException {
        StringBuilder sb = new StringBuilder();
        String indexName = getIndexName();
        sb.append(getClassName(indexName)).append("(")
                .append(",").append(getFinalCostEstimate(false).prettyFromBaseTableString());
        if (indexName != null)
            sb.append(",baseTable=").append(getPrettyTableName());
        List<String> qualifiers =  Lists.transform(PredicateUtils.PLtoList(RSUtils.getPreds(this)), PredicateUtils.predToString);
        if(qualifiers!=null && !qualifiers.isEmpty()) //add
            sb.append(",preds=[").append(Joiner.on(",").skipNulls().join(qualifiers)).append("]");
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String printExplainInformation(String attrDelim) throws StandardException {
        StringBuilder sb = new StringBuilder();
        String indexName = getIndexName();
        sb.append(spaceToLevel());
        sb.append(getClassName(indexName)).append("(");
        sb.append("n=").append(getResultSetNumber()).append(attrDelim);
        sb.append(getFinalCostEstimate(false).prettyFromBaseTableString(attrDelim));
        if (indexName != null)
            sb.append(attrDelim).append("baseTable=").append(getPrettyTableName());
        List<String> qualifiers = Lists.transform(PredicateUtils.PLtoList(RSUtils.getPreds(this)), PredicateUtils.predToString);
        if (qualifiers != null && !qualifiers.isEmpty())
            sb.append(attrDelim).append("preds=[").append(Joiner.on(",").skipNulls().join(qualifiers)).append("]");
        sb.append(")");
        return sb.toString();
    }

    private String getClassName(String niceIndexName) throws StandardException {
        String cName = "";
        if(niceIndexName!=null){
            cName = "IndexScan["+niceIndexName+"]";
        }else{
            cName = "TableScan["+getPrettyTableName()+"]";
        }
        if(isMultiProbing())
            cName = "MultiProbe"+cName;
        if (isDistinctScan())
            cName = "Distinct" + cName;
        if (specialMaxScan)
            cName = "LastKey" + cName;
        return cName;
    }

    private String getIndexName() {
        ConglomerateDescriptor cd = getTrulyTheBestAccessPath().getConglomerateDescriptor();
        if (cd.isIndex())
            return String.format("%s(%s)", cd.getConglomerateName(), cd.getConglomerateNumber());
        return null;
    }

    private String getPrettyTableName() throws StandardException {
        TableDescriptor tableDescriptor=getTableDescriptor();
        return String.format("%s(%s)",tableDescriptor.getName(),tableDescriptor.getHeapConglomerateId());
    }

    @Override
    public void buildTree(Collection<QueryTreeNode> tree, int depth) throws StandardException {
        setDepth(depth);
        tree.add(this);
        /* predicates in restrictionList after post-opt stage should be redundant, as all the predicates
           should have been either in storeRestrictionList or nonStoreRestrictionList.
           When searching the current FromBaseTable node for subqueries, we may get duplicate SubqueryNodes.
           So collect the subqueries directly from ResultColumns, storeRestrictionList and nonStoreRestrictionList.
        */
        splice.com.google.common.base.Predicate<Object> onAxis = Predicates.not(isRSN);
        CollectingVisitorBuilder<SubqueryNode> builder = CollectingVisitorBuilder.forClass(SubqueryNode.class).onAxis(onAxis);
        builder.collect(resultColumns);
        builder.collect(nonStoreRestrictionList);
        List<SubqueryNode> subqueryNodeList = builder.collect(storeRestrictionList);
        for (SubqueryNode sub: subqueryNodeList)
            sub.buildTree(tree,depth+1);
    }

    /**
     * Added by splice because we, unfortunately, manipulate the AST from post-optimization visitors.
     */
    public void clearAllPredicates() throws StandardException {
        if(this.baseTableRestrictionList != null) {
            this.baseTableRestrictionList.removeAllPredicates();
        }
        if(this.nonBaseTableRestrictionList != null) {
            this.nonBaseTableRestrictionList.removeAllPredicates();
        }
        if(this.restrictionList != null) {
            this.restrictionList.removeAllPredicates();
        }
        if(this.storeRestrictionList != null) {
            this.storeRestrictionList.removeAllPredicates();
        }
        if(this.nonStoreRestrictionList != null) {
            this.nonStoreRestrictionList.removeAllPredicates();
        }
        if(this.requalificationRestrictionList != null) {
            this.requalificationRestrictionList.removeAllPredicates();
        }
    }

    @Override
    public String toHTMLString() {
        return "tableName: " +  Objects.toString(tableName) + "<br/>" +
                "updateOrDelete: " + updateOrDelete + "<br/>" +
                "tableProperties: " + Objects.toString(tableProperties) + "<br/>" +
                "existsTable: " + existsTable + "<br/>" +
                "dependencyMap: " + Objects.toString(dependencyMap) +
                super.toHTMLString();
    }

    public void determineSpark() {
        setDataSetProcessorType(getDataSetProcessorTypeForAccessPath(getTrulyTheBestAccessPath()));
    }

    /**
     * Return the data set processor type for a given access path.
     *
     * @param accessPath the access path
     */
    public DataSetProcessorType getDataSetProcessorTypeForAccessPath(AccessPath accessPath) {
        if (! dataSetProcessorType.isDefaultControl()) {
            // No need to assess cost
            return dataSetProcessorType;
        }
        long sparkRowThreshold = getLanguageConnectionContext().getOptimizerFactory().getDetermineSparkRowThreshold();
        // we need to check not only the number of row scanned, but also the number of output rows for the
        // join result
        assert dataSetProcessorType.isDefaultControl();
        if (accessPath != null &&
                (accessPath.getCostEstimate().getScannedBaseTableRows() > sparkRowThreshold ||
                 accessPath.getCostEstimate().getEstimatedRowCount() > sparkRowThreshold)) {
            return DataSetProcessorType.COST_SUGGESTED_SPARK;
        }
        return dataSetProcessorType;
    }

    private boolean hasConstantPredicate(int tableNum, int colNum, OptimizablePredicateList predList) {
        if (predList instanceof PredicateList)
            return ((PredicateList)predList).constantColumn(tableNum, colNum);
        else
            return false;
    }

    public FormatableBitSet getReferencedCols() {
        return referencedCols;
    }

    public void setAggregateForSpecialMaxScan(AggregateNode aggrNode) {
        aggrForSpecialMaxScan = aggrNode;
    }

    public AggregateNode getAggregateForSpecialMaxScan() {
        return aggrForSpecialMaxScan;
    }

    public boolean useRealTableStats() { return useRealTableStats; }

    public List<Integer> getNoStatsColumnIds() {
        return new ArrayList<>(usedNoStatsColumnIds);
    }
}