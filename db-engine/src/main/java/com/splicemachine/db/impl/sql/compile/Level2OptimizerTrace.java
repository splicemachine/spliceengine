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

import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.IndexRowGenerator;

/**
 * @author Scott Fines
 *         Date: 4/3/15
 */
public class Level2OptimizerTrace implements OptimizerTrace{
    private final LanguageConnectionContext lcc;
    private final Level2OptimizerImpl optimizer;
    private final int cachedHashcode;

    public Level2OptimizerTrace(LanguageConnectionContext lcc,Level2OptimizerImpl optimizer){
        this.lcc=lcc;
        this.optimizer=optimizer;
        this.cachedHashcode = optimizer.hashCode();
    }

    @Override
    public void trace(OptimizerFlag flag,int intParam1,int intParam2,double doubleParam,Object... objectParams){
        ConglomerateDescriptor cd;
        String cdString;
        String traceString;
        Object objectParam1 = null;
        Object objectParam2 = null;
        Object objectParam3 = null;
        if (objectParams != null && objectParams.length > 0) {
            objectParam1 = objectParams[0];
            if (objectParams.length >= 2) {
                objectParam2 = objectParams[1];
            }
            if (objectParams.length >= 3) {
                objectParam3 = objectParams[2];
            }
        }

        switch(flag){
            case STARTED:
                traceString= "======================================================== " +
                    "Optimization started at time "+ optimizer.timeOptimizationStarted;
                break;
            case NEXT_ROUND:
                traceString="Preparing for next round.";
                break;
            case HAS_REMAINING_PERMUTATIONS:
                traceString="Permutations remaining? "+(intParam1 == 1 ? "TRUE" : "FALSE");
                break;
            case MAX_TIME_EXCEEDED:
                traceString="Optimization maximum time exceeded: "+doubleParam+" Max: "+optimizer.getMaxTimeout();
                break;
            case BEST_TIME_EXCEEDED:
                traceString="Optimization previous best time already exceeded: "+doubleParam+", Best: "+bestCost();
                break;
            case NO_TABLES:
                traceString="No tables to optimize.";
                break;
            case COMPLETE_JOIN_ORDER:
                traceString="We have a complete join order.";
                break;
            case COST_OF_SORTING:
                traceString="Cost of sorting is "+optimizer.sortCost;
                break;
            case NO_BEST_PLAN:
                traceString="No best plan found.";
                break;
            case MODIFYING_ACCESS_PATHS:
                traceString="Modifying access paths in this optimizer ";
                break;
            case SHORT_CIRCUITING:
                String basis=(optimizer.timeExceeded)?"time exceeded":"cost";
                int propJoinOrder=optimizer.proposedJoinOrder[optimizer.joinPosition];
                Optimizable thisOpt=optimizer.optimizableList.getOptimizable(propJoinOrder);
                if(thisOpt.getBestAccessPath().getCostEstimate()==null)
                    basis="no best plan found";
                traceString="Short circuiting based on "+basis+" at join position "+optimizer.joinPosition;
                break;
            case SKIPPING_JOIN_ORDER:
                traceString=buildJoinOrder("Skipping join order: ",true,intParam1,optimizer.proposedJoinOrder);
                break;
            case ILLEGAL_USER_JOIN_ORDER:
                traceString="User specified join order is not legal.";
                break;
            case USER_JOIN_ORDER_OPTIMIZED:
                traceString="User-specified join order has now been optimized.";
                break;
            case CONSIDERING_JOIN_ORDER:
                traceString=buildJoinOrder("Considering join order: ",false,intParam1,optimizer.proposedJoinOrder) + ", current cost starts with: " + objectParam1;
                break;
            case TOTAL_COST_NON_SA_PLAN:
                traceString="Total cost of non-sort-avoidance plan is "+optimizer.currentCost;
                break;
            case TOTAL_COST_SA_PLAN:
                traceString="Total cost of sort avoidance plan is "+optimizer.currentSortAvoidanceCost;
                break;
            case TOTAL_COST_WITH_SORTING:
                traceString="Total cost of non-sort-avoidance plan with sort cost added is "+optimizer.currentCost;
                break;
            case CURRENT_PLAN_IS_SA_PLAN:
                traceString="Current plan is a sort avoidance plan. Best cost is : "+optimizer.bestCost+
                    ". This cost is : "+optimizer.currentSortAvoidanceCost;
                break;
            case CHEAPEST_PLAN_SO_FAR:
                traceString="This is the cheapest plan so far.";
                break;
            case PLAN_TYPE:
                traceString="Plan is a "+(intParam1==Optimizer.NORMAL_PLAN?"normal":"sort avoidance")+" plan.";
                break;
            case COST_OF_CHEAPEST_PLAN_SO_FAR:
                traceString="Cost of cheapest plan is "+optimizer.currentCost;
                break;
            case SORT_NEEDED_FOR_ORDERING:
                traceString="Sort needed for ordering: "+(intParam1!=Optimizer.SORT_AVOIDANCE_PLAN)+
                    ". Row ordering: "+optimizer.requiredRowOrdering;
                break;
            case REMEMBERING_BEST_JOIN_ORDER:
                traceString=buildJoinOrder("Remembering join order as best: ",false,intParam1,optimizer.bestJoinOrder);
                break;
            case SKIPPING_DUE_TO_EXCESS_MEMORY:
                traceString="Skipping access path due to excess memory usage, maximum is "+optimizer.maxMemoryPerTable;
                break;
            case COST_OF_N_SCANS:
                traceString="Cost of "+doubleParam+" scans is: "+ objectParam1 +" for table "+intParam1 +" ("+objectParam2+")";
                break;
            case HJ_SKIP_NOT_MATERIALIZABLE:
                traceString="Skipping HASH JOIN because optimizable is not materializable";
                break;
            case HJ_SKIP_NO_JOIN_COLUMNS:
                traceString="Skipping HASH JOIN because there are no hash key columns";
                break;
            case HJ_NO_EQUIJOIN_COLUMNS:
                traceString="Warning: Applying HASH JOIN with no hash key columns";
                break;
            case HJ_HASH_KEY_COLUMNS:
                int[] hashKeyColumns=(int[]) objectParam1;
                StringBuilder buf = new StringBuilder("# hash key columns = ").append(hashKeyColumns.length)
                                                                              .append(" {");
                for(int index=0;index<hashKeyColumns.length;index++){
                    buf.append(" hashKeyColumns[").append(index).append("] = ").append(hashKeyColumns[index]);
                }
                buf.append("}");
                traceString = buf.toString();
                break;
            case CALLING_ON_JOIN_NODE:
                traceString="Calling optimizeIt() for join node";
                break;
            case JOIN_NODE_PREDICATE_MANIPULATION:
                String preds;
                if (objectParam2 instanceof PredicateList) {
                    preds = OperatorToString.toString((PredicateList) objectParam2);
                } else {
                    preds = OperatorToString.toString((Predicate) objectParam2);
                }
                traceString = "Predicate manipulation in join nodeL: "+objectParam1+": "+preds;
                break;
            case CONSIDERING_JOIN_STRATEGY:
                JoinStrategy js=(JoinStrategy) objectParam1;
                traceString="Considering join strategy "+js+" for table "+intParam1 + " ("+objectParam2+")";
                break;
            case REMEMBERING_BEST_ACCESS_PATH:
                traceString="Remembering access path "+ objectParam1 +
                    " as truly the best for table "+intParam1+" ("+ objectParam2+") for plan type "
                        +(intParam2==Optimizer.NORMAL_PLAN?" normal ":"sort avoidance")+"\n";
                break;
            case NO_MORE_CONGLOMERATES:
                traceString="No more conglomerates to consider for table "+intParam1+ " ("+objectParam1+")";
                break;
            case CONSIDERING_CONGLOMERATE:
                cd=(ConglomerateDescriptor) objectParam1;
                cdString=dumpConglomerateDescriptor(cd);
                traceString=" - Considering conglomerate "+cdString+" for table "+intParam1 + " ("+objectParam2+")";
                break;
            case SCANNING_HEAP_FULL_MATCH_ON_UNIQUE_KEY:
                traceString="Scanning heap, but we have a full match on a unique key.";
                break;
            case ADDING_UNORDERED_OPTIMIZABLE:
                traceString="Adding unordered optimizable, # of predicates = "+intParam1+": "+
                    OperatorToString.toString((PredicateList) objectParam1);
                break;
            case CHANGING_ACCESS_PATH_FOR_TABLE:
                traceString="Changing access path for table "+intParam1 +" ("+objectParam1+")";
                break;
            case TABLE_LOCK_NO_START_STOP:
                traceString="Lock mode set to MODE_TABLE because no start or stop position";
                break;
            case NON_COVERING_INDEX_COST:
                traceString="Index does not cover query - cost including base row fetch is: "
                    +doubleParam+" for table "+intParam1 +" ("+objectParam1+")";
                break;
            case ROW_LOCK_ALL_CONSTANT_START_STOP:
                traceString="Lock mode set to MODE_RECORD because all start and stop positions are constant";
                break;
            case ESTIMATING_COST_OF_CONGLOMERATE:
                cd=(ConglomerateDescriptor) objectParam1;
                cdString=dumpConglomerateDescriptor(cd);
                traceString="Estimating cost of conglomerate: "+costForTable(cdString,intParam1,objectParam2);
                break;
            case LOOKING_FOR_SPECIFIED_INDEX:
                traceString="Looking for user-specified index: "+ objectParam1 +" for table "+intParam1 +" ("+objectParam2+")";
                break;
            case MATCH_SINGLE_ROW_COST:
                traceString="Guaranteed to match a single row - cost is: "+doubleParam+" for table "+intParam1 +" ("+objectParam2+")";
                break;
            case COST_INCLUDING_EXTRA_1ST_COL_SELECTIVITY:
                traceString="Cost including extra first column selectivity is : "+ objectParam1 +" for table "+intParam1 +" ("+objectParam2+")";
                break;
            case CALLING_NEXT_ACCESS_PATH:
                traceString="Calling nextAccessPath() for base table "+ objectParam1 +" with "+intParam1+" predicates: "+
                OperatorToString.toString((PredicateList) objectParam2);
                break;
            case TABLE_LOCK_OVER_THRESHOLD:
                traceString=lockModeThreshold("MODE_TABLE","greater",doubleParam,intParam1);
                break;
            case ROW_LOCK_UNDER_THRESHOLD:
                traceString=lockModeThreshold("MODE_RECORD","less",doubleParam,intParam1);
                break;
            case COST_INCLUDING_EXTRA_START_STOP:
                traceString=costIncluding("start/stop", objectParam1,intParam1, objectParam2);
                break;
            case COST_INCLUDING_EXTRA_QUALIFIER_SELECTIVITY:
                traceString=costIncluding("qualifier", objectParam1,intParam1, objectParam2);
                break;
            case COST_INCLUDING_EXTRA_NONQUALIFIER_SELECTIVITY:
                traceString=costIncluding("non-qualifier", objectParam1,intParam1, objectParam2);
                break;
            case COST_INCLUDING_COMPOSITE_SEL_FROM_STATS:
                traceString=costIncluding("selectivity from statistics", objectParam1,intParam1, objectParam2);
                break;
            case COST_INCLUDING_STATS_FOR_INDEX:
                traceString=costIncluding("statistics for index being considered", objectParam1,intParam1, objectParam2);
                break;
            case COMPOSITE_SEL_FROM_STATS:
                traceString="Selectivity from statistics found. It is "+doubleParam;
                break;
            case COST_OF_NONCOVERING_INDEX:
                traceString="Index does not cover query: cost including row fetch is: "+
                    costForTable(objectParam1, intParam1, objectParam2);
                break;
            case REMEMBERING_JOIN_STRATEGY:
                traceString="Remembering join strategy "+ objectParam1 +" as best for table "+intParam1 +" ("+objectParam2+")";
                break;
            case REMEMBERING_BEST_ACCESS_PATH_SUBSTRING:
                traceString="\tin best access path : "+objectParam1;
                break;
            case REMEMBERING_BEST_SORT_AVOIDANCE_ACCESS_PATH_SUBSTRING:
                traceString="\tin best sort avoidance access path : "+objectParam1;
                break;
            case REMEMBERING_BEST_UNKNOWN_ACCESS_PATH_SUBSTRING:
                traceString="\tin best unknown access path : "+objectParam1;
                break;
            case COST_OF_CONGLOMERATE_SCAN1:
                cd=(ConglomerateDescriptor) objectParam1;
                cdString=dumpConglomerateDescriptor(cd);
                traceString="Cost of conglomerate "+cdString+" scan for table number "+intParam1 +
                    " ("+objectParam2+") is : " + objectParam3;
                break;
            case COST_OF_CONGLOMERATE_SCAN3:
                traceString="\tNumber of extra first column predicates is : "+
                        intParam1+
                        ", extra first column selectivity is : "+
                        doubleParam;
                break;
            case COST_OF_CONGLOMERATE_SCAN4:
                traceString="\tNumber of extra start/stop predicates is : "+
                        intParam1+
                        ", extra start/stop selectivity is : "+
                        doubleParam;
                break;
            case COST_OF_CONGLOMERATE_SCAN5:
                traceString="\tNumber of extra qualifiers is : "+
                        intParam1+
                        ", extra qualifier selectivity is : "+
                        doubleParam;
                break;
            case COST_OF_CONGLOMERATE_SCAN6:
                traceString="\tNumber of extra non-qualifiers is : "+
                        intParam1+
                        ", extra non-qualifier selectivity is : "+
                        doubleParam;
                break;
            case COST_OF_CONGLOMERATE_SCAN7:
                traceString="\tNumber of start/stop statistics predicates is : "+
                        intParam1+
                        ", statistics start/stop selectivity is : "+
                        doubleParam;
                break;
            case INFEASIBLE_JOIN:
                traceString="Skipping join Strategy "+ objectParam1 +" because it is infeasible";
                break;
            case SPARSE_INDEX_NOT_ELIGIBLE:
                cd=(ConglomerateDescriptor) objectParam1;
                cdString=dumpConglomerateDescriptor(cd);
                traceString="Skipping sparse index " + cdString + " because it is not eligible";
                break;
            case PULL_OPTIMIZABLE:
                if (intParam1 == 0) {
                    // for regular plan
                    traceString = "Join order: Pull optimizable " + intParam1 + ", current cost after pulling: " + objectParam1;
                } else {
                    // for sort avoidance plan
                    traceString = "Join order: Pull optimizable " + intParam1 + ", current sort avoidance cost after pulling: " + objectParam1;
                }
                break;
            case REWIND_JOINORDER:
                traceString="Join order: Rewind to " + objectParam1;
                break;
            default:
                throw new IllegalStateException("Unexpected Trace flag: "+ flag);
        }
        assert traceString!=null: "TraceString expected to be non-null";
        trace(flag.level(), " Optimizer:"+cachedHashcode+" "+traceString);
    }

    @Override
    public void trace(TraceLevel level, String traceString){
        lcc.appendOptimizerTraceOutput(traceString+"\n");
    }

    /* ***************************************************************************************************************/
    /*private helper methods*/
    private String buildJoinOrder(String prefix,boolean addJoinOrderNumber,
                                  int joinOrderNumber,int[] joinOrder){
        StringBuilder joinOrderString=new StringBuilder();
        joinOrderString.append(prefix);

        for(int i=0;i<=optimizer.joinPosition;i++){
            joinOrderString.append(" ").append(joinOrder[i]);
        }
        if(addJoinOrderNumber){
            joinOrderString.append(" ").append(joinOrderNumber);
        }

        joinOrderString.append(" with assignedTableMap = ").append(optimizer.assignedTableMap).append(" ");
        return joinOrderString.toString();
    }

    private String lockModeThreshold( String lockMode,String relop, double rowCount,int threshold){
        return "Lock mode set to "+lockMode+ " because estimated row count of "+rowCount+
                        " "+relop+" than threshold of "+threshold;
    }

    private String costIncluding(
        String selectivityType,Object cost,int tableNumber, Object tableName){
        return
                "Cost including extra "+selectivityType+
                        " start/stop selectivity is : "+
                    costForTable(cost,tableNumber, tableName);
    }

    private String dumpConglomerateDescriptor(ConglomerateDescriptor cd){
        if(SanityManager.DEBUG){
            return cd.toString();
        }

        StringBuilder keyString= new StringBuilder();
        String[] columnNames=cd.getColumnNames();

        if(cd.isIndex() && columnNames!=null){
            IndexRowGenerator irg=cd.getIndexDescriptor();

            int[] keyColumns=irg.baseColumnPositions();

            keyString = new StringBuilder(", key columns = {" + columnNames[keyColumns[0] - 1]);
            for(int index=1;index<keyColumns.length;index++){
                keyString.append(", ").append(columnNames[keyColumns[index] - 1]);
            }
            keyString.append("}");
        }

        return "CD: conglomerateNumber = "+cd.getConglomerateNumber()+
                " name = "+cd.getConglomerateName()+
                " uuid = "+cd.getUUID()+
                " indexable = "+cd.isIndex()+
                keyString;
    }

    private String costForTable(Object cost,int tableNumber, Object tableName){
        return cost+" for table "+tableNumber+" ("+tableName+")";
    }

    private double bestCost(){
        return optimizer.timeLimit;
    }
}
