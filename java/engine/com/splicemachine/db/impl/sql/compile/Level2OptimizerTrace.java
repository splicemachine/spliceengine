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

    public Level2OptimizerTrace(LanguageConnectionContext lcc,Level2OptimizerImpl optimizer){
        this.lcc=lcc;
        this.optimizer=optimizer;
    }

    @Override
    public void trace(OptimizerFlag flag,int intParam1,int intParam2,double doubleParam,Object objectParam1){
        ConglomerateDescriptor cd;
        String cdString;
        String traceString;

        switch(flag){
            case STARTED:
                traceString= "Optimization started at time "+ optimizer.timeOptimizationStarted+
                        " using optimizer "+this.hashCode();
                break;

            case TIME_EXCEEDED:
                traceString="Optimization time exceeded at time "+optimizer.currentTime+"\n"+bestCost();
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
                traceString="Modifying access paths using optimizer "+this.hashCode();
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
                traceString=buildJoinOrder("\n\nSkipping join order: ",true,intParam1,optimizer.proposedJoinOrder);
                break;
            case ILLEGAL_USER_JOIN_ORDER:
                traceString="User specified join order is not legal.";
                break;
            case USER_JOIN_ORDER_OPTIMIZED:
                traceString="User-specified join order has now been optimized.";
                break;
            case CONSIDERING_JOIN_ORDER:
                traceString=buildJoinOrder("\n\nConsidering join order: ",false,intParam1,optimizer.proposedJoinOrder);
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
                traceString="Current plan is a sort avoidance plan."+"\n\tBest cost is : "+optimizer.bestCost+
                        "\n\tThis cost is : "+optimizer.currentSortAvoidanceCost;
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
                        "\n\tRow ordering: "+optimizer.requiredRowOrdering;
                break;
            case REMEMBERING_BEST_JOIN_ORDER:
                traceString=buildJoinOrder("\n\nRemembering join order as best: ",false,intParam1,optimizer.bestJoinOrder);
                break;
            case SKIPPING_DUE_TO_EXCESS_MEMORY:
                traceString="Skipping access path due to excess memory usage, maximum is "+optimizer.maxMemoryPerTable;
                break;
            case COST_OF_N_SCANS:
                traceString="Cost of "+doubleParam+" scans is: "+objectParam1+" for table "+intParam1;
                break;
            case HJ_SKIP_NOT_MATERIALIZABLE:
                traceString="Skipping HASH JOIN because optimizable is not materializable";
                break;
            case HJ_SKIP_NO_JOIN_COLUMNS:
                traceString="Skipping HASH JOIN because there are no hash key columns";
                break;
            case HJ_HASH_KEY_COLUMNS:
                int[] hashKeyColumns=(int[])objectParam1;
                traceString="# hash key columns = "+hashKeyColumns.length;
                for(int index=0;index<hashKeyColumns.length;index++){
                    traceString="\n"+traceString+"hashKeyColumns["+index+"] = "+hashKeyColumns[index];
                }
                break;
            case CALLING_ON_JOIN_NODE:
                traceString="Calling optimizeIt() for join node";
                break;
            case CONSIDERING_JOIN_STRATEGY:
                JoinStrategy js=(JoinStrategy)objectParam1;
                traceString="\nConsidering join strategy "+js+" for table "+intParam1;
                break;
            case REMEMBERING_BEST_ACCESS_PATH:
                traceString="Remembering access path "+objectParam1+
                        " as truly the best for table "+intParam1+" for plan type "
                        +(intParam2==Optimizer.NORMAL_PLAN?" normal ":"sort avoidance")+"\n";
                break;
            case NO_MORE_CONGLOMERATES:
                traceString="No more conglomerates to consider for table "+intParam1;
                break;
            case CONSIDERING_CONGLOMERATE:
                cd=(ConglomerateDescriptor)objectParam1;
                cdString=dumpConglomerateDescriptor(cd);
                traceString="\nConsidering conglomerate "+cdString+" for table "+intParam1;
                break;
            case SCANNING_HEAP_FULL_MATCH_ON_UNIQUE_KEY:
                traceString="Scanning heap, but we have a full match on a unique key.";
                break;
            case ADDING_UNORDERED_OPTIMIZABLE:
                traceString="Adding unordered optimizable, # of predicates = "+intParam1;
                break;
            case CHANGING_ACCESS_PATH_FOR_TABLE:
                traceString="Changing access path for table "+intParam1;
                break;
            case TABLE_LOCK_NO_START_STOP:
                traceString="Lock mode set to MODE_TABLE because no start or stop position";
                break;
            case NON_COVERING_INDEX_COST:
                traceString="Index does not cover query - cost including base row fetch is: "
                        +doubleParam+" for table "+intParam1;
                break;
            case ROW_LOCK_ALL_CONSTANT_START_STOP:
                traceString="Lock mode set to MODE_RECORD because all start and stop positions are constant";
                break;
            case ESTIMATING_COST_OF_CONGLOMERATE:
                cd=(ConglomerateDescriptor)objectParam1;
                cdString=dumpConglomerateDescriptor(cd);
                traceString="Estimating cost of conglomerate: "+costForTable(cdString,intParam1);
                break;
            case LOOKING_FOR_SPECIFIED_INDEX:
                traceString="Looking for user-specified index: "+objectParam1+" for table "+intParam1;
                break;
            case MATCH_SINGLE_ROW_COST:
                traceString="Guaranteed to match a single row - cost is: "+doubleParam+" for table "+intParam1;
                break;
            case COST_INCLUDING_EXTRA_1ST_COL_SELECTIVITY:
                traceString="Cost including extra first column selectivity is : "+objectParam1+" for table "+intParam1;
                break;
            case CALLING_NEXT_ACCESS_PATH:
                traceString="Calling nextAccessPath() for base table "+objectParam1+" with "+intParam1+" predicates.";
                break;
            case TABLE_LOCK_OVER_THRESHOLD:
                traceString=lockModeThreshold("MODE_TABLE","greater",doubleParam,intParam1);
                break;
            case ROW_LOCK_UNDER_THRESHOLD:
                traceString=lockModeThreshold("MODE_RECORD","less",doubleParam,intParam1);
                break;
            case COST_INCLUDING_EXTRA_START_STOP:
                traceString=costIncluding("start/stop",objectParam1,intParam1);
                break;
            case COST_INCLUDING_EXTRA_QUALIFIER_SELECTIVITY:
                traceString=costIncluding("qualifier",objectParam1,intParam1);
                break;
            case COST_INCLUDING_EXTRA_NONQUALIFIER_SELECTIVITY:
                traceString=costIncluding("non-qualifier",objectParam1,intParam1);
                break;
            case COST_INCLUDING_COMPOSITE_SEL_FROM_STATS:
                traceString=costIncluding("selectivity from statistics",objectParam1,intParam1);
                break;
            case COST_INCLUDING_STATS_FOR_INDEX:
                traceString=costIncluding("statistics for index being considered",objectParam1,intParam1);
                break;
            case COMPOSITE_SEL_FROM_STATS:
                traceString="Selectivity from statistics found. It is "+doubleParam;
                break;
            case COST_OF_NONCOVERING_INDEX:
                traceString="Index does not cover query: cost including row fetch is: "+
                        costForTable(objectParam1,intParam1);
                break;
            case REMEMBERING_JOIN_STRATEGY:
                traceString="\nRemembering join strategy "+objectParam1+" as best for table "+intParam1;
                break;
            case REMEMBERING_BEST_ACCESS_PATH_SUBSTRING:
                traceString="in best access path";
                break;
            case REMEMBERING_BEST_SORT_AVOIDANCE_ACCESS_PATH_SUBSTRING:
                traceString="in best sort avoidance access path";
                break;
            case REMEMBERING_BEST_UNKNOWN_ACCESS_PATH_SUBSTRING:
                traceString="in best unknown access path";
                break;
            case COST_OF_CONGLOMERATE_SCAN1:
                cd=(ConglomerateDescriptor)objectParam1;
                cdString=dumpConglomerateDescriptor(cd);
                traceString="Cost of conglomerate "+cdString+" scan for table number "+intParam1+" is : ";
                break;
            case COST_OF_CONGLOMERATE_SCAN2:
                traceString=objectParam1.toString();
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
            default:
                throw new IllegalStateException("Unexpected Trace flag: "+ flag);
        }
        assert traceString!=null: "TraceString expected to be non-null";
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

        joinOrderString.append(" with assignedTableMap = ").append(optimizer.assignedTableMap).append("\n\n");
        return joinOrderString.toString();
    }

    private String lockModeThreshold( String lockMode,String relop, double rowCount,int threshold){
        return "Lock mode set to "+lockMode+ " because estimated row count of "+rowCount+
                        " "+relop+" than threshold of "+threshold;
    }

    private String costIncluding(
            String selectivityType,Object objectParam1,int intParam1){
        return
                "Cost including extra "+selectivityType+
                        " start/stop selectivity is : "+
                        costForTable(objectParam1,intParam1);
    }

    private String dumpConglomerateDescriptor(ConglomerateDescriptor cd){
        if(SanityManager.DEBUG){
            return cd.toString();
        }

        String keyString="";
        String[] columnNames=cd.getColumnNames();

        if(cd.isIndex() && columnNames!=null){
            IndexRowGenerator irg=cd.getIndexDescriptor();

            int[] keyColumns=irg.baseColumnPositions();

            keyString=", key columns = {"+columnNames[keyColumns[0]-1];
            for(int index=1;index<keyColumns.length;index++){
                keyString=keyString+", "+columnNames[keyColumns[index]-1];
            }
            keyString=keyString+"}";
        }

        return "CD: conglomerateNumber = "+cd.getConglomerateNumber()+
                " name = "+cd.getConglomerateName()+
                " uuid = "+cd.getUUID()+
                " indexable = "+cd.isIndex()+
                keyString;
    }

    private String costForTable(Object cost,int tableNumber){
        return cost+" for table "+tableNumber;
    }

    private String bestCost(){
        return "Best cost = "+optimizer.bestCost+"\n";
    }

}
