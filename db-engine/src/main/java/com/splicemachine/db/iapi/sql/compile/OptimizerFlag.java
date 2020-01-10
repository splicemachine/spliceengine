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

package com.splicemachine.db.iapi.sql.compile;

import static com.splicemachine.db.iapi.sql.compile.OptimizerTrace.TraceLevel.*;
/**
 * @author Scott Fines
 *         Date: 4/3/15
 */
public enum OptimizerFlag{
    // optimizer trace
    STARTED(TRACE),
    NEXT_ROUND(INFO),
    MAX_TIME_EXCEEDED(INFO),
    BEST_TIME_EXCEEDED(INFO),
    NO_TABLES(INFO),
    COMPLETE_JOIN_ORDER(DEBUG),
    COST_OF_SORTING(TRACE),
    NO_BEST_PLAN(ERROR),
    MODIFYING_ACCESS_PATHS(DEBUG),
    SHORT_CIRCUITING(TRACE),
    SKIPPING_JOIN_ORDER(DEBUG),
    ILLEGAL_USER_JOIN_ORDER(ERROR),
    USER_JOIN_ORDER_OPTIMIZED(INFO),
    CONSIDERING_JOIN_ORDER(DEBUG),
    TOTAL_COST_NON_SA_PLAN(TRACE),
    TOTAL_COST_SA_PLAN(TRACE),
    TOTAL_COST_WITH_SORTING(INFO),
    CURRENT_PLAN_IS_SA_PLAN(TRACE),
    HAS_REMAINING_PERMUTATIONS(TRACE),
    CHEAPEST_PLAN_SO_FAR(TRACE),
    PLAN_TYPE(DEBUG),
    COST_OF_CHEAPEST_PLAN_SO_FAR(TRACE),
    SORT_NEEDED_FOR_ORDERING(DEBUG),
    REMEMBERING_BEST_JOIN_ORDER(TRACE),
    SKIPPING_DUE_TO_EXCESS_MEMORY(WARN),
    COST_OF_N_SCANS(TRACE),
    HJ_SKIP_NOT_MATERIALIZABLE(DEBUG),
    HJ_SKIP_NO_JOIN_COLUMNS(DEBUG),
    HJ_HASH_KEY_COLUMNS(TRACE),
    CALLING_ON_JOIN_NODE(TRACE),
    JOIN_NODE_PREDICATE_MANIPULATION(TRACE),
    CONSIDERING_JOIN_STRATEGY(DEBUG),
    REMEMBERING_BEST_ACCESS_PATH(DEBUG),
    NO_MORE_CONGLOMERATES(INFO),
    CONSIDERING_CONGLOMERATE(DEBUG),
    SCANNING_HEAP_FULL_MATCH_ON_UNIQUE_KEY(TRACE),
    ADDING_UNORDERED_OPTIMIZABLE(TRACE),
    CHANGING_ACCESS_PATH_FOR_TABLE(TRACE),
    TABLE_LOCK_NO_START_STOP(TRACE),
    NON_COVERING_INDEX_COST(TRACE),
    ROW_LOCK_ALL_CONSTANT_START_STOP(TRACE),
    ESTIMATING_COST_OF_CONGLOMERATE(DEBUG),
    LOOKING_FOR_SPECIFIED_INDEX(DEBUG),
    MATCH_SINGLE_ROW_COST(TRACE),
    COST_INCLUDING_EXTRA_1ST_COL_SELECTIVITY(TRACE),
    CALLING_NEXT_ACCESS_PATH(TRACE),
    TABLE_LOCK_OVER_THRESHOLD(WARN),
    ROW_LOCK_UNDER_THRESHOLD(WARN),
    COST_INCLUDING_EXTRA_START_STOP(TRACE),
    COST_INCLUDING_EXTRA_QUALIFIER_SELECTIVITY(TRACE),
    COST_INCLUDING_EXTRA_NONQUALIFIER_SELECTIVITY(TRACE),
    COST_OF_NONCOVERING_INDEX(TRACE),
    REMEMBERING_JOIN_STRATEGY(DEBUG),
    REMEMBERING_BEST_ACCESS_PATH_SUBSTRING(TRACE),
    REMEMBERING_BEST_SORT_AVOIDANCE_ACCESS_PATH_SUBSTRING(TRACE),
    REMEMBERING_BEST_UNKNOWN_ACCESS_PATH_SUBSTRING(TRACE),
    COST_OF_CONGLOMERATE_SCAN1(TRACE),
    COST_OF_CONGLOMERATE_SCAN3(TRACE),
    COST_OF_CONGLOMERATE_SCAN4(TRACE),
    COST_OF_CONGLOMERATE_SCAN5(TRACE),
    COST_OF_CONGLOMERATE_SCAN6(TRACE),
    COST_OF_CONGLOMERATE_SCAN7(TRACE),
    COST_INCLUDING_COMPOSITE_SEL_FROM_STATS(TRACE),
    COMPOSITE_SEL_FROM_STATS(TRACE),
    COST_INCLUDING_STATS_FOR_INDEX(TRACE),
    INFEASIBLE_JOIN(DEBUG),
    SPARSE_INDEX_NOT_ELIGIBLE(TRACE),
    HJ_NO_EQUIJOIN_COLUMNS(DEBUG),
    REWIND_JOINORDER(TRACE),
    PULL_OPTIMIZABLE(TRACE);


    private final OptimizerTrace.TraceLevel level;

    OptimizerFlag(OptimizerTrace.TraceLevel level){
        this.level=level;
    }

    public OptimizerTrace.TraceLevel level(){
        return level;
    }
}
