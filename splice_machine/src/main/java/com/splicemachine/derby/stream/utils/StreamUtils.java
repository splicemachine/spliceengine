/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.stream.utils;

import java.util.Map;

import splice.com.google.common.collect.ImmutableMap;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.derby.stream.iapi.ScopeNamed;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

/**
 * Created by jleach on 4/16/15.
 */
public class StreamUtils {

    private static final Logger LOG = Logger.getLogger(StreamUtils.class);

    private static final Map<String, String> mapFxnNameToPrettyName =
        new ImmutableMap.Builder<String, String>()
            .put("AggregateFinisherFunction","Finish Aggregation")
            .put("AntiJoinFunction", "Execute Anti Join")
            .put("AntiJoinRestrictionFlatMapFunction", "Create Flat Map for Anti Join with Restriction")
            .put("BroadcastJoinFlatMapFunction", "Create Flat Map for Broadcast Join")
            .put("CoGroupAntiJoinRestrictionFlatMapFunction","Cogroup Flat Map for Anti Join with Restriction")
            .put("CoGroupBroadcastJoinRestrictionFlatMapFunction", "Cogroup Flat Map for Broadcast Join with Restriction")
            .put("CoGroupInnerJoinRestrictionFlatMapFunction", "Cogroup Flat Map for Inner Join with Restriction")
            .put("CoGroupOuterJoinRestrictionFlatMapFunction", "Cogroup Flat Map for Inner Join with Restriction")
            .put("CountJoinedLeftFunction", "Count Left Side Rows Joined")
            .put("CountJoinedRightFunction", "Count Right Side Rows Joined")
            .put("CountProducedFunction", "Count Rows Produced")
            .put("CountReadFunction", "Count Rows Read")
            .put("ExportFunction", "Export Rows")
            .put("FetchWithOffsetFunction", "Fetch With Offset")
            .put("FileFunction", "Parse CSV File")
            .put("GroupedAggregateRollupFlatMapFunction", "Create Flat Map for Grouped Aggregate Rollup")
            .put("HTableScanTupleFunction", "Deserialize Key-Values")
            .put("IndexToBaseRowFilterPredicateFunction", "Map Index to Base Row using Filter")
            .put("IndexToBaseRowFlatMapFunction", "Create Flat Map for Index to Base Row using Flat Map")
            .put("IndexTransformFunction", "Transform Index")
            .put("InnerJoinFunction", "Execute Inner Join")
            .put("InnerJoinRestrictionFlatMapFunction", "Create Flat Map for Inner Join with Restriction")
            .put("InnerJoinRestrictionFunction", "Execute Inner Join with Restriction")
            .put("JoinRestrictionPredicateFunction", "Execute Join with Restriction")
            .put("KeyerFunction", "Prepare Keys")
            .put("LocatedRowToRowLocationFunction", "Determine Row Location")
            .put("MergeAllAggregatesFunction", "Merge All Aggregates")
            .put("MergeAntiJoinFlatMapFunctionFunction", "Create Flat Map for Merge Anti Join")
            .put("MergeInnerJoinFlatMapFunctionFunction", "Create Flat Map for Merge Inner Join")
            .put("MergeNonDistinctAggregatesFunction", "Merge Non Distinct Aggregates")
            .put("MergeOuterJoinFlatMapFunction", "Create Flat Map for Merge Outer Join")
            .put("MergeWindowFunction", "Execute Window Function Logic")
            .put("NLJAntiJoinFunction", "Execute Nested Loop Anti Join")
            .put("NLJInnerJoinFunction", "Execute Nested Loop Inner Join")
            .put("NLJOneRowInnerJoinFunction", "Execute Nested Loop One Row Inner Join")
            .put("NLJOuterJoinFunction", "Execute Nested Loop Outer Join")
            .put("NormalizeFunction", "Normalize Rows")
            .put("OffsetFunction", "Offset Rows")
            .put("ProjectRestrictFlatMapFunction", "Create Flat Map for Project Restrict")
            .put("RowOperationFunction", "Locate Single Row")
            .put("RowTransformFunction", "Transform Row")
            .put("ScalarAggregateFunction", "Aggregate Scalar Values")
            .put("ScrollInsensitiveFunction", "Set and Count Current Located Row")
            .put("SetCurrentLocatedRowFunction", "Set Current Located Row")
            .put("SparkCompactionFunction", "Compact Files")
            .put("StreamFileFunction", "Parse CSV File")
            .put("SubtractByKeyBroadcastJoinFunction", "Subtract by Key for Broadcast Join")
            .put("TableScanTupleFunction", "Deserialize Key-Values")
            .put("TakeFunction", "Fetch Limited Rows")
            .put("WindowFinisherFunction", "Finish Window")
            .put("WindowFlatMapFunction", "Create Flat Map for Window Function")
            .build();
    
    public static String getPrettyFunctionName(String simpleClassName) {
        String s = mapFxnNameToPrettyName.get(simpleClassName);
        if (s == null) {
            return StringUtils.join(
                StringUtils.splitByCharacterTypeCamelCase(simpleClassName.replace("Function", "")), ' ');
        } else {
            return s;
        }
    }

    public static String getScopeString(Object obj) {
        String scope = null;
        if (obj instanceof String)
            scope = (String)obj;
        else if (obj instanceof ScopeNamed)
            scope = ((ScopeNamed)obj).getScopeName();
        else if (obj instanceof ConstantAction)
            scope = StringUtils.join(
                StringUtils.splitByCharacterTypeCamelCase(
                    obj.getClass().getSimpleName().
                        replace("ConstantAction", "").
                        replace("ConstantOperation", "")),
                ' ');
        else
            scope = obj.getClass().getSimpleName();
        return scope;
    }
}
