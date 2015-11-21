package com.splicemachine.derby.stream.utils;

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.store.access.BaseSpliceTransaction;
import com.splicemachine.derby.stream.control.HregionDataSetProcessor;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.control.ControlDataSetProcessor;
import com.splicemachine.derby.stream.spark.SparkDataSetProcessor;
import com.splicemachine.si.api.TxnView;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

/**
 * Created by jleach on 4/16/15.
 */
public class StreamUtils {

    private static final Logger LOG = Logger.getLogger(StreamUtils.class);
    public static final DataSetProcessor controlDataSetProcessor = new ControlDataSetProcessor();
    public static final DataSetProcessor sparkDataSetProcessor = new SparkDataSetProcessor();
    public static final DataSetProcessor hregionDataSetProcessor = new HregionDataSetProcessor();
    private static final double CONTROL_SIDE_THRESHOLD = 10*1000*1000; // based on a TPCC1000 run on an 8 node cluster
    private static final double CONTROL_SIDE_ROWCOUNT_THRESHOLD = 1E6;

    public static DataSetProcessor getControlDataSetProcessor() {
        return controlDataSetProcessor;
    }

    public static DataSetProcessor getSparkDataSetProcessor() {
        return sparkDataSetProcessor;
    }

    public static <Op extends SpliceOperation> DataSetProcessor getDataSetProcessorFromActivation(Activation activation, SpliceOperation op) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("op estimated cost " + op.getEstimatedCost());
            if (activation.getResultSet() != null) {
               LOG.trace("resultset estimated cost " + ((SpliceOperation) activation.getResultSet()).getEstimatedCost());
            }
        }
        double estimatedCost = activation.getResultSet() == null ? op.getEstimatedCost() : ((SpliceOperation)activation.getResultSet()).getEstimatedCost();
        double estimatedRowCount = activation.getResultSet() == null ? op.getEstimatedRowCount() : ((SpliceOperation)activation.getResultSet()).getEstimatedRowCount();
        return (estimatedCost > CONTROL_SIDE_THRESHOLD || estimatedRowCount > CONTROL_SIDE_ROWCOUNT_THRESHOLD) ?sparkDataSetProcessor:controlDataSetProcessor;
    }

    public static <Op extends SpliceOperation> DataSetProcessor getLocalDataSetProcessorFromActivation(Activation activation, SpliceOperation op) {
        LOG.warn("op estimated cost " + op.getEstimatedCost());
        if (activation.getResultSet() != null) {
            LOG.warn("resultset estimated cost " + ((SpliceOperation) activation.getResultSet()).getEstimatedCost());
        }
        double estimatedCost = activation.getResultSet() == null ? op.getEstimatedCost() : ((SpliceOperation)activation.getResultSet()).getEstimatedCost();

        if (estimatedCost > CONTROL_SIDE_THRESHOLD) {
            return hregionDataSetProcessor;
        } else {
            return controlDataSetProcessor;
        }
    }

    public static DataSetProcessor getDataSetProcessor() {
        return controlDataSetProcessor;
    }

    public static void setupSparkJob(DataSetProcessor dsp, Activation activation, String description,
                                     String schedulePool) throws StandardException {
        // TODO (wjk): this is mostly a copy/paste from SpliceBaseOperation.openCore() - consolidate?
        String sql = activation.getPreparedStatement().getSource();
        long txnId = getCurrentTransaction(activation).getTxnId();
        sql = (sql == null) ? description : sql;
        String userId = activation.getLanguageConnectionContext().getCurrentUserId(activation);
        String jobName = userId + " <" + txnId + ">";
        dsp.setJobGroup(jobName,sql);
        dsp.setSchedulerPool(schedulePool);
    }

    private static TxnView getCurrentTransaction(Activation activation) throws StandardException {
        TransactionController transactionExecute = activation.getLanguageConnectionContext().getTransactionExecute();
        Transaction rawStoreXact = ((TransactionManager) transactionExecute).getRawStoreXact();
        return ((BaseSpliceTransaction) rawStoreXact).getActiveStateTxn();
    }
    
    private static final Map<String, String> mapFxnNameToPrettyName =
        new ImmutableMap.Builder<String, String>()
            .put("AggregateFinisherFunction", "Finish Aggregation")
            .put("AntiJoinFunction", "Execute Anti Join")
            .put("AntiJoinRestrictionFlatMapFunction", "Create Flat Map for Anti Join with Restriction")
            .put("BroadcastJoinFlatMapFunction", "Create Flat Map for Broadcast Join")
            .put("CoGroupAntiJoinRestrictionFlatMapFunction", "Cogroup Flat Map for Anti Join with Restriction")
            .put("CoGroupBroadcastJoinRestrictionFlatMapFunction", "Cogroup Flat Map for Broadcast Join with Restriction")
            .put("CoGroupInnerJoinRestrictionFlatMapFunction", "Cogroup Flat Map for Inner Join with Restriction")
            .put("CoGroupOuterJoinRestrictionFlatMapFunction", "Cogroup Flat Map for Inner Join with Restriction")
            .put("CoGroupPairFunction", "Cogroup Rows")
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
            .put("InsertPairFunction", "Insert Row")
            .put("JoinPairFlatMapFunction", "Prepare Pair Flat Map for Join")
            .put("JoinRestrictionPredicateFunction", "Execute Join with Restriction")
            .put("KeyerFunction", "Prepare Keys")
            .put("LeftOuterJoinPairFlatMapFunction", "Prepare Pair Flat Map for Left Outer Join")
            .put("LocatedRowToRowLocationFunction", "Determine Row Location")
            .put("MergeAllAggregatesFunction", "Merge All Aggregates")
            .put("MergeAntiJoinFlatMapFunctionFunction", "Create Flat Map for Merge Anti Join")
            .put("MergeInnerJoinFlatMapFunctionFunction", "Create Flat Map for Merge Inner Join")
            .put("MergeNonDistinctAggregatesFunction", "Merge Non Distinct Aggregates")
            .put("MergeOuterJoinFlatMapFunction", "Create Flat Map for Merge Outer Join")
            .put("MergeWindowFunction", "Merge Window")
            .put("NLJAntiJoinFunction", "Execute Nested Loop Anti Join")
            .put("NLJInnerJoinFunction", "Execute Nested Loop Inner Join")
            .put("NLJOneRowInnerJoinFunction", "Execute Nested Loop One Row Inner Join")
            .put("NLJOuterJoinFunction", "Execute Nested Loop Outer Join")
            .put("NormalizeFunction", "Normalize Rows")
            .put("OffsetFunction", "Offset Rows")
            .put("ProjectRestrictFlatMapFunction", "Create Flat Map for Project Restrict")
            .put("RightOuterJoinPairFlatMapFunction", "Create Pair Flat Map for Right Outer Join")
            .put("RowOperationFunction", "Locate Single Row")
            .put("RowTransformFunction", "Transform Row")
            .put("ScalarAggregateFunction", "Aggregate Scalar Values")
            .put("ScrollInsensitiveFunction", "Set and Count Current Located Row")
            .put("SetCurrentLocatedRowFunction", "Set Current Located Row")
            .put("StreamFileFunction", "Parse CSV File")
            .put("SubtractByKeyBroadcastJoinFunction", "Subtract by Key for Broadcast Join")
            .put("SubtractByKeyPairFlatMapFunction", "Create Pair Flat Map for Subtract by Key")
            .put("TableScanTupleFunction", "Deserialize Key-Values")
            .put("TakeFunction", "Fetch Limited Rows")
            .put("WindowFinisherFunction", "Finish Window")
            .put("WindowFlatMapFunction", "Create Flat Map for Window Function")
            .put("WriteIndexFunction", "Write Index")
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
}
