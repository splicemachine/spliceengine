package com.splicemachine.derby.stream.utils;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.store.access.BaseSpliceTransaction;
import com.splicemachine.derby.stream.control.HregionDataSetProcessor;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.control.ControlDataSetProcessor;
import com.splicemachine.derby.stream.spark.SparkDataSetProcessor;
import com.splicemachine.si.api.TxnView;
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
}
