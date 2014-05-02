package com.splicemachine.si.coprocessors;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.regionserver.WrongRegionException;
import org.apache.log4j.Logger;

import com.splicemachine.si.api.TransactionStatus;
import com.splicemachine.si.data.hbase.RollForwardable;
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.si.impl.RollForwardAction;
import com.splicemachine.si.impl.Tracer;
import com.splicemachine.si.impl.Transaction;
import com.splicemachine.si.impl.TransactionStore;
import com.splicemachine.utils.Provider;

/**
 * @author Scott Fines
 *         Date: 2/13/14
 */
public class RegionRollForwardAction<Table> implements RollForwardAction {
    private static Logger LOG = Logger.getLogger(RegionRollForwardAction.class);
    private final RollForwardable region;
    private final Provider<TransactionStore> transactionStoreProvider;
    private final Provider<DataStore> dataStoreProvider;

    public RegionRollForwardAction(Table region,
                                   Provider<TransactionStore> transactionStoreProvider,
                                   Provider<DataStore> dataStoreProvider) {
        this.region = (RollForwardable) region;
        this.transactionStoreProvider = transactionStoreProvider;
        this.dataStoreProvider = dataStoreProvider;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Boolean rollForward(long transactionId, List<byte[]> rowList) throws IOException {
        final Transaction transaction = transactionStoreProvider.get().getTransaction(transactionId);
        TransactionStatus status = transaction.getEffectiveStatus();
        final Boolean isFinished = status.isFinished();
        boolean isCommitted = status.isCommitted();
        if (isFinished) {
            for (byte[] row : rowList) {
                try {
                    if (region.checkRegionForRow(row)) {
                        if (isCommitted) {
                            dataStoreProvider.get().setCommitTimestamp(region, row, transaction.getLongTransactionId(),
                                                                       transaction.getEffectiveCommitTimestamp());
                        } else if (status.equals(TransactionStatus.ERROR) || status.equals(TransactionStatus.ROLLED_BACK)) {

                            dataStoreProvider.get().setCommitTimestampToFail(region, row,
                                                                             transaction.getLongTransactionId());
                        } else {
                            LOG.warn("Transaction is finished but it's neither committed nor failed: " + transaction);
                        }
                        Tracer.traceRowRollForward(row);
                    }
                } catch (NotServingRegionException e) {
                    // If the region split and the row is not here, then just skip it
                } catch (WrongRegionException e) {
                    // If the region split and the row is not here, then just skip it
                }
            }
        }
        Tracer.traceTransactionRollForward(transactionId);
        return isFinished;
    }
}
