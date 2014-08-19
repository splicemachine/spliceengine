package com.splicemachine.si.impl.region;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.splicemachine.annotations.ThreadSafe;
import com.splicemachine.concurrent.MoreExecutors;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnSupplier;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.TxnUtils;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Asynchronous element for "resolving" transaction elements.
 *
 * This consists of two primary purposes: transaction timeouts and
 * global commits.
 *
 * <h3>Transaction Timeouts</h3>
 * Transactions are timed out if they remain active past a certain point
 * without an explicit keep-alive. This process requires some effort(albeit a minor amount).
 * We avoid that situation (as well as clarifying the data in the table) by explicitly setting
 * the transaction state to ROLLEDBACK.
 *
 * <h3>Global Commit Timestamps</h3>
 * When a dependent child transaction commits, it is not considered truly committed until every
 * dependent parent in the chain has been committed. Thus, in order to process whether a child
 * transaction is truly committed, we have to fetch its entire transaction hierarchy until we either
 * A) hit an active transaction or B) find the ROOT transaction. This is clearly very expensive.
 *
 * To avoid this expense, we have an additional field: the "global Commit Timestamp". This is the timestamp
 * at which the <em>highest dependent parent</em> of the child transaction has been committed. For example,
 * suppose that we have the following chain:
 *  T1: beginTimestamp=1,commitTimestamp=6
 *      T2: beginTimestamp=2,commitTimestamp=5
 *          T3: beginTimestamp=3,commitTimestamp=4
 *
 * In this example, T2 is a parent of T3, and T1 is a parent of T2. T3 then is not committed until T2 is, and T2
 * is not committed until T1 is. Thus, the "Global commit timestamp" of T3 is the commit timestamp of T1 (6), and
 * the "Global commit timestamp" of T2 is the commit timestamp of T1 (6).
 *
 * Whenever the global commit timestamp is present, then we know that the child transaction was committed, and has
 * an effective commit timestamp which is the same as the global commit timestamp.
 *
 * Unfortunately, there's no efficient mechanism which can construct the global commit timestamp on the fly. To avoid
 * this costly reconstruction each time, we add an asynchronous mechanism here. When, upon reading a transaction
 * entry, we detect a child transaction that has been committed, but does not have a global commit timestamp,
 * we submit it to this resolver to determine whether or not to add a commit timestamp. This resolver then
 * will asynchronously determine the proper global commit timestamp, and add it to the transaction entry
 * so that future calls will not need to resolve it.
 *
 * @author Scott Fines
 * Date: 8/19/14
 */
public class TransactionResolver {
    private static final Logger LOG = Logger.getLogger(TransactionResolver.class);
    @ThreadSafe private final TxnSupplier txnSupplier;

    private final RingBuffer<TxnResolveEvent> ringBuffer;
    private final Disruptor<TxnResolveEvent> disruptor;

    private final ThreadPoolExecutor consumerThreads;
    private volatile boolean stopped;

    public TransactionResolver(TxnSupplier txnSupplier, int numThreads, int bufferSize) {
        this.txnSupplier = txnSupplier;
        this.consumerThreads = MoreExecutors.namedThreadPool(numThreads, numThreads, "txn-resolve-%d", 60, true);
        this.consumerThreads.allowCoreThreadTimeOut(true);

        int bSize=1;
        while(bSize<bufferSize)
            bSize<<=1;

        disruptor = new Disruptor<TxnResolveEvent>(new EventFactory<TxnResolveEvent>() {
            @Override
            public TxnResolveEvent newInstance() {
                return new TxnResolveEvent();
            }
        },bSize,consumerThreads,
                ProducerType.MULTI,
                new BlockingWaitStrategy());
        disruptor.handleEventsWith(new ResolveEventHandler());
        ringBuffer = disruptor.getRingBuffer();
        disruptor.start();
    }

    public void resolveTimedOut(HRegion txnRegion,long txnId,boolean oldForm){
        if(stopped) return; //we aren't running, so do nothing
        long sequence;
        try{
            sequence = ringBuffer.tryNext();
        }catch(InsufficientCapacityException e){
            //just log a message if our capacity is too small
            if(LOG.isDebugEnabled())
                LOG.debug("Unable to submit for timeout resolution");
            return;
        }
        try{
            TxnResolveEvent event = ringBuffer.get(sequence);
            event.txnRegion = txnRegion;
            event.timedOut = true;
            event.txnId = txnId;
            event.oldForm = oldForm;
        }finally{
            ringBuffer.publish(sequence);
        }
    }

    public void resolveGlobalCommitTimestamp(HRegion txnRegion, long txn,boolean oldForm){
        if(stopped) return; //we aren't running, so do nothing
        long sequence;
        try{
            sequence = ringBuffer.tryNext();
        }catch(InsufficientCapacityException e){
            //just log a message if our capacity is too small
            if(LOG.isDebugEnabled())
                LOG.debug("Unable to submit for timeout resolution");
            return;
        }
        try{
            TxnResolveEvent event = ringBuffer.get(sequence);
            event.txnRegion = txnRegion;
            event.timedOut = false;
            event.txnId = txn;
            event.oldForm = oldForm;
        }finally{
            ringBuffer.publish(sequence);
        }
    }

    private static class TxnResolveEvent{
        private boolean timedOut;
        private long txnId;
        private HRegion txnRegion;
        private boolean oldForm;
    }


    private class ResolveEventHandler implements EventHandler<TxnResolveEvent> {
        @Override
        public void onEvent(TxnResolveEvent event, long sequence, boolean endOfBatch) throws Exception {
            if(event.timedOut){
                resolveTimeOut(event.txnRegion,event.txnId,event.oldForm);
            }else
                resolveCommit(event.txnRegion,event.txnId,event.oldForm);
        }
    }

    private void resolveCommit(HRegion txnRegion, long txnId,boolean oldForm) throws IOException {
        try{
            TxnView view = txnSupplier.getTransaction(txnId);
        /*
         * The logic necessary to acquire the transaction's global commit timestamp
         * is actually contained within the TxnView. We rely on the getEffectiveCommitTimestamp()
         * to properly obtain our global commit timestamp for us, then we just write it back to the proper
         * location.
         */
            long globalCommitTs = view.getEffectiveCommitTimestamp();
            if(globalCommitTs<0) return; //transaction isn't actually committed, so don't do it

            Put put = new Put(TxnUtils.getRowKey(txnId));
            if(oldForm)
                put.add(SIConstants.DEFAULT_FAMILY_BYTES,V1TxnDecoder.OLD_GLOBAL_COMMIT_TIMESTAMP_COLUMN, Bytes.toBytes(globalCommitTs));
            else
                put.add(SIConstants.DEFAULT_FAMILY_BYTES,V2TxnDecoder.GLOBAL_COMMIT_QUALIFIER_BYTES, Encoding.encode(globalCommitTs));
            //don't write to the WAL to avoid the write performance penalty
            put.setWriteToWAL(false);

            txnRegion.put(put);
        }catch(Exception e){
            logError(txnId, e);
        }
    }

    private void resolveTimeOut(HRegion txnRegion,long txnId,boolean oldForm) {
        try{
            Put put = new Put(TxnUtils.getRowKey(txnId));

            if(oldForm)
                put.add(SIConstants.DEFAULT_FAMILY_BYTES,V1TxnDecoder.OLD_STATUS_COLUMN, Txn.State.ROLLEDBACK.encode());
            else
                put.add(SIConstants.DEFAULT_FAMILY_BYTES,V2TxnDecoder.STATE_QUALIFIER_BYTES, Txn.State.ROLLEDBACK.encode());

            put.setWriteToWAL(false);

            txnRegion.put(put);
        }catch(Exception e){
            logError(txnId,e);
        }
    }

    private void logError(long txnId, Exception e) {
        if(LOG.isInfoEnabled()){
            /*
             * We don't want to print the whole stack trace all the time, just when we are interested in hearing
             * about it.
             */
            if(LOG.isDebugEnabled())
                LOG.info("Unable to resolve txn "+ txnId+" committed. Encountered error ",e);
            else
                LOG.info("Unable to resolve txn "+ txnId+" committed. Encountered error "+e.getMessage());
        }
    }
}
