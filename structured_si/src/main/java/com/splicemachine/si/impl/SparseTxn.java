package com.splicemachine.si.impl;

import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.ByteSlice;

import java.io.IOException;
import java.util.Collection;

/**
 * Represents a Txn where some (optional) fields are populated.
 *
 * @author Scott Fines
 * Date: 6/30/14
 */
public class SparseTxn{

		private final long txnId;
		private final long beginTimestamp;
		private final long parentTxnId;
		private final long commitTimestamp;
		private final long globalCommitTimestamp;

		private final boolean hasDependentField;
		private final boolean dependent;
		private final boolean hasAdditiveField;
		private final boolean additive;

		private final Txn.IsolationLevel isolationLevel;
		private final Txn.State state;

		private final ByteSlice destTableBuffer;


		public SparseTxn(long txnId,
										 long beginTimestamp,
										 long parentTxnId,
										 long commitTimestamp,
										 long globalCommitTimestamp,
										 boolean hasDependentField, boolean dependent,
										 boolean hasAdditiveField, boolean additive,
										 Txn.IsolationLevel isolationLevel,
										 Txn.State state,
										 ByteSlice destTableBuffer) {
				this.txnId = txnId;
				this.beginTimestamp = beginTimestamp;
				this.parentTxnId = parentTxnId;
				this.commitTimestamp = commitTimestamp;
				this.globalCommitTimestamp = globalCommitTimestamp;
				this.hasDependentField = hasDependentField;
				this.dependent = dependent;
				this.hasAdditiveField = hasAdditiveField;
				this.additive = additive;
				this.isolationLevel = isolationLevel;
				this.state = state;
				this.destTableBuffer = destTableBuffer;
		}

		public ByteSlice getDestinationTableBuffer(){ return destTableBuffer;}
		public long getTxnId() { return txnId; }
		public long getBeginTimestamp() { return beginTimestamp; }
		public long getParentTxnId() { return parentTxnId; }

		/**
		 * @return the commit timestamp for this transaction, or -1 if the transaction has
		 * not been committed.
		 */
		public long getCommitTimestamp() { return commitTimestamp; }
		/**
		 * @return the "global" commit timestamp for this transaction, or -1 if the transaction
		 * has not been "globally" committed. If this transaction is a dependent child transaction,
		 * the global commit timestamp is equivalent to the commit timestamp of the highest ancestor, otherwise
		 * it is the same as the commit timestamp itself.
		 */
		public long getGlobalCommitTimestamp() { return globalCommitTimestamp; }

		public boolean hasDependentField() { return hasDependentField; }
		public boolean isDependent() { return dependent; }

		public boolean hasAdditiveField() { return hasAdditiveField; }
		public boolean isAdditive() { return additive; }

		/**
		 * @return the isolation level for this transaction, or {@code null} if no isolation level is explicitly set.
		 */
		public Txn.IsolationLevel getIsolationLevel() { return isolationLevel; }

		/**
		 * @return the current state of this transaction.
		 */
		public Txn.State getState() { return state; }

    public void encodeForNetwork(MultiFieldEncoder encoder,
                                  boolean addKaTime,
                                  boolean addDestinationTables){
        encoder.encodeNext(getTxnId())
                .encodeNext(getParentTxnId())
                .encodeNext(getBeginTimestamp());
        Txn.IsolationLevel level = getIsolationLevel();
        if(level==null)encoder.encodeEmpty();
        else encoder.encodeNext(level.encode());

        if(hasDependentField())
            encoder.encodeNext(isDependent());
        else
            encoder.encodeEmpty();
        if(hasAdditiveField())
            encoder.encodeNext(isAdditive());
        else
            encoder.encodeEmpty();

        long commitTs = getCommitTimestamp();
        if(commitTs>=0)
            encoder.encodeNext(commitTs);
        else encoder.encodeEmpty();
        long globalCommitTs = getGlobalCommitTimestamp();
        if(globalCommitTs>=0)
            encoder.encodeNext(globalCommitTs);
        else encoder.encodeEmpty();
        encoder.encodeNext(getState().getId());

        if(addKaTime)
            addKaTime(encoder);
//        if(addKaTime){
//            if(transaction instanceof DenseTxn){
//                encoder.encodeNext(((DenseTxn) transaction).getLastKATime());
//            }
//        }

        if(addDestinationTables)
            encoder.setRawBytes(getDestinationTableBuffer());

    }

    protected void addKaTime(MultiFieldEncoder encoder) {
        //no-op for sparse
    }

    public static SparseTxn decodeFromNetwork(byte[] packedTxn) {
        return decodeFromNetwork(packedTxn,false);
    }
    public static SparseTxn decodeFromNetwork(byte[] packedTxn,boolean addKaTime) {
        MultiFieldDecoder decoder = MultiFieldDecoder.wrap(packedTxn);
        long txnId = decoder.decodeNextLong();
        long parentTxnId = -1l;
        if(decoder.nextIsNull()) decoder.skip();
        else  parentTxnId = decoder.decodeNextLong();

        long beginTs = decoder.decodeNextLong();
        Txn.IsolationLevel level = Txn.IsolationLevel.fromByte(decoder.decodeNextByte());
        boolean dependent = decoder.decodeNextBoolean();
        boolean additive = decoder.decodeNextBoolean();
        long commitTs = -1l;
        if(decoder.nextIsNull()) decoder.skip();
        else commitTs = decoder.decodeNextLong();

        long globalCommitTs = -1l;
        if(decoder.nextIsNull()) decoder.skip();
        else globalCommitTs = decoder.decodeNextLong();

        Txn.State state = Txn.State.fromByte(decoder.decodeNextByte());
        long lastKaTime;
        if(addKaTime)
            lastKaTime = decoder.decodeNextLong();
        else
            lastKaTime = System.currentTimeMillis();

        int destTableOffset = decoder.offset();
        int length=0;
        while(decoder.available()){
            length+=decoder.skip()-1;
        }
        ByteSlice destTable = new ByteSlice();
        destTable.set(decoder.array(),destTableOffset,length);
        if(addKaTime)
            return new DenseTxn(txnId,beginTs,
                    parentTxnId,
                    commitTs,globalCommitTs,
                    true,dependent,
                    true,additive,
                    level,state, destTable, lastKaTime);
        else
            return new SparseTxn(txnId,beginTs,
                    parentTxnId,
                    commitTs,globalCommitTs,
                    true,dependent,
                    true,additive,
                    level,state, destTable);
    }
}
