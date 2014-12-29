package com.splicemachine.si.impl.txnclient;

import com.carrotsearch.hppc.LongOpenHashSet;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.encoding.DecodingIterator;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.si.api.*;
import com.splicemachine.si.coprocessors.TxnLifecycleProtocol;
import com.splicemachine.si.impl.InheritingTxnView;
import com.splicemachine.si.impl.TxnUtils;
import com.splicemachine.annotations.ThreadSafe;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.client.coprocessor.Batch;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Transaction Store which uses the TxnLifecycleEndpoint to manage and access transactions
 * remotely.
 *
 * This class has no local cache. Callers are responsible for caching returned transactions
 * safely.
 *
 * @author Scott Fines
 * Date: 6/27/14
 */
@ThreadSafe
public class CoprocessorTxnStore implements TxnStore{
		private final HTableInterfaceFactory tableFactory;
		private TxnSupplier cache; //a transaction store which uses a global cache for us
		@ThreadSafe private final  TimestampSource timestampSource;

    /*monitoring fields*/
    private final AtomicLong lookups = new AtomicLong(0l);
    private final AtomicLong elevations = new AtomicLong(0l);
    private final AtomicLong txnsCreated = new AtomicLong(0l);
    private final AtomicLong rollbacks = new AtomicLong(0l);
    private final AtomicLong commits = new AtomicLong(0l);

		public CoprocessorTxnStore(HTableInterfaceFactory tableFactory,
															 TimestampSource timestampSource,
															 @ThreadSafe TxnSupplier txnCache) {
				this.tableFactory = tableFactory;
				if(txnCache==null)
						this.cache = this; //set itself to be the cache--not actually a cache, but just in case
				else
						this.cache = txnCache;
				this.timestampSource = timestampSource;
		}

		@Override
		public void recordNewTransaction(Txn txn) throws IOException {
            try (HTableInterface table = tableFactory.createHTableInterface(SpliceConstants.config, SIConstants.TRANSACTION_TABLE_BYTES)) {
                byte[] rowKey = TxnUtils.getRowKey(txn.getTxnId());
                TxnLifecycleProtocol txnLifecycleProtocol = table.coprocessorProxy(TxnLifecycleProtocol.class, rowKey);
                txnLifecycleProtocol.recordTransaction(txn.getTxnId(), encode(txn));
                txnsCreated.incrementAndGet(); //only record stats if the record succeeds
            }
		}


		@Override
		public void rollback(long txnId) throws IOException {
            try (HTableInterface table = tableFactory.createHTableInterface(SpliceConstants.config, SIConstants.TRANSACTION_TABLE_BYTES)) {
                byte[] rowKey = getTransactionRowKey(txnId);
                TxnLifecycleProtocol txnLifecycleProtocol = table.coprocessorProxy(TxnLifecycleProtocol.class, rowKey);
                txnLifecycleProtocol.rollback(txnId);
                rollbacks.incrementAndGet();
            }
		}

		@Override
		public long commit(long txnId) throws IOException {
            try (HTableInterface table = tableFactory.createHTableInterface(SpliceConstants.config, SIConstants.TRANSACTION_TABLE_BYTES)) {
                byte[] rowKey = getTransactionRowKey(txnId);
                TxnLifecycleProtocol txnLifecycleProtocol = table.coprocessorProxy(TxnLifecycleProtocol.class, rowKey);
                long commit = txnLifecycleProtocol.commit(txnId);
                commits.incrementAndGet();
                return commit;
            }
		}

		@Override
		public boolean keepAlive(long txnId) throws IOException {
            try (HTableInterface table = tableFactory.createHTableInterface(SpliceConstants.config, SIConstants.TRANSACTION_TABLE_BYTES)) {
                byte[] rowKey = getTransactionRowKey(txnId);
                TxnLifecycleProtocol txnLifecycleProtocol = table.coprocessorProxy(TxnLifecycleProtocol.class, rowKey);
                return txnLifecycleProtocol.keepAlive(txnId);
            }
		}

		@Override
		public void elevateTransaction(Txn txn, byte[] newDestinationTable) throws IOException {
            try (HTableInterface table = tableFactory.createHTableInterface(SpliceConstants.config, SIConstants.TRANSACTION_TABLE_BYTES)) {
                byte[] rowKey = getTransactionRowKey(txn.getTxnId());
                TxnLifecycleProtocol txnLifecycleProtocol = table.coprocessorProxy(TxnLifecycleProtocol.class, rowKey);
                txnLifecycleProtocol.elevateTransaction(txn.getTxnId(), newDestinationTable);
                elevations.incrementAndGet();
            }
		}

		@Override
		public long[] getActiveTransactionIds(Txn txn, byte[] table) throws IOException {
				return getActiveTransactionIds(timestampSource.retrieveTimestamp(), txn.getTxnId(), table);
		}

		@Override
		public long[] getActiveTransactionIds(final long minTxnId, final long maxTxnId, final byte[] writeTable) throws IOException {
            try (HTableInterface table = tableFactory.createHTableInterface(SpliceConstants.config, SIConstants.TRANSACTION_TABLE_BYTES)) {
                Map<byte[], byte[]> data = table.coprocessorExec(TxnLifecycleProtocol.class, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, new Batch.Call<TxnLifecycleProtocol, byte[]>() {
                    @Override
                    public byte[] call(TxnLifecycleProtocol instance) throws IOException {
                        return instance.getActiveTransactionIds(minTxnId, maxTxnId, writeTable);
                    }
                });

                LongOpenHashSet txns = LongOpenHashSet.newInstance(); //TODO -sf- do we really need to check for duplicates? In case of Transaction table splits?
                MultiFieldDecoder decoder = MultiFieldDecoder.create();
                for (byte[] packed : data.values()) {
                    decoder.set(packed);
                    while (decoder.available())
                        txns.add(decoder.decodeNextLong());
                }
                long[] finalTxns = txns.toArray();
                Arrays.sort(finalTxns);
                return finalTxns;

            } catch (Throwable throwable) {
                throw new IOException(throwable);
            }
		}

    @Override
    public List<TxnView> getActiveTransactions(final long minTxnid, final long maxTxnId, final byte[] activeTable) throws IOException {
        try (HTableInterface table = tableFactory.createHTableInterface(SpliceConstants.config, SIConstants.TRANSACTION_TABLE_BYTES)) {
            Map<byte[], List<byte[]>> data = table.coprocessorExec(TxnLifecycleProtocol.class, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW,
                    new Batch.Call<TxnLifecycleProtocol, List<byte[]>>() {
                        @Override
                        public List<byte[]> call(TxnLifecycleProtocol instance) throws IOException {
                            return instance.getActiveTransactions(minTxnid, maxTxnId, activeTable);
                        }
                    });

            List<TxnView> txns = Lists.newArrayList();
            MultiFieldDecoder txnDecoder = MultiFieldDecoder.create();

            for (List<byte[]> packed : data.values()) {
                for (byte[] bytes : packed) {
                    txnDecoder.set(bytes);
                    txns.add(decode(txnDecoder, true));
                }
            }
            Collections.sort(txns, new Comparator<TxnView>() {
                @Override
                public int compare(TxnView o1, TxnView o2) {
                    if (o1 == null) {
                        if (o2 == null) return 0;
                        else return -1;
                    } else if (o2 == null) return 1;
                    return Longs.compare(o1.getTxnId(), o2.getTxnId());
                }
            });
            return txns;

        } catch (Throwable throwable) {
            throw new IOException(throwable);
        }
    }

    @Override
		public TxnView getTransaction(long txnId) throws IOException {
			return getTransaction(txnId,false);
		}

		@Override
		public TxnView getTransaction(long txnId, boolean getDestinationTables) throws IOException {
        lookups.incrementAndGet(); //we are performing a lookup, so increment the counter
            try (HTableInterface table = tableFactory.createHTableInterface(SpliceConstants.config,
                    SIConstants.TRANSACTION_TABLE_BYTES)) {
                byte[] rowKey = TxnUtils.getRowKey(txnId);
                TxnLifecycleProtocol txnLifecycleProtocol = table.coprocessorProxy(TxnLifecycleProtocol.class, rowKey);

                byte[] transaction = txnLifecycleProtocol.getTransaction(txnId, getDestinationTables);
                if (transaction == null || transaction.length <= 0) return null; //no transaction found
                return decode(transaction);
            }
		}

    /*caching methods--since we don't have a cache, these are no-ops*/
		@Override public boolean transactionCached(long txnId) { return false; }
		@Override public void cache(TxnView toCache) {  }
    @Override public TxnView getTransactionFromCache(long txnId) { return null; }

    /**
     * Set the underlying transaction cache to use.
     *
     * This allows us to provide a transaction cache when looking up parent transactions,
     * which will reduce some of the cost of looking up a transaction.
     *
     * @param cacheStore the caching store to use
     */
    public void setCache(TxnSupplier cacheStore) { this.cache = cacheStore; }

    /*monitoring methods*/
    public long lookupCount() { return lookups.get(); }
    public long elevationCount() { return elevations.get(); }
    public long createdCount() { return txnsCreated.get(); }
    public long rollbackCount() { return rollbacks.get(); }
    public long commitCount() { return commits.get(); }

    /******************************************************************************************************************/
		/*private helper methods*/
		private TxnView decode(byte[] txnPackedBytes) throws IOException {
				assert txnPackedBytes.length>0: "No transaction found";
				MultiFieldDecoder decoder = MultiFieldDecoder.wrap(txnPackedBytes);
        return decode(decoder, false);
		}

    private TxnView decode(MultiFieldDecoder decoder,boolean hasKaTime) throws IOException {
        long txnId = decoder.decodeNextLong();
        long parentTxnId = -1l;
        if(decoder.nextIsNull()) decoder.skip();
        else parentTxnId = decoder.decodeNextLong();

        long beginTs = decoder.decodeNextLong();

        Txn.IsolationLevel isolationLevel = null;
        if(decoder.nextIsNull()) decoder.skip();
        else isolationLevel = Txn.IsolationLevel.fromByte(decoder.decodeNextByte());

        boolean hasAdditive = false;
        boolean additive = false;
        if(decoder.nextIsNull()) decoder.skip();
        else{
            hasAdditive = true;
            additive = decoder.decodeNextBoolean();
        }

        long commitTs = -1l;
        long globalCommitTs = -1l;
        if(decoder.nextIsNull()) decoder.skip();
        else commitTs = decoder.decodeNextLong();

        if(decoder.nextIsNull()) decoder.skip();
        else globalCommitTs = decoder.decodeNextLong();

        Txn.State state = Txn.State.fromByte(decoder.decodeNextByte());

        long kaTime = -1l;
        if(hasKaTime)
            kaTime = decoder.decodeNextLong();

        final Iterator<ByteSlice> destinationTables = new DecodingIterator(decoder) {
            @Override
            protected void advance(MultiFieldDecoder decoder) {
                decoder.skip();
            }
        };
        final ByteSlice slice = new ByteSlice();
        Iterator<ByteSlice> destTablesIterator = new Iterator<ByteSlice>(){

            @Override
            public boolean hasNext() {
                if(!destinationTables.hasNext()) return false;
                return false;
            }

            @Override
            public ByteSlice next() {
                ByteSlice dSlice = destinationTables.next();
                byte[] data= Encoding.decodeBytesUnsortd(dSlice.array(),dSlice.offset(),dSlice.length());
                slice.set(data);
                return slice;
            }

            @Override public void remove() { throw new UnsupportedOperationException(); }
        };


        TxnView parentTxn = cache.getTransaction(parentTxnId);
        return new InheritingTxnView(parentTxn,txnId,beginTs,
                isolationLevel,
                hasAdditive,additive,
                true,true,
                commitTs,globalCommitTs,
                state,destTablesIterator,kaTime);
    }

    private byte[] encode(Txn txn) {
        List<ByteSlice> destinationTables = Lists.newArrayList(txn.getDestinationTables());
				MultiFieldEncoder encoder = MultiFieldEncoder.create(8 + destinationTables.size());
				encoder.encodeNext(txn.getTxnId());

				TxnView parentTxn = txn.getParentTxnView();
				if(parentTxn!=null &&parentTxn.getTxnId()>=0)
						encoder = encoder.encodeNext(parentTxn.getTxnId());
				else encoder.encodeEmpty();

				encoder.encodeNext(txn.getBeginTimestamp())
								.encodeNext(txn.getIsolationLevel().encode())
								.encodeNext(txn.isAdditive());
				if(txn.getState()== Txn.State.COMMITTED){
						encoder = encoder.encodeNext(txn.getCommitTimestamp());
				}else{
						encoder.encodeEmpty();
				}
        /*
         * We only use this method if we are recording a new transaction. Because of that, we leave
         * off the effectiveCommitTimestamp(). Likely, we wouldn't use it anyway, because we don't have one
         * yet, but on the off chance that we do, we'll let the Transaction Resolver on the coprocessor
         * side handle it.
         *
         * However, we need this in place because we use the same encoding/decoding strategy in multiple
         * places, so we have to adhere to the same policy
         */
        encoder.encodeEmpty();

				encoder.encodeNext(txn.getState().getId());


				//encode the destination tables
				for(ByteSlice destTable:destinationTables){
						encoder = encoder.encodeNextUnsorted(destTable);
				}

				return encoder.build();
		}


    private byte[] getTransactionRowKey(long txnId) {
        byte[] newRowKey = new byte[10];
        newRowKey[0] = (byte)(txnId & (SpliceConstants.TRANSACTION_TABLE_BUCKET_COUNT-1)); //assign the bucket
        BytesUtil.longToBytes(txnId, newRowKey, 2);
        return newRowKey;
    }
}
