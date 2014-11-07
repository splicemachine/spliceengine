package com.splicemachine.si.impl.txnclient;

import com.carrotsearch.hppc.LongOpenHashSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.ZeroCopyLiteralByteString;
import com.splicemachine.annotations.ThreadSafe;
import com.google.common.primitives.Longs;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.encoding.DecodingIterator;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.si.api.*;
import com.splicemachine.hbase.table.SpliceRpcController;
import com.splicemachine.si.api.TimestampSource;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnStore;
import com.splicemachine.si.api.TxnSupplier;
import com.splicemachine.si.coprocessor.TxnMessage;
import com.splicemachine.si.impl.InheritingTxnView;
import com.splicemachine.si.impl.TxnUtils;
import com.splicemachine.annotations.ThreadSafe;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;

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
				HTableInterface table = tableFactory.createHTableInterface(SpliceConstants.config, SIConstants.TRANSACTION_TABLE_BYTES);
				try{
						byte[] rowKey = getTransactionRowKey(txn.getTxnId());
            TxnMessage.TxnLifecycleService service= getLifecycleService(table, rowKey);

            SpliceRpcController controller = new SpliceRpcController();
            TxnMessage.TxnInfo.Builder request = TxnMessage.TxnInfo.newBuilder()
                    .setTxnId(txn.getTxnId())
                    .setAllowsWrites(txn.allowsWrites())
                    .setIsAdditive(txn.isAdditive())
                    .setBeginTs(txn.getBeginTimestamp())
                    .setIsolationLevel(txn.getIsolationLevel().encode());
            if(!Txn.ROOT_TRANSACTION.equals(txn.getParentTxnView())){
                request = request.setParentTxnid(txn.getParentTxnId());
            }
            Iterator<ByteSlice> destinationTables = txn.getDestinationTables();
            List<byte[]> bytes = null;
            while(destinationTables.hasNext()){
                if(bytes==null)
                    bytes = Lists.newArrayList();
                bytes.add(destinationTables.next().getByteCopy());
            }
            if(bytes!=null){
                MultiFieldEncoder encoder = MultiFieldEncoder.create(bytes.size());
                for(int i=0;i<bytes.size();i++){
                    encoder = encoder.encodeNextUnsorted(bytes.get(i));
                }
                ByteString bs = ZeroCopyLiteralByteString.wrap(encoder.build());
                request = request.setDestinationTables(bs);
            }
            service.beginTransaction(controller,request.build(),new BlockingRpcCallback<TxnMessage.VoidResponse>());
            dealWithError(controller);
            txnsCreated.incrementAndGet();
				}finally{
						table.close();
				}
		}



    @Override
		public void rollback(long txnId) throws IOException {
				HTableInterface table = tableFactory.createHTableInterface(SpliceConstants.config, SIConstants.TRANSACTION_TABLE_BYTES);
				try{
						byte[] rowKey = getTransactionRowKey(txnId);
            TxnMessage.TxnLifecycleService service = getLifecycleService(table, rowKey);
            TxnMessage.TxnLifecycleMessage lifecycle = TxnMessage.TxnLifecycleMessage.newBuilder()
                    .setTxnId(txnId).setAction(TxnMessage.LifecycleAction.ROLLBACk).build();

            SpliceRpcController controller = new SpliceRpcController();
            BlockingRpcCallback<TxnMessage.ActionResponse> done = new BlockingRpcCallback<TxnMessage.ActionResponse>();
            service.lifecycleAction(controller,lifecycle, done);
            dealWithError(controller);
            rollbacks.incrementAndGet();
				}finally{
						table.close();
				}
		}

		@Override
		public long commit(long txnId) throws IOException {
				HTableInterface table = tableFactory.createHTableInterface(SpliceConstants.config, SIConstants.TRANSACTION_TABLE_BYTES);
				try{
						byte[] rowKey = getTransactionRowKey(txnId);
            TxnMessage.TxnLifecycleService service = getLifecycleService(table, rowKey);
            TxnMessage.TxnLifecycleMessage lifecycle = TxnMessage.TxnLifecycleMessage.newBuilder()
                    .setTxnId(txnId).setAction(TxnMessage.LifecycleAction.COMMIT).build();

            SpliceRpcController controller = new SpliceRpcController();
            BlockingRpcCallback<TxnMessage.ActionResponse> done = new BlockingRpcCallback<TxnMessage.ActionResponse>();
            service.lifecycleAction(controller,lifecycle, done);
            dealWithError(controller);
            commits.incrementAndGet();
            return done.get().getCommitTs();
				}finally{
						table.close();
				}
		}

		@Override
		public boolean keepAlive(long txnId) throws IOException {
				HTableInterface table = tableFactory.createHTableInterface(SpliceConstants.config, SIConstants.TRANSACTION_TABLE_BYTES);
				try{
						byte[] rowKey = getTransactionRowKey(txnId);TxnMessage.TxnLifecycleService service = getLifecycleService(table, rowKey);

            TxnMessage.TxnLifecycleMessage lifecycle = TxnMessage.TxnLifecycleMessage.newBuilder()
                    .setTxnId(txnId).setAction(TxnMessage.LifecycleAction.KEEPALIVE).build();

            SpliceRpcController controller = new SpliceRpcController();
            BlockingRpcCallback<TxnMessage.ActionResponse> done = new BlockingRpcCallback<TxnMessage.ActionResponse>();
            service.lifecycleAction(controller,lifecycle, done);
            dealWithError(controller);
            return done.get().getContinue();
				}finally{
						table.close();
				}
		}

		@Override
		public void elevateTransaction(Txn txn, byte[] newDestinationTable) throws IOException {
				HTableInterface table = tableFactory.createHTableInterface(SpliceConstants.config, SIConstants.TRANSACTION_TABLE_BYTES);
				try{
						byte[] rowKey = getTransactionRowKey(txn.getTxnId());
            TxnMessage.TxnLifecycleService service = getLifecycleService(table, rowKey);
            TxnMessage.ElevateRequest elevateRequest = TxnMessage.ElevateRequest.newBuilder()
                    .setTxnId(txn.getTxnId())
                    .setNewDestinationTable(ZeroCopyLiteralByteString.wrap(newDestinationTable)).build();

            SpliceRpcController controller = new SpliceRpcController();
            service.elevateTransaction(controller,elevateRequest,new BlockingRpcCallback<TxnMessage.VoidResponse>());
            dealWithError(controller);
            elevations.incrementAndGet();
				}finally{
						table.close();
				}
		}

		@Override
		public long[] getActiveTransactionIds(Txn txn, byte[] table) throws IOException {
				return getActiveTransactionIds(timestampSource.retrieveTimestamp(), txn.getTxnId(), table);
		}

		@Override
		public long[] getActiveTransactionIds(final long minTxnId, final long maxTxnId, final byte[] writeTable) throws IOException {
				HTableInterface table = tableFactory.createHTableInterface(SpliceConstants.config,SIConstants.TRANSACTION_TABLE_BYTES);
				try{
            TxnMessage.ActiveTxnRequest.Builder requestBuilder = TxnMessage.ActiveTxnRequest
                    .newBuilder().setStartTxnId(minTxnId).setEndTxnId(maxTxnId);
            if(writeTable!=null)
                requestBuilder = requestBuilder.setDestinationTables(ZeroCopyLiteralByteString.wrap(writeTable));

            final TxnMessage.ActiveTxnRequest request = requestBuilder.build();
            Map<byte[], TxnMessage.ActiveTxnIdResponse> data = table.coprocessorService(TxnMessage.TxnLifecycleService.class,
                    HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, new Batch.Call<TxnMessage.TxnLifecycleService, TxnMessage.ActiveTxnIdResponse>() {
                @Override
                public TxnMessage.ActiveTxnIdResponse call(TxnMessage.TxnLifecycleService instance) throws IOException {
                    SpliceRpcController controller = new SpliceRpcController();
                    BlockingRpcCallback<TxnMessage.ActiveTxnIdResponse> response = new BlockingRpcCallback<TxnMessage.ActiveTxnIdResponse>();

                    instance.getActiveTransactionIds(controller, request, response);
                    dealWithError(controller);
                    return response.get();
                }
            });

						LongOpenHashSet txns = LongOpenHashSet.newInstance(); //TODO -sf- do we really need to check for duplicates? In case of Transaction table splits?
						for(TxnMessage.ActiveTxnIdResponse response:data.values()){
                int activeTxnIdsCount = response.getActiveTxnIdsCount();
                for(int i=0;i< activeTxnIdsCount;i++){
                   txns.add(response.getActiveTxnIds(i));
                }
						}
						long[] finalTxns = txns.toArray();
						Arrays.sort(finalTxns);
						return finalTxns;

				} catch (Throwable throwable) {
						throw new IOException(throwable);
				} finally{
						table.close();
				}
		}

    @Override
    public List<TxnView> getActiveTransactions(final long minTxnid, final long maxTxnId, final byte[] activeTable) throws IOException {
        HTableInterface table = tableFactory.createHTableInterface(SpliceConstants.config,SIConstants.TRANSACTION_TABLE_BYTES);
        try{
            TxnMessage.ActiveTxnRequest.Builder requestBuilder = TxnMessage.ActiveTxnRequest
                    .newBuilder().setStartTxnId(minTxnid).setEndTxnId(maxTxnId);
            if(activeTable!=null)
                requestBuilder = requestBuilder.setDestinationTables(ZeroCopyLiteralByteString.wrap(activeTable));

            final TxnMessage.ActiveTxnRequest request = requestBuilder.build();
            Map<byte[], TxnMessage.ActiveTxnResponse> data = table.coprocessorService(TxnMessage.TxnLifecycleService.class,
                    HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, new Batch.Call<TxnMessage.TxnLifecycleService, TxnMessage.ActiveTxnResponse>() {
                @Override
                public TxnMessage.ActiveTxnResponse call(TxnMessage.TxnLifecycleService instance) throws IOException {
                    SpliceRpcController controller = new SpliceRpcController();
                    BlockingRpcCallback<TxnMessage.ActiveTxnResponse> response = new BlockingRpcCallback<TxnMessage.ActiveTxnResponse>();

                    instance.getActiveTransactions(controller, request, response);
                    dealWithError(controller);
                    return response.get();
                }
            });

            List<TxnView> txns = Lists.newArrayList();

            for(TxnMessage.ActiveTxnResponse response:data.values()){
                int size = response.getTxnsCount();
                for(int i=0;i<size;i++){
                    txns.add(decode(response.getTxns(i)));
                }
            }
            Collections.sort(txns,new Comparator<TxnView>() {
                @Override
                public int compare(TxnView o1, TxnView o2) {
                    if(o1==null){
                        if(o2==null) return 0;
                        else return -1;
                    }else if (o2==null) return 1;
                    return Longs.compare(o1.getTxnId(), o2.getTxnId());
                }
            });
            return txns;

        } catch (Throwable throwable) {
            throw new IOException(throwable);
        } finally{
            table.close();
        }
    }

    @Override
		public TxnView getTransaction(long txnId) throws IOException {
			return getTransaction(txnId,false);
		}

		@Override
		public TxnView getTransaction(long txnId, boolean getDestinationTables) throws IOException {
        lookups.incrementAndGet(); //we are performing a lookup, so increment the counter
				HTableInterface table =
								tableFactory.createHTableInterface(SpliceConstants.config,
												SIConstants.TRANSACTION_TABLE_BYTES);
				try{
						byte[] rowKey = getTransactionRowKey(txnId);
            TxnMessage.TxnLifecycleService service = getLifecycleService(table, rowKey);
            TxnMessage.TxnRequest request = TxnMessage.TxnRequest.newBuilder().setTxnId(txnId).build();
            SpliceRpcController controller = new SpliceRpcController();
            BlockingRpcCallback<TxnMessage.Txn> done = new BlockingRpcCallback<TxnMessage.Txn>();
            service.getTransaction(controller,request, done);
            dealWithError(controller);
            TxnMessage.Txn messageTxn= done.get();
            return decode(messageTxn);
				}finally{
						table.close();
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
		private TxnView decode(TxnMessage.Txn message) throws IOException {
        TxnMessage.TxnInfo info = message.getInfo();
        if(info.getTxnId()<0) return null; //we didn't find it

				long txnId = info.getTxnId();
				long parentTxnId = info.getParentTxnid();
				long beginTs = info.getBeginTs();

				Txn.IsolationLevel isolationLevel = Txn.IsolationLevel.fromInt(info.getIsolationLevel());

				boolean hasAdditive = info.hasIsAdditive();
				boolean additive = hasAdditive && info.getIsAdditive();

				long commitTs = message.getCommitTs();
				long globalCommitTs = message.getGlobalCommitTs();

				Txn.State state = Txn.State.fromInt(message.getState());

        final Iterator<ByteSlice> destinationTables;
        if(info.hasDestinationTables()){
            ByteString bs = info.getDestinationTables();
            MultiFieldDecoder decoder = MultiFieldDecoder.wrap(bs.toByteArray());
            destinationTables = new DecodingIterator(decoder) {
                @Override
                protected void advance(MultiFieldDecoder decoder) {
                  decoder.skip();  
                }
            };
        }else
            destinationTables = Iterators.emptyIterator();

        long kaTime = -1l;
        if(message.hasLastKeepAliveTime())
            kaTime = message.getLastKeepAliveTime();

        Iterator<ByteSlice> destTablesIterator = new Iterator<ByteSlice>(){

            @Override
            public boolean hasNext() {
                return destinationTables.hasNext();
            }

            @Override
            public ByteSlice next() {
                ByteSlice dSlice = destinationTables.next();
                byte[] data= Encoding.decodeBytesUnsortd(dSlice.array(),dSlice.offset(),dSlice.length());
                dSlice.set(data);
                return dSlice;
            }

            @Override public void remove() { throw new UnsupportedOperationException(); }
        };

        TxnView parentTxn = parentTxnId<0? Txn.ROOT_TRANSACTION : cache.getTransaction(parentTxnId);
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

    private TxnMessage.TxnLifecycleService getLifecycleService(HTableInterface table, byte[] rowKey) throws IOException {
        TxnMessage.TxnLifecycleService service;CoprocessorRpcChannel coprocessorRpcChannel = table.coprocessorService(rowKey);
        try {
            service = ProtobufUtil.newServiceStub(TxnMessage.TxnLifecycleService.class, coprocessorRpcChannel);
        } catch (Exception e) {
            throw new IOException(e);
        }
        return service;
    }

    private void dealWithError(SpliceRpcController controller) throws IOException{
        if(!controller.failed()) return; //nothing to worry about
        Throwable t = controller.getThrowable();
        if(t instanceof IOException)
            throw (IOException)t;
        else throw new IOException(t);

    }		
}