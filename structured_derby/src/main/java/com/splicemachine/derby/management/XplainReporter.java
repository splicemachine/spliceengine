package com.splicemachine.derby.management;

import com.google.common.cache.*;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.writer.*;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.api.TransactorControl;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.storage.EntryEncoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.*;

/**
 * @author Scott Fines
 * Date: 1/22/14
 */
public abstract class XplainReporter<T> {
		private static final Logger LOG = Logger.getLogger(XplainReporter.class);
		private final ExecutorService writers;
		private final LinkedBlockingQueue<Pair<String,T>> taskQueue;
		private final LoadingCache<String,CallBuffer<KVPair>> bufferCache;

		public XplainReporter(int numWorkers) {
				ThreadFactory factory = new ThreadFactoryBuilder().setDaemon(true).build();
				this.writers = Executors.newFixedThreadPool(numWorkers,factory);
				this.taskQueue = new LinkedBlockingQueue<Pair<String, T>>();
				TransactorControl transactorControl = HTransactorFactory.getTransactorControl();
				final String txnId;
				try {
						TransactionId transactionId = transactorControl.beginTransaction();
						txnId = transactionId.getTransactionIdString();
						transactorControl.commit(transactionId);
				} catch (IOException e) {
						throw new RuntimeException(e); //TODO -sf- do something about this
				}
				bufferCache = CacheBuilder.newBuilder()
								.expireAfterAccess(120l, TimeUnit.SECONDS)
								.maximumSize(100)
								.removalListener(new RemovalListener<String, CallBuffer<KVPair>>() {
										@Override
										public void onRemoval(
														RemovalNotification<String, CallBuffer<KVPair>> notification) {
												try {
														CallBuffer<KVPair> callBuffer = notification.getValue();
														if (callBuffer != null) {
																callBuffer.flushBuffer();
																callBuffer.close();
														}

												} catch (IOException e) {
														LOG.info("Error closing buffer from cache", e);
												} catch (Exception e) {
														LOG.info("Error closing buffer from cache", e);
												}
										}
								}).build(new CacheLoader<String, CallBuffer<KVPair>>() {
										@Override
										public CallBuffer<KVPair> load(String schema) throws Exception {
												//TODO -sf- deal with transactions correctly
												long conglomId = getConglomerateId(schema);
												CallBufferFactory<KVPair> nonThreadSafeBufferFactory = SpliceDriver.driver().getTableWriter();
												return new ConcurrentWriteBuffer(10, nonThreadSafeBufferFactory.writeBuffer(
																Bytes.toBytes(Long.toString(conglomId)),
																txnId,
																10));
										}
								});
		}

		public void start(int numWorkers){
				for(int i=0;i<numWorkers;i++){
						writers.submit(new Writer(getKeyHash(),getDataHash()));
				}
		}

		public void report(String xplainSchema,T info){
				taskQueue.add(Pair.newPair(xplainSchema,info));
		}

		protected abstract DataHash<T> getDataHash();

		protected abstract DataHash<T> getKeyHash();

		protected abstract long getConglomerateId(String schemaName) throws SQLException;

		protected static abstract class WriteableHash<T> implements DataHash<T>{
				protected T element;

				@Override public void setRow(T rowToEncode) { this.element = rowToEncode;	 }
				@Override public KeyHashDecoder getDecoder() { return null; }

				protected abstract void doEncode(MultiFieldEncoder encoder, T element);
		}

		protected static abstract class EntryWriteableHash<T> extends WriteableHash<T>{
				private EntryEncoder entryEncoder;

				@Override
				public final byte[] encode() throws StandardException, IOException {
						if(entryEncoder==null)
								entryEncoder = buildEncoder();

						MultiFieldEncoder fieldEncoder = entryEncoder.getEntryEncoder();
						fieldEncoder.reset();
						doEncode(fieldEncoder,element);
						return entryEncoder.encode();
				}

				protected abstract void doEncode(MultiFieldEncoder encoder, StatementInfo statementInfo);

				protected abstract EntryEncoder buildEncoder();
		}

		protected  static abstract class KeyWriteableHash<T> extends WriteableHash<T>{
				private MultiFieldEncoder entryEncoder;

				@Override
				public final byte[] encode() throws StandardException, IOException {
						if(entryEncoder==null)
								entryEncoder = MultiFieldEncoder.create(SpliceDriver.getKryoPool(),getNumFields());
						else
							entryEncoder.reset();

						doEncode(entryEncoder,element);
						return entryEncoder.build();
				}

				protected abstract int getNumFields();
		}

		private class Writer implements Runnable{
				private final DataHash<T> keyHash;
				private final DataHash<T> dataHash;

				public Writer(DataHash<T> keyHash, DataHash<T> dataHash) {

						this.keyHash = keyHash;
						this.dataHash = dataHash;
				}

				@Override
				public void run() {
						while(true){
								try {
										Pair<String,T> next = taskQueue.poll(5, TimeUnit.SECONDS);
										if(next==null){
												cleanUpCache();
										}else{
												try {
														CallBuffer<KVPair> buffer = bufferCache.get(next.getFirst());
														keyHash.setRow(next.getSecond());
														dataHash.setRow(next.getSecond());
														buffer.add(new KVPair(keyHash.encode(),dataHash.encode()));
												}catch (Exception e) {
														LOG.info("Error reporting data", e);
												}
										}
								} catch (InterruptedException e) {
										Thread.currentThread().interrupt();
										return;
								}
						}
				}

				protected void cleanUpCache() {
						//nothing happened in the last few seconds, so cleanup the cache
						bufferCache.cleanUp();
						for(CallBuffer<KVPair> buffer:bufferCache.asMap().values()){
								try{
										//TODO -sf- remove buffers that have been inactive for too long
										buffer.flushBuffer();
								}catch(Exception e){
										LOG.info("Error attempting to report data",e);
								}
						}
				}
		}
}
