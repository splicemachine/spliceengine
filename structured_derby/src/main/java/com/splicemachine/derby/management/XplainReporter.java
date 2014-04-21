package com.splicemachine.derby.management;

import com.google.common.cache.*;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.ErrorState;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.writer.*;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.api.TransactionManager;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.storage.EntryEncoder;
import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @author Scott Fines
 * Date: 1/22/14
 */
public abstract class XplainReporter<T> {
		private final Logger LOG;
		private final ExecutorService writers;
		private final LinkedBlockingQueue<Pair<String,T>> taskQueue;
		private final LoadingCache<String,CallBuffer<KVPair>> bufferCache;

		public XplainReporter(final String tableName,int numWorkers) {
				this.LOG = Logger.getLogger(this.getClass());
				ThreadFactory factory = new ThreadFactoryBuilder().setDaemon(true).build();
				this.writers = Executors.newFixedThreadPool(numWorkers,factory);
				this.taskQueue = new LinkedBlockingQueue<Pair<String, T>>();
				TransactionManager transactionManager = HTransactorFactory.getTransactionManager();
				final String txnId;
				try {
						TransactionId transactionId = transactionManager.beginTransaction();
						txnId = transactionId.getTransactionIdString();
						transactionManager.commit(transactionId);
				} catch (IOException e) {
						throw new RuntimeException(e); //TODO -sf- do something about this
				}
				bufferCache = CacheBuilder.newBuilder()
								.expireAfterWrite(120l, TimeUnit.SECONDS)
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
												long conglomId = getConglomerateId(schema,tableName);
												CallBufferFactory<KVPair> nonThreadSafeBufferFactory = SpliceDriver.driver().getTableWriter();
												return new ConcurrentWriteBuffer(10, nonThreadSafeBufferFactory.writeBuffer(
                                                    TableName.valueOf(Long.toString(conglomId)),
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

		protected long getConglomerateId(String schemaName,String tableName) throws SQLException{
				Connection dbConn = SpliceDriver.driver().getInternalConnection();

				PreparedStatement s = null;
				ResultSet resultSet = null;
				try{
						s = dbConn.prepareStatement("select conglomeratenumber from " +
										"sys.systables t, sys.sysschemas s,sys.sysconglomerates c " +
										"where " +
										"        t.schemaid = s.schemaid and s.schemaname = ?" +
										"        and t.tableid = c.tableid" +
										"        and t.tablename = ?");
						s.setString(1,schemaName);
						s.setString(2,tableName);
						resultSet = s.executeQuery();
						if(resultSet.next()){
								return resultSet.getLong(1);
						}
						throw PublicAPI.wrapStandardException(ErrorState.TABLE_NOT_FOUND.newException(tableName));
				}finally{
						if(resultSet!=null)
								resultSet.close();
						if(s!=null)
								s.close();
				}
		}

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
						doEncode(fieldEncoder, element);
						return entryEncoder.encode();
				}

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
												CallBuffer<KVPair> buffer = null;
												for(int i=0;i<5;i++){
														try {
																buffer = bufferCache.get(next.getFirst());
														}catch (Exception e) {
																if(LOG.isDebugEnabled())
																		LOG.debug("Error reporting data", e);
																bufferCache.invalidate(next.getFirst());
														}
												}
												if(buffer==null){
														LOG.warn("Unable to obtain a buffer for "+XplainReporter.this.getClass().getSimpleName()+":"+next.getFirst()+", will not be able to report metrics");
														continue;
												}

												try{
														keyHash.setRow(next.getSecond());
														dataHash.setRow(next.getSecond());
														buffer.add(new KVPair(keyHash.encode(),dataHash.encode()));
												}catch (Exception e) {
														LOG.info("Unable to report information ");
														bufferCache.invalidate(next.getFirst());
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
						for(Map.Entry<String,CallBuffer<KVPair>> bufferEntry: bufferCache.asMap().entrySet()){
								try{
										//TODO -sf- remove buffers that have been inactive for too long
										bufferEntry.getValue().flushBuffer();
								}catch(Exception e){
										LOG.info("Error attempting to report data",e);
										bufferCache.invalidate(bufferEntry.getKey());
								}
						}
				}
		}
}
