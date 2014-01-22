package com.splicemachine.derby.management;

import com.carrotsearch.hppc.BitSet;
import com.google.common.cache.*;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.ErrorState;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.hbase.writer.CallBufferFactory;
import com.splicemachine.hbase.writer.ConcurrentWriteBuffer;
import com.splicemachine.hbase.writer.KVPair;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.api.TransactorControl;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.storage.EntryEncoder;
import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.*;

/**
 * @author Scott Fines
 * Date: 1/21/14
 */
public class BasicStatementXplainReporter implements StatementXplainReporter{
		private static final Logger LOG = Logger.getLogger(BasicStatementXplainReporter.class);
		private final LoadingCache<Long,CallBuffer<KVPair>> bufferCache;
		private final LoadingCache<String,Long> conglomIdCache;
		private final ThreadPoolExecutor writers;

		private final ScheduledExecutorService cleanupTask;

		public BasicStatementXplainReporter() {
				bufferCache = CacheBuilder.newBuilder()
								.expireAfterAccess(120l, TimeUnit.SECONDS)
								.maximumSize(100)
								.removalListener(new RemovalListener<Long, CallBuffer<KVPair>>() {
										@Override
										public void onRemoval(
														RemovalNotification<Long, CallBuffer<KVPair>> notification) {
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
								}).build(new CacheLoader<Long, CallBuffer<KVPair>>() {
										@Override
										public CallBuffer<KVPair> load(Long conglomId) throws Exception {
												//TODO -sf- deal with transactions correctly
												CallBufferFactory<KVPair> nonThreadSafeBufferFactory = SpliceDriver.driver().getTableWriter();
												return new ConcurrentWriteBuffer(10,nonThreadSafeBufferFactory.writeBuffer(
																Bytes.toBytes(Long.toString(conglomId)),
																SpliceConstants.NA_TRANSACTION_ID,
																10));
										}
								});

				conglomIdCache = CacheBuilder.newBuilder()
								.expireAfterAccess(120l,TimeUnit.SECONDS)
								.maximumSize(100)
								.build(new CacheLoader<String, Long>() {
										@Override
										public Long load(String schema) throws Exception {
												return getConglomerate(schema);
										}
								});
				ThreadFactory factory = new ThreadFactoryBuilder().setDaemon(true).build();
				cleanupTask = Executors.newSingleThreadScheduledExecutor(factory);
				cleanupTask.schedule(new Runnable() {
						@Override
						public void run() {
								bufferCache.cleanUp();
								for(CallBuffer<KVPair> buffer: bufferCache.asMap().values()){
										try {
												buffer.flushBuffer();
										} catch (Exception e) {
												LOG.warn("Error encountered attempting to flush statement history data",e);
										}
								}
						}
				},1l,TimeUnit.SECONDS);

				this.writers = new ThreadPoolExecutor(2,
								2,
								60l,
								TimeUnit.SECONDS,
								new LinkedBlockingQueue<Runnable>(),new ThreadFactoryBuilder().setDaemon(true).build());
				writers.allowCoreThreadTimeOut(true);
		}

		@Override
		public void reportStatement(String xplainSchema, StatementInfo info) {
			writers.submit(new ReporterTask(xplainSchema,info));
		}

		/*protected helper methods*/
		private class ReporterTask implements Runnable{
				private final String xplainSchema;
				private final StatementInfo statementInfo;

				private ReporterTask(String xplainSchema, StatementInfo statementInfo) {
						this.xplainSchema = xplainSchema;
						this.statementInfo = statementInfo;
				}

				@Override
				public void run() {
						Long conglomId = null;
						try {
								conglomId =conglomIdCache.get(xplainSchema);
								CallBuffer<KVPair> xplainTable = bufferCache.get(conglomId);
								DataHash<StatementInfo> keyHash = new KeyHash();
								keyHash.setRow(statementInfo);

								DataHash<StatementInfo> rowHash = new StatementHash();
								rowHash.setRow(statementInfo);

								xplainTable.add(new KVPair(keyHash.encode(),rowHash.encode()));
						} catch (Exception e) {
								if(conglomId!=null)
										bufferCache.invalidate(conglomId);
								LOG.warn("Error detected when attempting to report statement information",e);
						}
				}
		}

		protected static long getConglomerate(String schema) throws SQLException {
				Connection dbConn = SpliceDriver.driver().getInternalConnection();

				PreparedStatement s = null;
				ResultSet resultSet = null;
				try{
						s = dbConn.prepareStatement("select conglomeratenumber from " +
										"sys.systables t, sys.sysschemas s,sys.sysconglomerates c " +
										"where " +
										"        t.schemaid = s.schemaid and s.schemaname = ?" +
										"        and t.tableid = c.tableid" +
										"        and t.tablename = 'SYSXPLAIN_STATEMENTHISTORY'");
						s.setString(1,schema);
						resultSet = s.executeQuery();
						if(resultSet.next()){
								return resultSet.getLong(1);
						}
						throw PublicAPI.wrapStandardException(ErrorState.TABLE_NOT_FOUND.newException());
				}finally{
						if(resultSet!=null)
								resultSet.close();
						if(s!=null)
								s.close();
				}
		}

		private static class KeyHash implements DataHash<StatementInfo>{
				private StatementInfo statementInfo;
				private MultiFieldEncoder encoder;

				@Override public void setRow(StatementInfo rowToEncode) { this.statementInfo = rowToEncode; }

				@Override
				public byte[] encode() throws StandardException, IOException {
						if(encoder==null)
								encoder = MultiFieldEncoder.create(SpliceDriver.getKryoPool(),3);
						else
							encoder.reset();
						String localhost = InetAddress.getLocalHost().getHostName();
						return encoder.encodeNext(statementInfo.getStatementUuid())
										.encodeNext(localhost)
										.encodeNext(statementInfo.getUser()).build();
				}

				@Override
				public KeyHashDecoder getDecoder() {
						//don't need it for our purposes
						return null;
				}
		}

		private class StatementHash implements DataHash<StatementInfo> {
				private StatementInfo statementInfo;

				@Override public void setRow(StatementInfo rowToEncode) { this.statementInfo = rowToEncode; }

				@Override
				public byte[] encode() throws StandardException, IOException {
						BitSet fields = new BitSet(12);
						fields.set(0,12);
						BitSet scalarFields = new BitSet(12);
						scalarFields.set(0);
						scalarFields.set(3);
						scalarFields.set(6,12);
						BitSet floatFields = new BitSet(0);
						BitSet doubleFields = new BitSet(0);
						EntryEncoder entryEncoder = EntryEncoder.create(SpliceDriver.getKryoPool(),12,
										fields,scalarFields,floatFields,doubleFields);
						MultiFieldEncoder mfe = entryEncoder.getEntryEncoder();
						mfe.encodeNext(statementInfo.getStatementUuid()) //0 - long
										.encodeNext(InetAddress.getLocalHost().getHostName().trim()) //1 - varchar
										.encodeNext(statementInfo.getUser().trim()) //2 - varchar
										.encodeNext(Long.parseLong(statementInfo.getTxnId())) //3 - long
										.encodeNext(statementInfo.status()) //4 - varchar
										.encodeNext(statementInfo.getSql()) //5 - varchar
										.encodeNext(statementInfo.getNumJobs()) //6 - int
										.encodeNext(statementInfo.numSuccessfulJobs())
										.encodeNext(statementInfo.numFailedJobs()) //7 - int
										.encodeNext(statementInfo.numCancelledJobs()) //8 - int
										.encodeNext(statementInfo.getStartTimeMs()) //9 - long
										.encodeNext(statementInfo.getStopTimeMs()); //10 -long
						return entryEncoder.encode();
				}

				@Override
				public KeyHashDecoder getDecoder() {
						return null; //don't need it for our purposes
				}
		}
}
