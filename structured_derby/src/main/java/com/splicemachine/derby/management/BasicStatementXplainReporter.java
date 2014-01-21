package com.splicemachine.derby.management;

import com.carrotsearch.hppc.BitSet;
import com.google.common.cache.*;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.sql.catalog.SpliceXplainUtils;
import com.splicemachine.derby.utils.ErrorState;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.hbase.writer.CallBufferFactory;
import com.splicemachine.hbase.writer.KVPair;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author Scott Fines
 * Date: 1/21/14
 */
public class BasicStatementXplainReporter implements StatementXplainReporter{
		private static final Logger LOG = Logger.getLogger(BasicStatementXplainReporter.class);
		private final LoadingCache<String,CallBuffer<KVPair>> schemaCache;

		public BasicStatementXplainReporter() {
				schemaCache = CacheBuilder.newBuilder()
								.expireAfterAccess(60l, TimeUnit.SECONDS)
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
												long conglomId = getConglomerate(schema);
												CallBufferFactory<KVPair> nonThreadSafeBufferFactory = SpliceDriver.driver().getTableWriter();
												return nonThreadSafeBufferFactory.writeBuffer(
																Bytes.toBytes(Long.toString(conglomId)),
																SpliceConstants.NA_TRANSACTION_ID,
																10);
										}
								});
		}

		@Override
		public void reportStatement(String xplainSchema, StatementInfo info) {
				try {
						CallBuffer<KVPair> xplainTable = schemaCache.get(xplainSchema);
						DataHash<StatementInfo> keyHash = new KeyHash();
						keyHash.setRow(info);

						DataHash<StatementInfo> rowHash = new StatementHash();
						rowHash.setRow(info);

						xplainTable.add(new KVPair(keyHash.encode(),rowHash.encode()));
				} catch (ExecutionException e) {
						LOG.warn("Error detected when attempting to report statement information", e);
				} catch (Exception e) {
						LOG.warn("Error detected when attempting to report statement information",e);
				}
		}

/*protected helper methods*/
		protected static long getConglomerate(String schema) throws SQLException {
				Connection dbConn = SpliceXplainUtils.getDefaultConn();
				PreparedStatement s = null;
				ResultSet resultSet = null;
				try{
						s = dbConn.prepareStatement("select conglomeratenumber from " +
										"sys.systables t, sys.sysschemas s,sys.sysconglomerates c " +
										"where " +
										"        t.schemaid = s.schemaid and s.schemaname = '?'" +
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
						scalarFields.set(6,11);
						BitSet floatFields = new BitSet(0);
						BitSet doubleFields = new BitSet(0);
						EntryEncoder entryEncoder = EntryEncoder.create(SpliceDriver.getKryoPool(),12,
										fields,scalarFields,floatFields,doubleFields);
						MultiFieldEncoder mfe = entryEncoder.getEntryEncoder();
						mfe.encodeNext(statementInfo.getStatementUuid())
										.encodeNext(InetAddress.getLocalHost().getHostName())
										.encodeNext(statementInfo.getUser())
										.encodeNext(statementInfo.getTxnId())
										.encodeNext(statementInfo.status())
										.encodeNext(statementInfo.getSql())
										.encodeNext(statementInfo.getNumJobs())
										.encodeNext(statementInfo.numFailedJobs())
										.encodeNext(statementInfo.numCancelledJobs())
										.encodeNext(statementInfo.getStartTimeMs())
										.encodeNext(statementInfo.getStopTimeMs());
						return entryEncoder.encode();
				}

				@Override
				public KeyHashDecoder getDecoder() {
						return null; //don't need it for our purposes
				}
		}
}
