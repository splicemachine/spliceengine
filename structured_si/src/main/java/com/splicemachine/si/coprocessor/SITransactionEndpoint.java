package com.splicemachine.si.coprocessor;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.splicemachine.constants.TxnConstants.TableEnv;
import com.splicemachine.si.utils.SIConstants;
import com.splicemachine.si.utils.SIUtils;
import com.splicemachine.utils.SpliceLogUtils;

public class SITransactionEndpoint extends BaseEndpointCoprocessor implements SITransactionProtocol {
	private static Logger LOG = Logger.getLogger(SITransactionEndpoint.class);
	protected Cache<Long,List<SIRegion>> transactionNotificationCache;	
	protected SIObserver siObserver;
	
	@Override
	public void start(CoprocessorEnvironment e) {
		SpliceLogUtils.trace(LOG, "starting %s",SITransactionEndpoint.class);
		if (SIUtils.getTableEnv((RegionCoprocessorEnvironment) e).equals(TableEnv.TRANSACTION_TABLE)) {
			transactionNotificationCache = CacheBuilder.newBuilder().maximumSize(50000).expireAfterWrite(100, TimeUnit.SECONDS).removalListener(new RemovalListener<Long,List<SIRegion>>() {
				@Override
				public void onRemoval(RemovalNotification<Long, List<SIRegion>> notification) {
					SpliceLogUtils.trace(LOG, "transaction %s removed from notification cache ",notification.getValue());
				}
			}).build();
		}
		siObserver = (SIObserver) ((RegionCoprocessorEnvironment)this.getEnvironment()).getRegion().getCoprocessorHost().findCoprocessor(SIObserver.class.getSimpleName());
		super.start(e);
	}

	@Override
	public void stop(CoprocessorEnvironment e) {
		SpliceLogUtils.trace(LOG, "stopping %s",SITransactionEndpoint.class);
		super.stop(e);
	}

	@Override
	public void registerTXNCallback(long startTimestamp, byte[] beginKey, byte[] endKey) throws IOException {
		List<SIRegion> siRegions;
		if ((siRegions = transactionNotificationCache.getIfPresent(startTimestamp)) != null) {
			siRegions.add(new SIRegion(beginKey,endKey));
			transactionNotificationCache.put(startTimestamp, siRegions);
		} else {
			siRegions = Arrays.asList(new SIRegion(beginKey,endKey));
			transactionNotificationCache.put(startTimestamp, siRegions);
		}
	}

	@Override
	public void rollTXNForward(SITransactionResponse transactionResponse) throws IOException {
		List<byte[]> rowsInvolved = siObserver.getTransactionRowCallbackCache().getIfPresent(transactionResponse.getStartTimestamp());
		if (rowsInvolved != null) {
			HRegion region = ((RegionCoprocessorEnvironment)this.getEnvironment()).getRegion();
			switch(transactionResponse.getTransactionState()) {
			case ABORT:
			case ERROR:
				for (byte[] row: rowsInvolved) {
					Delete delete = new Delete(row);
					delete.setTimestamp(transactionResponse.getStartTimestamp());
					region.delete(delete, null, false);
				}
				break;
			case ACTIVE:
				break;
			case COMMIT:
				for (byte[] row: rowsInvolved) {
					Put put = new Put(row,transactionResponse.getStartTimestamp());
					put.add(SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES, SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN, Bytes.toBytes(transactionResponse.getCommitTimestamp()));
					try {
						region.put(put,false);
					} catch (Exception e) {
						e.printStackTrace();
						SpliceLogUtils.logAndThrowRuntime(LOG, e);
					}		
				}
				break;
			default:
				break;
			}
		}
		siObserver.getTransactionRowCallbackCache().invalidate(transactionResponse.getStartTimestamp());			
	}
	

}
