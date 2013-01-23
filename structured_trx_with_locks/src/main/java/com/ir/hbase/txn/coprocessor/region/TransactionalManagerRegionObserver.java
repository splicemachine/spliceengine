package com.ir.hbase.txn.coprocessor.region;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.log4j.Logger;
import com.ir.constants.TxnConstants;
import com.ir.constants.TxnConstants.TableEnv;
import com.ir.hbase.txn.logger.TxnLogger;

public class TransactionalManagerRegionObserver extends BaseTxnRegionObserver {
	private static Logger LOG = Logger.getLogger(TransactionalManagerRegionObserver.class);
	private boolean tableEnvMatch = false;

	@Override
	public void start(CoprocessorEnvironment e) throws IOException {
		if (LOG.isInfoEnabled())
			LOG.info("Starting the CoProcessor " + TransactionalManagerRegionObserver.class);
		tableEnvMatch = TxnUtils.getTableEnv((RegionCoprocessorEnvironment) e).equals(TableEnv.TRANSACTION_TABLE);
		if(!tableEnvMatch) {
			super.start(e);
			return;
		}
		initZooKeeper(e);
	}

	@Override
	public void stop(CoprocessorEnvironment e) throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("Stopping the CoProcessor " + TransactionalManagerRegionObserver.class);
		super.stop(e);
	}
	
	@Override
	public void preGet(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<KeyValue> results) throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("preGet in CoProcessor " + TransactionalManagerRegionObserver.class);
		if (tableEnvMatch && Bytes.equals(get.getRow(), TxnConstants.INITIALIZE_TRANSACTION_ID_BYTES)) {
			results.add(new KeyValue(Bytes.toBytes(TxnUtils.beginTransaction(this.transactionPath, zkw)), HConstants.LATEST_TIMESTAMP));
			e.bypass();
			e.complete();
		}
	}

	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> e,Put put, WALEdit edit, boolean writeToWAL) throws IOException {	
		if (LOG.isDebugEnabled())
			LOG.debug("prePut in CoProcessor " + TransactionalManagerRegionObserver.class);
		if (tableEnvMatch) {
			if (put.has(TxnConstants.TRANSACTION_TABLE_PREPARE_FAMILY_BYTES, TxnConstants.TRANSACTION_QUALIFIER)) {
				if (LOG.isTraceEnabled())
					LOG.trace("Prepare Commit transaction: " + Bytes.toString(put.getRow()));
				TxnUtils.prepareCommit(Bytes.toString(put.getRow()), rzk);
				TxnLogger.logTxnTableRegionsToZk(regionLogPath, zkw, e.getEnvironment().getConfiguration());
			} else if (put.has(TxnConstants.TRANSACTION_TABLE_DO_FAMILY_BYTES, TxnConstants.TRANSACTION_QUALIFIER)) {
				if (LOG.isTraceEnabled())
					LOG.trace("Do Commit transaction: " + Bytes.toString(put.getRow()));
				TxnUtils.doCommit(Bytes.toString(put.getRow()), rzk);				
			} else if (put.has(TxnConstants.TRANSACTION_TABLE_ABORT_FAMILY_BYTES, TxnConstants.TRANSACTION_QUALIFIER)) {
				if (LOG.isTraceEnabled())
					LOG.trace("Abort transaction: " + Bytes.toString(put.getRow()));				
				TxnUtils.abort(Bytes.toString(put.getRow()), rzk);
			}
			e.bypass();
			e.complete();
		}
	}

	@Override
	public void preSplit(ObserverContext<RegionCoprocessorEnvironment> e) {
		if (LOG.isDebugEnabled())
			LOG.debug("Splitting ");
		super.preSplit(e);
	}
	
}
