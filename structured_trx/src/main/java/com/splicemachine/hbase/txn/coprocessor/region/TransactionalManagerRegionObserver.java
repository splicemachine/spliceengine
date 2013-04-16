package com.splicemachine.hbase.txn.coprocessor.region;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.splicemachine.hbase.txn.TxnConstants;
import com.splicemachine.constants.TransactionConstants.TableEnv;
import com.splicemachine.hbase.txn.logger.TxnLogger;
import com.splicemachine.utils.SpliceLogUtils;

public class TransactionalManagerRegionObserver extends BaseTxnRegionObserver {
	private static Logger LOG = Logger.getLogger(TransactionalManagerRegionObserver.class);
	private boolean tableEnvMatch = false;

	@Override
	public void start(CoprocessorEnvironment e) throws IOException {	
		SpliceLogUtils.trace(LOG, "Starting TransactionalManagerRegionObserver CoProcessor " + TransactionalManagerRegionObserver.class);
		LOG.debug("TxnUtils.getTableEnv((RegionCoprocessorEnvironment) e)="+TxnUtils.getTableEnv((RegionCoprocessorEnvironment) e));
		tableEnvMatch = TxnUtils.getTableEnv((RegionCoprocessorEnvironment) e).equals(TableEnv.TRANSACTION_TABLE);
		LOG.info("inside TransactionalManagerRegionObserver Start CoProcessor, tableEnvMatch=" + tableEnvMatch);
		if(!tableEnvMatch) {
			super.start(e);
			return;
		}
		initZooKeeper(e);
	}

	@Override
	public void stop(CoprocessorEnvironment e) throws IOException {
		SpliceLogUtils.trace(LOG, "Stopping the CoProcessor " + TransactionalManagerRegionObserver.class);
		super.stop(e);
	}
	
	@Override
	public void preGet(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<KeyValue> results) throws IOException {
		SpliceLogUtils.debug(LOG, "preGet in CoProcessor " + TransactionalManagerRegionObserver.class+",tableEnvMatch="+tableEnvMatch);
		if (tableEnvMatch && Bytes.equals(get.getRow(), TxnConstants.INITIALIZE_TRANSACTION_ID_BYTES)) {
			SpliceLogUtils.trace(LOG, "preGet in CoProcessor in INITIALIZE_TRANSACTION_ID_BYTES");
			//TODO: why do we create new transaction id/zookeeper nodes here?
			results.add(new KeyValue(Bytes.toBytes(TxnUtils.beginTransaction(this.transactionPath, null)), HConstants.LATEST_TIMESTAMP));
			//results.add(new KeyValue(get.getAttribute(TxnConstants.TRANSACTION_ID), HConstants.LATEST_TIMESTAMP));
			e.bypass();
			e.complete();
		}
	}

	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> e,Put put, WALEdit edit, boolean writeToWAL) throws IOException {	
		SpliceLogUtils.debug(LOG,"manager prePut in CoProcessor " + TransactionalManagerRegionObserver.class+",tableEnvMatch="+tableEnvMatch);
		if (tableEnvMatch) {
			if (put.has(TxnConstants.TRANSACTION_TABLE_PREPARE_FAMILY_BYTES, TxnConstants.TRANSACTION_QUALIFIER)) {
				SpliceLogUtils.debug(LOG,"Prepare Commit transaction: " + Bytes.toString(put.getRow()));
				TxnUtils.prepareCommit(Bytes.toString(put.getRow()), rzk);
				TxnLogger.logTxnTableRegionsToZk(regionLogPath, zkw, e.getEnvironment().getConfiguration());
			} else if (put.has(TxnConstants.TRANSACTION_TABLE_DO_FAMILY_BYTES, TxnConstants.TRANSACTION_QUALIFIER)) {
				SpliceLogUtils.debug(LOG,"Do Commit transaction: " + Bytes.toString(put.getRow()));
				TxnUtils.doCommit(Bytes.toString(put.getRow()), rzk);				
			} else if (put.has(TxnConstants.TRANSACTION_TABLE_ABORT_FAMILY_BYTES, TxnConstants.TRANSACTION_QUALIFIER)) {
				SpliceLogUtils.debug(LOG,"Abort transaction: " + Bytes.toString(put.getRow()));				
				TxnUtils.abort(Bytes.toString(put.getRow()), rzk);
			} else {
				SpliceLogUtils.debug(LOG,"on prePut: no transaction status sets on Put");	
			}
			e.bypass();
			e.complete();
		}
	}

	@Override
	public void preSplit(ObserverContext<RegionCoprocessorEnvironment> e) {
		SpliceLogUtils.debug(LOG,"Splitting ");
		super.preSplit(e);
	}
	
}
