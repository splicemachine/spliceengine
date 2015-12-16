package com.splicemachine.mrio.api.core;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SITableScanner;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.hbase.RegionScanIterator;
import com.splicemachine.hbase.SimpleMeasuredRegionScanner;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.TxnSupplier;
import com.splicemachine.si.coprocessor.TxnMessage;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.SIFactoryDriver;
import com.splicemachine.si.impl.TransactionStorage;
import com.splicemachine.si.impl.TransactionalRegions;
import com.splicemachine.si.impl.region.STransactionLib;
import com.splicemachine.si.impl.region.TxnDecoder;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SMTxnRecordReaderImpl extends RecordReader<RowLocation, TxnMessage.Txn> {
    protected static final Logger LOG = Logger.getLogger(SMTxnRecordReaderImpl.class);
	protected Table htable;
	protected HRegion hregion;
	protected Configuration config;
	protected MeasuredRegionScanner<Cell> mrs;
	protected long txnId;
	protected Scan scan;
	protected TxnMessage.Txn currentTransaction;
	protected RowLocation rowLocation;
	private List<AutoCloseable> closeables = new ArrayList<>();
	private RegionScanIterator<Cell, Put, Delete, Get, Scan, Tuple2<RowLocation, TxnMessage.Txn>> iterator;
	private TableSplit tableSplit;

	public SMTxnRecordReaderImpl(Configuration config) {
		this.config = config;
	}	
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {	
		if (LOG.isDebugEnabled())
			SpliceLogUtils.debug(LOG, "initialize with split=%s", split);
		init(config==null?context.getConfiguration():config,split);
	}

	@SuppressWarnings("unchecked")
	public void init(Configuration config, InputSplit split) throws IOException, InterruptedException {	
		if (LOG.isDebugEnabled())
			SpliceLogUtils.debug(LOG, "init");
		long afterTs = config.getLong(MRConstants.SPLICE_TXN_MIN_TIMESTAMP, Long.MAX_VALUE);
		long beforeTs = config.getLong(MRConstants.SPLICE_TXN_MAX_TIMESTAMP, 0);
		byte[] destinationTable = Bytes.toBytes(config.get(MRConstants.SPLICE_TXN_DEST_TABLE));
		tableSplit = ((SMSplit) split).getSplit();
		Scan scan = setupScanOnRange(beforeTs, afterTs);
		final SDataLib dataLib = SIFactoryDriver.siFactory.getDataLib();
		scan.setFilter(dataLib.getActiveTransactionFilter(afterTs,beforeTs,destinationTable));
		this.scan = scan;
		restart();
		final STransactionLib transactionlib = SIFactoryDriver.siFactory.getTransactionLib();
		final TxnDecoder newTransactionDecoder = transactionlib.getV2TxnDecoder();
		final TxnSupplier txnStore = TransactionStorage.getTxnSupplier();
		this.iterator = new RegionScanIterator<>(mrs,new RegionScanIterator.IOFunction<Tuple2<RowLocation, TxnMessage.Txn>,Cell>() {
			@Override
			public Tuple2<RowLocation, TxnMessage.Txn> apply(@Nullable List<Cell> keyValues) throws IOException{
				TxnMessage.Txn txn = (TxnMessage.Txn) newTransactionDecoder.decode(dataLib,keyValues);
				/*
				 * In normal circumstances, we would say that this transaction is active
				 * (since it passed the ActiveTxnFilter).
				 *
				 * However, a child transaction may need to be returned even though
				 * he is committed, because a parent along the chain remains active. In this case,
				 * we need to resolve the effective commit timestamp of the parent, and if that value
				 * is -1, then we return it. Otherwise, just mark the child transaction with a global
				 * commit timestamp and move on.
				 */
				Cell kv = keyValues.get(0);
				ByteSlice slice = ByteSlice.wrap(kv.getRowArray(), kv.getRowOffset(), kv.getRowLength());
				long parentTxnId =transactionlib.getParentTxnId(txn);
				if(parentTxnId<0){
					//we are a top-level transaction
					return new Tuple2(new HBaseRowLocation(slice), txn);
				}

				switch(txnStore.getTransaction(parentTxnId).getEffectiveState()){
					case ACTIVE:
						return new Tuple2(new HBaseRowLocation(slice), txn);
					case ROLLEDBACK:
						return null;
					case COMMITTED:
						return null;
				}

				return new Tuple2(new HBaseRowLocation(slice), txn);
			}
		},dataLib);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (iterator.hasNext()) {
			Tuple2<RowLocation, TxnMessage.Txn> next = iterator.next();
			currentTransaction = next._2();
			rowLocation = next._1();
			return true;
		} else {
			return false;
		}
	}

	@Override
	public RowLocation getCurrentKey() throws IOException, InterruptedException {
		return rowLocation;
	}

	@Override
	public TxnMessage.Txn getCurrentValue() throws IOException, InterruptedException {
		return currentTransaction;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public void close() throws IOException {
		IOException lastThrown = null;
		if (LOG.isDebugEnabled())
			SpliceLogUtils.debug(LOG, "close");
		for (AutoCloseable c : closeables) {
			if (c != null) {
				try {
					c.close();
				} catch (Exception e) {
					lastThrown = e instanceof IOException ? (IOException) e : new IOException(e);
				}
			}
		}
		if (lastThrown != null) {
			throw lastThrown;
		}
	}
	
	public void setScan(Scan scan) {
		this.scan = scan;
	}
	
	public void setHTable(Table htable) {
		this.htable = htable;
		addCloseable(htable);
	}
	
	public void restart() throws IOException {
		if(htable != null) {
			SpliceRegionScanner splitRegionScanner = DerbyFactoryDriver.derbyFactory.getSplitRegionScanner(scan,htable);
			this.hregion = splitRegionScanner.getRegion();
			this.mrs = new SimpleMeasuredRegionScanner(splitRegionScanner,Metrics.basicMetricFactory());
		} else {
			throw new IOException("htable not set");
		}
	}

	public void addCloseable(AutoCloseable closeable) {
		closeables.add(closeable);
	}

	private Scan setupScanOnRange(long afterTs, long beforeTs) {
	  /*
	   * Get the bucket id for the region.
	   *
	   * The way the transaction table is built, a region may have an empty start
	   * OR an empty end, but will never have both
	   */
		byte[] regionKey = tableSplit.getStartRow();
		byte bucket;
		if(regionKey.length<=0)
			bucket = 0;
		else
			bucket = regionKey[0];
		byte[] startKey = Bytes.concat(Arrays.asList(new byte[]{bucket}, Bytes.toBytes(afterTs)));
		if(Bytes.startComparator.compare(tableSplit.getStartRow(),startKey)>0)
			startKey = tableSplit.getStartRow();
		byte[] stopKey = Bytes.concat(Arrays.asList(new byte[]{bucket}, Bytes.toBytes(beforeTs+1)));
		if(Bytes.endComparator.compare(tableSplit.getEndRow(),stopKey)<0)
			stopKey = tableSplit.getEndRow();
		org.apache.hadoop.hbase.client.Scan scan = new org.apache.hadoop.hbase.client.Scan(startKey,stopKey);
		scan.setMaxVersions(1);
		return scan;
	}
}