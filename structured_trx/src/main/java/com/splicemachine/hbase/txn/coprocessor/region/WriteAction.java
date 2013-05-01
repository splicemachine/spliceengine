package com.splicemachine.hbase.txn.coprocessor.region;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.log4j.Logger;

import com.google.common.primitives.Bytes;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.SortableByteUtil;
import com.splicemachine.constants.environment.EnvUtils;
import com.splicemachine.hbase.txn.logger.LogConstants;
import com.splicemachine.hbase.txn.logger.TxnLogger;
import com.splicemachine.utils.SpliceLogUtils;

public class WriteAction extends LogConstants {
	private static Logger LOG = Logger.getLogger(WriteAction.class);
	private Put put;
	private Delete delete;
	private HRegion region;
	private String regionId;
	private String transactionID;

	public WriteAction(final Mutation mutation, final HRegion region, final String transactionID) {
		this.region = region;
		this.regionId = EnvUtils.getRegionId(region);
		this.transactionID = transactionID;
		if (mutation instanceof Put) {
			this.put = (Put) mutation;
		} else if (mutation instanceof Delete) {
			this.delete = (Delete) mutation;
		} else {
			throw new IllegalArgumentException("WriteAction requires a Put or a Delete.");
		}
	}

	public Put getPut() {
		return put;
	}

	public Delete getDelete() {
		return delete;
	}
	
	/**
	 * The sequence number indicate the postion of this write action is orderingWriteAction list. splitKey could be null for txn log.
	 */
	public Put generateLogPut(LogRecordType logType, byte[] beginKey, byte[] endKey, int sequenceNum) {
		SpliceLogUtils.debug(LOG,"generateLogPut, logTYpe="+logType);
		byte[] logRowKey;
		if (logType.equals(LogRecordType.TXN_LOG)) {
			logRowKey = Bytes.concat(TxnLogger.getTxnLogScanKey(region.getTableDesc().getNameAsString(), regionId, transactionID), LogConstants.LOG_DELIMITER_BYTES, SortableByteUtil.toBytes(sequenceNum));
		} else if (logType.equals(LogRecordType.SPLIT_LOG)) {
			logRowKey = Bytes.concat(TxnLogger.getSplitLogScanKey(logType, region.getTableDesc().getNameAsString(), beginKey, endKey, transactionID), LogConstants.LOG_DELIMITER_BYTES, SortableByteUtil.toBytes(sequenceNum));
		} else {
			throw new RuntimeException("Log Record Type not supported.");
		}
		Put logPut = new Put(logRowKey);
		ByteArrayOutputStream ostream = new ByteArrayOutputStream();
		DataOutput out = new DataOutputStream(ostream);
		try {
			if (put != null) {
				SpliceLogUtils.debug(LOG,"generateLogPut, put"+put);
				put.write(out);
				logPut.add(SpliceConstants.DEFAULT_FAMILY_BYTES, ACTION_TYPE_BYTES, WriteActionType.PUT.toString().getBytes());
			} else if (delete != null) {
				SpliceLogUtils.debug(LOG,"generateLogPut, delete"+delete);
				delete.write(out);
				logPut.add(SpliceConstants.DEFAULT_FAMILY_BYTES, ACTION_TYPE_BYTES, WriteActionType.DELETE.toString().getBytes());
			}
		} catch (IOException e) {
			SpliceLogUtils.error(LOG,"Failed to write WriteAction into ByteArrayOutputStream.",e);	
		}
		logPut.add(SpliceConstants.DEFAULT_FAMILY_BYTES, ROW_KEY_BYTES, this.getRow());
		logPut.add(SpliceConstants.DEFAULT_FAMILY_BYTES, TXN_ID_COLUMN_BYTES, transactionID.getBytes());
		logPut.add(SpliceConstants.DEFAULT_FAMILY_BYTES, ACTION_WRITABLE_BYTE, ostream.toByteArray());
		SpliceLogUtils.debug(LOG,"Generated log put with Row key " + org.apache.hadoop.hbase.util.Bytes.toString(logRowKey) + 
					". The original row key " + org.apache.hadoop.hbase.util.Bytes.toString(this.getRow()));
		return logPut;
	}

	public byte[] getRow() {
		if (put != null) {
			return put.getRow();
		} else if (delete != null) {
			return delete.getRow();
		}
		throw new IllegalStateException("WriteAction is invalid");
	}

	List<KeyValue> getKeyValues() {
		List<KeyValue> edits = new ArrayList<KeyValue>();
		Collection<List<KeyValue>> kvsList;

		if (put != null) {
			kvsList = put.getFamilyMap().values();
		} else if (delete != null) {
			if (delete.getFamilyMap().isEmpty()) {
				// If whole-row delete then we need to expand for each
				// family
				kvsList = new ArrayList<List<KeyValue>>(1);
				for (byte[] family : region.getTableDesc().getFamiliesKeys()) {
					KeyValue familyDelete = new KeyValue(delete.getRow(), family, null, delete.getTimeStamp(),
							KeyValue.Type.DeleteFamily);
					kvsList.add(Collections.singletonList(familyDelete));
				}
			} else {
				kvsList = delete.getFamilyMap().values();
			}
		} else {
			throw new IllegalStateException("WriteAction is invalid");
		}

		for (List<KeyValue> kvs : kvsList) {
			for (KeyValue kv : kvs) {
				edits.add(kv);
			}
		}
		return edits;
	}	
}