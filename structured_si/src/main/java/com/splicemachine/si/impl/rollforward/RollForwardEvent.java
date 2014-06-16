package com.splicemachine.si.impl.rollforward;

import com.splicemachine.si.impl.RollForwardAction;
/**
 * Placeholder for a RollForwardEvent
 *
 */
public class RollForwardEvent {
	private long transactionId;
	private byte[] rowKey;
	private long effectiveTimestamp;
	private RollForwardAction rollForwardAction;
	
	public RollForwardEvent() {
	}
	
	public void setData(RollForwardAction rollForwardAction, long transactionId, byte[] rowKey) {
		this.rollForwardAction = rollForwardAction;
		this.transactionId = transactionId;
		this.rowKey = rowKey;
	}

	public void setData(RollForwardAction rollForwardAction, long transactionId, byte[] rowKey, long effectiveTimestamp) {
		this.rollForwardAction = rollForwardAction;
		this.transactionId = transactionId;
		this.rowKey = rowKey;
		this.effectiveTimestamp = effectiveTimestamp;
	}

	public long getTransactionId() {
		return transactionId;
	}

	public void setTransactionId(long transactionId) {
		this.transactionId = transactionId;
	}

	public byte[] getRowKey() {
		return rowKey;
	}

	public void setRowKey(byte[] rowKey) {
		this.rowKey = rowKey;
	}

	public long getEffectiveTimestamp() {
		return effectiveTimestamp;
	}

	public void setEffectiveTimestamp(long effectiveTimestamp) {
		this.effectiveTimestamp = effectiveTimestamp;
	}

	public RollForwardAction getRollForwardAction() {
		return rollForwardAction;
	}

	public void setRollForwardAction(RollForwardAction rollForwardAction) {
		this.rollForwardAction = rollForwardAction;
	}
}
