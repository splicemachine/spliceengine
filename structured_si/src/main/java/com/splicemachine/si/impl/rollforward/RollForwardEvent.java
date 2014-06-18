package com.splicemachine.si.impl.rollforward;

import com.splicemachine.si.impl.RollForwardAction;
/**
 * Placeholder for a RollForwardEvent
 *
 */
public class RollForwardEvent {
	private long transactionId;
	private byte[] rowKey;
	private Long effectiveTimestamp;
	private RollForwardAction rollForwardAction;
	
	public RollForwardEvent() {

	}
	
	public void set(Object rollForwardAction, Object transactionId, Object rowKey, Object effectiveTimestamp) {
		this.rollForwardAction = (RollForwardAction) rollForwardAction;
		this.transactionId = (Long) transactionId;
		this.rowKey = (byte[]) rowKey;
		this.effectiveTimestamp = (Long) effectiveTimestamp;
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

	public Long getEffectiveTimestamp() {
		return effectiveTimestamp;
	}

	public void setEffectiveTimestamp(Long effectiveTimestamp) {
		this.effectiveTimestamp = effectiveTimestamp;
	}

	public RollForwardAction getRollForwardAction() {
		return rollForwardAction;
	}

	public void setRollForwardAction(RollForwardAction rollForwardAction) {
		this.rollForwardAction = rollForwardAction;
	}
	
	@Override
	public String toString() {
		return String.format("RollForwardEvent {rollForwardAction=%s, transactionId=%d, rowKey=%s, effectiveTimestamp=%d}",rollForwardAction,transactionId,rowKey,effectiveTimestamp);
	}
	
}
