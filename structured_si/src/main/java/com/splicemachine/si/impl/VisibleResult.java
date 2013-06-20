package com.splicemachine.si.impl;

/**
 * Represents the multi-valued return value from the visible method.
 */
public class VisibleResult {
    final boolean visible;
    final TransactionStatus effectiveStatus;

    public VisibleResult(boolean visible, TransactionStatus effectiveStatus) {
        this.visible = visible;
        this.effectiveStatus = effectiveStatus;
    }
}
