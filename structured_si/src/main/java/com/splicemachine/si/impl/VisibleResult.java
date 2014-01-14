package com.splicemachine.si.impl;

import com.splicemachine.si.api.TransactionStatus;

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
