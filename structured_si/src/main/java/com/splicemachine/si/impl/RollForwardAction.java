package com.splicemachine.si.impl;

import java.util.List;

import com.splicemachine.si.impl.rollforward.RollForwardEvent;

/**
 * Abstraction over the details of what it means to roll-forward a transaction. This allows the RollForwardQueue to be
 * decoupled.
 */
public interface RollForwardAction {
	boolean write(List<RollForwardEvent> rollForwardEvents);
}
