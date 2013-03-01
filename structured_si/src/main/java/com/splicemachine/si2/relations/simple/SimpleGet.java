package com.splicemachine.si2.relations.simple;

import com.splicemachine.si2.relations.api.TupleGet;

import java.util.List;

public class SimpleGet implements TupleGet {
	final Object startTupleKey;
	final Object endTupleKey;
	final java.util.List<Object> families;
	final List<List<Object>> columns;
	final Long effectiveTimestamp;

	public SimpleGet(Object startTupleKey, Object endTupleKey, List<Object> families, List<List<Object>> columns,
					 Long effectiveTimestamp) {
		this.startTupleKey = startTupleKey;
		this.endTupleKey = endTupleKey;
		this.families = families;
		this.columns = columns;
		this.effectiveTimestamp = effectiveTimestamp;
	}
}
