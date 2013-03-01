package com.splicemachine.si2.relations.api;

import java.util.List;

/**
 * Abstraction over the low-level manipulations of tuples going into or coming out of a Relation.
 * Tuples are made up of cells.
 */
public interface TupleHandler {
	Object makeTupleKey(Object[] args);
	Object makeFamily(String familyIdentifier);
	Object makeQualifier(Object qualifierIdentifier);

	boolean familiesMatch(Object family1, Object family2);
	boolean qualifiersMatch(Object qualifier1, Object qualifier2);

	Object getKey(Object tuple);
	List getCells(Object tuple);

	List getCellsForColumn(Object tuple, Object family, Object qualifier);
	Object getLatestCellForColumn(Object tuple, Object family, Object qualifier);

	Object getCellFamily(Object cell);
	Object getCellQualifier(Object cell);
	Object getCellValue(Object cell);
	long getCellTimestamp(Object cell);

	Object makeValue(Object value);
	Object fromValue(Object value, Class type);
	TuplePut makeTuplePut(Object key, List cells);
	TupleGet makeTupleGet(Object startTupleKey, Object endTupleKey,
						  List<Object> families, List<List<Object>> columns,
						  Long effectiveTimestamp);
	void addCellToTuple(Object tuple, Object family, Object qualifier, Long timestamp, Object value);
	void addAttributeToTuple(Object tuple, String attributeName, Object value);
	Object getAttribute(Object tuple, String attributeName);
}
