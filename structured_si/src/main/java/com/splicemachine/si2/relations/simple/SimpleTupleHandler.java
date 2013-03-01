package com.splicemachine.si2.relations.simple;

import com.splicemachine.si2.relations.api.TupleGet;
import com.splicemachine.si2.relations.api.TupleHandler;
import com.splicemachine.si2.relations.api.TuplePut;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class SimpleTupleHandler implements TupleHandler {

	@Override
	public Object makeTupleKey(Object... args) {
		StringBuilder builder = new StringBuilder();
		for (Object a : args) {
			builder.append(a);
		}
		return builder.toString();
	}

	@Override
	public Object makeFamily(String familyIdentifier) {
		return familyIdentifier;
	}

	@Override
	public Object makeQualifier(Object qualifierIdentifier) {
		return qualifierIdentifier;
	}

	private boolean nullSafeComparison(Object o1, Object o2) {
		return (o1 == null && o2 == null) || ((o1 != null) && o1.equals(o2));
	}

	@Override
	public boolean familiesMatch(Object family1, Object family2) {
		return nullSafeComparison(family1, family2);
	}

	@Override
	public boolean qualifiersMatch(Object qualifier1, Object qualifier2) {
		return nullSafeComparison(qualifier1, qualifier2);
	}

	@Override
	public Object makeValue(Object value) {
		return value;
	}

	@Override
	public Object fromValue(Object value, Class type) {
		if (type.equals(value.getClass())) {
			return value;
		}
		throw new RuntimeException( "types don't match " + value.getClass().getName() + " " + type.getName() + " " + value);
	}

	@Override
	public void addCellToTuple(Object tuple, Object family, Object qualifier, Long timestamp, Object value) {
		SimpleTuple simpleTuple = (SimpleTuple) tuple;
		final SimpleCell newCell = new SimpleCell((String) family, (String) qualifier, timestamp, value);
		simpleTuple.values.add(newCell);
	}

	@Override
	public void addAttributeToTuple(Object tuple, String attributeName, Object value) {
		SimpleTuple simpleTuple = (SimpleTuple) tuple;
		simpleTuple.attributes.put(attributeName, value);
	}

	@Override
	public Object getAttribute(Object tuple, String attributeName) {
		SimpleTuple simpleTuple = (SimpleTuple) tuple;
		return simpleTuple.attributes.get(attributeName);
	}

	@Override
	public Object makeTuple(Object key, List cells) {
		return new SimpleTuple((String) key, new ArrayList<SimpleCell>(cells));
	}

	@Override
	public TuplePut makeTuplePut(Object key, List cells) {
		if (cells == null) {
			cells = new ArrayList();
		}
		return new SimpleTuple((String) key, cells);
	}

	@Override
	public TupleGet makeTupleGet(Object startTupleKey, Object endTupleKey, List<Object> families, List<List<Object>> columns, Long effectiveTimestamp) {
		return new SimpleGet(startTupleKey, endTupleKey, families, columns, effectiveTimestamp);
	}

	@Override
	public Object getKey(Object tuple) {
		return ((SimpleTuple) tuple).key;
	}

	private List getValuesForColumn(SimpleTuple tuple, Object family, Object qualifier) {
		List<SimpleCell> values = ((SimpleTuple) tuple).values;
		List<SimpleCell> results = new ArrayList<SimpleCell>();
		for (Object vRaw : values) {
			SimpleCell v = (SimpleCell) vRaw;
			if (familiesMatch(v.family, family) && qualifiersMatch(v.qualifier, qualifier)) {
				results.add(v);
			}
		}
		sort(results);
		return results;
	}

	@Override
	public List getCellsForColumn(Object tuple, Object family, Object qualifier) {
		List<Object> values = getValuesForColumn((SimpleTuple) tuple, family, qualifier);
		sort(values);
		return values;
	}

	private void sort(List results) {
		Collections.sort(results, new Comparator<Object>() {
			@Override
			public int compare(Object simpleCell, Object simpleCell2) {
				return Long.valueOf(((SimpleCell) simpleCell2).timestamp).compareTo(((SimpleCell) simpleCell).timestamp);
			}
		});
	}

	@Override
	public Object getLatestCellForColumn(Object tuple, Object family, Object qualifier) {
		final List valuesForColumn = getValuesForColumn((SimpleTuple) tuple, family, qualifier);
		if (valuesForColumn.isEmpty()) {
			return null;
		}
		return ((SimpleCell) valuesForColumn.get(0)).value;
	}

	@Override
	public List getCells(Object tuple) {
		return ((SimpleTuple) tuple).values;
	}

	@Override
	public Object getCellFamily(Object cell) {
		return ((SimpleCell) cell).family;
	}

	@Override
	public Object getCellQualifier(Object cell) {
		return ((SimpleCell) cell).qualifier;
	}

	@Override
	public Object getCellValue(Object cell) {
		return ((SimpleCell) cell).value;
	}

	@Override
	public long getCellTimestamp(Object cell) {
		return ((SimpleCell) cell).timestamp;
	}

}
