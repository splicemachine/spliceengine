package com.splicemachine.si2.relations.simple;

import com.splicemachine.si2.relations.api.Clock;
import com.splicemachine.si2.relations.api.Relation;
import com.splicemachine.si2.relations.api.RelationReader;
import com.splicemachine.si2.relations.api.RelationWriter;
import com.splicemachine.si2.relations.api.TupleGet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SimpleStore implements RelationReader, RelationWriter {
	private final Map<String, List<SimpleTuple>> relations = new HashMap<String, List<SimpleTuple>>();
	private final SimpleTupleHandler tupleReaderWriter = new SimpleTupleHandler();
	private final Clock clock;

	public SimpleStore(Clock clock) {
		this.clock = clock;
	}

	public String toString() {
		StringBuilder result = new StringBuilder();
		for (Map.Entry<String, List<SimpleTuple>> entry : relations.entrySet()) {
			String relationName = entry.getKey();
			result.append(relationName);
			result.append("\n");
			List<SimpleTuple> tuples = entry.getValue();
			for (SimpleTuple t : tuples) {
				result.append(t.toString());
				result.append("\n");
			}
			result.append("----");
		}
		return result.toString();
	}

	@Override
	public Relation open(String relationIdentifier) {
		return new SimpleRelation(relationIdentifier);
	}

	@Override
	public Iterator read(Relation relation, TupleGet get) {
		SimpleGet simpleGet = (SimpleGet) get;
		List<SimpleTuple> tuples = relations.get(((SimpleRelation) relation).relationIdentifier);
		List<SimpleTuple> results = new ArrayList<SimpleTuple>();
		for (SimpleTuple t : tuples) {
			if ((t.key.equals((String) simpleGet.startTupleKey)) ||
					((t.key.compareTo((String) simpleGet.startTupleKey) > 0) && (t.key.compareTo((String) simpleGet.endTupleKey) < 0))) {
				results.add(filterCells(t, simpleGet.families, simpleGet.columns, simpleGet.effectiveTimestamp));
			}
		}
		return results.iterator();
	}

	private SimpleTuple filterCells(SimpleTuple t, List<Object> families,
									List<List<Object>> columns, Long effectiveTimestamp) {
		List<SimpleCell> newCells = new ArrayList<SimpleCell>();
		for (SimpleCell c : t.values) {
			if ((families == null && columns == null) ||
					((families != null) && families.contains(c.family)) ||
					((columns != null) && columnsContain(columns, c))) {
				if (effectiveTimestamp != null) {
					if (c.timestamp >= effectiveTimestamp) {
						newCells.add(c);
					}
				} else {
					newCells.add(c);
				}
			}
		}
		return new SimpleTuple(t.key, newCells);
	}

	private boolean columnsContain(List<List<Object>> columns, SimpleCell c) {
		for (List<Object> column : columns) {
			if ((column.get(0).equals(c.family)) && (column.get(1).equals(c.qualifier))) {
				return true;
			}
		}
		return false;
	}

	@Override
	public void close(Relation relation) {
	}

	@Override
	public void write(Relation relation, List tuples) {
		synchronized (this) {
			final String relationIdentifier = ((SimpleRelation) relation).relationIdentifier;
			List<SimpleTuple> newTuples = relations.get(relationIdentifier);
			if (newTuples == null) {
				newTuples = new ArrayList<SimpleTuple>();
			}
			for (Object t : tuples) {
				newTuples = writeSingle(relation, (SimpleTuple) t, newTuples);
			}
			relations.put(relationIdentifier, newTuples);
		}
	}

	private long getCurrentTimestamp() {
		return clock.getTime();
	}

	private List<SimpleTuple> writeSingle(Relation relation, SimpleTuple newTuple, List<SimpleTuple> currentTuples) {
		List<SimpleCell> newValues = new ArrayList<SimpleCell>();
		for (SimpleCell c : newTuple.values) {
			if (c.timestamp == null) {
				newValues.add(new SimpleCell(c.family, c.qualifier, getCurrentTimestamp(), c.value));
			} else {
				newValues.add(c);
			}
		}
		SimpleTuple modifiedNewTuple = new SimpleTuple(newTuple.key, newValues);

		List<SimpleTuple> newTuples = new ArrayList<SimpleTuple>();
		boolean matched = false;
		for (SimpleTuple t : currentTuples) {
			if (newTuple.key.equals(t.key)) {
				matched = true;
				List<SimpleCell> values = new ArrayList<SimpleCell>(t.values);
				values.addAll(newValues);
				newTuples.add(new SimpleTuple(newTuple.key, values));
			} else {
				newTuples.add(t);
			}
		}
		if (!matched) {
			newTuples.add(modifiedNewTuple);
		}
		return newTuples;
	}
}
