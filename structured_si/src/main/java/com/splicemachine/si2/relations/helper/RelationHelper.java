package com.splicemachine.si2.relations.helper;

import com.splicemachine.si2.relations.api.Relation;
import com.splicemachine.si2.relations.api.RelationReader;
import com.splicemachine.si2.relations.api.RelationWriter;
import com.splicemachine.si2.relations.api.TupleHandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RelationHelper {
	private Relation relation;
	private final TupleHandler tupleHandler;
	private final RelationReader reader;
	private final RelationWriter writer;

	public RelationHelper(TupleHandler tupleHandler, RelationReader reader, RelationWriter writer) {
		this.tupleHandler = tupleHandler;
		this.reader = reader;
		this.writer = writer;
	}

	public void open(String relationIdentifier) {
		relation = reader.open(relationIdentifier);
	}

	public void write(Object[] keyParts, String family, Object qualifier, Object value, Long timestamp) {
		final Object newKey = tupleHandler.makeTupleKey(keyParts);
		Object tuple = tupleHandler.makeTuplePut(newKey, null);
		tupleHandler.addCellToTuple(tuple, tupleHandler.makeFamily(family), tupleHandler.makeQualifier(qualifier),
				timestamp, tupleHandler.makeValue(value));
		writer.write(relation, Arrays.asList(tuple));
	}
}
