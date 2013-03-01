package com.splicemachine.si2.relations.helper;

import com.splicemachine.si2.relations.api.Relation;
import com.splicemachine.si2.relations.api.RelationReader;
import com.splicemachine.si2.relations.api.TupleGet;
import com.splicemachine.si2.relations.api.TupleHandler;

import java.util.Iterator;

public class RelationReaderHelper {
	private final TupleHandler handler;
	private final RelationReader reader;

	public RelationReaderHelper(TupleHandler handler, RelationReader reader) {
		this.handler = handler;
		this.reader = reader;
	}

	private Object getFirst(Iterator listResult) {
		if (listResult.hasNext()) {
			return listResult.next();
		}
		return null;
	}

	public Object read(Relation relation, Object tupleKey) {
		TupleGet get = handler.makeTupleGet(tupleKey, tupleKey, null, null, null);
		return getFirst(reader.read(relation, get));
	}

}
