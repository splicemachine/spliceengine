package com.splicemachine.si2.relations.api;

import java.util.Iterator;
import java.util.List;

/**
 * Means of opening relations and reading data from them.
 */
public interface RelationReader {
	Relation open(String relationIdentifier);
	void close(Relation relation);

	Iterator read(Relation relation, TupleGet get);
}
