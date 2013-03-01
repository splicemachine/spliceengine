package com.splicemachine.si2.relations.api;

import java.util.List;

/**
 * Means of writing to relations. To be used in conjunction with RelationReader.
 */
public interface RelationWriter {
	void write(Relation relation, List tuples);
}
