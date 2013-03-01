package com.splicemachine.si2.relations.simple;

import com.splicemachine.si2.relations.api.Relation;

public class SimpleRelation implements Relation {
	final String relationIdentifier;

	public SimpleRelation(String relationIdentifier) {
		this.relationIdentifier = relationIdentifier;
	}
}
