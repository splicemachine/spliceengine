package com.splicemachine.si2.si.api;

import com.splicemachine.si2.relations.api.TupleGet;
import com.splicemachine.si2.relations.api.TuplePut;

import java.util.List;

/**
 * Transaction capabilities exposed to client processes (i.e. they don't have direct access to the transaction store).
 */
public interface ClientTransactor {
	void initializeTuplePuts(List<TuplePut> tuples);
}
