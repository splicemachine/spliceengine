package com.splicemachine.storage;

/**
 * @author Scott Fines
 *         Date: 2/8/14
 */
public interface HasPredicateFilter {

		EntryPredicateFilter getFilter();

		long getBytesVisited();
}
