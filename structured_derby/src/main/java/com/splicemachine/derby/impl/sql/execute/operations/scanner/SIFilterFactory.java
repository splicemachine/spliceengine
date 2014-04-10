package com.splicemachine.derby.impl.sql.execute.operations.scanner;

import com.splicemachine.si.api.SIFilter;
import com.splicemachine.storage.EntryAccumulator;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 4/10/14
 */
public interface SIFilterFactory {
				SIFilter newFilter(EntryPredicateFilter predicateFilter,
													 EntryDecoder rowEntryDecoder,
													 EntryAccumulator accumulator,
													 boolean isCountStar) throws IOException;
}
