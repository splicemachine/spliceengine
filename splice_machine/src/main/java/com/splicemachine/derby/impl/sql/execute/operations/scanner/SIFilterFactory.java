/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.sql.execute.operations.scanner;

import com.splicemachine.si.api.filter.SIFilter;
import com.splicemachine.storage.EntryAccumulator;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 4/10/14
 */
public interface SIFilterFactory<Data> {
				SIFilter newFilter(EntryPredicateFilter predicateFilter,
													 EntryDecoder rowEntryDecoder,
													 EntryAccumulator accumulator,
													 boolean isCountStar) throws IOException;
}
