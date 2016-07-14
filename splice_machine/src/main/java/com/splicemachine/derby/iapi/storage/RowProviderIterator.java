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

package com.splicemachine.derby.iapi.storage;

import com.splicemachine.db.iapi.error.StandardException;

import java.io.IOException;
import java.lang.Deprecated;

/**
 * This class mimics the Iterator interface (minus the remove) but throws Derby's Standard Exception.
 * Standard Exceptions need to be propogated throughout the Splice Machine or we lose multi-lingual error
 * handling.
 * 
 * @see java.util.Iterator
 * @author John Leach
 * 
 */
@Deprecated
public interface RowProviderIterator<T> {
	/**
	 * Returns true if the iteration has more elements.
	 * 
	 * @return boolean
	 * @throws StandardException
	 */
	boolean hasNext() throws StandardException, IOException;
	/**
	 * Returns the next element in the iteration.
	 * 
	 * @return boolean
	 * @throws StandardException
	 */
	T next() throws StandardException, IOException;
}
