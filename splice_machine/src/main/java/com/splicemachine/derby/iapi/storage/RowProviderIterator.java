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
