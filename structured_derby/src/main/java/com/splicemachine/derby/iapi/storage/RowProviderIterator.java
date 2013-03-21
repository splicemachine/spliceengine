package com.splicemachine.derby.iapi.storage;

import org.apache.derby.iapi.error.StandardException;

/**
 * This class mimics the Iterator interface (minus the remove) but throws Derby's Standard Exception.
 * Standard Exceptions need to be propogated throughout the Splice Machine or we lose multi-lingual error
 * handling.
 * 
 * @see java.util.Iterator
 * @author John Leach
 * 
 */
public interface RowProviderIterator<T> {
	/**
	 * Returns true if the iteration has more elements.
	 * 
	 * @return boolean
	 * @throws StandardException
	 */
	public boolean hasNext() throws StandardException;
	/**
	 * Returns the next element in the iteration.
	 * 
	 * @return boolean
	 * @throws StandardException
	 */
	public T next() throws StandardException;
}
