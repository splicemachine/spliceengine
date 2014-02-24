package com.splicemachine.utils;

import java.util.Iterator;

/**
 * @author Scott Fines
 *         Date: 2/20/14
 */
public abstract class ForwardingCloseableIterator<E> implements CloseableIterator<E>{
		private final Iterator<E> delegate;

		protected ForwardingCloseableIterator(Iterator<E> delegate) {
				this.delegate = delegate;
		}

		@Override public boolean hasNext() { return delegate.hasNext(); }
		@Override public E next() { return delegate.next(); }
		@Override public void remove() { delegate.remove(); }

}
