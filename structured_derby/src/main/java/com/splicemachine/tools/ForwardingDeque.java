package com.splicemachine.tools;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * @author Scott Fines
 * Date: 12/2/13
 */
public abstract class ForwardingDeque<E> extends AbstractQueue<E> implements BlockingDeque<E> {

		@Override public Iterator<E> iterator() { throw new UnsupportedOperationException(); }
		@Override public Iterator<E> descendingIterator() { throw new UnsupportedOperationException(); }
		@Override public void push(E e) {addFirst(e);}
		@Override public E pop() { return removeFirst(); }

		@Override
		public void addFirst(E e) {
				if(!offerFirst(e))
						throw new IllegalStateException("Unable to addFirst");
		}

		@Override
		public void addLast(E e) {
				if(!offerLast(e))
						throw new IllegalStateException("Unable to addLast");
		}

		@Override
		public E removeFirst() {
				E x = pollFirst();
				if(x==null)
						throw new NoSuchElementException();
				return x;
		}

		@Override
		public E removeLast() {
				E x = pollLast();
				if(x==null)
						throw new NoSuchElementException();
				return x;
		}
		@Override
		public E getFirst() {
				E x = peekFirst();
				if(x==null)
						throw new NoSuchElementException();
				return x;
		}

		@Override
		public E getLast() {
				E x = peekLast();
				if(x==null)
						throw new NoSuchElementException();
				return x;
		}

		@Override
		public void putFirst(E e) throws InterruptedException {
				offerFirst(e,Long.MAX_VALUE,TimeUnit.NANOSECONDS);
		}

		@Override
		public void putLast(E e) throws InterruptedException {
				offerLast(e,Long.MAX_VALUE,TimeUnit.NANOSECONDS);
		}

		@Override
		public E takeFirst() throws InterruptedException {
				return pollFirst(Long.MAX_VALUE,TimeUnit.NANOSECONDS);
		}

		@Override
		public E takeLast() throws InterruptedException {
				return pollLast(Long.MAX_VALUE,TimeUnit.NANOSECONDS);
		}

		@Override public boolean removeFirstOccurrence(Object o) { throw new UnsupportedOperationException(); }
		@Override public boolean removeLastOccurrence(Object o) { throw new UnsupportedOperationException(); }
		@Override public int drainTo(Collection<? super E> c) { throw new UnsupportedOperationException(); }
		@Override public int drainTo(Collection<? super E> c, int maxElements) { throw new UnsupportedOperationException(); }

		@Override
		public void put(E e) throws InterruptedException {
				offerLast(e,Long.MAX_VALUE,TimeUnit.NANOSECONDS);
		}

		@Override
		public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
				return offerLast(e,timeout,unit);
		}

		@Override public E take() throws InterruptedException { return pollFirst(Long.MAX_VALUE,TimeUnit.NANOSECONDS); }
		@Override public E poll(long timeout, TimeUnit unit) throws InterruptedException { return pollFirst(timeout,unit); }
		@Override public boolean offer(E e) { return offerLast(e); }
		@Override public E poll() { return pollFirst(); }
		@Override public E peek() { return peekFirst(); }
}
