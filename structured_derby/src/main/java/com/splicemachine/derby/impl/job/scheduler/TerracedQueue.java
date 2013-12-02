package com.splicemachine.derby.impl.job.scheduler;

import com.splicemachine.tools.ForwardingDeque;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * BlockingQueue which has many levels.
 *
 * This is suitable only for Thread-executor pools and other
 * situations in which the "Work-stealing" concept makes sense.
 *
 * The main concept here is that of a "Terrace"--a level of fixed size,
 * which is allowed to "overflow" onto a lower terrace when full. Additionally,
 * lower terraces are allowed to "drain" from a higher terrace when empty.
 *
 * In practice, this means that, as elements are added to the queue, they are associated
 * with a terrace level by means of a caller-defined PlacementStrategy. First, an attempt
 * is made to add it to the terrace that is assigned to it, but if that terrace is full, it will
 * attempt to assign it to a lower terrace which is empty. If no lower terraces are empty, then
 * it will either return failure or wait for the assigned terrace to have available slots.
 *
 * Terraces are also keyed to a specific thread via the PlacementStrategy. This means that,
 * when an element is desired from the queue, the calling thread is first assigned a Terrace. First,
 * an attempt is made to remove an element from the assigned terrace, but if that terrace is empty,
 * it will attempt to take an entry from a higher terrace. If no higher terraces have entries available
 * to steal, it will either return failure or wait for the assigned terrace to get entries.
 *
 * In addition to normal Deque operations, there is a {@code forceAdd} method, which can be used to
 * forcibly push an entry on to the specific terrace assigned, without assigning it to a lower level. This
 * can be useful as a means to create soft-limited queues which auto-expand as necessary.
 *
 * This class is thread safe, but the intended usage is <em>not</em> for high-throughput activity. If you need
 * a high-throughput work-stealing system, then try the JDK's ForkJoinPool. This is intended more
 * for controlling longer-running task usages where control of the individual level is more important than
 * overall throughput.
 *
 * @author Scott Fines
 * Date: 11/27/13
 */
public class TerracedQueue<E> extends ForwardingDeque<E> implements BlockingDeque<E>{
		private AtomicInteger size = new AtomicInteger(0);

		/**
		 * A Strategy for sizing the queue into specific terraces.
		 */
		public static interface TerraceSizingStrategy{
				/**
				 * @return the total number of terraces in this queue
				 */
				int getNumTerraces();

				/**
				 * Get the size of the specified terrace
				 * @param terrace the terrace to get the size for
				 * @return the size of this terrace
				 */
				int getSize(int terrace);
		}

		/**
		 * A Placement strategy for determining terrace levels for individual items and threads.
		 *
		 * Allows user control over how Terraces are filled and emptied.
		 */
		public static interface PlacementStrategy<E>{
				/**
				 * Assign a terrace to the specified item, based on properties of this
				 * item.
				 *
				 * This method is <em>not</em> guaranteed to be called from within a lock.
				 * If it relies on shared mutable state, it must be synchronized by the implementation.
				 *
				 * @param item the item to assign
				 * @return the terrace to assign the item to
				 */
				public int assignTerrace(E item);

				/**
				 * Get the assigned terrace for the specified thread, based on either the type of thread,
				 * or some other form of thread-local state.
				 *
				 * This method is <em>not</em> guaranteed to be called from within a lock. If it relies
				 * on shared mutable state, it must be synchronized by the implementation.
				 *
				 * @param thread the thread asking for assignment
				 * @return the terrace this thread is assigned to
				 */
				public int getAssignedTerrace(Thread thread);

				/**
				 * Transform this element, using information including the Terrace that it was found at.
				 *
				 * @param item the item to transform
				 * @param terrace the terrace that the item was found at
				 * @return a transformed element, or {@code item} if no transformation is necessary
				 */
				E transform(E item, int terrace);

				/**
				 * Filter this element, using information including the Terrace that it was found at.
				 *
				 * @param item the item to transform
				 * @param terrace the terrace that the item was found at
				 * @return true if the element should be included, false if it should be ignored
				 */
				boolean filter(E item,int terrace);
		}

		private final PlacementStrategy<E> placementStrategy;
		private final Terrace highestLevel;

		public TerracedQueue(PlacementStrategy<E> placementStrategy,TerraceSizingStrategy sizingStrategy) {
				this(placementStrategy,sizingStrategy,200l); //TODO -sf- move this to SpliceConstants
		}

		public TerracedQueue(PlacementStrategy<E> placementStrategy,
												 TerraceSizingStrategy sizingStrategy,
												 long workStealTimeoutMs){
				this.placementStrategy = placementStrategy;

				int numTerraces = sizingStrategy.getNumTerraces();
				Terrace lowerTerrace = new Terrace(0, sizingStrategy.getSize(0), workStealTimeoutMs);
				for(int i=1;i<numTerraces;i++){
						Terrace terrace = new Terrace(i,sizingStrategy.getSize(i),workStealTimeoutMs);
						lowerTerrace.higher = terrace;
						terrace.lower = lowerTerrace;
						lowerTerrace = terrace;
				}
				highestLevel = lowerTerrace;
		}

		/**
		 * Forcibly add an item to the placed Terrace, ignoring size constraints.
		 *
		 * @param item the item to add
		 */
		public void forceAdd(E item){
				Terrace t = findTerrace(placementStrategy.assignTerrace(item));
				t.forceAdd(item);
				size.incrementAndGet();
		}

		public BlockingDeque<E> terraceView(int terrace){
				return new TerraceDequeue(findTerrace(terrace));
		}

		/*Functional methods --these do something*/
		@Override public int remainingCapacity() { return Integer.MAX_VALUE; }
		@Override public int size() { return size.get(); }

		public int size(int terrace){
				return findTerrace(terrace).size();
		}

		@Override
		public boolean offerFirst(E e) {
				int terraceNum = placementStrategy.assignTerrace(e);
				Terrace level = findTerrace(terraceNum);
				boolean success = level.offer(e,false);
				if(success)
						size.incrementAndGet();
				return success;
		}

		@Override
		public boolean offerLast(E e) {
				int terraceNum = placementStrategy.assignTerrace(e);
				Terrace level = findTerrace(terraceNum);
				boolean success= level.offer(e,true);
				if(success)
						size.incrementAndGet();
				return success;
		}

		@Override
		public E pollFirst() {
				Terrace t = findTerrace(getTerraceId());
				E x = t.poll(false);
				if(x!=null)
						size.decrementAndGet();
				return x;
		}

		@Override
		public E pollLast() {
				Terrace t = findTerrace(getTerraceId());
				E x = t.poll(true);
				if(x!=null)
						size.decrementAndGet();
				return x;
		}

		@Override
		public E peekFirst() {
				Terrace t = findTerrace(getTerraceId());
				return t.peek(false);
		}

		@Override
		public E peekLast() {
				Terrace t = findTerrace(getTerraceId());
				return t.peek(true);
		}


		@Override
		public boolean offerFirst(E e, long timeout, TimeUnit unit) throws InterruptedException {
				Terrace t = findTerrace(getTerraceId());
				boolean success = t.offer(e,timeout,unit,false);
				if(success)
						size.incrementAndGet();
				return success;
		}

		@Override
		public boolean offerLast(E e, long timeout, TimeUnit unit) throws InterruptedException {
				Terrace t = findTerrace(getTerraceId());
				boolean success = t.offer(e, timeout, unit,true);
				if(success)
						size.incrementAndGet();
				return success;
		}


		@Override
		public E pollFirst(long timeout, TimeUnit unit) throws InterruptedException {
				Terrace t = findTerrace(getTerraceId());
				E x = t.poll(timeout,unit,false);
				if(x!=null)
						size.decrementAndGet();
				return x;
		}


		@Override
		public E pollLast(long timeout, TimeUnit unit) throws InterruptedException {
				Terrace t = findTerrace(getTerraceId());
				E x =  t.poll(timeout,unit,true);
				if(x!=null)
						size.decrementAndGet();
				return x;
		}

		/*Private helper methods*/
		private Terrace findTerrace(int terrace){
				Terrace t = highestLevel;
				while(t!=null && t.terrace>terrace){
						t = t.lower;
				}
				return t;
		}

		private int getTerraceId() {
				return placementStrategy.getAssignedTerrace(Thread.currentThread());
		}

		/*Private helper classes*/
		private class Terrace{
				private Terrace higher;
				private Terrace lower;
				private Node<E> head;
				private Node<E> tail;

				private final Lock terraceLock = new ReentrantLock();
				private final Condition terraceEmptyCondition = terraceLock.newCondition();
				private final Condition terraceFullCondition = terraceLock.newCondition();
				private final int terrace;
				private final long workStealTimeout;

				private AtomicInteger count = new AtomicInteger(0);
				private volatile int softLimit;


				private Terrace(int terrace,int softLimit,long workStealTimeout) {
						this.softLimit = softLimit;
						this.terrace = terrace;
						this.workStealTimeout = workStealTimeout;
						head = tail = new Node<E>(null);
				}

				public int getSoftLimit() { return softLimit; }
				public void setSoftLimit(int softLimit) { this.softLimit = softLimit; }

				public void forceAdd(E item){
						/*
						 * Forcibly add to this terrace
						 */
						count.incrementAndGet();
						terraceLock.lock();
						try{
								addLast(item);
								terraceEmptyCondition.signal();
						}finally{
								terraceLock.unlock();
						}
				}

				public int size(){ return count.get(); }

				/*Delegating methods --These have no logic of their own*/

/**********************************************************************************************************************/
				/*private helper methods*/

				private E poll(long timeout, TimeUnit unit, boolean last) throws InterruptedException {
						long timeNanos = unit.toNanos(timeout);
						E x = null;
						do{
								int cnt = count.get();
								if(cnt<=0){
										if(higher!=null){
												x = higher.poll(true);
												if(x!=null) return x;
										}
										//wait for something to show up
										terraceLock.lockInterruptibly();
										try{
												timeNanos = terraceEmptyCondition.awaitNanos(Math.min(timeNanos,workStealTimeout));
										}finally{
												terraceLock.unlock();
										}
								}else{
										terraceLock.lockInterruptibly();
										try{
												cnt = count.get();
												if(cnt<=0){
														if(higher!=null){
																x = higher.poll(true);
																if(x!=null) return x;
														}else{
																//wait for something to show up
																timeNanos = terraceEmptyCondition.awaitNanos(Math.min(timeNanos,workStealTimeout));
														}
												}else{
														x = last?pollLastUnlocked(): pollFirstUnlocked();
														if(x!=null) return placementStrategy.transform(x,terrace);

														if(higher != null){
																//unlock this level so that we can (potentially) lock the next level.
																//calling unlock twice does not hurt anything
																x = higher.poll(true);
																if(x!=null) return x;
														}
														timeNanos = terraceEmptyCondition.awaitNanos(Math.min(timeNanos,workStealTimeout));
												}
										}finally{
												terraceLock.unlock();
										}
								}
						}while(timeNanos>0l);
						return x;
				}

				private void addLast(E item) {
						//add it to the end of the deque
						Node<E> next = new Node<E>(item);
						next.previous=tail;
						tail.next = next;
				}

				private boolean offer(E item, long timeout, TimeUnit unit, boolean last) throws InterruptedException{
						long timeNanos = unit.toNanos(timeout);
						boolean shouldContinue = false;
						do{
								if(timeNanos<=0l)
										return false;
								int cnt = count.get();
								if(cnt>=softLimit){
										boolean shouldWait = lower!=null && lower.offer(item,false);
										if(shouldWait){
												/*
												 * There were no slots available at a lower lever, so
												 * wait until a slot becomes available at this level.
												 * DO NOT attempt to wait at a lower level.
												 */
												terraceLock.lock();
												try{
														timeNanos = terraceFullCondition.awaitNanos(timeNanos);
												}finally{
														terraceLock.unlock();
												}
										}
								}else
										shouldContinue = count.compareAndSet(cnt,cnt+1);
						}while(!shouldContinue);
						//a slot has become available, so place it
						terraceLock.lockInterruptibly();
						try{
								if(last)
										addLast(item);
								else
										addFirst(item);
								terraceEmptyCondition.signal();
								return true;
						}finally{
								terraceLock.unlock();
						}
				}

				private boolean offer(E item,boolean last) {
						/*
						 * Attempt to place this item in this level (if there
						 * is room below the softLimit space). If there is no
						 * room (i.e. the size is larger than softLimit), then
						 * "overflow" to the next lowest terrace--that is, attempt
						 * to place this item on a lower terrace which has available
						 * space. If no lower terrace has available space, then
						 * we are unable to place this item, so return false.
						 *
						 * A naive implementation would be to simply lock
						 * the terrace, then check size. If the size is too large,
						 * then recursively attempt the same operation at a lower
						 * level. However, this results in this thread acquiring (or
						 * attempting to acquire) the lock on every terrace below it
						 * until either running out of terraces or finding an available
						 * terrace. This is a lot of locking and significantly removes
						 * the usefulness of per-terrace locking.
						 *
						 * A better approach is to use CAS to determine if there
						 * is space available. First, get the current count, and if
						 * it is too high, then recursively attempt to place this at
						 * a lower level. Otherwise, attempt to atomically increment
						 * the count. If the atomic increment succeeds, then we know that
						 * we safely have enough room to add to this level, at which point
						 * we lock and set the data.
						 *
						 * One could ask: why lock at all? We could write the linked list
						 * code using entirely CAS operations as well. The answer is Conditions--
						 * we want the lock because we are going to use it to notify
						 * waiting operations that there is something available on this level.
						 */
						boolean shouldContinue;
						do{
								int cnt = count.get();
								if(cnt>=softLimit){
										return lower!=null && lower.offer(item,false);
								}else
										shouldContinue = count.compareAndSet(cnt,cnt+1);
						}while(!shouldContinue);
						//we have successfully allocated a slot, so lock and add

						terraceLock.lock();
						try{
								if(last)
										addLast(item);
								else
										addFirst(item);
								//tell someone who is waiting that there is an item on this level
								terraceEmptyCondition.signal();
								return true;
						}finally{
								terraceLock.unlock();
						}
				}

				private void addFirst(E item) {
						//add it to the end of the deque
						Node<E> next = new Node<E>(item);
						next.next = head;
						head.previous = next;
				}

				private E peek(boolean last){
						/*
						 * Attempt a cheap CAS operation first to see
						 * if the terrace is empty. If it isn't, THEN
						 * attempt a lock
						 */
						int cnt = count.get();
						if(cnt<=0){
								if(higher!=null)
										return higher.peek(true);
								return null; //nothing in the queue above us either
						}

						terraceLock.lock();
						try{
								cnt = count.get();
								if(cnt<=0){
										if(higher!=null){
												terraceLock.unlock();
												return higher.peek(true);
										}
										return null; //nothing available
								}

								E x = last? peekLastUnlocked(): peekFirstUnlocked();
								if(x!=null) return placementStrategy.transform(x,terrace);
								if(higher != null){
										terraceLock.unlock();
										x = higher.peek(true);
								}
								return x;
						}finally{
								terraceLock.unlock();
						}
				}

				private E poll(boolean last) {
						/*
						 * Attempt a cheap CAS operation first to see
						 * if the terrace is empty. If it isn't, THEN
						 * attempt a lock
						 */
						int cnt = count.get();
						if(cnt<=0){
								//always go to the back end of the higher terrace, not the front
								if(higher!=null)
										return higher.poll(true);
								return null; //no queue above us, so we're done
						}

						terraceLock.lock();
						try{
								cnt = count.get();
								if(cnt<=0){
										if(higher!=null){
												terraceLock.unlock();
												return higher.poll(true);
										}else return null;
								}
								E x = last?pollLastUnlocked(): pollFirstUnlocked();
								if(x!=null)
										return placementStrategy.transform(x,terrace);
								if(higher != null){
										//unlock this level so that we can (potentially) lock the next level.
										//calling unlock twice does not hurt anything
										terraceLock.unlock();
										x = higher.poll(true);
								}
								return x;
						}finally{
								terraceLock.unlock();
						}
				}


				private void unlink(Node<E> h) {
						count.decrementAndGet();
						Node<E> previous = h.previous;
						Node<E> next = h.next;
						if(previous!=null)
								previous.next = next;
						if(next!=null)
								next.previous = previous;
				}

				private E pollFirstUnlocked(){
						Node<E> t = head;
						E x = null;
						while(t !=null && !placementStrategy.filter((x = t.item), terrace)){
								t = t.next;
								x = null;
						}
						if(x!=null){
								unlink(t);
								terraceFullCondition.signal();
						}
						return x;
				}

				private E pollLastUnlocked(){
						Node<E> t = tail;
						E x = null;
						while(t !=null && !placementStrategy.filter((x = t.item),terrace)){
								t = t.previous;
								x = null;
						}
						if(x!=null){
								unlink(t);
								terraceFullCondition.signal();
						}
						return x;
				}

				private E peekFirstUnlocked() {
						Node<E> t = head;
						E x = null;
						while(t !=null && !placementStrategy.filter((x = t.item),terrace)){
								t = t.next;
								x = null;
						}
						return x;
				}

				private E peekLastUnlocked(){
						Node<E> t = tail;
						E x = null;
						while(t !=null && !placementStrategy.filter((x = t.item), terrace)){
								t = t.previous;
								x = null;
						}
						return x;
				}
		}

		private class TerraceDequeue extends ForwardingDeque<E> implements BlockingDeque<E>{
				private final Terrace terrace;
				private TerraceDequeue(Terrace terrace) { this.terrace = terrace; }

				@Override public E peekFirst() { return terrace.peek(false); }
				@Override public E peekLast() { return terrace.peek(true); }
				@Override public int size() { return terrace.size(); }

				@Override
				public boolean offerFirst(E e) {
						boolean success = terrace.offer(e,false);
						if(success)size.incrementAndGet();
						return success;
				}

				@Override
				public boolean offerFirst(E e, long timeout, TimeUnit unit) throws InterruptedException {
						boolean success = terrace.offer(e,timeout,unit,false);
						if(success)size.incrementAndGet();
						return success;
				}

				@Override
				public boolean offerLast(E e) {
						boolean success = terrace.offer(e,true);
						if(success)size.incrementAndGet();
						return success;
				}

				@Override
				public boolean offerLast(E e, long timeout, TimeUnit unit) throws InterruptedException {
						boolean success = terrace.offer(e,timeout,unit,true);
						if(success)size.incrementAndGet();
						return success;
				}

				@Override
				public E pollFirst() {
						E x = terrace.poll(false);
						if(x!=null)
								size.decrementAndGet();
						return x;
				}

				@Override
				public E pollFirst(long timeout, TimeUnit unit) throws InterruptedException {
						E x = terrace.poll(timeout,unit,true);
						if(x!=null)
								size.decrementAndGet();
						return x;
				}

				@Override
				public E pollLast() {
						E x = terrace.poll(true);
						if(x!=null)
								size.decrementAndGet();
						return x;
				}

				@Override
				public E pollLast(long timeout, TimeUnit unit) throws InterruptedException {
						E x = terrace.poll(timeout,unit,true);
						if(x!=null)
								size.decrementAndGet();
						return x;
				}

				@Override
				public int remainingCapacity() {
						return terrace.softLimit-terrace.size();
				}
		}

		private static class Node<E>{
				E item;

				Node<E> next;
				Node<E> previous;

				public Node(E item) { this.item = item; }
		}

}
