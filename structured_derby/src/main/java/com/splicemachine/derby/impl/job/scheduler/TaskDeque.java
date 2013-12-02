package com.splicemachine.derby.impl.job.scheduler;

import com.google.common.base.Predicate;
import com.splicemachine.job.Task;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A Blocking Deque which
 * @author Scott Fines
 * Date: 11/27/13
 */
public class TaskDeque<T extends Task> {

		private final Predicate<T> runnablePredicate;

		private final ReentrantLock lock = new ReentrantLock();
		private final Condition notEmpty = lock.newCondition();

		/**
		 * The limit on the number of entries for this queue. This
		 * is soft, as a caller can call forcePut() which will force
		 * the queue to accept an entry
		 */
		private final int softSizeLimit;

		private final Data<T> data;

		public TaskDeque(int softSizeLimit,Predicate<T> runnablePredicate) {
				this.runnablePredicate = runnablePredicate;
				this.softSizeLimit = softSizeLimit;
				this.data = new Data<T>();
		}

		public T poll(){
				lock.lock();
				try{
						return data.poll(runnablePredicate);
				}finally{
						lock.unlock();
				}
		}

		public T poll(long timeout, TimeUnit unit) throws InterruptedException{
				long timeLeftNanos = unit.toNanos(timeout);
				lock.lockInterruptibly();
				try{
						T x;
						while((x = data.poll(runnablePredicate))==null){
								if(timeLeftNanos <=0) return null;
								timeLeftNanos = notEmpty.awaitNanos(timeLeftNanos);
						}
						return x;
				}finally{
						lock.unlock();
				}
		}

		public T pollLast(){
				try{
						return data.pollLast(runnablePredicate);
				}finally{
						lock.unlock();
				}
		}

		public T pollLast(long timeout, TimeUnit unit) throws InterruptedException{
				long timeLeftNanos = unit.toNanos(timeout);
				lock.lockInterruptibly();
				try{
						T x;
						while((x = data.pollLast(runnablePredicate))==null){
								if(timeLeftNanos <=0) return null;
								timeLeftNanos = notEmpty.awaitNanos(timeLeftNanos);
						}
						return x;
				}finally{
						lock.unlock();
				}
		}

		public boolean offer(T item){
				if(data.count.get()>=softSizeLimit) return false;
				lock.lock();
				try{
						if(data.count.get()>=softSizeLimit) return false;
						data.offer(item);
						return true;
				}finally{
						lock.unlock();
				}
		}

		public void forcePut(T item){
				lock.lock();
				try{
						data.offer(item);
				}finally{
						lock.unlock();
				}
		}

		public int size(){
				return data.count.get();
		}

		private class Data<T>{
				private Node<T> head;
				private Node<T> tail;

				private AtomicInteger count = new AtomicInteger(0);

				private Data() {
						head = tail = new Node<T>(null);
				}

				T poll(Predicate<T> takePredicate){
						assert lock.isHeldByCurrentThread();
						return dequeueHead(takePredicate);
				}

				T pollLast(Predicate<T> takePredicate){
						assert lock.isHeldByCurrentThread();
						return dequeueTail(takePredicate);
				}

				void offer(T item){
						assert lock.isHeldByCurrentThread();

						Node<T> t = tail;
						Node<T> newNode = new Node<T>(item);
						newNode.previous = t;
						t.next = newNode;
						tail = newNode;
						count.incrementAndGet();
				}

				private T dequeueTail(Predicate<T> takePredicate) {
						Node<T> t = tail;
						T item = t.item;
						while(t!=null && !takePredicate.apply(item)){
								t =t.previous;
								if(t!=null)
										item = t.item;
						}
						if(t==null) return null;
						unlink(t);
						return item;
				}

				private T dequeueHead(Predicate<T> takePredicate){
						Node<T> h = head;
						T item = h.item;
						while(h!=null && !takePredicate.apply(item)){
								h = h.next;
								if(h!=null)
										item = h.item;
						}
						if(h==null) return null;

						unlink(h);
						return item;
				}

				private void unlink(Node<T> h) {
						Node<T> next = h.next;
						Node<T> previous = h.previous;
						previous.next = next;
						next.previous = previous;
				}

		}

		private static class Node<E>{
				E item;

				Node<E> next;
				Node<E> previous;

				Node(E item) { this.item = item;}
		}
}
