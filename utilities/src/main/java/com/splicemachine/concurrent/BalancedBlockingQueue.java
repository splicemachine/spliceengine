/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.concurrent;

import com.splicemachine.annotations.ThreadSafe;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Exponentially Balanced BlockingQueue.
 *
 * Elements are fed off the queue as in a tiered priority system. Each element is assigned a
 * <em>priority level</em>, which determines what tier that element resides on. Elements are taken off
 * the highest priority level first, then the next highest, and so on. However, because blind priority
 * overrides can lead to starvation of lower-priority items, this also has an overflow valve. After {@code N} elements
 * of one priority are fed, one element of the next level is fed.
 *
 * For example, suppose you use a levelBound of 2 with 3 distinct priority levels. The order of feeding occurs
 * as follows:
 *
 * 1. 2 elements of priority 3 are fed off the queue
 * 2. 1 element of priority 2 is fed off the queue
 * 3. 2 elements of P3 are fed
 * 4. 1 element of P2 is fed
 * 5. 2 elements of P3 are fed
 * 6. 1 element of P1
 *
 * Thus, if the {@code levelBound} is {@code N}, and there are {@code M} priority levels, then the number of
 * elements that must be fed off the queue before an item of priority {@code k} is {@code sum(i=0,i<k){N^(k-i)}}.
 * In this sense, this queue is <em>exponentially fair</em>
 *
 * The above analysis assumes infinitely full levels, so that there are always items available at each priority level.
 * Obviously, in the real world, this is not guaranteed to be true. If a given level has no items, then it will
 * feed an item of the next priority level lower. However, once successfully done, it will reset its level; as long
 * as that level is empty, it will feed lower priority tasks, but as soon as an item becomes available on that level,
 * that level will behave as if it just started the expontial count over again (e.g the next priority level will have
 * to wait until a full level bound number of elements are fed off the level, or the level to become empty again).
 *
 * This class is thread-safe, and implements all the optional operations of Iterator and Collection.
 *
 *
 * @author Scott Fines
 * Created on: 4/11/13
 */
public class BalancedBlockingQueue<E> extends AbstractQueue<E> implements BlockingQueue<E> {
    /*
     * Thread-safety is performed by a split-lock: There is one gate for take and poll operations, and a separate
     * lock for offer and put operations. Thus, adding a single item to this queue need not wait for someone else
     * to take an item first.
    */
    private final ReentrantLock takeLock = new ReentrantLock();
    private final Condition notEmpty = takeLock.newCondition();

    private final ReentrantLock offerLock = new ReentrantLock();

    private final AtomicInteger count = new AtomicInteger(0);

    private final Level<E> firstLevel;
    private final int numLevels;

    @ThreadSafe
    public interface PriorityFunction<E>{

        int getPriority(E item);
    }
    /*Must be Thread-safe, as it is NOT called under a lock!*/
    private final PriorityFunction<E> priorityMapper;

    public BalancedBlockingQueue(int numLevels,int levelBound,PriorityFunction<E> priorityMapper) {
        this.numLevels = numLevels;
        this.priorityMapper = priorityMapper;

        Level<E> firstLevel = new Level<E>(levelBound);
        Level<E> previousLevel = firstLevel;
        for(int i=1;i<numLevels;i++){
            Level<E> currentLevel = new Level<E>(numLevels);
            previousLevel.next = currentLevel;

            previousLevel = currentLevel;
        }
        this.firstLevel = firstLevel;
    }

    @Override
    public Iterator<E> iterator() {
        return new Itr();
    }

    @Override
    public int size() {
        return count.get();
    }

    @Override
    public void put(E e) throws InterruptedException {
        //TODO -sf- implement once capacity is defined
        offer(e);
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        //TODO -sf- implement once capacity is defined
        return offer(e);
    }

    @Override
    public E take() throws InterruptedException {
        E x;
        int c = -1;

        takeLock.lockInterruptibly();
        try{
            while(count.get()==0){
                notEmpty.await();
            }
            x = firstLevel.poll();
            c = count.getAndDecrement();
            if(c >1)
                notEmpty.signal();
        }finally{
            takeLock.unlock();
        }
        //TODO -sf- uncomment when capacity is implemented
        /*
        if (c == capacity)
            signalNotFull();
         */
        return x;
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        E x = null;
        int c = -1;
        long nanos = unit.toNanos(timeout);

        takeLock.lockInterruptibly();
        try{
            while(count.get()==0){
                if(nanos <=0) return null;
                nanos = notEmpty.awaitNanos(nanos);
            }
            x = firstLevel.poll();
            c = count.getAndDecrement();
            if(c > 1)
                notEmpty.signal();
        }finally{
            takeLock.unlock();
        }
        //TODO -sf- uncomment when capacity is implemented
        /*
        if (c == capacity)
            signalNotFull();
         */
        return x;
    }

    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE; //TODO -sf- implement when capacity is implemented
    }

    @Override
    public boolean remove(Object o) {
        if(o==null) return false;
        fullyLock();
        try{
            for(Level<E> level = firstLevel;level!=null;level = level.next){
                if(level.remove(o)) return true;
            }
            return false;
        }finally{
            fullyUnlock();
        }
    }

    @Override
    public boolean contains(Object o) {
        if(o==null) return false;
        fullyLock();
        try{
            for(Level<E> level = firstLevel;level!=null;level = level.next){
                if(level.contains(o)) return true;
            }
            return false;
        }finally{
            fullyUnlock();
        }
    }

    @Override
    public Object[] toArray() {
        fullyLock();
        try{
            int size = count.get();
            Object[] a= new Object[size];
            int position =0;
            for(Level<E> level = firstLevel;level!=null;level = level.next){
                position+=level.toArray(a,position);
            }
            return a;
        }finally{
            fullyUnlock();
        }
    }

    @Override
    public <T> T[] toArray(T[] a) {
        fullyLock();
        try{
            int size = count.get();
            if(a.length< size){
                a = (T[])java.lang.reflect.Array.newInstance(a.getClass().getComponentType(),size);
            }
            int position = 0;
            for(Level<E> level = firstLevel;level!=null;level = level.next){
                position+=level.toArray(a,position);
            }
            return a;
        }finally{
            fullyUnlock();
        }
    }

    @Override
    public void clear() {
        fullyLock();
        try{
           for(Level<E> level = firstLevel;level!=null;level = level.next){
               level.clear();
           }
            count.getAndSet(0);
        }finally{
            fullyUnlock();
        }
    }

    private void fullyUnlock() {
        takeLock.unlock();
        offerLock.unlock();
    }

    private void fullyLock() {
        offerLock.lock();
        takeLock.lock();;
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        return drainTo(c,Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        //TODO -sf- deal with capacity stuff
        assert c!=null : "no collection to drain to!";
        assert c!=this : "Cannot drain into itself";

        takeLock.lock();
        int drainedCount=0;
        E x;
        try{
            int n = Math.min(maxElements,count.get());
            while(drainedCount<n){
                x = firstLevel.poll();
                c.add(x);
                drainedCount++;
            }
            return n;
        }finally{
            if(drainedCount>0){
                count.getAndAdd(-drainedCount);
            }
            takeLock.unlock();
        }
    }

    @Override
    public boolean offer(E e) {
        assert e!=null: "cannot add null elements";
        Level<E> queueLevel = getLevel(e);

        int c = -1;
        offerLock.lock();
        try{
            if(queueLevel.offer(e)){
                c = count.getAndIncrement();
                //TODO -sf- uncomment when capacity is enabled
                /*
                 if( c+1 < capacity)
                    notFull.signal()
                 */
            }
        }finally{
            offerLock.unlock();
        }
        if(c==0)
            signalNotEmpty();
        return c >=0;
    }

    @Override
    public E poll() {
        if(count.get()==0) return null;
        E x = null;
        int c = -1;
        takeLock.lock();
        try{
            if(count.get()>0){
                x = firstLevel.poll();
                c = count.getAndDecrement();
                if(c >1)
                    notEmpty.signal();
            }
        }finally{
            takeLock.unlock();
        }
        //TODO -sf- uncomment when capacity is implemented!
        /*
            if(c==capacity)
                signalNotFull();
         */
        return x;
    }

    @Override
    public E peek() {
        if(count.get()==0) return null;

        takeLock.lock();
        try{
            return firstLevel.peek();
        }finally{
            takeLock.unlock();
        }
    }


    /******************************************************************************************************/
    /*private helper methods*/

    private class Itr implements Iterator<E>{

        private Node<E> current;
        private Node<E> lastRet;

        private Level<E> currentLevel;
        private E currentElement;

        private Itr() {
            currentLevel = firstLevel;
            fullyLock();
            try{
                current = currentLevel.head.next;
                if(current!=null)
                    currentElement = current.item;
            }finally{
                fullyUnlock();
            }
        }

        @Override
        public boolean hasNext() {
            return current!=null;
        }

        @Override
        public E next() {
            fullyLock();
            try{
                if(current==null)
                    throw new NoSuchElementException();
                E x = currentElement;
                lastRet = current;
                current = nextNode(current);
                currentElement = (current==null)? null: current.item;
                return x;
            }finally{
                fullyUnlock();
            }
        }

        private Node<E> nextNode(Node<E> p) {
            for(;;){
                Node<E> s = p.next;
                if(s == p)
                    return currentLevel.head.next;
                if(s==null){
                   if(currentLevel.next!=null){
                       currentLevel = currentLevel.next;
                       return nextNode(p);
                   }else return null;
                }
                if(s.item!=null)
                    return s;
                p = s;
            }
        }

        @Override
        public void remove() {
            if(lastRet==null)
                throw new IllegalStateException();
            fullyLock();
            try{
                Node<E> node = lastRet;
                lastRet = null;
                for(Node<E> trail = currentLevel.head,p = trail.next;p!=null;trail = p,p = p.next){
                    if(p == node){
                        currentLevel.unlink(trail,p);
                        currentLevel.count.getAndDecrement();
                        break;
                    }
                }
            }finally{
                fullyUnlock();
            }
        }
    }

    private Level<E> getLevel(E e) {
        //find the right level
        int level = priorityMapper.getPriority(e);
        if(level>numLevels) level = numLevels;
        if(level <1) level = 1;

            /*
             * Since higher numbers indicate higher priority, they are actually lower in the
             * Level list. So we need to invert the priority to find the position
             */
        int levelPos = numLevels-level;
        Level<E> queueLevel = firstLevel;
        for(int i=0;i<levelPos;i++){
            queueLevel = queueLevel.next;
        }
        return queueLevel;
    }

    private void signalNotEmpty() {
        /*
         * Used to notify anyone waiting to take an item that something just got added
         */
        takeLock.lock();
        try{
            notEmpty.signal();
        }finally{
            takeLock.unlock();
        }
    }

    /*
     * All operations within this class are not thread-safe. It is assumed that all operations
     * occur under either the takeLock or the offerLock.
     *
     */
    private class Level<E> {
        private Level<E> next;

        /* head.item == null always */
        private Node<E> head;

        private Node<E> tail;

        /*
         * The number of items we've fed from this level. When it reaches iterationBound, then we reset
         * and start again from zero.
         */
        private int fedCount = 0;

        private final int iterationBound;
        /*
         * Must be atomic, because it's accessed from under two different locks (takeLock and offerLock).
         */
        private AtomicInteger count = new AtomicInteger(0);

        private Level(int levelBound) {
            this.iterationBound = levelBound;

            head = tail = new Node<E>(null);
        }

        E poll(){
            /*
             * The assumption is that
             */
            assert takeLock.isHeldByCurrentThread();
            E x;
            if(next==null){
               /*
                * We are the lowest possible priority in the queue; act just like a normal queue
                */
                if(count.get()==0) return null; //nothing on our queue
                x = dequeue();
                count.getAndDecrement();
            }else if(count.get()==0){
                /*
                 * This level is empty. Try and get one from a level below us. If a level below us
                 * has a task, then reset fedCount. If All levels below are empty, just return null
                 */
                x = next.poll();
                if(x!=null){
                     // We've essentially upgraded the priority of x. Reset fedCount to prevent further upgrading
                    fedCount=0;
                }
            }else if(fedCount==iterationBound){
                /*
                 * We are at our inflection point. Try and take an item which is lower priority than us. If
                 * we don't get an item back, take one from our queue instead. We can't be empty (or else a
                 * previous condition would be selected).
                 */
                fedCount=0;
                x = next.poll();
                if(x==null){
                    //no lower-priority tasks to do. That's cool. just take another item from our level
                    x = dequeue();
                    count.getAndDecrement();
                }
            }else{
                /*
                 * We are not empty, the lowest level, or at our inflection point. Take an item direct from our queue
                 */
                x = dequeue();
                count.getAndDecrement();
                fedCount++;
            }

            return x;
        }

        E peek(){
            assert takeLock.isHeldByCurrentThread();
            if(next==null){
                if(count.get()==0) return null;
                Node<E> first = head.next;
                if(first==null) return null;
                else
                    return first.item;
            }else if(count.get()==0||fedCount==iterationBound){
                return next.peek();
            }else{
                Node<E> first = head.next;
                if(first==null){
                    return next.peek();
                }else
                    return first.item;
            }
        }

        boolean offer(E item){
            //make sure we're safely locked
            assert offerLock.isHeldByCurrentThread();

            Node<E> node = new Node<E>(item);
            tail = tail.next = node;
            count.getAndIncrement();
            //TODO -sf- capacity bound this queue?
            return true;
        }

        private E dequeue(){
            Node<E> h = head;
            Node<E> first = h.next;
            h.next = h;
            head = first;
            E x = first.item;
            first.item = null;
            return x;
        }

        public boolean remove(Object o) {
            for(Node<E> trail = head, p = trail.next;p!=null;trail = p,p = p.next){
                if(o.equals(p.item)){
                    unlink(trail, p);
                    return true;
                }
            }
            return false;
        }

        private void unlink(Node<E> trail, Node<E> p) {
            p.item = null;
            trail.next = p.next;
            if(tail == p)
                tail = trail;
            count.getAndDecrement();
        }

        public boolean contains(Object o) {
            for(Node<E> p = head.next;p!=null;p = p.next){
                if(o.equals(p.item)){
                    return true;
                }
            }
            return false;
        }

        public int toArray(Object[] a,int position) {
            int spot = position;
            for(Node<E> p = head.next;p!=null;p = p.next){
                a[spot++] = p.item;
            }
            return spot;
        }

        public void clear() {
            for(Node<E> p,h = head; (p = h.next)!=null;h=p){
                h.next = h;
                p.item = null;
            }
            head = tail;
            count.getAndSet(0);
        }
    }

    private static class Node<E>{
        E item;

        Node<E> next;

        Node(E item) { this.item = item;}
    }

}
