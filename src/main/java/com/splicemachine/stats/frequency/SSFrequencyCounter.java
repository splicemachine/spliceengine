package com.splicemachine.stats.frequency;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.splicemachine.hash.Hash32;

import javax.annotation.Nullable;
import java.lang.reflect.Array;
import java.util.*;

/**
 * Base Frequency counter using the <em>SpaceSaver</em> algorithm(see
 * <a href="https://icmi.cs.ucsb.edu/research/tech_reports/reports/2005-23.pdf" />
 * for the formal paper on the algorithm).
 *
 * <p>SpaceSaver allows you to estimate the solution to two separate problems:
 * <em>Frequent Elements</em> and <em>Top K</em>. Frequent Elements returns all the elements which
 * occur more frequently than a set number of occurrences (e.g. all elements which occur more than 1% of the time),
 * while Top-K find the {@code k} most frequent elements (e.g. the top 10 most frequent elements).</p>
 *
 * //TODO -sf- add documentation on the algorithm
 * @author Scott Fines
 * Date: 3/25/14
 */
class SSFrequencyCounter<T> implements FrequencyCounter<T> {
		//the maximum size of the hash table
		private static final int MAXIMUM_CAPACITY = 1 << 30;

		/*Comparator that sorts frequency estimates according to their estimated count*/
		private static final Comparator<FrequencyEstimate<?>> countComparator = new Comparator<FrequencyEstimate<?>>() {
				@Override
				public int compare(FrequencyEstimate<?> o1, FrequencyEstimate<?> o2) {
						int compare =  o1.count() < o2.count() ? 1 : o1.count() > o2.count() ? -1 : 0;
						if(compare!=0) return compare;
						compare = o1.error() < o2.error() ? -1 : o1.error() > o2.error() ? 1:0;
						if(compare!=0) return compare;
						/*
						 * At this point, the two elements are logically equivalent, and the only thing to do is
						 * distinguish based on the element's value. However, since the element itself isn't comparable, we
						 * can't do much about it. However, we also don't care about their order now. Taking their hashCode is a
						 * reasonable way of dealing with this. It doesn't order data, of course, but in most implementations
						 * it WILL distinguish between values, and at the very least it will distinguish between different object
						 * instances, so we won't lose anything
						 */
						return o1.hashCode()-o2.hashCode();
				}
		};

		/*Comparator that sorts frequency estimates according to their guaranteed count*/
		private static final Comparator<FrequencyEstimate<?>> guaranteedCountComparator = new Comparator<FrequencyEstimate<?>>() {
				@Override
				public int compare(FrequencyEstimate<?> o1, FrequencyEstimate<?> o2) {
						long g1 = o1.count()-o1.error();
						long g2 = o2.count()-o2.error();
						int compare = g1 < g2 ? 1 : g1 > g2 ? -1 : 0;
						if(compare!=0) return compare;
						/*
						 * If the two entities are the same here, then we just need to create a distinction that doesn't
						 * rely on us assuming Comparability of the elements. Easiest way to do that is to take their hash code.
						 * unless the hash code implementation is COMPLETELY crap (e.g. return 1), this will work.
						 */
						return o1.hashCode()-o2.hashCode();
				}
		};

		/* The maximum number of elements to store */
		private final int maxSize;
		/* The current capacity of the hashtable. */
		protected int capacity;
		/* The hashtable itself */
    protected int[] hashCodes;
		protected Element[] hashtable;
    protected int longestProbeLength = 0;
    protected int expansionLimit; //the point at which we need to expand the hash table
    private final float loadFactor; //the loadFactor
		/*
		 * Hash functions for multiple-hashing on the hashtable. The larger the array, the less memory is required,
		 * but the more steps may be necessary to insert a new entry.
		 */
		protected final Hash32 hashFunction;

		/* The current number of occupied counters. */
		protected int size;
		/*
		 * A linked list of Buckets, kept in ascending order. Thus, the first non-null entry
		 * in buckets points to the minimum count;
		 */
		private SizeBucket minBucket = new SizeBucket(1l);

		/* Convenience handler so that we can iterate the linked list in reverse order*/
		private SizeBucket maxBucket = minBucket;

		/* Keeps track of the total number of elements seen. Needed for frequent elements estimates */
		protected int elementsSeen;

    protected SSFrequencyCounter(int maxSize,
                                 int initialCapacity,
                                 Hash32 hashFunction) {
        this(maxSize, initialCapacity, hashFunction,0.9f);
    }

		protected SSFrequencyCounter(int maxSize,
																 int initialCapacity,
																 Hash32 hashFunction,float loadFactor) {
				this.hashFunction = hashFunction;

				this.maxSize = maxSize;

				int initialCap = Math.min(initialCapacity,maxSize);
				int s = 1;
				while(s<initialCap)
						s<<=1;
				this.capacity = s;
				this.hashtable = newHashArray(capacity);
        this.hashCodes = new int[capacity];
        this.loadFactor = loadFactor;
        this.expansionLimit = (int)(loadFactor*capacity);
		}

		@Override
		public void update(T item, long count) {
				assert item!=null: "Null values are not allowed!";

        Element staleElement = doPut(item,count);

        if(staleElement!=null){
            //adding a new element. If necessary, evict an old
            size++;
            staleElement.epsilon = evictAndSet(staleElement);
            staleElement.setValue(item);
            increment(staleElement,count);

						staleElement.stale = false;
						elementsSeen++;
        }

        if(size>=expansionLimit){
            //make sure we are big enough for the next one
            expandCapacity();
        }
    }

    protected Element doPut(T item, long count){
        int hashCode = hash(item,hashFunction);
        int pos = hashCode & (capacity - 1);
        int probeLength=0;
        Element staleElement = null;
        Element toPlace = null;
        while(true){
            int hC = hashCodes[pos];
            if(hC==hashCode){
                Element e = hashtable[pos];
                if(e==null){
                    if(toPlace==null) toPlace= staleElement= newElement();

                    hashtable[pos] = toPlace;
                    break;
                }else if(e.stale){
                    staleElement = e;
                    break;
                }else if(e.matchingValue(item)){
                    elementsSeen++;
                    increment(e,count);
                    return null;
                }
            }else if(hC==0){
                //we have an empty slot, so place it directly
                hashCodes[pos] = hashCode;
                if(toPlace==null) toPlace = staleElement = newElement();
                hashtable[pos] = toPlace;
                break;
            }
            int pC = getProbeDistance(pos,hC);
            if(pC>probeLength){
                /*
                 * We've reached an element with a shorter probe length than us. In this case,
                 * swap the element, and adjust the probe length
                 */
                Element e = hashtable[pos];
                hashCodes[pos] = hashCode;
                if(toPlace==null) toPlace = staleElement = newElement();
                hashtable[pos] = toPlace;
                toPlace= e;
                probeLength = pC;
                hashCode = hC;
            }
            probeLength++;
            pos = (pos+1) & (capacity-1);
        }
        if(longestProbeLength<probeLength)
            longestProbeLength = probeLength;
        return staleElement;
    }

    protected int getProbeDistance(int currentPosition, int hash) {
        /*
         * Get the distance from the current position to where the hashCode
         * says it *should* be located.
         */
        int hc = (hash & (capacity-1));
        int dist = currentPosition-hc;
        if(currentPosition<hc)
            dist+=capacity;
        return dist;
    }

    @Override
		public void update(T item) {
				update(item,1l);
		}

		@Override
		public Set<FrequencyEstimate<T>> getFrequentElements(final float support) {
				Set<FrequencyEstimate<T>> freqElements = new TreeSet<FrequencyEstimate<T>>(countComparator);
				final long threshold = (long)Math.ceil(support*elementsSeen);
				Iterator<FrequencyEstimate<T>> iter = Iterators.filter(iterator(), new Predicate<FrequencyEstimate<T>>() {
						@Override
						public boolean apply(@Nullable FrequencyEstimate<T> input) {
								//noinspection ConstantConditions
								return (input.count() - input.error()) >= threshold;
						}
				});
				while(iter.hasNext()){
						FrequencyEstimate<T> next = iter.next();
						freqElements.add(next);
				}
				return freqElements;
		}

		@Override
		public Set<FrequencyEstimate<T>> getMostFrequentElements(int k) {
				Set<FrequencyEstimate<T>> topK = new TreeSet<FrequencyEstimate<T>>(guaranteedCountComparator);
				Iterator<FrequencyEstimate<T>> iter = new LinkedIterator(maxBucket);
				int visited =0;
				while(visited<k && iter.hasNext()){
						topK.add(iter.next());
						visited++;
				}
				return topK;
		}

		//convenient for debugging purposes, since debuggers will use this to look at data
		@Override public Iterator<FrequencyEstimate<T>> iterator() { return new LinkedIterator(maxBucket); }

		/**
		 * Hash the element using the hash function. This is mainly in place so that subclasses
		 * can hash elements efficiently (avoiding autoboxing, etc.)
		 *
		 * @param item the item to hash. Guaranteed to be non-null.
		 * @param hashFunction the hash function to hash with.
		 * @return the hash of the item;
		 */
		protected int hash(T item, Hash32 hashFunction){
				return hashFunction.hash(item.hashCode());
		}

		/**
		 * Hash the element using the hash function. The default implementation delegates to
		 * {@link #hash(Object, com.splicemachine.hash.Hash32)}, but subclasses are free
		 * to override this for more efficient hashing when possible.
		 *
		 * @param element the element to hash
		 * @param hashFunction the hash function to hash with.
		 * @return the hash of the element's value;
		 */
		protected int hash(Element element, Hash32 hashFunction){
				return hash(element.value(), hashFunction);
		}

		/**
		 * Construct a new array of elements. Used to resize the Hashtable.
		 *
		 * Subclasses are allowed to override this to provide more precise implementations of the element.
		 *
		 * @param size the size of the array to build.
		 * @return a new array of size {@code size}
		 */
		protected Element[] newHashArray(int size){
				//noinspection unchecked
				return (TElement[])Array.newInstance(TElement.class,size);
		}

		/**
		 * Construct a new element.
		 *
		 * Subclasses are allowed to override this to provide a more precise implementation.
		 *
		 * @return a new Element entity.
		 */
		protected Element newElement(){ return new TElement(); }

		/**
		 * Expand the capacity of the hash table.
		 */
		protected final void expandCapacity() {
				if(capacity==MAXIMUM_CAPACITY) return;
				int newCapacity =2*capacity;

        int[] oldHashCode = hashCodes;
        Element[] oldHashtable = hashtable;

        hashCodes= new int[newCapacity];
				hashtable = newHashArray(newCapacity);
        capacity = newCapacity;
        expansionLimit = (int)(loadFactor*capacity);
        for(int i=0;i<oldHashCode.length;i++){
            int hashCode = oldHashCode[i];
            if(hashCode==0) continue;

            int pos = hashCode & (capacity-1);
            Element toPlace = oldHashtable[i];
            int probeLength = 0;
            while(true){
                int hC = hashCodes[pos];
                if(hC==0){
                    hashCodes[pos] = hashCode;
                    hashtable[pos] = toPlace;
                    break;
                }
                int pC = getProbeDistance(pos,hC);
                if(pC>probeLength){
                /*
                 * We've reached an element with a shorter probe length than us. In this case,
                 * swap the element, and adjust the probe length
                 */
                    Element e = hashtable[pos];
                    hashCodes[pos] = hashCode;
                    hashtable[pos] = toPlace;
                    toPlace= e;
                    probeLength = pC;
                    hashCode = hC;
                }
                probeLength++;
                pos = (pos+1) & (capacity-1);
            }
        }
		}

		/**
		 * Increments the counter for the specified element.
		 *
		 * @param element the element whose counter needs to increment
		 */
		protected final void increment(Element element, long count) {
				SizeBucket formerBucket = element.bucket;
				if(formerBucket==null){
						SizeBucket newBucket = getBucket(null,count);
						link(element,newBucket);
						newBucket.numElements++;
						return;
				}
				long newCount = formerBucket.count+count;

				SizeBucket newBucket = getBucket(formerBucket,newCount);

				if(newBucket!=formerBucket){
						//remove from old bucket and add to new
						unlink(element, formerBucket);
						decrementBucket(formerBucket);
						link(element,newBucket);
						newBucket.numElements++;
				}
		}

		/**
		 * Link the specified element with the specified bucket.
		 *
		 * @param element the element to insert
		 * @param bucket the bucket containing the element's count.
		 */
		protected final void link(Element element, SizeBucket bucket) {
				Element e = bucket.e;
				if(e==null){
						bucket.e = element;
						element.next = null;
						element.previous = null;
						element.bucket = bucket;
						return;
				}
				Element n = e.next;
				while(n!=null &&n.epsilon<element.epsilon){
						e = n;
						n = n.next;
				}
				element.previous = e;
				element.next = e.next;
				if(e.next!=null)
						e.next.previous = element;
				e.next = element;

				element.bucket = bucket;
		}

		/**
		 * Evict an entry (if necessary). If an eviction occurs, {@code newElement}
		 * will have it's bucket and epsilon values set, and will automatically be linked to the
		 * correct bucket.
		 *
		 * @param newElement the new element to be inserted and/or modified
		 * @return the estimated error in the eviction, or 0 if no evicition was needed.
		 */
		protected final long evictAndSet(Element newElement) {
				if(size<=maxSize)
						return 0l; //we don't need to evict

				SizeBucket bucket = minBucket;
				Element e = bucket.e;
				//unlink e
				unlink(e,bucket);
				link(newElement,bucket);
				newElement.bucket = bucket;
				newElement.epsilon = bucket.count;
				size--;
				e.stale = true;
				return bucket.count;
		}

		/*
		 * Get the correct bucket for the specified count. If no bucket with {@code count}
		 * exists, this method will create one and attach it in the proper location before returning.
		 *
		 * @param startBucket the bucket to start at (small performance optimization)
		 * @param count the count for the bucket to get.
		 * @return the Bucket which has the specified count.
		 */
		private SizeBucket getBucket(SizeBucket startBucket, long count) {
				if(startBucket==null)
						startBucket = minBucket;

				//look for the bucket that == your count. If none exists, create it
				SizeBucket bucket = startBucket;
				if(bucket == maxBucket && bucket.numElements==1 && bucket.count<count){
						bucket.count = count;
						return bucket;
				}
				SizeBucket next = bucket.next;
				while(next!=null && next.count<=count){
						bucket = next;
						next = bucket.next;
				}
				if(bucket.count<count){
						if(bucket==startBucket && bucket.numElements==1 &&bucket.count < count){
								bucket.count = count;
								return bucket;
						}
						//add a new bucket
						SizeBucket newBucket = new SizeBucket(count);
						newBucket.previous = bucket;
						newBucket.next = bucket.next;
						if(bucket.next!=null)
								bucket.next.previous = newBucket;
						bucket.next = newBucket;
						if(bucket==maxBucket)
								maxBucket = newBucket;
						return newBucket;
				}else if(bucket.count>count){
						//add a new bucket
						SizeBucket newBucket = new SizeBucket(count);
						newBucket.previous = bucket.previous;
						newBucket.next = bucket;
						bucket.previous = newBucket;
						if(bucket==minBucket)
								minBucket= newBucket;
						return newBucket;
				} else{
						//bucket is ==, so return
						return bucket;
				}
		}

		/* Removes the specified element from the bucket */
		private void unlink(Element element, SizeBucket bucket) {
				if(element.previous!=null)
						element.previous.next = element.next;
				if(element.next!=null)
						element.next.previous = element.previous;

				if(bucket.e==element){
						bucket.e = element.next;
				}
		}

		/* Decrement's the specified bucket's element count. If the element count reaches 0, it is deleted */
		private void decrementBucket(SizeBucket bucket) {
				bucket.numElements--;
				if(bucket.numElements<=0){
						if(bucket.next!=null)
								bucket.next.previous = bucket.previous;
						if(bucket.previous!=null)
								bucket.previous.next = bucket.next;
						if(bucket==maxBucket){
								//move maxBucket
								maxBucket = bucket.previous;
						}
						if(bucket==minBucket){
								minBucket = bucket.next;
						}
				}
		}

		/**
		 * Represents an (item, count, epsilon) triplet.
		 *
		 * Subclasses are expected to provide an optimally-efficient storage container (e.g. use primitives
		 * instead of objects).
		 */
		protected abstract class Element implements FrequencyEstimate<T>{
				private Element next;
				private Element previous;
				private SizeBucket bucket;

				/**
				 * The estimated error in the count.
				 */
				protected long epsilon;
				/**
				 * A reference to whether or not this Element is <em>stale</em>--that is, that
				 * the object itself is still in the hashmap, but the entity that it wraps out should be
				 * considered logically absent. This way, Element objects can be re-used whenever possible, as
				 * opposed to creating new wrappers each time.
				 */
				protected boolean stale = true; //when true, indicates that this Element should be treated as absent

				@Override public long count() { return bucket.count; }
				@Override public long error() { return epsilon; }
				@Override public String toString() { return "("+value()+","+bucket.count+","+epsilon+")"; }

				/**
				 * Set the latest value on the element. Subclasses may not necessarily use this,
				 * if they have a more efficient mechanism of performing the same thing.
				 *
				 * @param value the value to set
				 */
				public abstract void setValue(T value);

				/**
				 * Determines if the specified item matches that held by this instance.
				 *
				 * @param item the item to check.
				 * @return true if the item matches the value stored in this element, false otherwise
				 */
				public abstract boolean matchingValue(T item);
		}

		/**
		 * Default implementation of an Element. Useful for Objects, but should be avoided
		 * in the event of primitives (and replaced with primitive-specific implementations).
		 */
		protected final class TElement extends Element {
				private T value;

				@Override public void setValue(T value) { this.value = value; }
				@Override public boolean matchingValue(T item) { return value.equals(item); }
				@Override public T value() { return value; }
		}

		/*
		 * Represents a group of elements which have the same count. Kept in sorted order internally.
		 * Subclasses should <em>never</em> create their own.
		 * Use {@link #getBucket(com.splicemachine.stats.frequency.SSFrequencyCounter.SizeBucket, long)}
		 * instead.
		 */
		private class SizeBucket implements Comparable<SizeBucket>{
				private long count;
				private Element e;
				private SizeBucket next;
				private SizeBucket previous;
				private int numElements = 0;

				private SizeBucket(long count) { this.count = count; }
				@Override public int compareTo(SizeBucket o) { return count < o.count? -1: count > o.count? 1: 0; }
		}

		/*
		 * Iterator which iterates over the stored data in reverse order (highest counted elements first).
		 */
		private class LinkedIterator implements Iterator<FrequencyEstimate<T>> {
				private SizeBucket currentBucket;
				private Element nextElement;

				public LinkedIterator(SizeBucket maxBucket) {
						this.currentBucket = maxBucket;
						this.nextElement = currentBucket.e;
				}

				@Override
				public boolean hasNext() {
						if(nextElement !=null){
								return true;
						}
						while(nextElement ==null && currentBucket.previous!=null){
								currentBucket = currentBucket.previous;
								nextElement = currentBucket.e;
						}
						return nextElement !=null;
				}

				@Override
				public FrequencyEstimate<T> next() {
						Element e = nextElement;
						nextElement = nextElement.next;
						return e;
				}

				@Override public void remove() { throw new UnsupportedOperationException(); }
		}
}
