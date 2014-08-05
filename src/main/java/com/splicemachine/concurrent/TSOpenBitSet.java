package com.splicemachine.concurrent;

import java.util.Arrays;
import java.io.Serializable;
import java.util.BitSet;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * An "open" BitSet implementation that allows direct access to the arrays of words
 * storing the bits.  Derived from Lucene's OpenBitSet, but with a paged backing array
 * (see bits delaration, below).
 * <p/>
 * Unlike java.util.bitset, the fact that bits are packed into an array of longs
 * is part of the interface.  This allows efficient implementation of other algorithms
 * by someone other than the author.  It also allows one to efficiently implement
 * alternate serialization or interchange formats.
 * <p/>
 * <code>OpenBitSet</code> is faster than <code>java.util.BitSet</code> in most operations
 * and *much* faster at calculating cardinality of sets and results of set operations.
 * It can also handle sets of larger cardinality (up to 64 * 2**32-1)
 * <p/>
 * The goals of <code>OpenBitSet</code> are the fastest implementation possible, and
 * maximum code reuse.  Extra safety and encapsulation
 * may always be built on top, but if that's built in, the cost can never be removed (and
 * hence people re-implement their own version in order to get better performance).
 * If you want a "safe", totally encapsulated (and slower and limited) BitSet
 * class, use <code>java.util.BitSet</code>.
 */

public class TSOpenBitSet implements Serializable {
  /**
   * We break the bitset up into multiple arrays to avoid promotion failure caused by attempting to allocate
   * large, contiguous arrays (CASSANDRA-2466).  All sub-arrays but the last are uniformly PAGE_SIZE words;
   * to avoid waste in small bloom filters (of which Cassandra has many: one per row) the last sub-array
   * is sized to exactly the remaining number of words required to achieve the desired set size (CASSANDRA-3618).
   */
  private final AtomicLongArray[] bits;
  private int wlen; // number of words (elements) used in the array
  private final int pageCount;
  private static final int PAGE_SIZE = 4096;

  /**
   * Constructs an OpenBitSet large enough to hold numBits.
   * @param numBits
   */
  public TSOpenBitSet(long numBits) {
      wlen = bits2words(numBits);
      int lastPageSize = wlen % PAGE_SIZE;
      int fullPageCount = wlen / PAGE_SIZE;
      pageCount = fullPageCount + (lastPageSize == 0 ? 0 : 1);
      bits = new AtomicLongArray[pageCount];
      for (int i = 0; i < fullPageCount; ++i)
          bits[i] = new AtomicLongArray(PAGE_SIZE);
      if (lastPageSize != 0)
          bits[bits.length - 1] = new AtomicLongArray(lastPageSize);
  }

  public TSOpenBitSet() {
    this(64);
  }

  /**
   * @return the pageSize
   */
  public int getPageSize() {
      return PAGE_SIZE;
  }
  
  public int getPageCount() {
      return pageCount;
  }

  public AtomicLongArray getPage(int pageIdx) {
      return bits[pageIdx];
  }
  
  /** Contructs an OpenBitset from a BitSet
  */
  public TSOpenBitSet(BitSet bits) {
    this(bits.length());
  }

  /** Returns the current capacity in bits (1 greater than the index of the last bit) */
  public long capacity() { return ((long)wlen) << 6; }

 /**
  * Returns the current capacity of this set.  Included for
  * compatibility.  This is *not* equal to {@link #cardinality}
  */
  public long size() {
      return capacity();
  }

  // @Override -- not until Java 1.6
  public long length() {
    return capacity();
  }

  /** Returns true if there are no set bits */
  public boolean isEmpty() { return cardinality()==0; }


  /** Expert: gets the number of longs in the array that are in use */
  public int getNumWords() { return wlen; }


  /**
   * Returns true or false for the specified bit index.
   * The index should be less than the OpenBitSet size
   */
  public boolean get(int index) {
    int i = index >> 6;               // div 64
    // signed shift will keep a negative index and force an
    // array-index-out-of-bounds-exception, removing the need for an explicit check.
    int bit = index & 0x3f;           // mod 64
    long bitmask = 1L << bit;
    // TODO perfectionist one can implement this using bit operations
    return (bits[i / PAGE_SIZE].get(i % PAGE_SIZE) & bitmask) != 0;
  }

  /**
   * Returns true or false for the specified bit index.
   * The index should be less than the OpenBitSet size.
   */
  public boolean get(long index) {
    int i = (int)(index >> 6);               // div 64
    int bit = (int)index & 0x3f;           // mod 64
    long bitmask = 1L << bit;
    // TODO perfectionist one can implement this using bit operations
    return (bits[i / PAGE_SIZE].get(i % PAGE_SIZE ) & bitmask) != 0;
  }

  /** returns 1 if the bit is set, 0 if not.
   * The index should be less than the OpenBitSet size
   */
  public int getBit(int index) {
    int i = index >> 6;                // div 64
    int bit = index & 0x3f;            // mod 64
    return ((int)(bits[i / PAGE_SIZE].get(i % PAGE_SIZE )>>>bit)) & 0x01;
  }

  /**
   * Sets the bit at the specified index.
   * The index should be less than the OpenBitSet size.
   */
  public void set(long index) {
    int wordNum = (int)(index >> 6);
    int bit = (int)index & 0x3f;
    long bitmask = 1L << bit;
    setBits(wordNum,bits[ wordNum / PAGE_SIZE ].get(wordNum % PAGE_SIZE) | bitmask);
  }

  /**
   * Sets the bit at the specified index.
   * The index should be less than the OpenBitSet size.
   */
  public void set(int index) {
    int wordNum = index >> 6;      // div 64
    int bit = index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    setBits(wordNum,bits[ wordNum / PAGE_SIZE ].get(wordNum % PAGE_SIZE) | bitmask);
  }

  /**
   * clears a bit.
   * The index should be less than the OpenBitSet size.
   */
  public void clear(int index) {
    int wordNum = index >> 6;
    int bit = index & 0x03f;
    long bitmask = 1L << bit;
    setBits(wordNum,bits[ wordNum / PAGE_SIZE ].get(wordNum % PAGE_SIZE) & ~bitmask);    
    // hmmm, it takes one more instruction to clear than it does to set... any
    // way to work around this?  If there were only 63 bits per word, we could
    // use a right shift of 10111111...111 in binary to position the 0 in the
    // correct place (using sign extension).
    // Could also use Long.rotateRight() or rotateLeft() *if* they were converted
    // by the JVM into a native instruction.
    // bits[word] &= Long.rotateLeft(0xfffffffe,bit);
  }

  /**
   * clears a bit.
   * The index should be less than the OpenBitSet size.
   */
  public void clear(long index) {
    int wordNum = (int)(index >> 6); // div 64
    int bit = (int)index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    setBits(wordNum,bits[ wordNum / PAGE_SIZE ].get(wordNum % PAGE_SIZE) & ~bitmask);
  }

  /**
   * Clears a range of bits.  Clearing past the end does not change the size of the set.
   *
   * @param startIndex lower index
   * @param endIndex one-past the last bit to clear
   */
  public void clear(int startIndex, int endIndex) {
    if (endIndex <= startIndex) return;

    int startWord = (startIndex>>6);
    if (startWord >= wlen) return;

    // since endIndex is one past the end, this is index of the last
    // word to be changed.
    int endWord   = ((endIndex-1)>>6);

    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex;  // 64-(endIndex&0x3f) is the same as -endIndex due to wrap

    // invert masks since we are clearing
    startmask = ~startmask;
    endmask = ~endmask;

    if (startWord == endWord) {
      setBits(startWord,bits[ startWord / PAGE_SIZE ].get(startWord % PAGE_SIZE) & (startmask | endmask));	
      return;
    }
    setBits(startWord,bits[ startWord / PAGE_SIZE ].get(startWord % PAGE_SIZE) & startmask);	    

    int middle = Math.min(wlen, endWord);
    if (startWord / PAGE_SIZE == middle / PAGE_SIZE) {
    	zeroFill(startWord,(startWord+1) % PAGE_SIZE, middle % PAGE_SIZE);
    } else {
        while (++startWord<middle)
            setBits(startWord,0L);	
    }
    if (endWord < wlen) {
        setBits(endWord,bits[ endWord / PAGE_SIZE ].get(endWord % PAGE_SIZE) & endmask);	    
    }
  }


  /** Clears a range of bits.  Clearing past the end does not change the size of the set.
   *
   * @param startIndex lower index
   * @param endIndex one-past the last bit to clear
   */
  public void clear(long startIndex, long endIndex) {
    if (endIndex <= startIndex) return;

    int startWord = (int)(startIndex>>6);
    if (startWord >= wlen) return;

    // since endIndex is one past the end, this is index of the last
    // word to be changed.
    int endWord   = (int)((endIndex-1)>>6);

    long startmask = -1L << startIndex;
    long endmask = -1L >>> -endIndex;  // 64-(endIndex&0x3f) is the same as -endIndex due to wrap

    // invert masks since we are clearing
    startmask = ~startmask;
    endmask = ~endmask;

    if (startWord == endWord) {
        setBits(startWord,bits[ startWord / PAGE_SIZE ].get(startWord % PAGE_SIZE) & (startmask | endmask));	    
        return;
    }

    setBits(startWord,bits[ startWord / PAGE_SIZE ].get(startWord % PAGE_SIZE) & startmask);	    

    int middle = Math.min(wlen, endWord);
    if (startWord / PAGE_SIZE == middle / PAGE_SIZE) {
    	zeroFill(startWord,(startWord+1) % PAGE_SIZE, middle % PAGE_SIZE);
    } else
    {
        while (++startWord<middle)
            setBits(startWord,0L);	    
    }
    if (endWord < wlen) {
        setBits(endWord,bits[ endWord / PAGE_SIZE ].get(endWord % PAGE_SIZE) & endmask);	    
    }
  }



  /** Sets a bit and returns the previous value.
   * The index should be less than the OpenBitSet size.
   */
  public boolean getAndSet(int index) {
    int wordNum = index >> 6;      // div 64
    int bit = index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    boolean val = (bits[wordNum / PAGE_SIZE].get(wordNum % PAGE_SIZE) & bitmask) != 0;
    setBits(wordNum,bits[ wordNum / PAGE_SIZE ].get(wordNum % PAGE_SIZE) | bitmask);	    
    return val;
  }

  /** Sets a bit and returns the previous value.
   * The index should be less than the OpenBitSet size.
   */
  public boolean getAndSet(long index) {
    int wordNum = (int)(index >> 6);      // div 64
    int bit = (int)index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    boolean val = (bits[wordNum / PAGE_SIZE].get(wordNum % PAGE_SIZE) & bitmask) != 0;
    setBits(wordNum,bits[ wordNum / PAGE_SIZE ].get(wordNum % PAGE_SIZE) | bitmask);	    
    return val;
  }

  /** flips a bit.
   * The index should be less than the OpenBitSet size.
   */
  public void flip(int index) {
    int wordNum = index >> 6;      // div 64
    int bit = index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    setBits(wordNum,bits[ wordNum / PAGE_SIZE ].get(wordNum % PAGE_SIZE) ^ bitmask);	    
  }

  /**
   * flips a bit.
   * The index should be less than the OpenBitSet size.
   */
  public void flip(long index) {
    int wordNum = (int)(index >> 6);   // div 64
    int bit = (int)index & 0x3f;       // mod 64
    long bitmask = 1L << bit;
    setBits(wordNum,bits[ wordNum / PAGE_SIZE ].get(wordNum % PAGE_SIZE) ^ bitmask);	    
  }

  /** flips a bit and returns the resulting bit value.
   * The index should be less than the OpenBitSet size.
   */
  public boolean flipAndGet(int index) {
    int wordNum = index >> 6;      // div 64
    int bit = index & 0x3f;     // mod 64
    long bitmask = 1L << bit;
    setBits(wordNum,bits[ wordNum / PAGE_SIZE ].get(wordNum % PAGE_SIZE) ^ bitmask);	    
    return (bits[wordNum / PAGE_SIZE].get(wordNum % PAGE_SIZE) & bitmask) != 0;
  }

  /** flips a bit and returns the resulting bit value.
   * The index should be less than the OpenBitSet size.
   */
  public boolean flipAndGet(long index) {
    int wordNum = (int)(index >> 6);   // div 64
    int bit = (int)index & 0x3f;       // mod 64
    long bitmask = 1L << bit;
    setBits(wordNum,bits[ wordNum / PAGE_SIZE ].get(wordNum % PAGE_SIZE) ^ bitmask);	    
    return (bits[wordNum / PAGE_SIZE].get(wordNum % PAGE_SIZE) & bitmask) != 0;
  }

  /** @return the number of set bits */
  public long cardinality() 
  {
    long bitCount = 0L;
    for (int i=getPageCount();i-->0;)
        bitCount+=pop_array(bits[i],0,wlen);
    
    return bitCount;
  }

  /** Returns the index of the first set bit starting at the index specified.
   *  -1 is returned if there are no more set bits.
   */
  public int nextSetBit(int index) {
    int i = index>>6;
    if (i>=wlen) return -1;
    int subIndex = index & 0x3f;      // index within the word
    long word = bits[i / PAGE_SIZE].get( i % PAGE_SIZE) >> subIndex;  // skip all the bits to the right of index

    if (word!=0) {
      return (i<<6) + subIndex + BitUtil.ntz(word);
    }

    while(++i < wlen) {
      word = bits[i / PAGE_SIZE].get(i % PAGE_SIZE);
      if (word!=0) return (i<<6) + BitUtil.ntz(word);
    }

    return -1;
  }

  /** Returns the index of the first set bit starting at the index specified.
   *  -1 is returned if there are no more set bits.
   */
  public long nextSetBit(long index) {
    int i = (int)(index>>>6);
    if (i>=wlen) return -1;
    int subIndex = (int)index & 0x3f; // index within the word
    long word = bits[i / PAGE_SIZE].get(i % PAGE_SIZE) >>> subIndex;  // skip all the bits to the right of index

    if (word!=0) {
      return (((long)i)<<6) + (subIndex + BitUtil.ntz(word));
    }

    while(++i < wlen) {
      word = bits[i / PAGE_SIZE].get(i % PAGE_SIZE);
      if (word!=0) return (((long)i)<<6) + BitUtil.ntz(word);
    }

    return -1;
  }

  /** this = this AND other */
  public void intersect(TSOpenBitSet other) {
    int newLen= Math.min(this.wlen,other.wlen);
    AtomicLongArray[] thisArr = this.bits;
    AtomicLongArray[] otherArr = other.bits;
    int thisPageSize = this.PAGE_SIZE;
    int otherPageSize = other.PAGE_SIZE;
    // testing against zero can be more efficient
    int pos=newLen;
    while(--pos>=0) {
    	long currentValue = thisArr[pos / thisPageSize].get( pos % thisPageSize);
	    while (!thisArr[pos / thisPageSize].compareAndSet(pos % thisPageSize, currentValue, currentValue & otherArr[pos / otherPageSize].get(pos % otherPageSize))) {
		     	 currentValue = thisArr[pos / thisPageSize].get( pos % thisPageSize);
		}
    }
    
    if (this.wlen > newLen) {
      // fill zeros from the new shorter length to the old length
      for (pos=wlen;pos-->newLen;) {
    	  long currentValue = thisArr[pos / thisPageSize].get( pos % thisPageSize);
  	    	while (!thisArr[pos / thisPageSize].compareAndSet(pos % thisPageSize, currentValue, 0L)) {
  		     	 currentValue = thisArr[pos / thisPageSize].get( pos % thisPageSize);
  	    	}
      }
    }
    this.wlen = newLen;
  }

  // some BitSet compatability methods

  //** see {@link intersect} */
  public void and(TSOpenBitSet other) {
    intersect(other);
  }

  /** Lowers numWords, the number of words in use,
   * by checking for trailing zero words.
   */
  public void trimTrailingZeros() {
    int idx = wlen-1;
    while (idx>=0 && bits[idx / PAGE_SIZE].get(idx % PAGE_SIZE)==0) idx--;
    wlen = idx+1;
  }

  /** returns the number of 64 bit words it would take to hold numBits */
  public static int bits2words(long numBits) {
   return (int)(((numBits-1)>>>6)+1);
  }

  /** returns true if both sets have the same bits set */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof TSOpenBitSet)) return false;
    TSOpenBitSet a;
    TSOpenBitSet b = (TSOpenBitSet)o;
    // make a the larger set.
    if (b.wlen > this.wlen) {
      a = b; b=this;
    } else {
      a=this;
    }
    
    int aPageSize = this.PAGE_SIZE;
    int bPageSize = b.PAGE_SIZE;

    // check for any set bits out of the range of b
    for (int i=a.wlen-1; i>=b.wlen; i--) {
      if (a.bits[i/aPageSize].get(i % aPageSize)!=0) return false;
    }

    for (int i=b.wlen-1; i>=0; i--) {
      if (a.bits[i/aPageSize].get(i % aPageSize) != b.bits[i/bPageSize].get(i % bPageSize)) return false;
    }

    return true;
  }


  @Override
  public int hashCode() {
    // Start with a zero hash and use a mix that results in zero if the input is zero.
    // This effectively truncates trailing zeros without an explicit check.
    long h = 0;
    for (int i = wlen; --i>=0;) {
      h ^= bits[i / PAGE_SIZE].get(i % PAGE_SIZE);
      h = (h << 1) | (h >>> 63); // rotate left
    }
    // fold leftmost bits into right and add a constant to prevent
    // empty sets from returning 0, which is too common.
    return (int)((h>>32) ^ h) + 0x98761234;
  }

  public void setBits(int wordNum, long newValue) {
      long currentValue = bits[wordNum / PAGE_SIZE].get(wordNum % PAGE_SIZE);
      while (!bits[ wordNum / PAGE_SIZE ].compareAndSet(wordNum % PAGE_SIZE, currentValue, newValue)) {
     	 currentValue = bits[wordNum / PAGE_SIZE].get(wordNum % PAGE_SIZE);
      }
  }
  
  public void zeroFill(int wordNum,int fromIndex, int toIndex) {
	  for (int i = fromIndex;i<toIndex; i++) {
	      long currentValue = bits[wordNum / PAGE_SIZE].get(i);
	      while (!bits[ wordNum / PAGE_SIZE ].compareAndSet(wordNum % PAGE_SIZE, currentValue, 0L)) {
	     	 currentValue = bits[wordNum / PAGE_SIZE].get(i);
	      }
	  }
  }
  
  public static long pop_array(AtomicLongArray A, int wordOffset, int numWords) {
	    /*
	    * Robert Harley and David Seal's bit counting algorithm, as documented
	    * in the revisions of Hacker's Delight
	    * http://www.hackersdelight.org/revisions.pdf
	    * http://www.hackersdelight.org/HDcode/newCode/pop_arrayHS.cc
	    *
	    * This function was adapted to Java, and extended to use 64 bit words.
	    * if only we had access to wider registers like SSE from java...
	    *
	    * This function can be transformed to compute the popcount of other functions
	    * on bitsets via something like this:
	    * sed 's/A\[\([^]]*\)\]/\(A[\1] \& B[\1]\)/g'
	    *
	    */
	    int n = wordOffset+numWords;
	    long tot=0, tot8=0;
	    long ones=0, twos=0, fours=0;

	    int i;
	    for (i = wordOffset; i <= n - 8; i+=8) {
	      /***  C macro from Hacker's Delight
	       #define CSA(h,l, a,b,c) \
	       {unsigned u = a ^ b; unsigned v = c; \
	       h = (a & b) | (u & v); l = u ^ v;}
	       ***/

	      long twosA,twosB,foursA,foursB,eights;

	      // CSA(twosA, ones, ones, A[i], A[i+1])
	      {
	        long b=A.get(i), c=A.get(i+1);
	        long u=ones ^ b;
	        twosA=(ones & b)|( u & c);
	        ones=u^c;
	      }
	      // CSA(twosB, ones, ones, A[i+2], A[i+3])
	      {
	        long b=A.get(i+2), c=A.get(i+3);
	        long u=ones^b;
	        twosB =(ones&b)|(u&c);
	        ones=u^c;
	      }
	      //CSA(foursA, twos, twos, twosA, twosB)
	      {
	        long u=twos^twosA;
	        foursA=(twos&twosA)|(u&twosB);
	        twos=u^twosB;
	      }
	      //CSA(twosA, ones, ones, A[i+4], A[i+5])
	      {
	        long b=A.get(i+4), c=A.get(i+5);
	        long u=ones^b;
	        twosA=(ones&b)|(u&c);
	        ones=u^c;
	      }
	      // CSA(twosB, ones, ones, A[i+6], A[i+7])
	      {
	        long b=A.get(i+6), c=A.get(i+7);
	        long u=ones^b;
	        twosB=(ones&b)|(u&c);
	        ones=u^c;
	      }
	      //CSA(foursB, twos, twos, twosA, twosB)
	      {
	        long u=twos^twosA;
	        foursB=(twos&twosA)|(u&twosB);
	        twos=u^twosB;
	      }

	      //CSA(eights, fours, fours, foursA, foursB)
	      {
	        long u=fours^foursA;
	        eights=(fours&foursA)|(u&foursB);
	        fours=u^foursB;
	      }
	      tot8 += pop(eights);
	    }

	    // handle trailing words in a binary-search manner...
	    // derived from the loop above by setting specific elements to 0.
	    // the original method in Hackers Delight used a simple for loop:
	    //   for (i = i; i < n; i++)      // Add in the last elements
	    //  tot = tot + pop(A[i]);

	    if (i<=n-4) {
	      long twosA, twosB, foursA, eights;
	      {
	        long b=A.get(i), c=A.get(i+1);
	        long u=ones ^ b;
	        twosA=(ones & b)|( u & c);
	        ones=u^c;
	      }
	      {
	        long b=A.get(i+2), c=A.get(i+3);
	        long u=ones^b;
	        twosB =(ones&b)|(u&c);
	        ones=u^c;
	      }
	      {
	        long u=twos^twosA;
	        foursA=(twos&twosA)|(u&twosB);
	        twos=u^twosB;
	      }
	      eights=fours&foursA;
	      fours=fours^foursA;

	      tot8 += pop(eights);
	      i+=4;
	    }

	    if (i<=n-2) {
	      long b=A.get(i), c=A.get(i+1);
	      long u=ones ^ b;
	      long twosA=(ones & b)|( u & c);
	      ones=u^c;

	      long foursA=twos&twosA;
	      twos=twos^twosA;

	      long eights=fours&foursA;
	      fours=fours^foursA;

	      tot8 += pop(eights);
	      i+=2;
	    }

	    if (i<n) {
	      tot += pop(A.get(i));
	    }

	    tot += (pop(fours)<<2)
	            + (pop(twos)<<1)
	            + pop(ones)
	            + (tot8<<3);

	    return tot;
	  }
  public static int pop(long x) {
	  /* Hacker's Delight 32 bit pop function:
	   * http://www.hackersdelight.org/HDcode/newCode/pop_arrayHS.cc
	   *
	  int pop(unsigned x) {
	     x = x - ((x >> 1) & 0x55555555);
	     x = (x & 0x33333333) + ((x >> 2) & 0x33333333);
	     x = (x + (x >> 4)) & 0x0F0F0F0F;
	     x = x + (x >> 8);
	     x = x + (x >> 16);
	     return x & 0x0000003F;
	    }
	  ***/

	    // 64 bit java version of the C function from above
	    x = x - ((x >>> 1) & 0x5555555555555555L);
	    x = (x & 0x3333333333333333L) + ((x >>>2 ) & 0x3333333333333333L);
	    x = (x + (x >>> 4)) & 0x0F0F0F0F0F0F0F0FL;
	    x = x + (x >>> 8);
	    x = x + (x >>> 16);
	    x = x + (x >>> 32);
	    return ((int)x) & 0x7F;
	  }

}
