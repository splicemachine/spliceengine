package com.splicemachine.stats.frequency;


import com.splicemachine.hash.Hash32;
import com.splicemachine.hash.HashFunctions;
import com.splicemachine.primitives.ByteComparator;
import com.splicemachine.primitives.Bytes;

/**
 * Utility class for constructing Frequency Counters.
 *
 * @author Scott Fines
 * Date: 3/27/14
 */
public class FrequencyCounters {
    private static final Hash32 TABLE_HASH_FUNCTION = HashFunctions.murmur3(0);

		private static final int INITIAL_HASH_TABLE_SIZE = 16;

		public static BooleanFrequencyCounter booleanCounter(){
				return new SimpleBooleanFrequencyCounter();
		}

		public static ByteFrequencyCounter byteCounter(){
				return new EnumeratingByteFrequencyCounter();
		}


		public static BytesFrequencyCounter byteArrayCounter(int maxCounters){
			return new BytesSSFrequencyCounter(Bytes.basicByteComparator(),maxCounters,INITIAL_HASH_TABLE_SIZE, TABLE_HASH_FUNCTION);
		}

		public static BytesFrequencyCounter byteArrayCounter(int maxCounters, int initialSize){
				return new BytesSSFrequencyCounter(Bytes.basicByteComparator(),maxCounters,initialSize, TABLE_HASH_FUNCTION);
		}

    public static BytesFrequencyCounter byteArrayCounter(ByteComparator byteComparator,int maxCounters){
        return new BytesSSFrequencyCounter(byteComparator,maxCounters,INITIAL_HASH_TABLE_SIZE, TABLE_HASH_FUNCTION);
    }

    public static BytesFrequencyCounter byteArrayCounter(ByteComparator byteComparator,int maxCounters, int initialSize){
        return new BytesSSFrequencyCounter(byteComparator,maxCounters,initialSize, TABLE_HASH_FUNCTION);
    }

		public static DoubleFrequencyCounter doubleCounter(int maxCounters){
				return new DoubleSSFrequencyCounter(maxCounters,INITIAL_HASH_TABLE_SIZE, TABLE_HASH_FUNCTION);
		}

		public static DoubleFrequencyCounter doubleCounter(int maxCounters, int initialSize){
				return new DoubleSSFrequencyCounter(maxCounters,initialSize, TABLE_HASH_FUNCTION);
		}

		public static FloatFrequencyCounter floatCounter(int maxCounters){
				return new FloatSSFrequencyCounter(maxCounters,INITIAL_HASH_TABLE_SIZE, TABLE_HASH_FUNCTION);
		}

		public static FloatFrequencyCounter floatCounter(int maxCounters, int initialSize){
				return new FloatSSFrequencyCounter(maxCounters,initialSize, TABLE_HASH_FUNCTION);
		}

		public static ShortFrequencyCounter shortCounter(short maxCounters){
				return new ShortSSFrequencyCounter(maxCounters,INITIAL_HASH_TABLE_SIZE, TABLE_HASH_FUNCTION);
		}

		public static ShortFrequencyCounter shortCounter(short maxCounters, short initialSize){
				return new ShortSSFrequencyCounter(maxCounters,initialSize, TABLE_HASH_FUNCTION);
		}

		public static IntFrequencyCounter intCounter(int maxCounters){
				return new IntSSFrequencyCounter(maxCounters,INITIAL_HASH_TABLE_SIZE, TABLE_HASH_FUNCTION);
		}

		public static IntFrequencyCounter intCounter(int maxCounters, int initialSize){
				return new IntSSFrequencyCounter(maxCounters,initialSize, TABLE_HASH_FUNCTION);
		}

		public static LongFrequencyCounter longCounter(int maxCounters){
				return new LongSSFrequencyCounter(maxCounters,INITIAL_HASH_TABLE_SIZE, TABLE_HASH_FUNCTION);
		}

		public static LongFrequencyCounter longCounter(int maxCounters, int initialSize){
				return new LongSSFrequencyCounter(maxCounters,initialSize, TABLE_HASH_FUNCTION);
		}

		public static <T> FrequencyCounter<T> counter(int maxCounters){
				return new SSFrequencyCounter<T>(maxCounters,INITIAL_HASH_TABLE_SIZE, TABLE_HASH_FUNCTION);
		}

		public static <T> FrequencyCounter<T> counter(int maxCounters,int initialSize){
				return new SSFrequencyCounter<T>(maxCounters,initialSize, TABLE_HASH_FUNCTION);
		}
}
