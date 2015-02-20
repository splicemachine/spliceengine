package com.splicemachine.stats.frequency;


import com.splicemachine.hash.Hash32;
import com.splicemachine.hash.HashFunctions;
import com.splicemachine.primitives.ByteComparator;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.tools.Comparables;
import com.splicemachine.utils.ComparableComparator;

import java.util.Comparator;

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
        return new BytesSpaceSaver(Bytes.basicByteComparator(),TABLE_HASH_FUNCTION,maxCounters);
		}

		public static BytesFrequencyCounter byteArrayCounter(int maxCounters, int initialSize){
        return new BytesSpaceSaver(Bytes.basicByteComparator(),TABLE_HASH_FUNCTION,maxCounters,initialSize,0.9f);
		}

    public static BytesFrequencyCounter byteArrayCounter(ByteComparator byteComparator,int maxCounters){
        return new BytesSpaceSaver(byteComparator,TABLE_HASH_FUNCTION,maxCounters);
    }

    public static BytesFrequencyCounter byteArrayCounter(ByteComparator byteComparator,int maxCounters, int initialSize){
        return new BytesSpaceSaver(byteComparator,TABLE_HASH_FUNCTION,maxCounters,initialSize,0.9f);
    }

		public static DoubleFrequencyCounter doubleCounter(int maxCounters){
        return new DoubleSpaceSaver(TABLE_HASH_FUNCTION,maxCounters);
		}

		public static DoubleFrequencyCounter doubleCounter(int maxCounters, int initialSize){
        return new DoubleSpaceSaver(TABLE_HASH_FUNCTION,maxCounters,initialSize,0.9f);
		}

		public static FloatFrequencyCounter floatCounter(int maxCounters){
        return new FloatSpaceSaver(TABLE_HASH_FUNCTION,maxCounters);
		}

		public static FloatFrequencyCounter floatCounter(int maxCounters, int initialSize){
        return new FloatSpaceSaver(TABLE_HASH_FUNCTION,maxCounters,initialSize,0.9f);
		}

		public static ShortFrequencyCounter shortCounter(short maxCounters){
        return new ShortSpaceSaver(TABLE_HASH_FUNCTION,maxCounters);
		}

		public static ShortFrequencyCounter shortCounter(short maxCounters, short initialSize){
        return new ShortSpaceSaver(TABLE_HASH_FUNCTION,maxCounters,initialSize,0.9f);
		}

		public static IntFrequencyCounter intCounter(int maxCounters){
        return new IntSpaceSaver(TABLE_HASH_FUNCTION,maxCounters);
		}

		public static IntFrequencyCounter intCounter(int maxCounters, int initialSize){
        return new IntSpaceSaver(TABLE_HASH_FUNCTION,maxCounters,initialSize,0.9f);
		}

		public static LongFrequencyCounter longCounter(int maxCounters){
        return new LongSpaceSaver(TABLE_HASH_FUNCTION,maxCounters);
		}

		public static LongFrequencyCounter longCounter(int maxCounters, int initialSize){
        return new LongSpaceSaver(TABLE_HASH_FUNCTION,maxCounters,initialSize,0.9f);
		}

    public static <T extends Comparable<T>> FrequencyCounter<T> counter(int maxCounters){
        return new ObjectSpaceSaver<>(ComparableComparator.<T>newComparator(),TABLE_HASH_FUNCTION,maxCounters);
    }

    public static <T> FrequencyCounter<T> counter(Comparator<T> comparator, int maxCounters,int initialCounters){
        return new ObjectSpaceSaver<>(comparator,TABLE_HASH_FUNCTION,maxCounters,initialCounters,0.9f);
    }

    public static <T> FrequencyCounter<T> counter(Comparator<T> comparator, int maxCounters){
        return new ObjectSpaceSaver<>(comparator,TABLE_HASH_FUNCTION,maxCounters);
    }

    public static <T extends Comparable<T>> FrequencyCounter<T> counter(int maxCounters,int initialCounters){
        return new ObjectSpaceSaver<>(ComparableComparator.<T>newComparator(),TABLE_HASH_FUNCTION,maxCounters,initialCounters,0.9f);
    }
}
