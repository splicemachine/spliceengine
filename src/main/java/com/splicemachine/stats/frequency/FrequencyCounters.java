package com.splicemachine.stats.frequency;


import com.splicemachine.encoding.Encoder;
import com.splicemachine.hash.Hash32;
import com.splicemachine.hash.HashFunctions;
import com.splicemachine.primitives.ByteComparator;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.utils.ComparableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

/**
 * Utility class for constructing Frequency Counters.
 *
 * @author Scott Fines
 * Date: 3/27/14
 */
public class FrequencyCounters {
    private static final Hash32 TABLE_HASH_FUNCTION = HashFunctions.murmur3(0);
    public static final float DEFAULT_LOAD_FACTOR = 0.9f;

    public static BooleanFrequencyCounter booleanCounter(){
				return new SimpleBooleanFrequencyCounter();
		}

    public static Encoder<BooleanFrequentElements> booleanEncoder(){
        return SimpleBooleanFrequencyCounter.EncoderDecoder.INSTANCE;
    }

		public static ByteFrequencyCounter byteCounter(){
				return new EnumeratingByteFrequencyCounter();
		}

    public static Encoder<ByteFrequentElements> byteEncoder(){ return SwitchingByteEncoder.INSTANCE; }

		public static BytesFrequencyCounter byteArrayCounter(int maxCounters){
        return new BytesSpaceSaver(Bytes.basicByteComparator(),TABLE_HASH_FUNCTION,maxCounters);
		}

		public static BytesFrequencyCounter byteArrayCounter(int maxCounters, int initialSize){
        return new BytesSpaceSaver(Bytes.basicByteComparator(),TABLE_HASH_FUNCTION,maxCounters,initialSize, DEFAULT_LOAD_FACTOR);
		}

    public static BytesFrequencyCounter byteArrayCounter(ByteComparator byteComparator,int maxCounters){
        return new BytesSpaceSaver(byteComparator,TABLE_HASH_FUNCTION,maxCounters);
    }

    public static BytesFrequencyCounter byteArrayCounter(ByteComparator byteComparator,int maxCounters, int initialSize){
        return new BytesSpaceSaver(byteComparator,TABLE_HASH_FUNCTION,maxCounters,initialSize, DEFAULT_LOAD_FACTOR);
    }

    public static Encoder<BytesFrequentElements> byteArrayEncoder(){
        return BytesFrequentElements.newEncoder(Bytes.basicByteComparator());
    }

    public static Encoder<BytesFrequentElements> byteArrayEncoder(ByteComparator byteComparator){
        return BytesFrequentElements.newEncoder(byteComparator);
    }

		public static DoubleFrequencyCounter doubleCounter(int maxCounters){
        return new DoubleSpaceSaver(TABLE_HASH_FUNCTION,maxCounters);
		}

		public static DoubleFrequencyCounter doubleCounter(int maxCounters, int initialSize){
        return new DoubleSpaceSaver(TABLE_HASH_FUNCTION,maxCounters,initialSize, DEFAULT_LOAD_FACTOR);
		}

    public static Encoder<DoubleFrequentElements> doubleEncoder(){
        return DoubleFrequentElements.EncoderDecoder.INSTANCE;
    }

		public static FloatFrequencyCounter floatCounter(int maxCounters){
        return new FloatSpaceSaver(TABLE_HASH_FUNCTION,maxCounters);
		}

		public static FloatFrequencyCounter floatCounter(int maxCounters, int initialSize){
        return new FloatSpaceSaver(TABLE_HASH_FUNCTION,maxCounters,initialSize, DEFAULT_LOAD_FACTOR);
		}

    public static Encoder<FloatFrequentElements> floatEncoder(){
        return FloatFrequentElements.EncoderDecoder.INSTANCE;
    }

		public static ShortFrequencyCounter shortCounter(short maxCounters){
        return new ShortSpaceSaver(TABLE_HASH_FUNCTION,maxCounters);
		}

		public static ShortFrequencyCounter shortCounter(short maxCounters, short initialSize){
        return new ShortSpaceSaver(TABLE_HASH_FUNCTION,maxCounters,initialSize, DEFAULT_LOAD_FACTOR);
		}

    public static Encoder<ShortFrequentElements> shortEncoder(){
        return ShortFrequentElements.EncoderDecoder.INSTANCE;
    }

		public static IntFrequencyCounter intCounter(int maxCounters){
        return new IntSpaceSaver(TABLE_HASH_FUNCTION,maxCounters);
		}

		public static IntFrequencyCounter intCounter(int maxCounters, int initialSize){
        return new IntSpaceSaver(TABLE_HASH_FUNCTION,maxCounters,initialSize, DEFAULT_LOAD_FACTOR);
		}

    public static Encoder<IntFrequentElements> intEncoder(){
        return IntFrequentElements.EncoderDecoder.INSTANCE;
    }

		public static LongFrequencyCounter longCounter(int maxCounters){
        return new LongSpaceSaver(TABLE_HASH_FUNCTION,maxCounters);
		}

		public static LongFrequencyCounter longCounter(int maxCounters, int initialSize){
        return new LongSpaceSaver(TABLE_HASH_FUNCTION,maxCounters,initialSize, DEFAULT_LOAD_FACTOR);
		}

    public static Encoder<LongFrequentElements> longEncoder(){
        return LongFrequentElements.EncoderDecoder.INSTANCE;
    }

    public static <T extends Comparable<T>> FrequencyCounter<T> counter(int maxCounters){
        return new ObjectSpaceSaver<>(ComparableComparator.<T>newComparator(),TABLE_HASH_FUNCTION,maxCounters);
    }

    public static <T extends Comparable<T>> FrequencyCounter<T> counter(int maxCounters,int initialCounters){
        return new ObjectSpaceSaver<>(ComparableComparator.<T>newComparator(),TABLE_HASH_FUNCTION,maxCounters,initialCounters, DEFAULT_LOAD_FACTOR);
    }

    public static <T extends Comparable<T>> Encoder<FrequentElements<T>> objectEncoder(Encoder<T> valueEncoder){
        return new ObjectFrequentElements.EncoderDecoder<>(valueEncoder,ComparableComparator.<T>newComparator());
    }

    public static <T> FrequencyCounter<T> counter(Comparator<T> comparator, int maxCounters,int initialCounters){
        return new ObjectSpaceSaver<>(comparator,TABLE_HASH_FUNCTION,maxCounters,initialCounters, DEFAULT_LOAD_FACTOR);
    }

    public static <T> FrequencyCounter<T> counter(Comparator<T> comparator, int maxCounters){
        return new ObjectSpaceSaver<>(comparator,TABLE_HASH_FUNCTION,maxCounters);
    }

    public static <T> Encoder<FrequentElements<T>> objectEncoder(Encoder<T> valueEncoder,
                                                                       Comparator<? super T> comparator){
        return new ObjectFrequentElements.EncoderDecoder<>(valueEncoder,comparator);
    }

    /* ***************************************************************************************************************/
    /*private helper methods and classes*/
    private static class SwitchingByteEncoder implements Encoder<ByteFrequentElements>{
        public static final Encoder<ByteFrequentElements> INSTANCE = new SwitchingByteEncoder();

        @Override
        public void encode(ByteFrequentElements item, DataOutput dataInput) throws IOException {
           if(item instanceof ByteFrequencies)
               dataInput.writeByte(0x00);
            else if(item instanceof ByteHeavyHitters)
               dataInput.writeByte(0x01);
            else throw new IllegalArgumentException("Cannot encode ByteFrequentElements of type "+ item.getClass());
        }

        @Override
        public ByteFrequentElements decode(DataInput input) throws IOException {
            byte b = input.readByte();
            if(b == 0x00)
                return ByteFrequencies.EncoderDecoder.INSTANCE.decode(input);
            else if(b==0x01)
                return ByteHeavyHitters.EncoderDecoder.INSTANCE.decode(input);
            else throw new IllegalArgumentException("Unknown type: "+ b);
        }
    }
}
