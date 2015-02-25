package com.splicemachine.stats.frequency;


import com.splicemachine.encoding.Encoder;
import com.splicemachine.hash.Hash32;
import com.splicemachine.hash.HashFunctions;
import com.splicemachine.primitives.ByteComparator;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.utils.ComparableComparator;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

/**
 * Utility class for constructing {@link com.splicemachine.stats.frequency.FrequencyCounter} instances.
 *
 * This class will generally construct a {@code FrequencyCounter} which has been optimized for space and
 * update performance, and are <em>not</em> guaranteed to return exact results.
 *
 * @author Scott Fines
 * Date: 3/27/14
 */
public class FrequencyCounters {
    private static final Hash32 TABLE_HASH_FUNCTION = HashFunctions.murmur3(0);
    public static final float DEFAULT_LOAD_FACTOR = 0.9f;

    /**
     * @return A FrequencyCounter which is specially designed to be efficient for boolean primitive types.
     */
    public static BooleanFrequencyCounter booleanCounter(){ return new SimpleBooleanFrequencyCounter(); }

    /**
     * @return a FrequencyCounter which is specially designed to be efficient for byte primitive types.
     */
		public static ByteFrequencyCounter byteCounter(){ return new EnumeratingByteFrequencyCounter(); }

    /**
     * @param maxCounters the maximum number of counters to use. This means that the maximum number of
     *                    {@code Top-K} elements which can possibly recorded (within the bounds of the error
     *                    inherint in the <em>SpaceSaver</em> algorithm) is {@code maxCounters}--elements
     *                    outside of this are guaranteed (within an error metric) not to be included
     *                    in the resulting FrequentElements instance.
     * @return a FrequencyCounter specifically designed to be efficient for handling byte[] types. This method
     * uses the default {@link com.splicemachine.primitives.ByteComparator} to perform equality checking.
     */
		public static BytesFrequencyCounter byteArrayCounter(int maxCounters){
        return new BytesSpaceSaver(Bytes.basicByteComparator(),TABLE_HASH_FUNCTION,maxCounters);
		}

    /**
     * @param initialSize the initial number of counters to keep. When the size of the data set is known
     *                    to be very large, setting this to {@code maxCounters} will prevent some table resizing,
     *                    and therefore there will be some slight memory improvements. However, setting this
     *                    too high may result in wasted memory when there are not many elements in the stream.
     * @param maxCounters the maximum number of counters to use. This means that the maximum number of
     *                    {@code Top-K} elements which can possibly recorded (within the bounds of the error
     *                    inherint in the <em>SpaceSaver</em> algorithm) is {@code maxCounters}--elements
     *                    outside of this are guaranteed (within an error metric) not to be included
     *                    in the resulting FrequentElements instance.
     * @return a FrequencyCounter specifically designed to be efficient for handling byte[] types. This
     * version uses the default {@link com.splicemachine.primitives.ByteComparator} to perform equality checking.
     */
		public static BytesFrequencyCounter byteArrayCounter(int maxCounters, int initialSize){
        return new BytesSpaceSaver(Bytes.basicByteComparator(),TABLE_HASH_FUNCTION,maxCounters,initialSize, DEFAULT_LOAD_FACTOR);
		}

    /**
     * @param byteComparator the comparison algorithm for comparing two byte arrays. Equality in particular
     *                       will use methods provided here.
     * @param maxCounters the maximum number of counters to use. This means that the maximum number of
     *                    {@code Top-K} elements which can possibly recorded (within the bounds of the error
     *                    inherint in the <em>SpaceSaver</em> algorithm) is {@code maxCounters}--elements
     *                    outside of this are guaranteed (within an error metric) not to be included
     *                    in the resulting FrequentElements instance.
     * @return a FrequencyCounter specifically designed to be efficient for handling byte[] types.
     */
    public static BytesFrequencyCounter byteArrayCounter(ByteComparator byteComparator,int maxCounters){
        return new BytesSpaceSaver(byteComparator,TABLE_HASH_FUNCTION,maxCounters);
    }

    /**
     * @param byteComparator the comparison algorithm for comparing two byte arrays. Equality in particular
     *                       will use methods provided here.
     * @param initialSize the initial number of counters to keep. When the size of the data set is known
     *                    to be very large, setting this to {@code maxCounters} will prevent some table resizing,
     *                    and therefore there will be some slight memory improvements. However, setting this
     *                    too high may result in wasted memory when there are not many elements in the stream.
     * @param maxCounters the maximum number of counters to use. This means that the maximum number of
     *                    {@code Top-K} elements which can possibly recorded (within the bounds of the error
     *                    inherint in the <em>SpaceSaver</em> algorithm) is {@code maxCounters}--elements
     *                    outside of this are guaranteed (within an error metric) not to be included
     *                    in the resulting FrequentElements instance.
     * @return a FrequencyCounter specifically designed to be efficient for handling byte[] types.
     */
    public static BytesFrequencyCounter byteArrayCounter(ByteComparator byteComparator,int maxCounters, int initialSize){
        return new BytesSpaceSaver(byteComparator,TABLE_HASH_FUNCTION,maxCounters,initialSize, DEFAULT_LOAD_FACTOR);
    }

    /**
     * @param maxCounters the maximum number of counters to use. This means that the maximum number of
     *                    {@code Top-K} elements which can possibly recorded (within the bounds of the error
     *                    inherint in the <em>SpaceSaver</em> algorithm) is {@code maxCounters}--elements
     *                    outside of this are guaranteed (within an error metric) not to be included
     *                    in the resulting FrequentElements instance.
     * @return A FrequencyCounter specifically designed to be efficient with double data types.
     */
		public static DoubleFrequencyCounter doubleCounter(int maxCounters){
        return new DoubleSpaceSaver(TABLE_HASH_FUNCTION,maxCounters);
		}

    /**
     *
     * @param initialSize the initial number of counters to keep. When the size of the data set is known
     *                    to be very large, setting this to {@code maxCounters} will prevent some table resizing,
     *                    and therefore there will be some slight memory improvements. However, setting this
     *                    too high may result in wasted memory when there are not many elements in the stream.
     * @param maxCounters the maximum number of counters to use. This means that the maximum number of
     *                    {@code Top-K} elements which can possibly recorded (within the bounds of the error
     *                    inherint in the <em>SpaceSaver</em> algorithm) is {@code maxCounters}--elements
     *                    outside of this are guaranteed (within an error metric) not to be included
     *                    in the resulting FrequentElements instance.
     * @return a FrequencyCounter specifically designed to be efficient with double data types.
     */
		public static DoubleFrequencyCounter doubleCounter(int maxCounters, int initialSize){
        return new DoubleSpaceSaver(TABLE_HASH_FUNCTION,maxCounters,initialSize, DEFAULT_LOAD_FACTOR);
		}

    /**
     * @param maxCounters the maximum number of counters to use. This means that the maximum number of
     *                    {@code Top-K} elements which can possibly recorded (within the bounds of the error
     *                    inherint in the <em>SpaceSaver</em> algorithm) is {@code maxCounters}--elements
     *                    outside of this are guaranteed (within an error metric) not to be included
     *                    in the resulting FrequentElements instance.
     * @return A FrequencyCounter specifically designed to be efficient with float data types.
     */
		public static FloatFrequencyCounter floatCounter(int maxCounters){
        return new FloatSpaceSaver(TABLE_HASH_FUNCTION,maxCounters);
		}

    /**
     * @param initialSize the initial number of counters to keep. When the size of the data set is known
     *                    to be very large, setting this to {@code maxCounters} will prevent some table resizing,
     *                    and therefore there will be some slight memory improvements. However, setting this
     *                    too high may result in wasted memory when there are not many elements in the stream.
     * @param maxCounters the maximum number of counters to use. This means that the maximum number of
     *                    {@code Top-K} elements which can possibly recorded (within the bounds of the error
     *                    inherint in the <em>SpaceSaver</em> algorithm) is {@code maxCounters}--elements
     *                    outside of this are guaranteed (within an error metric) not to be included
     *                    in the resulting FrequentElements instance.
     * @return a FrequencyCounter specifically designed to be efficient with float data types.
     */
		public static FloatFrequencyCounter floatCounter(int maxCounters, int initialSize){
        return new FloatSpaceSaver(TABLE_HASH_FUNCTION,maxCounters,initialSize, DEFAULT_LOAD_FACTOR);
		}

    /**
     * @param maxCounters the maximum number of counters to use. This means that the maximum number of
     *                    {@code Top-K} elements which can possibly recorded (within the bounds of the error
     *                    inherint in the <em>SpaceSaver</em> algorithm) is {@code maxCounters}--elements
     *                    outside of this are guaranteed (within an error metric) not to be included
     *                    in the resulting FrequentElements instance.
     * @return A FrequencyCounter specifically designed to be efficient with short data types.
     */
		public static ShortFrequencyCounter shortCounter(short maxCounters){
        return new ShortSpaceSaver(TABLE_HASH_FUNCTION,maxCounters);
		}

    /**
     * @param initialSize the initial number of counters to keep. When the size of the data set is known
     *                    to be very large, setting this to {@code maxCounters} will prevent some table resizing,
     *                    and therefore there will be some slight memory improvements. However, setting this
     *                    too high may result in wasted memory when there are not many elements in the stream.
     * @param maxCounters the maximum number of counters to use. This means that the maximum number of
     *                    {@code Top-K} elements which can possibly recorded (within the bounds of the error
     *                    inherint in the <em>SpaceSaver</em> algorithm) is {@code maxCounters}--elements
     *                    outside of this are guaranteed (within an error metric) not to be included
     *                    in the resulting FrequentElements instance.
     * @return a FrequencyCounter specifically designed to be efficient with short data types.
     */
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

    /**
     * @return an Encoder for converting a {@link com.splicemachine.stats.frequency.BooleanFrequentElements}
     * to and from data streams and/or byte arrays.
     */
    public static Encoder<BooleanFrequentElements> booleanEncoder(){
        return SimpleBooleanFrequencyCounter.EncoderDecoder.INSTANCE;
    }

    /**
     * @return an Encoder for converting a {@link com.splicemachine.stats.frequency.ByteFrequentElements}
     * to and from data streams and/or byte arrays.
     */
    public static Encoder<ByteFrequentElements> byteEncoder(){ return SwitchingByteEncoder.INSTANCE; }

    /**
     * @return an Encoder for converting a {@link com.splicemachine.stats.frequency.BytesFrequentElements}
     * to and from output streams and/or byte arrays. This will use the default
     * {@link com.splicemachine.primitives.ByteComparator} for use in necessary byte[] comparisons.
     */
    public static Encoder<BytesFrequentElements> byteArrayEncoder(){
        return BytesFrequentElements.newEncoder(Bytes.basicByteComparator());
    }

    /**
     * @param byteComparator the Byte comparison algorithm to use when comparing two byte arrays.
     * @return an Encoder for converting a {@link com.splicemachine.stats.frequency.BytesFrequentElements}
     * to and from output streams and/or byte arrays.
     */
    public static Encoder<BytesFrequentElements> byteArrayEncoder(ByteComparator byteComparator){
        return BytesFrequentElements.newEncoder(byteComparator);
    }

    /**
     * @return an Encoder for converting a {@link com.splicemachine.stats.frequency.DoubleFrequentElements}
     * to and from output streams and/or byte arrays.
     */
    public static Encoder<DoubleFrequentElements> doubleEncoder(){
        return DoubleFrequentElements.EncoderDecoder.INSTANCE;
    }

    /**
     * @return an Encoder for converting a {@link com.splicemachine.stats.frequency.FloatFrequentElements}
     * to and from byte streams and/or byte arrays.
     */
    public static Encoder<FloatFrequentElements> floatEncoder(){
        return FloatFrequentElements.EncoderDecoder.INSTANCE;
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
