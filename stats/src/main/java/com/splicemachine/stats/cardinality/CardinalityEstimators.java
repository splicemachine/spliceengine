/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.stats.cardinality;


import com.splicemachine.encoding.Encoder;
import com.splicemachine.hash.Hash64;
import com.splicemachine.hash.HashFunctions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

/**
 * @author Scott Fines
 * Date: 6/5/14
 */
public class CardinalityEstimators {

    private static final Hash64 DEFAULT_HASH_FUNCTION = HashFunctions.murmur2_64(0);

    private CardinalityEstimators(){} //can't make me, I'm a utility class!

    public static ByteCardinalityEstimator byteEstimator(){ return new EnumeratingByteCardinalityEstimator(); }

    public static Encoder<ByteCardinalityEstimator> byteEncoder(){
        return EnumeratingByteCardinalityEstimator.newEncoder();
    }


    public static ShortCardinalityEstimator hyperLogLogShort(int precision){
        return hyperLogLogShort(precision,DEFAULT_HASH_FUNCTION);
    }

    public static ShortCardinalityEstimator hyperLogLogShort(int precision, Hash64 hashFunction){
        BaseLogLogCounter counter = new SparseHyperLogLog(precision, hashFunction);
        return new ShortHyperLogLog(counter);
    }

    public static Encoder<ShortCardinalityEstimator> shortEncoder(){
        return new ShortEncoder(DEFAULT_HASH_FUNCTION);
    }

    public static IntCardinalityEstimator hyperLogLogInt(int precision){
        return hyperLogLogInt(precision, DEFAULT_HASH_FUNCTION);
    }

    public static IntCardinalityEstimator hyperLogLogInt(int precision,Hash64 hashFunction){
        BaseLogLogCounter counter = new SparseHyperLogLog(precision, hashFunction);
        return new IntHyperLogLog(counter);
    }

    public static Encoder<IntCardinalityEstimator> intEncoder(){
        return new IntEncoder(DEFAULT_HASH_FUNCTION);
    }

    public static LongCardinalityEstimator hyperLogLogLong(int precision){
        return hyperLogLogLong(precision, DEFAULT_HASH_FUNCTION);
    }

    public static LongCardinalityEstimator hyperLogLogLong(int precision,Hash64 hashFunction){
        BaseLogLogCounter counter = new SparseHyperLogLog(precision, hashFunction);
        return new LongHyperLogLog(counter);
    }

    public static Encoder<LongCardinalityEstimator> longEncoder(){
        return new LongEncoder(DEFAULT_HASH_FUNCTION);
    }

    public static FloatCardinalityEstimator hyperLogLogFloat(int precision){
        return hyperLogLogFloat(precision, DEFAULT_HASH_FUNCTION);
    }

    public static FloatCardinalityEstimator hyperLogLogFloat(int precision,Hash64 hashFunction){
        BaseLogLogCounter counter = new SparseHyperLogLog(precision, hashFunction);
        return new FloatHyperLogLog(counter);
    }

    public static Encoder<FloatCardinalityEstimator> floatEncoder(){
        return new FloatEncoder(DEFAULT_HASH_FUNCTION);
    }

    public static DoubleCardinalityEstimator hyperLogLogDouble(int precision){
        return hyperLogLogDouble(precision, DEFAULT_HASH_FUNCTION);
    }

    public static DoubleCardinalityEstimator hyperLogLogDouble(int precision,Hash64 hashFunction){
        BaseLogLogCounter counter = new SparseHyperLogLog(precision, hashFunction);
        return new DoubleHyperLogLog(counter);
    }

    public static Encoder<DoubleCardinalityEstimator> doubleEncoder(){
        return new DoubleEncoder(DEFAULT_HASH_FUNCTION);
    }

    public static CardinalityEstimator<String> hyperLogLogString(int precision){
        return hyperLogLogString(precision, DEFAULT_HASH_FUNCTION);
    }

    public static CardinalityEstimator<String> hyperLogLogString(int precision,Hash64 hashFunction){
        BaseLogLogCounter counter = new SparseHyperLogLog(precision, hashFunction);
        return new HyperLogLog<String>(counter){
            @Override
            public void update(String item, long count) {
            }
        };
    }
    public static Encoder<CardinalityEstimator<String>> stringEncoder(){
        return new ObjectEncoder<>(DEFAULT_HASH_FUNCTION);
    }

    public static CardinalityEstimator<BigDecimal> hyperLogLogBigDecimal(int precision){
        return hyperLogLogBigDecimal(precision, DEFAULT_HASH_FUNCTION);
    }

    public static CardinalityEstimator<BigDecimal> hyperLogLogBigDecimal(int precision,Hash64 hashFunction){
        BaseLogLogCounter counter = new SparseHyperLogLog(precision, hashFunction);
        return new HyperLogLog<>(counter);
    }

    public static Encoder<CardinalityEstimator<BigDecimal>> bigDecimalEncoder(){
        return new ObjectEncoder<>(DEFAULT_HASH_FUNCTION);
    }

    public static <T> CardinalityEstimator<T> hyperLogLog(int precision){
        return hyperLogLog(precision, DEFAULT_HASH_FUNCTION);
    }

    public static <T> CardinalityEstimator<T> hyperLogLog(int precision,Hash64 hashFunction){
        BaseLogLogCounter counter = new SparseHyperLogLog(precision, hashFunction);
        return new HyperLogLog<>(counter);
    }

    public static <T> Encoder<CardinalityEstimator<T>> objectEncoder(){
        return new ObjectEncoder<>(DEFAULT_HASH_FUNCTION);
    }


    public static BytesCardinalityEstimator hyperLogLogBytes(int precision){
        BaseLogLogCounter counter = new SparseHyperLogLog(precision, DEFAULT_HASH_FUNCTION);
        return new BytesHyperLogLog(counter);
    }

    public static BytesCardinalityEstimator hyperLogLogBytes(int precision, Hash64 hashFunction){
        BaseLogLogCounter counter = new SparseHyperLogLog(precision, hashFunction);
        return new BytesHyperLogLog(counter);
    }

    public static Encoder<BytesCardinalityEstimator> bytesEncoder(){
        return new BytesEncoder(DEFAULT_HASH_FUNCTION);
    }

    /* ****************************************************************************************************************/
    /*private classes*/
    private static class BytesHyperLogLog implements BytesCardinalityEstimator {
        private final BaseLogLogCounter counter;

        private BytesHyperLogLog(BaseLogLogCounter counter) { this.counter = counter; }
        @Override public long getEstimate() { return counter.getEstimate(); }
        @Override public void update(byte[] bytes, int offset, int length) { counter.update(bytes,offset,length);	 }
        @Override public void update(byte[] bytes, int offset, int length, long count) { counter.update(bytes,offset,length); }
        @Override public void update(ByteBuffer bytes) { counter.update(bytes); }
        @Override public void update(ByteBuffer bytes, long count) { counter.update(bytes); }

        @Override
        public CardinalityEstimator<ByteBuffer> merge(CardinalityEstimator<ByteBuffer> otherEstimator) {
            assert otherEstimator instanceof BytesCardinalityEstimator: "Cannot merge with a non-loglog cardinality estimator";
            return merge((BytesCardinalityEstimator)otherEstimator);
        }

        @Override
        public BytesCardinalityEstimator merge(BytesCardinalityEstimator otherEstimator) {
            if (otherEstimator instanceof BytesHyperLogLog) {
                counter.merge(((BytesHyperLogLog) otherEstimator).counter);
            }
            return this;
        }

        @Override
        public BytesCardinalityEstimator getClone() {
            return new BytesHyperLogLog(counter.getClone());
        }
    }

    private static class DoubleHyperLogLog implements DoubleCardinalityEstimator {
        private final BaseLogLogCounter counter;

        private DoubleHyperLogLog(BaseLogLogCounter counter) { this.counter = counter; }
        @Override public long getEstimate() { return counter.getEstimate(); }

        @Override public CardinalityEstimator<Double> getClone() { return newCopy();}
        @Override public DoubleCardinalityEstimator newCopy(){ return new DoubleHyperLogLog(counter.getClone()); }

        @Override public void update(double item) { counter.update(item); }
        @Override public void update(double item, long count) { counter.update(item); }

        @Override
        public void update(Double item) {
            assert item!=null: "Cannot estimate the cardinality of null values";
            update(item.doubleValue(),1);
        }

        @Override
        public void update(Double item, long count) {
            assert item!=null: "Cannot estimate the cardinality of null values";
            update(item.floatValue());
        }

        @Override
        public CardinalityEstimator<Double> merge(CardinalityEstimator<Double> otherEstimator) {
            assert otherEstimator instanceof DoubleCardinalityEstimator: "Cannot merge with a non-loglog cardinality estimator";
            return merge((DoubleCardinalityEstimator)otherEstimator);
        }

        @Override
        public DoubleCardinalityEstimator merge(DoubleCardinalityEstimator otherEstimator) {
            if (otherEstimator instanceof DoubleHyperLogLog) {
                counter.merge(((DoubleHyperLogLog) otherEstimator).counter);
            }
            return this;
        }
    }

    private static class FloatHyperLogLog implements FloatCardinalityEstimator {
        private final BaseLogLogCounter counter;

        private FloatHyperLogLog(BaseLogLogCounter counter) { this.counter = counter; }
        @Override public long getEstimate() { return counter.getEstimate(); }
        @Override public void update(float item) { counter.update(item); }
        @Override public void update(float item, long count) { counter.update(item); }

        @Override
        public void update(Float item) {
            assert item!=null: "Cannot estimate the cardinality of null values";
            update(item.floatValue(),1);
        }

        @Override
        public void update(Float item, long count) {
            assert item!=null: "Cannot estimate the cardinality of null values";
            update(item.floatValue());
        }

        @Override public CardinalityEstimator<Float> getClone() { return newCopy(); }
        @Override public FloatCardinalityEstimator newCopy(){ return new FloatHyperLogLog(counter.getClone()); }

        @Override
        public CardinalityEstimator<Float> merge(CardinalityEstimator<Float> otherEstimator) {
            assert otherEstimator instanceof FloatCardinalityEstimator: "Cannot merge with a non-loglog cardinality estimator";
            return merge((FloatCardinalityEstimator)otherEstimator);
        }

        @Override
        public FloatCardinalityEstimator merge(FloatCardinalityEstimator otherEstimator) {
            if (otherEstimator instanceof FloatHyperLogLog) {
                counter.merge(((FloatHyperLogLog) otherEstimator).counter);
            }
            return this;
        }
    }

    private static class LongHyperLogLog implements LongCardinalityEstimator {
        private final BaseLogLogCounter counter;

        private LongHyperLogLog(BaseLogLogCounter counter) { this.counter = counter; }
        @Override public long getEstimate() { return counter.getEstimate(); }
        @Override public void update(long item) { counter.update(item); }
        @Override public void update(long item, long count) { counter.update(item); }

        @Override public CardinalityEstimator<Long> getClone() { return newCopy(); }
        @Override public LongCardinalityEstimator newCopy(){ return new LongHyperLogLog(counter.getClone()); }

        @Override
        public void update(Long item) {
            assert item!=null: "Cannot estimate the cardinality of null values";
            update(item.shortValue(),1);
        }

        @Override
        public void update(Long item, long count) {
            assert item!=null: "Cannot estimate the cardinality of null values";
            update(item.shortValue(),count);
        }

        @Override
        public CardinalityEstimator<Long> merge(CardinalityEstimator<Long> otherEstimator) {
            assert otherEstimator instanceof LongCardinalityEstimator: "Cannot merge with a non-loglog cardinality estimator";
            return merge((LongCardinalityEstimator)otherEstimator);
        }

        @Override
        public LongCardinalityEstimator merge(LongCardinalityEstimator otherEstimator) {
            if (otherEstimator instanceof LongHyperLogLog) {
                counter.merge(((LongHyperLogLog) otherEstimator).counter);
            }
            return this;
        }
    }

    private static class IntHyperLogLog implements IntCardinalityEstimator{
        private final BaseLogLogCounter counter;

        private IntHyperLogLog(BaseLogLogCounter counter) { this.counter = counter; }
        @Override public long getEstimate() { return counter.getEstimate(); }

        @Override public void update(int item) { update(item,1l); }
        @Override public void update(int item, long count) { counter.update(item,count); }

        @Override public CardinalityEstimator<Integer> getClone() { return newCopy(); }
        @Override public IntCardinalityEstimator newCopy(){ return new IntHyperLogLog(counter.getClone()); }

        @Override
        public void update(Integer item) {
            assert item!=null: "Cannot estimate the cardinality of null values";
            update(item.shortValue(),1);
        }

        @Override
        public void update(Integer item, long count) {
            assert item!=null: "Cannot estimate the cardinality of null values";
            update(item.shortValue(),count);
        }

        @Override
        public CardinalityEstimator<Integer> merge(CardinalityEstimator<Integer> other) {
            assert other instanceof IntCardinalityEstimator: "Cannot merge with a non-loglog cardinality estimator";
            return merge((IntCardinalityEstimator)other);
        }

        @Override
        public IntCardinalityEstimator merge(IntCardinalityEstimator other) {
            if (other instanceof IntHyperLogLog) {
                counter.merge(((IntHyperLogLog) other).counter);
            }
            return this;
        }
    }

    private static class ShortHyperLogLog implements ShortCardinalityEstimator{
        private final BaseLogLogCounter counter;

        private ShortHyperLogLog(BaseLogLogCounter counter) { this.counter = counter; }
        @Override public long getEstimate() { return counter.getEstimate(); }
        @Override public void update(short item) { counter.update(item,1l); }
        @Override public void update(short item, long count) { counter.update(item,count); }

        @Override public CardinalityEstimator<Short> getClone() { return newCopy(); }

        @Override
        public ShortCardinalityEstimator newCopy(){
            return new ShortHyperLogLog(counter.getClone());
        }

        @Override
        public void update(Short item) {
            assert item!=null: "Cannot estimate the cardinality of null values";
            update(item.shortValue());
        }

        @Override
        public void update(Short item, long count) {
            assert item!=null: "Cannot estimate the cardinality of null values";
            update(item.shortValue());
        }

        @Override
        public CardinalityEstimator<Short> merge(CardinalityEstimator<Short> otherEstimator) {
            assert otherEstimator instanceof ShortCardinalityEstimator: "Cannot merge with a non-loglog cardinality estimator";
            return merge((ShortCardinalityEstimator)otherEstimator);
        }

        @Override
        public ShortCardinalityEstimator merge(ShortCardinalityEstimator otherEstimator) {
            if (otherEstimator instanceof ShortHyperLogLog) {
                counter.merge(((ShortHyperLogLog) otherEstimator).counter);
            }
            return this;
        }
    }

    private static class HyperLogLog<T> implements CardinalityEstimator<T> {
        private final BaseLogLogCounter counter;

        public HyperLogLog(BaseLogLogCounter counter) { this.counter = counter; }
        @Override public long getEstimate() { return counter.getEstimate(); }


        @Override
        public CardinalityEstimator<T> getClone() {
            return new HyperLogLog<>(counter.getClone());
        }

        @Override
        public void update(T item) {
            update(item,1l);
        }

        @Override
        public void update(T item, long count) {
            assert item!=null: "Cannot collect cardinality estimates for null values";
            counter.update(item.hashCode());
        }

        @Override
        public CardinalityEstimator<T> merge(CardinalityEstimator<T> otherEstimator) {
            if (otherEstimator instanceof HyperLogLog) {
                counter.merge(((HyperLogLog) otherEstimator).counter);
            }
            return this;
        }
    }

    private static class ShortEncoder extends SparseEncoder<ShortCardinalityEstimator>{

        public ShortEncoder(Hash64 hashFunction) {
            super(hashFunction);
        }

        @Override
        protected SparseHyperLogLog getCounter(ShortCardinalityEstimator item) {
            assert item instanceof ShortHyperLogLog : "Cannot encode estimator of type "+ item.getClass();
            return (SparseHyperLogLog)((ShortHyperLogLog) item).counter;
        }

        @Override
        protected ShortCardinalityEstimator newEstimator(SparseHyperLogLog count) {
            return new ShortHyperLogLog(count);
        }
    }

    private static class IntEncoder extends SparseEncoder<IntCardinalityEstimator>{

        public IntEncoder(Hash64 hashFunction) {
            super(hashFunction);
        }

        @Override
        protected SparseHyperLogLog getCounter(IntCardinalityEstimator item) {
            assert item instanceof IntHyperLogLog : "Cannot encode estimator of type "+ item.getClass();
            return (SparseHyperLogLog)((IntHyperLogLog) item).counter;
        }

        @Override
        protected IntCardinalityEstimator newEstimator(SparseHyperLogLog count) {
            return new IntHyperLogLog(count);
        }
    }

    private static class LongEncoder extends SparseEncoder<LongCardinalityEstimator>{

        public LongEncoder(Hash64 hashFunction) {
            super(hashFunction);
        }

        @Override
        protected SparseHyperLogLog getCounter(LongCardinalityEstimator item) {
            assert item instanceof LongHyperLogLog : "Cannot encode estimator of type "+ item.getClass();
            return (SparseHyperLogLog)((LongHyperLogLog) item).counter;
        }

        @Override
        protected LongCardinalityEstimator newEstimator(SparseHyperLogLog count) {
            return new LongHyperLogLog(count);
        }
    }

    private static class FloatEncoder extends SparseEncoder<FloatCardinalityEstimator>{

        public FloatEncoder(Hash64 hashFunction) {
            super(hashFunction);
        }

        @Override
        protected SparseHyperLogLog getCounter(FloatCardinalityEstimator item) {
            assert item instanceof FloatHyperLogLog : "Cannot encode estimator of type "+ item.getClass();
            return (SparseHyperLogLog)((FloatHyperLogLog) item).counter;
        }

        @Override
        protected FloatCardinalityEstimator newEstimator(SparseHyperLogLog count) {
            return new FloatHyperLogLog(count);
        }
    }

    private static class DoubleEncoder extends SparseEncoder<DoubleCardinalityEstimator>{

        public DoubleEncoder(Hash64 hashFunction) {
            super(hashFunction);
        }

        @Override
        protected SparseHyperLogLog getCounter(DoubleCardinalityEstimator item) {
            assert item instanceof DoubleHyperLogLog : "Cannot encode estimator of type "+ item.getClass();
            return (SparseHyperLogLog)((DoubleHyperLogLog) item).counter;
        }

        @Override
        protected DoubleCardinalityEstimator newEstimator(SparseHyperLogLog count) {
            return new DoubleHyperLogLog(count);
        }
    }

    private static class BytesEncoder extends SparseEncoder<BytesCardinalityEstimator>{

        public BytesEncoder(Hash64 hashFunction) {
            super(hashFunction);
        }

        @Override
        protected SparseHyperLogLog getCounter(BytesCardinalityEstimator item) {
            assert item instanceof BytesHyperLogLog : "Cannot encode estimator of type "+ item.getClass();
            return (SparseHyperLogLog)((BytesHyperLogLog) item).counter;
        }

        @Override
        protected BytesCardinalityEstimator newEstimator(SparseHyperLogLog count) {
            return new BytesHyperLogLog(count);
        }
    }

    private static class ObjectEncoder<T> extends SparseEncoder<CardinalityEstimator<T>>{

        public ObjectEncoder(Hash64 hashFunction) {
            super(hashFunction);
        }

        @Override
        protected SparseHyperLogLog getCounter(CardinalityEstimator<T> item) {
            assert item instanceof HyperLogLog : "Cannot encode estimator of type "+ item.getClass();
            return (SparseHyperLogLog)((HyperLogLog) item).counter;
        }

        @Override
        protected CardinalityEstimator<T> newEstimator(SparseHyperLogLog count) {
            return new HyperLogLog<>(count);
        }
    }

    private static abstract class SparseEncoder<T> implements Encoder<T> {
        protected final Encoder<SparseHyperLogLog> baseEncoder;

        public SparseEncoder(Hash64 hashFunction) {
            this.baseEncoder = new SparseHyperLogLog.EncoderDecoder(hashFunction);
        }

        @Override
        public void encode(T item, DataOutput dataInput) throws IOException {
            baseEncoder.encode(getCounter(item),dataInput);
        }

        @Override
        public T decode(DataInput input) throws IOException {
            SparseHyperLogLog count = baseEncoder.decode(input);
            return newEstimator(count);
        }

        protected abstract SparseHyperLogLog getCounter(T item);

        protected abstract T newEstimator(SparseHyperLogLog count);
    }
}
