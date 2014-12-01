package com.splicemachine.stats.cardinality;


import com.splicemachine.hash.Hash64;
import com.splicemachine.hash.HashFunctions;

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

		public static ShortCardinalityEstimator hyperLogLogShort(int precision, Hash64 hashFunction){
				BaseLogLogCounter counter = SparseAdjustedHyperLogLogCounter.adjustedCounter(precision, hashFunction);
				return new ShortHyperLogLog(counter);
		}

		public static IntCardinalityEstimator hyperLogLogInt(int precision){
        return hyperLogLogInt(precision, DEFAULT_HASH_FUNCTION);
		}

		public static IntCardinalityEstimator hyperLogLogInt(int precision,Hash64 hashFunction){
        BaseLogLogCounter counter = SparseAdjustedHyperLogLogCounter.adjustedCounter(precision, hashFunction);
				return new IntHyperLogLog(counter);
		}

		public static LongCardinalityEstimator hyperLogLogLong(int precision){
				return hyperLogLogLong(precision, DEFAULT_HASH_FUNCTION);
		}

		public static LongCardinalityEstimator hyperLogLogLong(int precision,Hash64 hashFunction){
        BaseLogLogCounter counter = SparseAdjustedHyperLogLogCounter.adjustedCounter(precision, hashFunction);
				return new LongHyperLogLog(counter);
		}

		public static FloatCardinalityEstimator hyperLogLogFloat(int precision){
				return hyperLogLogFloat(precision, DEFAULT_HASH_FUNCTION);
		}

		public static FloatCardinalityEstimator hyperLogLogFloat(int precision,Hash64 hashFunction){
        BaseLogLogCounter counter = SparseAdjustedHyperLogLogCounter.adjustedCounter(precision, hashFunction);
				return new FloatHyperLogLog(counter);
		}
		public static DoubleCardinalityEstimator hyperLogLogDouble(int precision){
				return hyperLogLogDouble(precision, DEFAULT_HASH_FUNCTION);
		}

		public static DoubleCardinalityEstimator hyperLogLogDouble(int precision,Hash64 hashFunction){
        BaseLogLogCounter counter = SparseAdjustedHyperLogLogCounter.adjustedCounter(precision, hashFunction);
				return new DoubleHyperLogLog(counter);
		}

		public static CardinalityEstimator<String> hyperLogLogString(int precision){
				return hyperLogLogString(precision, DEFAULT_HASH_FUNCTION);
		}

		public static CardinalityEstimator<String> hyperLogLogString(int precision,Hash64 hashFunction){
        BaseLogLogCounter counter = SparseAdjustedHyperLogLogCounter.adjustedCounter(precision, hashFunction);
				return new HyperLogLog<String>(counter){
            @Override
            public void update(String item, long count) {
            }
        };
		}

		public static CardinalityEstimator<BigDecimal> hyperLogLogBigDecimal(int precision){
				return hyperLogLogBigDecimal(precision, DEFAULT_HASH_FUNCTION);
		}

		public static CardinalityEstimator<BigDecimal> hyperLogLogBigDecimal(int precision,Hash64 hashFunction){
        BaseLogLogCounter counter = SparseAdjustedHyperLogLogCounter.adjustedCounter(precision, hashFunction);
				return new HyperLogLog<BigDecimal>(counter);
		}

		public static BytesCardinalityEstimator hyperLogLogBytes(int precision, Hash64 hashFunction){
        BaseLogLogCounter counter = SparseAdjustedHyperLogLogCounter.adjustedCounter(precision, hashFunction);
				return new BytesHyperLogLog(counter);

		}

		private static class BytesHyperLogLog implements BytesCardinalityEstimator {
				private final BaseLogLogCounter counter;

				private BytesHyperLogLog(BaseLogLogCounter counter) { this.counter = counter; }
				@Override public long getEstimate() { return counter.getEstimate(); }
				@Override public void update(byte[] bytes, int offset, int length) { counter.update(bytes,offset,length);	 }
				@Override public void update(byte[] bytes, int offset, int length, long count) { counter.update(bytes,offset,length); }
				@Override public void update(ByteBuffer bytes) { counter.update(bytes); }
				@Override public void update(ByteBuffer bytes, long count) { counter.update(bytes); }
				@Override public void update(byte[] item) { counter.update(item,0,item.length); }
				@Override public void update(byte[] item, long count) { counter.update(item,0,item.length);  }

        @Override
        public CardinalityEstimator<byte[]> merge(CardinalityEstimator<byte[]> otherEstimator) {
            assert otherEstimator instanceof BaseLogLogCounter: "Cannot merge with a non-loglog cardinality estimator";
            counter.merge((BaseLogLogCounter)otherEstimator);
            return this;
        }

        @Override
        public BytesCardinalityEstimator merge(BytesCardinalityEstimator otherEstimator) {
            assert otherEstimator instanceof BaseLogLogCounter: "Cannot merge with a non-loglog cardinality estimator";
            counter.merge((BaseLogLogCounter)otherEstimator);
            return this;
        }
		}

		private static class DoubleHyperLogLog implements DoubleCardinalityEstimator {
				private final BaseLogLogCounter counter;

				private DoubleHyperLogLog(BaseLogLogCounter counter) { this.counter = counter; }
				@Override public long getEstimate() { return counter.getEstimate(); }
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
            assert otherEstimator instanceof BaseLogLogCounter: "Cannot merge with a non-loglog cardinality estimator";
            counter.merge((BaseLogLogCounter)otherEstimator);
            return this;
        }

        @Override
        public DoubleCardinalityEstimator merge(DoubleCardinalityEstimator otherEstimator) {
            assert otherEstimator instanceof BaseLogLogCounter: "Cannot merge with a non-loglog cardinality estimator";
            counter.merge((BaseLogLogCounter)otherEstimator);
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

        @Override
        public CardinalityEstimator<Float> merge(CardinalityEstimator<Float> otherEstimator) {
            assert otherEstimator instanceof BaseLogLogCounter: "Cannot merge with a non-loglog cardinality estimator";
            counter.merge((BaseLogLogCounter)otherEstimator);
            return this;
        }

        @Override
        public FloatCardinalityEstimator merge(FloatCardinalityEstimator otherEstimator) {
            assert otherEstimator instanceof BaseLogLogCounter: "Cannot merge with a non-loglog cardinality estimator";
            counter.merge((BaseLogLogCounter)otherEstimator);
            return this;
        }
		}

		private static class LongHyperLogLog implements LongCardinalityEstimator {
				private final BaseLogLogCounter counter;

				private LongHyperLogLog(BaseLogLogCounter counter) { this.counter = counter; }
				@Override public long getEstimate() { return counter.getEstimate(); }
				@Override public void update(long item) { counter.update(item); }
				@Override public void update(long item, long count) { counter.update(item); }

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
            assert otherEstimator instanceof BaseLogLogCounter: "Cannot merge with a non-loglog cardinality estimator";
            counter.merge((BaseLogLogCounter)otherEstimator);
            return this;
        }

        @Override
        public LongCardinalityEstimator merge(LongCardinalityEstimator otherEstimator) {
            assert otherEstimator instanceof BaseLogLogCounter: "Cannot merge with a non-loglog cardinality estimator";
            counter.merge((BaseLogLogCounter)otherEstimator);
            return this;
        }
		}

		private static class IntHyperLogLog implements IntCardinalityEstimator{
				private final BaseLogLogCounter counter;

				private IntHyperLogLog(BaseLogLogCounter counter) { this.counter = counter; }
				@Override public long getEstimate() { return counter.getEstimate(); }

				@Override
				public void done() {
					//no-op
				}

				@Override public void update(int item) { update(item,1l); }
				@Override public void update(int item, long count) { counter.update(item,count); }

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
            assert other instanceof BaseLogLogCounter: "Cannot merge with a non-loglog cardinality estimator";
            counter.merge((BaseLogLogCounter)other);
            return this;
        }

        @Override
        public IntCardinalityEstimator merge(IntCardinalityEstimator other) {
            assert other instanceof BaseLogLogCounter: "Cannot merge with a non-loglog cardinality estimator";
            counter.merge((BaseLogLogCounter)other);
            return this;
        }
    }

		private static class ShortHyperLogLog implements ShortCardinalityEstimator{
				private final BaseLogLogCounter counter;

				private ShortHyperLogLog(BaseLogLogCounter counter) { this.counter = counter; }
				@Override public long getEstimate() { return counter.getEstimate(); }
				@Override public void update(short item) { counter.update(item,1l); }
				@Override public void update(short item, long count) { counter.update(item,count); }

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
            assert otherEstimator instanceof BaseLogLogCounter: "Cannot merge with a non-loglog cardinality estimator";
            counter.merge((BaseLogLogCounter)otherEstimator);
            return this;
        }

        @Override
        public ShortCardinalityEstimator merge(ShortCardinalityEstimator otherEstimator) {
            assert otherEstimator instanceof BaseLogLogCounter: "Cannot merge with a non-loglog cardinality estimator";
            counter.merge((BaseLogLogCounter)otherEstimator);
            return this;
        }
    }

		private static class HyperLogLog<T> implements CardinalityEstimator<T> {
				private final BaseLogLogCounter counter;

				public HyperLogLog(BaseLogLogCounter counter) { this.counter = counter; }
				@Override public long getEstimate() { return counter.getEstimate(); }

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
            assert otherEstimator instanceof BaseLogLogCounter: "Cannot merge with a non-loglog cardinality estimator";
            counter.merge((BaseLogLogCounter)otherEstimator);
            return this;
        }
		}
}
