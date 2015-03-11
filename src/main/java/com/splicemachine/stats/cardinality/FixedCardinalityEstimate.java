package com.splicemachine.stats.cardinality;

/**
 * @author Scott Fines
 *         Date: 3/9/15
 */
public class FixedCardinalityEstimate<T> implements CardinalityEstimator<T> {
    protected final long cardinalityEstimate;

    public FixedCardinalityEstimate(long cardinalityEstimate) {
        this.cardinalityEstimate = cardinalityEstimate;
    }

    @Override public long getEstimate() { return cardinalityEstimate; }

    @Override
    public CardinalityEstimator<T> getClone() {
        return new FixedCardinalityEstimate<>(cardinalityEstimate);
    }

    @Override
    public CardinalityEstimator<T> merge(CardinalityEstimator<T> other) {
        throw new UnsupportedOperationException("Fixed cardinality estimates cannot be merged!");
    }

    @Override
    public void update(T item) {
        update(item,1l);
    }

    @Override
    public void update(T item, long count) {
        throw new UnsupportedOperationException("Cannot update Fixed cardinality estimates!");
    }

    public static ShortCardinalityEstimator shortEstimate(long estimate){
        return new FixedShort(estimate);
    }

    public static IntCardinalityEstimator intEstimate(long estimate){
        return new FixedInteger(estimate);
    }

    public static LongCardinalityEstimator longEstimate(long estimate){
        return new FixedLong(estimate);
    }

    public static FloatCardinalityEstimator floatEstimate(long estimate){
        return new FixedFloat(estimate);
    }

    public static DoubleCardinalityEstimator doubleEstimate(long estimate){
        return new FixedDouble(estimate);
    }


    /* ***************************************************************************************************************/
    /*private helper methods*/
    private static class FixedShort extends FixedCardinalityEstimate<Short> implements ShortCardinalityEstimator{

        public FixedShort(long cardinalityEstimate) {
            super(cardinalityEstimate);
        }

        @Override
        public ShortCardinalityEstimator merge(ShortCardinalityEstimator other) {
            return (ShortCardinalityEstimator)super.merge(other);
        }

        @Override public ShortCardinalityEstimator newCopy() { return new FixedShort(cardinalityEstimate); }

        @Override public void update(short item) { update(item,1l); }
        @Override
        public void update(short item, long count) {
            throw new UnsupportedOperationException("Cannot update Fixed cardinalit estimates!");
        }
    }

    private static class FixedInteger extends FixedCardinalityEstimate<Integer> implements IntCardinalityEstimator{

        public FixedInteger(long cardinalityEstimate) {
            super(cardinalityEstimate);
        }

        @Override
        public IntCardinalityEstimator merge(IntCardinalityEstimator other) {
            return (IntCardinalityEstimator)super.merge(other);
        }

        @Override public IntCardinalityEstimator newCopy() { return new FixedInteger(cardinalityEstimate); }

        @Override public void update(int item) { update(item,1l); }
        @Override
        public void update(int item, long count) {
            throw new UnsupportedOperationException("Cannot update Fixed cardinality estimates!");
        }
    }

    private static class FixedLong extends FixedCardinalityEstimate<Long> implements LongCardinalityEstimator{

        public FixedLong(long cardinalityEstimate) {
            super(cardinalityEstimate);
        }

        @Override
        public LongCardinalityEstimator merge(LongCardinalityEstimator other) {
            return (LongCardinalityEstimator)super.merge(other);
        }

        @Override public LongCardinalityEstimator newCopy() { return new FixedLong(cardinalityEstimate); }

        @Override public void update(long item) { update(item,1l); }
        @Override
        public void update(long item, long count) {
            throw new UnsupportedOperationException("Cannot update Fixed cardinality estimates!");
        }
    }

    private static class FixedFloat extends FixedCardinalityEstimate<Float> implements FloatCardinalityEstimator{

        public FixedFloat(long cardinalityEstimate) {
            super(cardinalityEstimate);
        }

        @Override
        public FloatCardinalityEstimator merge(FloatCardinalityEstimator other) {
            return (FloatCardinalityEstimator)super.merge(other);
        }

        @Override public FloatCardinalityEstimator newCopy() { return new FixedFloat(cardinalityEstimate); }

        @Override public void update(float item) { update(item,1l); }
        @Override
        public void update(float item, long count) {
            throw new UnsupportedOperationException("Cannot update Fixed cardinality estimates!");
        }
    }

    private static class FixedDouble extends FixedCardinalityEstimate<Double> implements DoubleCardinalityEstimator{

        public FixedDouble(long cardinalityEstimate) {
            super(cardinalityEstimate);
        }

        @Override
        public DoubleCardinalityEstimator merge(DoubleCardinalityEstimator other) {
            return (DoubleCardinalityEstimator)super.merge(other);
        }

        @Override public DoubleCardinalityEstimator newCopy() { return new FixedDouble(cardinalityEstimate); }

        @Override public void update(double item) { update(item,1l); }
        @Override
        public void update(double item, long count) {
            throw new UnsupportedOperationException("Cannot update Fixed cardinality estimates!");
        }
    }


}
