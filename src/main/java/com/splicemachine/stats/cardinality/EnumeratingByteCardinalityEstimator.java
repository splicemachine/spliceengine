package com.splicemachine.stats.cardinality;


import java.util.BitSet;

/**
 * ByteEstimator which enumerates the possible values, and therefore maintains an exact estimate.
 *
 * This uses a BitSet under the hood to be as memory efficient as possible.
 *
 * @author Scott Fines
 * Date: 10/7/14
 */
public class EnumeratingByteCardinalityEstimator implements ByteCardinalityEstimator {
    private final BitSet bitSet = new BitSet(256);

    @Override public void update(byte item) { bitSet.set(item & 0xff); }
    @Override public long getEstimate() { return bitSet.cardinality(); }

    @Override public void update(byte item, long count) { update(item);  }//don't care about counts for cardinality estimates

    @Override
    public void update(Byte item) {
        assert item!=null: "Cannot estimate null bytes!";
        update(item.byteValue());
    }

    @Override
    public void update(Byte item, long count) {
        assert item!=null: "Cannot estimate null bytes!";
        update(item.byteValue(),count);
    }

    @Override
    public CardinalityEstimator<Byte> merge(CardinalityEstimator<Byte> other) {
        assert other instanceof EnumeratingByteCardinalityEstimator:
                "Cannot merge byte estimator that is not of type EnumeratingByteCardinalityEstimator";
        BitSet otherBits = ((EnumeratingByteCardinalityEstimator)other).bitSet;
        bitSet.or(otherBits);
        return this;
    }
}
