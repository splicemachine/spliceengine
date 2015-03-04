package com.splicemachine.stats.cardinality;


import com.splicemachine.encoding.Encoder;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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
    private final BitSet bitSet;

    public EnumeratingByteCardinalityEstimator() {
        this(new BitSet(256));
    }

    /*Serialization constructor*/
    private EnumeratingByteCardinalityEstimator(BitSet bitSet) {
        this.bitSet = bitSet;
    }


    /* ****************************************************************************************************************/
    /*Accessors*/
    @Override public void update(byte item) { bitSet.set(item & 0xff); }
    @Override public long getEstimate() { return bitSet.cardinality(); }

    /* ****************************************************************************************************************/
    /*Modifiers*/
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
        assert other instanceof  ByteCardinalityEstimator: "Cannot merge estimator of type "+ other.getClass();
        return merge((ByteCardinalityEstimator)other);
    }

    @Override
    public ByteCardinalityEstimator merge(ByteCardinalityEstimator other) {
        assert other instanceof EnumeratingByteCardinalityEstimator:
                "Cannot merge byte estimator that is not of type EnumeratingByteCardinalityEstimator";
        BitSet otherBits = ((EnumeratingByteCardinalityEstimator)other).bitSet;
        bitSet.or(otherBits);
        return this;
    }

    @Override
    public ByteCardinalityEstimator getClone() {
        return new EnumeratingByteCardinalityEstimator((BitSet)bitSet.clone());
    }

    /*Encoding logic*/
    public static Encoder<ByteCardinalityEstimator> newEncoder() {
        return EncoderDecoder.INSTANCE;
    }

    private static class EncoderDecoder implements Encoder<ByteCardinalityEstimator>{
        private static final EncoderDecoder INSTANCE = new EncoderDecoder();

        @Override
        public void encode(ByteCardinalityEstimator item, DataOutput encoder) throws IOException {
            assert item instanceof EnumeratingByteCardinalityEstimator: "Cannot serialize estimator instance of type "+item.getClass();
            EnumeratingByteCardinalityEstimator it = (EnumeratingByteCardinalityEstimator)item;
            byte[] bytes = it.bitSet.toByteArray();
            encoder.write(bytes.length);
            encoder.write(bytes);
        }

        @Override
        public ByteCardinalityEstimator decode(DataInput input) throws IOException {
            int size = input.readInt();
            byte[] bytes = new byte[size];
            input.readFully(bytes);
            BitSet bs = BitSet.valueOf(bytes);
            return new EnumeratingByteCardinalityEstimator(bs);
        }
    }
}
