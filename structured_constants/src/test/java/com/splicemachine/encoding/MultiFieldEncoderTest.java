package com.splicemachine.encoding;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;

/**
 * @author Scott Fines
 * Created on: 7/31/13
 */
@RunWith(Parameterized.class)
public class MultiFieldEncoderTest {

    public static final double FLOAT_SIZE = Math.pow(10, -6);
    public static final double DOUBLE_SIZE = Math.pow(10, -12);
    private static final int MAX_STRING_SIZE = 100;
    private static final int MAX_BYTE_SIZE = 10;
    private static final int NUM_RANDOM_TESTS = 10;
    private static final int MAX_FIELDS_PER_TEST = 10;

    private static interface Checker {

        Object generateRandom(Random random);

        void load(MultiFieldEncoder encoder,Object correct,boolean descending);

        void check(MultiFieldDecoder decoder, Object correct,boolean descending);
    }

    private static enum Type implements Checker {
        BOOLEAN{
            @Override
            public Object generateRandom(Random random) {
                return random.nextBoolean();
            }

            @Override
            public void load(MultiFieldEncoder encoder, Object correct,boolean descending) {
                encoder.encodeNext((Boolean) correct,descending);
            }

            @Override
            public void check(MultiFieldDecoder decoder, Object correct,boolean descending) {
                Assert.assertEquals("Incorrect boolean encode/decode", (Boolean) correct,decoder.decodeNextBoolean(descending));
            }
        },
        BYTE{
            @Override public Object generateRandom(Random random) { return (byte)random.nextInt(); }

            @Override
            public void load(MultiFieldEncoder encoder, Object correct,boolean descending) {
                byte c = (Byte) correct;
                encoder.encodeNext(c,descending);
            }

            @Override
            public void check(MultiFieldDecoder decoder, Object correct,boolean descending) {
                Assert.assertEquals("Incorrect Byte encode/decode",((Byte)correct).byteValue(), decoder.decodeNextByte(descending));
            }
        },
        SHORT{
            @Override public Object generateRandom(Random random) { return (short)random.nextInt(); }
            @Override
            public void load(MultiFieldEncoder encoder, Object correct,boolean descending) {
                encoder.encodeNext((Short)correct,descending);
            }

            @Override
            public void check(MultiFieldDecoder decoder, Object correct,boolean descending) {
                Assert.assertEquals("Incorrect Short encode/decode",((Short)correct).shortValue(), decoder.decodeNextShort(descending));
            }
        },
        INTEGER{
            @Override public Object generateRandom(Random random) { return random.nextInt(); }
            @Override
            public void load(MultiFieldEncoder encoder, Object correct,boolean descending) {
                encoder.encodeNext((Integer)correct,descending);
            }

            @Override
            public void check(MultiFieldDecoder decoder, Object correct,boolean descending) {
                Assert.assertEquals("Incorrect Integer encode/decode",((Integer)correct).intValue(), decoder.decodeNextInt(descending));
            }
        },
        LONG{
            @Override public Object generateRandom(Random random) { return random.nextLong(); }
            @Override
            public void load(MultiFieldEncoder encoder, Object correct,boolean descending) {
                encoder.encodeNext((Long)correct,descending);
            }

            @Override
            public void check(MultiFieldDecoder decoder, Object correct,boolean descending) {
                Assert.assertEquals("Incorrect Long encode/decode",((Long)correct).longValue(), decoder.decodeNextLong(descending));
            }
        },
        FLOAT{
            @Override public Object generateRandom(Random random) { return random.nextFloat(); }
            @Override
            public void load(MultiFieldEncoder encoder, Object correct,boolean descending) {
                encoder.encodeNext((Float)correct,descending);
            }

            @Override
            public void check(MultiFieldDecoder decoder, Object correct,boolean descending) {
                Assert.assertEquals("Incorrect Float encode/decode", (Float) correct, decoder.decodeNextFloat(descending),FLOAT_SIZE);
            }
        },
        DOUBLE{
            @Override public Object generateRandom(Random random) { return random.nextDouble(); }
            @Override
            public void load(MultiFieldEncoder encoder, Object correct,boolean descending) {
                encoder.encodeNext((Double)correct,descending);
            }

            @Override
            public void check(MultiFieldDecoder decoder, Object correct,boolean descending) {
                Assert.assertEquals("Incorrect Double encode/decode", (Double) correct, decoder.decodeNextDouble(descending),DOUBLE_SIZE);
            }
        },
        DECIMAL{
            @Override public Object generateRandom(Random random) { return new BigDecimal(random.nextDouble()); }
            @Override
            public void load(MultiFieldEncoder encoder, Object correct,boolean descending) {
                encoder.encodeNext((BigDecimal)correct,descending);
            }

            @Override
            public void check(MultiFieldDecoder decoder, Object correct,boolean descending) {
                BigDecimal c = (BigDecimal)correct;
                BigDecimal actual = decoder.decodeNextBigDecimal(descending);

                int compare = c.compareTo(actual);
                Assert.assertTrue("Incorrect BigDecimal encode/decode: Expected: " + c + ", Actual: " + actual, compare == 0);
            }
        },
        STRING{
            @Override public Object generateRandom(Random random) {
                char[] string = new char[random.nextInt(MAX_STRING_SIZE)];
                Charset charset = Charsets.UTF_8;
                CharsetEncoder encoder = charset.newEncoder().onMalformedInput(CodingErrorAction.REPORT);
                for(int i=0;i<string.length;i++){
                    char next = (char)random.nextInt();
                    while(!encoder.canEncode(next))
                        next = (char)random.nextInt();

                    string[i] = next;
                }

                return new String(string);
            }

            @Override
            public void load(MultiFieldEncoder encoder,Object correct,boolean descending) {
                encoder.encodeNext((String)correct,descending);
            }

            @Override
            public void check(MultiFieldDecoder decoder, Object correct,boolean descending) {
                String c = (String)correct;
                String actual = decoder.decodeNextString(descending);
                Assert.assertEquals("Incorrect String encode/decode", c, actual);
            }
        },
        UNSORTED_BYTES{
            @Override
            public Object generateRandom(Random random) {
                byte[] bytes = new byte[random.nextInt(MAX_BYTE_SIZE)];

                random.nextBytes(bytes);
                return bytes;
            }

            @Override
            public void load(MultiFieldEncoder encoder,Object correct,boolean descending) {
                encoder.encodeNextUnsorted((byte[]) correct);
            }

            @Override
            public void check(MultiFieldDecoder decoder, Object correct,boolean descending) {
                Assert.assertArrayEquals("Incorrect unsorted byte[] encode/decode", (byte[]) correct, decoder.decodeNextBytesUnsorted());
            }
        },
        SORTED_BYTES{
            @Override
            public Object generateRandom(Random random) {
                byte[] bytes = new byte[random.nextInt(MAX_BYTE_SIZE)];
                random.nextBytes(bytes);
                return bytes;
            }
            @Override
            public void load(MultiFieldEncoder encoder,Object correct,boolean descending) {
                encoder.encodeNext((byte[]) correct, descending);
            }

            @Override
            public void check(MultiFieldDecoder decoder, Object correct,boolean descending) {
                Assert.assertArrayEquals("Incorrect sorted byte[] encode/decode",(byte[])correct,decoder.decodeNextBytes(descending));
            }
        },
        NULL{
            @Override
            public Object generateRandom(Random random) {
                return null;
            }

            @Override
            public void load(MultiFieldEncoder encoder, Object correct, boolean descending) {
                encoder.encodeEmpty();
            }

            @Override
            public void check(MultiFieldDecoder decoder, Object correct, boolean descending) {
                Assert.assertTrue("Value is not null!",decoder.nextIsNull());
                decoder.skip();
            }
        },
        NULL_FLOAT{
            @Override
            public Object generateRandom(Random random) {
                return null;
            }

            @Override
            public void load(MultiFieldEncoder encoder, Object correct, boolean descending) {
                encoder.encodeEmptyFloat();
            }

            @Override
            public void check(MultiFieldDecoder decoder, Object correct, boolean descending) {
                Assert.assertTrue("Value is not null!",decoder.nextIsNullFloat());
                decoder.skipFloat();
            }
        },
        NULL_DOUBLE{
            @Override
            public Object generateRandom(Random random) {
                return null;
            }

            @Override
            public void load(MultiFieldEncoder encoder, Object correct, boolean descending) {
                encoder.encodeEmptyDouble();
            }

            @Override
            public void check(MultiFieldDecoder decoder, Object correct, boolean descending) {
                Assert.assertTrue("Value is not null!",decoder.nextIsNullDouble());
                decoder.skipDouble();
            }
        }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> data = Lists.newArrayListWithCapacity(NUM_RANDOM_TESTS);
        //Test just serializing a single field in all possible combinations
        Random random = new Random(0l);
//        for(Type type:Type.values()){
//            data.add(new Object[]{Arrays.asList(Pair.newPair(type,type.generateRandom(random)))});
//        }

        //add some fixed tests for known edge cases

        /*
         * -8.6...E307 encodes to [0,32,0,0,0,0,0,0], which has a leading zero. This is problematic
         * if all you do is check the next entry for zeros for nullity
         */
        data.add(new Object[]{
                Arrays.asList(Pair.newPair(Type.DOUBLE,Double.parseDouble("-8.98846567431158E307")),
                        Pair.newPair(Type.BOOLEAN,true))
        });

        /*
         * -Infinity encodes to [0,-128,0,0], which has a leading zero. This is problematic if all you
         * do is check the next entry for zeros for nullity
         */
        data.add(new Object[]{
                Arrays.asList(Pair.newPair(Type.FLOAT,Float.parseFloat("-Infinity")),
                        Pair.newPair(Type.BOOLEAN,true))
        });

        //test all possible combinations of two fields
        for(Type type:Type.values()){
            for(Type secondType:Type.values()){
                data.add(new Object[]{Arrays.asList(Pair.newPair(type,type.generateRandom(random)),
                        Pair.newPair(secondType,secondType.generateRandom(random)))});
            }
        }

        //test all combinations of three fields
         for(Type type:Type.values()){
            for(Type secondType:Type.values()){
                for(Type thirdType:Type.values()){
                    data.add(new Object[]{Arrays.asList(Pair.newPair(type,type.generateRandom(random)),
                         Pair.newPair(secondType,secondType.generateRandom(random)),
                            Pair.newPair(thirdType,thirdType.generateRandom(random)))});
                }
            }
        }

        //you can't test for combinations of 4 fields with the default Java Heap Size--there are too many permutations
        //if you want to test it, up your heap size, and uncomment the following section.
//        for(Type type:Type.values()){
//            for(Type secondType:Type.values()){
//                for(Type thirdType:Type.values()){
//                    for(Type fourthType:Type.values()){
//                    data.add(new Object[]{Arrays.asList(Pair.newPair(type,type.generateRandom(random)),
//                            Pair.newPair(secondType,secondType.generateRandom(random)),
//                            Pair.newPair(thirdType,thirdType.generateRandom(random)),
//                        Pair.newPair(fourthType,fourthType.generateRandom(random)))});
//                    }
//                }
//            }
//        }

        return data;
    }

    private final List<Pair<Type,Object>> types;

    public MultiFieldEncoderTest(List<Pair<Type,Object>> types) {
        this.types = types;
    }

    @Test
    public void testCanEncodeAndDecodeAllFieldsCorrectly() throws Exception {
        MultiFieldEncoder encoder = MultiFieldEncoder.create(KryoPool.defaultPool(),types.size());
        for(Pair<Type,Object> type:types){
            Type t = type.getFirst();
            Object c = type.getSecond();
            t.load(encoder,c,false);
        }

        MultiFieldDecoder decoder = MultiFieldDecoder.wrap(encoder.build(),KryoPool.defaultPool());
        for(Pair<Type,Object> cType:types){
            Type cT = cType.getFirst();
            Object correct = cType.getSecond();
            cT.check(decoder,correct,false);
        }
    }
}
