/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.encoding;

import splice.com.google.common.base.Charsets;
import org.junit.Assert;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.util.Random;

/**
 * @author Scott Fines
 *         Created on: 10/2/13
 */
public enum TestType  {
    BOOLEAN{
        @Override public Object generateRandom(Random random) { return random.nextBoolean(); }

        @Override
        public void load(MultiFieldEncoder encoder, Object correct) {
            encoder.encodeNext((Boolean) correct);
        }

        @Override
        public void load(MultiFieldEncoder encoder, Object correct,boolean descending) {
            encoder.encodeNext((Boolean) correct,descending);
        }

        @Override
        public void check(MultiFieldDecoder decoder, Object correct,boolean descending) {
            Assert.assertEquals("Incorrect boolean encode/decode", (Boolean) correct, decoder.decodeNextBoolean(descending));
        }

        @Override
        public void check(MultiFieldDecoder decoder, Object correct) {
            Assert.assertEquals("Incorrect boolean encode/decode", (Boolean) correct, decoder.decodeNextBoolean());
        }
    },
    BYTE{
        @Override public Object generateRandom(Random random) { return (byte)random.nextInt(); }

        @Override
        public void load(MultiFieldEncoder encoder, Object correct) {
            byte c = (Byte) correct;
            encoder.encodeNext(c);
        }

        @Override
        public void load(MultiFieldEncoder encoder, Object correct,boolean descending) {
            byte c = (Byte) correct;
            encoder.encodeNext(c,descending);
        }

        @Override
        public void check(MultiFieldDecoder decoder, Object correct,boolean descending) {
            Assert.assertEquals("Incorrect Byte encode/decode",((Byte)correct).byteValue(), decoder.decodeNextByte(descending));
        }

        @Override
        public void check(MultiFieldDecoder decoder, Object correct) {
            Assert.assertEquals("Incorrect Byte encode/decode",((Byte)correct).byteValue(), decoder.decodeNextByte());
        }

        @Override
        public boolean isScalarType() {
            return true;
        }
    },
    SHORT{
        @Override public Object generateRandom(Random random) { return (short)random.nextInt(); }
        @Override
        public void load(MultiFieldEncoder encoder, Object correct,boolean descending) {
            encoder.encodeNext((Short)correct,descending);
        }

        @Override
        public void load(MultiFieldEncoder encoder, Object correct) {
            encoder.encodeNext((Short)correct);
        }

        @Override
        public void check(MultiFieldDecoder decoder, Object correct,boolean descending) {
            Assert.assertEquals("Incorrect Short encode/decode",((Short)correct).shortValue(), decoder.decodeNextShort(descending));
        }

        @Override
        public void check(MultiFieldDecoder decoder, Object correct) {
            Assert.assertEquals("Incorrect Short encode/decode",((Short)correct).shortValue(), decoder.decodeNextShort());
        }
        @Override
        public boolean isScalarType() {
            return true;
        }
    },
    INTEGER{
        @Override public Object generateRandom(Random random) { return random.nextInt(); }
        @Override
        public void load(MultiFieldEncoder encoder, Object correct,boolean descending) {
            encoder.encodeNext((Integer)correct,descending);
        }

        @Override
        public void load(MultiFieldEncoder encoder, Object correct) {
            encoder.encodeNext((Integer)correct);
        }

        @Override
        public void check(MultiFieldDecoder decoder, Object correct,boolean descending) {
            Assert.assertEquals("Incorrect Integer encode/decode",((Integer)correct).intValue(), decoder.decodeNextInt(descending));
        }

        @Override
        public void check(MultiFieldDecoder decoder, Object correct) {
            Assert.assertEquals("Incorrect Integer encode/decode",((Integer)correct).intValue(), decoder.decodeNextInt());
        }
        @Override
        public boolean isScalarType() {
            return true;
        }
    },
    LONG{
        @Override public Object generateRandom(Random random) { return random.nextLong(); }
        @Override
        public void load(MultiFieldEncoder encoder, Object correct,boolean descending) {
            encoder.encodeNext((Long)correct,descending);
        }

        @Override
        public void load(MultiFieldEncoder encoder, Object correct) {
            encoder.encodeNext((Long)correct);
        }

        @Override
        public void check(MultiFieldDecoder decoder, Object correct,boolean descending) {
            Assert.assertEquals("Incorrect Long encode/decode",((Long)correct).longValue(), decoder.decodeNextLong(descending));
        }

        @Override
        public void check(MultiFieldDecoder decoder, Object correct) {
            Assert.assertEquals("Incorrect Long encode/decode",((Long)correct).longValue(), decoder.decodeNextLong());
        }
        @Override public boolean isScalarType() {
            return true;
        }
    },
    FLOAT{
        @Override public Object generateRandom(Random random) { return random.nextFloat(); }
        @Override
        public void load(MultiFieldEncoder encoder, Object correct,boolean descending) {
            encoder.encodeNext((Float)correct,descending);
        }

        @Override
        public void load(MultiFieldEncoder encoder, Object correct) {
            encoder.encodeNext((Float)correct);
        }
        @Override
        public void check(MultiFieldDecoder decoder, Object correct,boolean descending) {
            Assert.assertEquals("Incorrect Float encode/decode", (Float) correct, decoder.decodeNextFloat(descending), FLOAT_SIZE);
        }
        @Override
        public void check(MultiFieldDecoder decoder, Object correct) {
            Assert.assertEquals("Incorrect Float encode/decode", (Float) correct, decoder.decodeNextFloat(),FLOAT_SIZE);
        }
    },
    DOUBLE{
        @Override public Object generateRandom(Random random) { return random.nextDouble(); }
        @Override
        public void load(MultiFieldEncoder encoder, Object correct,boolean descending) {
            encoder.encodeNext((Double)correct,descending);
        }

        @Override
        public void load(MultiFieldEncoder encoder, Object correct) {
            encoder.encodeNext((Double)correct);
        }

        @Override
        public void check(MultiFieldDecoder decoder, Object correct,boolean descending) {
            Assert.assertEquals("Incorrect Double encode/decode", (Double) correct, decoder.decodeNextDouble(descending),DOUBLE_SIZE);
        }

        @Override
        public void check(MultiFieldDecoder decoder, Object correct) {
            Assert.assertEquals("Incorrect Double encode/decode", (Double) correct, decoder.decodeNextDouble(),DOUBLE_SIZE);
        }
    },
    DECIMAL{
        @Override public Object generateRandom(Random random) { return new BigDecimal(random.nextDouble()); }
        @Override
        public void load(MultiFieldEncoder encoder, Object correct,boolean descending) {
            encoder.encodeNext((BigDecimal)correct,descending);
        }
        @Override
        public void load(MultiFieldEncoder encoder, Object correct) {
            encoder.encodeNext((BigDecimal)correct);
        }

        @Override
        public void check(MultiFieldDecoder decoder, Object correct,boolean descending) {
            BigDecimal c = (BigDecimal)correct;
            BigDecimal actual = decoder.decodeNextBigDecimal(descending);

            int compare = c.compareTo(actual);
            Assert.assertTrue("Incorrect BigDecimal encode/decode: Expected: " + c + ", Actual: " + actual, compare == 0);
        }

        @Override
        public void check(MultiFieldDecoder decoder, Object correct) {
            BigDecimal c = (BigDecimal)correct;
            BigDecimal actual = decoder.decodeNextBigDecimal();

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
        public void load(MultiFieldEncoder encoder,Object correct) {
            encoder.encodeNext((String)correct);
        }

        @Override
        public void check(MultiFieldDecoder decoder, Object correct,boolean descending) {
            String c = (String)correct;
            String actual = decoder.decodeNextString(descending);
            Assert.assertEquals("Incorrect String encode/decode", c, actual);
        }

        @Override
        public void check(MultiFieldDecoder decoder, Object correct) {
            String c = (String)correct;
            String actual = decoder.decodeNextString();
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
        public void load(MultiFieldEncoder encoder,Object correct) {
            encoder.encodeNextUnsorted((byte[]) correct);
        }

        @Override
        public void check(MultiFieldDecoder decoder, Object correct,boolean descending) {
            Assert.assertArrayEquals("Incorrect unsorted byte[] encode/decode", (byte[]) correct, decoder.decodeNextBytesUnsorted());
        }

        @Override
        public void check(MultiFieldDecoder decoder, Object correct) {
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
        public void load(MultiFieldEncoder encoder,Object correct) {
            encoder.encodeNext((byte[]) correct);
        }

        @Override
        public void check(MultiFieldDecoder decoder, Object correct,boolean descending) {
            Assert.assertArrayEquals("Incorrect sorted byte[] encode/decode",(byte[])correct,decoder.decodeNextBytes(descending));
        }

        @Override
        public void check(MultiFieldDecoder decoder, Object correct) {
            Assert.assertArrayEquals("Incorrect sorted byte[] encode/decode",(byte[])correct,decoder.decodeNextBytes());
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
        public void load(MultiFieldEncoder encoder, Object correct ) {
            encoder.encodeEmpty();
        }

        @Override
        public void check(MultiFieldDecoder decoder, Object correct, boolean descending) {
            Assert.assertTrue("Value is not null!",decoder.nextIsNull());
            decoder.skip();
        }

        @Override
        public void check(MultiFieldDecoder decoder, Object correct) {
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
        public void load(MultiFieldEncoder encoder, Object correct ) {
            encoder.encodeEmptyFloat();
        }

        @Override
        public void check(MultiFieldDecoder decoder, Object correct, boolean descending) {
            Assert.assertTrue("Value is not null!",decoder.nextIsNullFloat());
            decoder.skipFloat();
        }

        @Override
        public void check(MultiFieldDecoder decoder, Object correct) {
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
        public void load(MultiFieldEncoder encoder, Object correct) {
            encoder.encodeEmptyDouble();
        }

        @Override
        public void check(MultiFieldDecoder decoder, Object correct, boolean descending) {
            Assert.assertTrue("Value is not null!",decoder.nextIsNullDouble());
            decoder.skipDouble();
        }

        @Override
        public void check(MultiFieldDecoder decoder, Object correct) {
            Assert.assertTrue("Value is not null!",decoder.nextIsNullDouble());
            decoder.skipDouble();
        }
    };

    public static final double FLOAT_SIZE = Math.pow(10, -6);
    public static final double DOUBLE_SIZE = Math.pow(10, -12);
    private static final int MAX_STRING_SIZE = 100;
    private static final int MAX_BYTE_SIZE = 10;


    public Object generateRandom(Random random){
        throw new UnsupportedOperationException();
    }

    public void load(MultiFieldEncoder encoder, Object correct){
        throw new UnsupportedOperationException();
    }

    public void load(MultiFieldEncoder encoder, Object correct, boolean descending){
        throw new UnsupportedOperationException();
    }

    public void check(MultiFieldDecoder decoder, Object correct, boolean descending){
        throw new UnsupportedOperationException();
    }

    public void check(MultiFieldDecoder decoder, Object correct){
        throw new UnsupportedOperationException();
    }

    public boolean isScalarType() {
        return false;
    }
}
