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
package com.splicemachine.orc;

import com.google.common.base.Strings;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.spark.sql.types.*;
import org.joda.time.DateTimeZone;
import org.junit.BeforeClass;
import org.junit.Test;
import org.weakref.jmx.internal.guava.collect.ContiguousSet;
import org.weakref.jmx.internal.guava.collect.DiscreteDomain;
import org.weakref.jmx.internal.guava.collect.Range;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.sql.Timestamp;
import java.util.*;
import static com.splicemachine.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static com.google.common.collect.Iterables.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.*;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.getCharTypeInfo;
import static org.junit.Assert.assertEquals;

public abstract class AbstractTestOrcReader
{
    private static final int CHAR_LENGTH = 10;

    private static final JavaHiveDecimalObjectInspector DECIMAL_INSPECTOR_PRECISION_2 =
            new JavaHiveDecimalObjectInspector(new DecimalTypeInfo(2, 1));
    private static final JavaHiveDecimalObjectInspector DECIMAL_INSPECTOR_PRECISION_4 =
            new JavaHiveDecimalObjectInspector(new DecimalTypeInfo(4, 2));
    private static final JavaHiveDecimalObjectInspector DECIMAL_INSPECTOR_PRECISION_8 =
            new JavaHiveDecimalObjectInspector(new DecimalTypeInfo(8, 4));
    private static final JavaHiveDecimalObjectInspector DECIMAL_INSPECTOR_PRECISION_17 =
            new JavaHiveDecimalObjectInspector(new DecimalTypeInfo(17, 8));
    private static final JavaHiveDecimalObjectInspector DECIMAL_INSPECTOR_PRECISION_18 =
            new JavaHiveDecimalObjectInspector(new DecimalTypeInfo(18, 8));
    private static final JavaHiveDecimalObjectInspector DECIMAL_INSPECTOR_PRECISION_38 =
            new JavaHiveDecimalObjectInspector(new DecimalTypeInfo(38, 16));
    private static final JavaHiveCharObjectInspector CHAR_INSPECTOR =
            new JavaHiveCharObjectInspector(getCharTypeInfo(CHAR_LENGTH));

    private static final DecimalType DECIMAL_TYPE_PRECISION_2 = DecimalType.apply(2, 1);
    private static final DecimalType DECIMAL_TYPE_PRECISION_4 = DecimalType.apply(4, 2);
    private static final DecimalType DECIMAL_TYPE_PRECISION_8 = DecimalType.apply(8, 4);
    private static final DecimalType DECIMAL_TYPE_PRECISION_17 = DecimalType.apply(17, 8);
    private static final DecimalType DECIMAL_TYPE_PRECISION_18 = DecimalType.apply(18, 8);
    private static final DecimalType DECIMAL_TYPE_PRECISION_38 = DecimalType.apply(38, 16);
    private static final StringType CHAR = (StringType)DataTypes.StringType;

    private final OrcTester tester;

    public AbstractTestOrcReader(OrcTester tester)
    {
        this.tester = tester;
    }

    @BeforeClass
    public static void setUp()
    {
        assertEquals(DateTimeZone.getDefault(), HIVE_STORAGE_TIME_ZONE);
    }

    @Test
    public void testBooleanSequence()
            throws Exception {
        tester.testRoundTrip(javaBooleanObjectInspector, limit(cycle(ImmutableList.of(true, false, false)), 30_000), DataTypes.BooleanType);
    }

    @Test
    public void testLongSequence()
            throws Exception
    {
        testRoundTripNumeric(intsBetween(0, 31_234));
    }

    @Test
    public void testLongSequenceWithHoles()
            throws Exception
    {
        testRoundTripNumeric(skipEvery(5, intsBetween(0, 31_234)));
    }

    @Test
    public void testLongDirect()
            throws Exception
    {
        testRoundTripNumeric(limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17)), 30_000));
    }

    @Test
    public void testLongDirect2()
            throws Exception
    {
        List<Integer> values = new ArrayList<>(31_234);
        for (int i = 0; i < 31_234; i++) {
            values.add(i);
        }
        Collections.shuffle(values, new Random(0));
        testRoundTripNumeric(values);
    }

    @Test
    public void testLongShortRepeat()
            throws Exception
    {
        testRoundTripNumeric(limit(repeatEach(4, cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17))), 30_000));
    }

    @Test
    public void testLongPatchedBase()
            throws Exception
    {
        testRoundTripNumeric(limit(cycle(concat(intsBetween(0, 18), ImmutableList.of(30_000, 20_000))), 30_000));
    }

    @Test
    public void testLongStrideDictionary()
            throws Exception
    {
        testRoundTripNumeric(concat(ImmutableList.of(1), nCopies(9999, 123), ImmutableList.of(2), nCopies(9999, 123)));
    }

    private void testRoundTripNumeric(Iterable<? extends Number> values)
            throws Exception
    {
        List<Long> writeValues = ImmutableList.copyOf(values).stream()
                .map(Number::longValue)
                .collect(toList());
        tester.testRoundTrip(
                javaLongObjectInspector,
                writeValues.stream()
                        .map(value -> (long) value.byteValue()) // truncate values to byte range
                        .collect(toList()),
                DataTypes.LongType);

        tester.testRoundTrip(
                javaShortObjectInspector,
                writeValues.stream()
                        .map(value -> (long) value.shortValue()) // truncate values to short range
                        .collect(toList()),
                DataTypes.LongType);

        tester.testRoundTrip(javaIntObjectInspector, writeValues, DataTypes.LongType);
        tester.testRoundTrip(javaLongObjectInspector, writeValues, DataTypes.LongType);
        DateType type;
        tester.testRoundTrip(
                javaDateObjectInspector,
                writeValues.stream()
                        .map(Long::intValue)
                        .collect(toList()),
                DataTypes.DateType);

        tester.testRoundTrip(
                javaTimestampObjectInspector,
                writeValues.stream()
                        .collect(toList()),
                DataTypes.TimestampType);
    }

    @Test
    public void testFloatSequence()
            throws Exception
    {
        tester.testRoundTrip(javaFloatObjectInspector, floatSequence(0.0f, 0.1f, 30_000), DataTypes.FloatType);
    }

    @Test
    public void testFloatNaNInfinity()
            throws Exception
    {
        tester.testRoundTrip(javaFloatObjectInspector, ImmutableList.of(1000.0f, -1.23f, Float.POSITIVE_INFINITY), DataTypes.FloatType);
        tester.testRoundTrip(javaFloatObjectInspector, ImmutableList.of(-1000.0f, Float.NEGATIVE_INFINITY, 1.23f), DataTypes.FloatType);
        tester.testRoundTrip(javaFloatObjectInspector, ImmutableList.of(0.0f, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY), DataTypes.FloatType);

        tester.testRoundTrip(javaFloatObjectInspector, ImmutableList.of(Float.NaN, -0.0f, 1.0f), DataTypes.FloatType);
        tester.testRoundTrip(javaFloatObjectInspector, ImmutableList.of(Float.NaN, -1.0f, Float.POSITIVE_INFINITY), DataTypes.FloatType);
        tester.testRoundTrip(javaFloatObjectInspector, ImmutableList.of(Float.NaN, Float.NEGATIVE_INFINITY, 1.0f), DataTypes.FloatType);
        tester.testRoundTrip(javaFloatObjectInspector, ImmutableList.of(Float.NaN, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY), DataTypes.FloatType);
    }

    @Test
    public void testDoubleSequence()
            throws Exception
    {
        tester.testRoundTrip(javaDoubleObjectInspector, doubleSequence(0, 0.1, 30_000), DataTypes.DoubleType);
    }

    @Test
    public void testDecimalSequence()
            throws Exception
    {
       // tester.testRoundTrip(DECIMAL_INSPECTOR_PRECISION_2, decimalSequence("0", "1", 30, 2, 1), DECIMAL_TYPE_PRECISION_2);
       // tester.testRoundTrip(DECIMAL_INSPECTOR_PRECISION_4, decimalSequence("0", "1", 30_00, 4, 2), DECIMAL_TYPE_PRECISION_4);
       // tester.testRoundTrip(DECIMAL_INSPECTOR_PRECISION_8, decimalSequence("0", "100", 30_000, 8, 4), DECIMAL_TYPE_PRECISION_8);
       // tester.testRoundTrip(DECIMAL_INSPECTOR_PRECISION_17, decimalSequence("0", "1000000", 30_000, 17, 8), DECIMAL_TYPE_PRECISION_17);
       // tester.testRoundTrip(DECIMAL_INSPECTOR_PRECISION_18, decimalSequence("0", "1000000", 30_000, 18, 8), DECIMAL_TYPE_PRECISION_18);
        tester.testRoundTrip(DECIMAL_INSPECTOR_PRECISION_38, decimalSequence("0", "100000000000000", 30_000, 38, 16), DECIMAL_TYPE_PRECISION_38);
    }

    @Test
    public void testDoubleNaNInfinity()
            throws Exception
    {
        tester.testRoundTrip(javaDoubleObjectInspector, ImmutableList.of(1000.0, -1.0, Double.POSITIVE_INFINITY), DataTypes.DoubleType);
        tester.testRoundTrip(javaDoubleObjectInspector, ImmutableList.of(-1000.0, Double.NEGATIVE_INFINITY, 1.0), DataTypes.DoubleType);
        tester.testRoundTrip(javaDoubleObjectInspector, ImmutableList.of(0.0, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY), DataTypes.DoubleType);

        tester.testRoundTrip(javaDoubleObjectInspector, ImmutableList.of(Double.NaN, -1.0, 1.0), DataTypes.DoubleType);
        tester.testRoundTrip(javaDoubleObjectInspector, ImmutableList.of(Double.NaN, -1.0, Double.POSITIVE_INFINITY), DataTypes.DoubleType);
        tester.testRoundTrip(javaDoubleObjectInspector, ImmutableList.of(Double.NaN, Double.NEGATIVE_INFINITY, 1.0), DataTypes.DoubleType);
        tester.testRoundTrip(javaDoubleObjectInspector, ImmutableList.of(Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY), DataTypes.DoubleType);
    }

    @Test
    public void testStringUnicode()
            throws Exception
    {
        tester.testRoundTrip(javaStringObjectInspector, limit(cycle(ImmutableList.of("apple", "apple pie", "apple\uD835\uDC03", "apple\uFFFD")), 30_000), DataTypes.StringType);
    }

    @Test
    public void testStringDirectSequence()
            throws Exception
    {
        tester.testRoundTrip(javaStringObjectInspector, transform(intsBetween(0, 30_000), Object::toString), DataTypes.StringType);
    }

    @Test
    public void testStringDictionarySequence()
            throws Exception
    {
        tester.testRoundTrip(javaStringObjectInspector, limit(cycle(transform(ImmutableList.of(1, 3, 5, 7, 11, 13, 17), Object::toString)), 30_000), DataTypes.StringType);
    }

    @Test
    public void testStringStrideDictionary()
            throws Exception
    {
        tester.testRoundTrip(javaStringObjectInspector, concat(ImmutableList.of("a"), nCopies(9999, "123"), ImmutableList.of("b"), nCopies(9999, "123")), DataTypes.StringType);
    }

    @Test
    public void testEmptyStringSequence()
            throws Exception
    {
        tester.testRoundTrip(javaStringObjectInspector, limit(cycle(""), 30_000), DataTypes.StringType);
    }

    @Test
    public void testCharDirectSequence()
            throws Exception
    {
        tester.testRoundTrip(CHAR_INSPECTOR, transform(intsBetween(0, 30_000), this::toCharValue), CHAR);
    }

    @Test
    public void testCharDictionarySequence()
            throws Exception
    {
        tester.testRoundTrip(CHAR_INSPECTOR, limit(cycle(transform(ImmutableList.of(1, 3, 5, 7, 11, 13, 17), this::toCharValue)), 30_000), CHAR);
    }

    @Test
    public void testEmptyCharSequence()
            throws Exception
    {
        tester.testRoundTrip(CHAR_INSPECTOR, limit(cycle("          "), 30_000), CHAR);
    }

    private String toCharValue(Object value)
    {
        return Strings.padEnd(value.toString(), CHAR_LENGTH, ' ');
    }

    @Test
    public void testBinaryDirectSequence()
            throws Exception
    {
        tester.testRoundTrip(
                javaByteArrayObjectInspector,
                intsBetween(0, 30_000).stream()
                        .map(Object::toString)
                        .map(string -> string.getBytes(UTF_8))
                        .map(String::new)
                        .collect(toList()),
                DataTypes.BinaryType);
    }

    @Test
    public void testBinaryDictionarySequence()
            throws Exception
    {
        tester.testRoundTrip(
                javaByteArrayObjectInspector,
                ImmutableList.copyOf(limit(cycle(ImmutableList.of(1, 3, 5, 7, 11, 13, 17)), 30_000)).stream()
                        .map(Object::toString)
                        .map(string -> string.getBytes(UTF_8))
                        .map(String::new)
                        .collect(toList()),
                DataTypes.BinaryType);
    }

    @Test
    public void testEmptyBinarySequence()
            throws Exception
    {
        tester.testRoundTrip(javaByteArrayObjectInspector, nCopies(30_000, new String(new byte[0])), DataTypes.BinaryType);
    }

    @Test
    public void testDwrfInvalidCheckpointsForRowGroupDictionary()
            throws Exception
    {
        Iterable<Integer> values = limit(cycle(concat(
                        ImmutableList.of(1), nCopies(9999, 123),
                        ImmutableList.of(2), nCopies(9999, 123),
                        ImmutableList.of(3), nCopies(9999, 123),
                        nCopies(1_000_000, null))),
                200_000);

        tester.assertRoundTrip(javaIntObjectInspector, transform(values, value -> value == null ? null : (long) value), DataTypes.LongType);

        Iterable<String> stringValue = transform(values, value -> value == null ? null : String.valueOf(value));
        tester.assertRoundTrip(javaStringObjectInspector, stringValue, DataTypes.StringType);
    }

    @Test
    public void testDwrfInvalidCheckpointsForStripeDictionary()
            throws Exception
    {
        Iterable<String> values = limit(cycle(transform(ImmutableList.of(1, 3, 5, 7, 11, 13, 17), Object::toString)), 200_000);
        tester.testRoundTrip(javaStringObjectInspector, values, DataTypes.StringType);
    }

    private static <T> Iterable<T> skipEvery(int n, Iterable<T> iterable)
    {
        return () -> new AbstractIterator<T>()
        {
            private final Iterator<T> delegate = iterable.iterator();
            private int position;

            @Override
            protected T computeNext()
            {
                while (true) {
                    if (!delegate.hasNext()) {
                        return endOfData();
                    }

                    T next = delegate.next();
                    position++;
                    if (position <= n) {
                        return next;
                    }
                    position = 0;
                }
            }
        };
    }

    private static <T> Iterable<T> repeatEach(int n, Iterable<T> iterable)
    {
        return () -> new AbstractIterator<T>()
        {
            private final Iterator<T> delegate = iterable.iterator();
            private int position;
            private T value;

            @Override
            protected T computeNext()
            {
                if (position == 0) {
                    if (!delegate.hasNext()) {
                        return endOfData();
                    }
                    value = delegate.next();
                }

                position++;
                if (position >= n) {
                    position = 0;
                }
                return value;
            }
        };
    }

    private static List<Double> doubleSequence(double start, double step, int items)
    {
        List<Double> values = new ArrayList<>();
        double nextValue = start;
        for (int i = 0; i < items; i++) {
            values.add(nextValue);
            nextValue += step;
        }
        return values;
    }

    private static List<Float> floatSequence(float start, float step, int items)
    {
        Builder<Float> values = ImmutableList.builder();
        float nextValue = start;
        for (int i = 0; i < items; i++) {
            values.add(nextValue);
            nextValue += step;
        }
        return values.build();
    }

    private static List<Decimal> decimalSequence(String start, String step, int items, int precision, int scale)
    {
        BigInteger decimalStep = new BigInteger(step);
        MathContext context = new MathContext(precision);
        List<Decimal> values = new ArrayList<>();
        BigInteger nextValue = new BigInteger(start);

        //BigDecimal bd = new BigDecimal(nextValue,scale,)

        for (int i = 0; i < items; i++) {
            BigDecimal bd = new BigDecimal(nextValue,scale,context);
            values.add(Decimal.apply(bd,precision,scale));
            nextValue = nextValue.add(decimalStep);
        }
        return values;
    }

    private static ContiguousSet<Integer> intsBetween(int lowerInclusive, int upperExclusive)
    {
        return ContiguousSet.create(Range.openClosed(lowerInclusive, upperExclusive), DiscreteDomain.integers());
    }
}
