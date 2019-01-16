/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package com.splicemachine.orc.metadata;

import com.splicemachine.orc.metadata.PostScript.HiveWriterVersion;
import com.splicemachine.orc.metadata.statistics.StringStatistics;
import com.facebook.presto.orc.proto.OrcProto;
import com.facebook.presto.orc.protobuf.ByteString;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static com.splicemachine.orc.metadata.OrcMetadataReader.findStringStatisticTruncationPosition;
import static com.splicemachine.orc.metadata.OrcMetadataReader.maxStringTruncateToValidRange;
import static com.splicemachine.orc.metadata.OrcMetadataReader.minStringTruncateToValidRange;
import static com.splicemachine.orc.metadata.PostScript.HiveWriterVersion.ORC_HIVE_8732;
import static com.splicemachine.orc.metadata.PostScript.HiveWriterVersion.ORIGINAL;
import static io.airlift.slice.SliceUtf8.codePointToUtf8;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Character.MAX_CODE_POINT;
import static java.lang.Character.MAX_SURROGATE;
import static java.lang.Character.MIN_CODE_POINT;
import static java.lang.Character.MIN_SURROGATE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestOrcMetadataReader
{
    @Test
    public void testGetMinSlice()
            throws Exception
    {
        int startCodePoint = MIN_CODE_POINT;
        int endCodePoint = MAX_CODE_POINT;
        Slice minSlice = utf8Slice("");

        for (int codePoint = startCodePoint; codePoint < endCodePoint; codePoint++) {
            if (MIN_SURROGATE <= codePoint && codePoint <= MAX_SURROGATE) {
                continue;
            }
            Slice value = codePointToUtf8(codePoint);
            if (findStringStatisticTruncationPosition(value, ORIGINAL) == value.length()) {
                assertEquals(minStringTruncateToValidRange(value, ORIGINAL), value);
            }
            else {
                assertEquals(minStringTruncateToValidRange(value, ORIGINAL), minSlice);
            }
        }

        // Test with prefix
        Slice prefix = utf8Slice("apple");
        for (int codePoint = startCodePoint; codePoint < endCodePoint; codePoint++) {
            if (MIN_SURROGATE <= codePoint && codePoint <= MAX_SURROGATE) {
                continue;
            }
            Slice value = concatSlice(prefix, codePointToUtf8(codePoint));
            if (findStringStatisticTruncationPosition(value, ORIGINAL) == value.length()) {
                assertEquals(minStringTruncateToValidRange(value, ORIGINAL), value);
            }
            else {
                assertEquals(minStringTruncateToValidRange(value, ORIGINAL), prefix);
            }
        }
    }

    @Test
    public void testGetMaxSlice()
            throws Exception
    {
        int startCodePoint = MIN_CODE_POINT;
        int endCodePoint = MAX_CODE_POINT;
        Slice maxByte = wrappedBuffer((byte) 0xFF);

        for (int codePoint = startCodePoint; codePoint < endCodePoint; codePoint++) {
            if (MIN_SURROGATE <= codePoint && codePoint <= MAX_SURROGATE) {
                continue;
            }
            Slice value = codePointToUtf8(codePoint);
            if (findStringStatisticTruncationPosition(value, ORIGINAL) == value.length()) {
                assertEquals(maxStringTruncateToValidRange(value, ORIGINAL), value);
            }
            else {
                assertEquals(maxStringTruncateToValidRange(value, ORIGINAL), maxByte);
            }
        }

        // Test with prefix
        Slice prefix = utf8Slice("apple");
        Slice maxSlice = concatSlice(prefix, maxByte);
        for (int codePoint = startCodePoint; codePoint < endCodePoint; codePoint++) {
            if (MIN_SURROGATE <= codePoint && codePoint <= MAX_SURROGATE) {
                continue;
            }
            Slice value = concatSlice(prefix, codePointToUtf8(codePoint));
            if (findStringStatisticTruncationPosition(value, ORIGINAL) == value.length()) {
                assertEquals(maxStringTruncateToValidRange(value, ORIGINAL), value);
            }
            else {
                assertEquals(maxStringTruncateToValidRange(value, ORIGINAL), maxSlice);
            }
        }
    }

    private static final Slice STRING_APPLE = utf8Slice("apple");
    private static final Slice STRING_OESTERREICH = utf8Slice("\u00D6sterreich");
    private static final Slice STRING_DULIOE_DULIOE = utf8Slice("Duli\u00F6 duli\u00F6");
    private static final Slice STRING_FAITH_HOPE_LOVE = utf8Slice("\u4FE1\u5FF5,\u7231,\u5E0C\u671B");
    private static final Slice STRING_NAIVE = utf8Slice("na\u00EFve");
    private static final Slice STRING_OO = utf8Slice("\uD801\uDC2Dend");

    // length increase when cast to lower case, and ends with invalid character
    private static final Slice INVALID_SEQUENCE_TO_LOWER_EXPANDS = wrappedBuffer((byte) 0xC8, (byte) 0xBA, (byte) 0xFF);
    private static final Slice INVALID_UTF8_1 = wrappedBuffer(new byte[] {-127});
    private static final Slice INVALID_UTF8_2 = wrappedBuffer(new byte[] {50, -127, 52, 50});
    private static final byte CONTINUATION_BYTE = (byte) 0b1011_1111;
    private static final Slice EM_SPACE_SURROUNDED_BY_CONTINUATION_BYTE =
            wrappedBuffer(CONTINUATION_BYTE, (byte) 0xE2, (byte) 0x80, (byte) 0x83, CONTINUATION_BYTE);

    private static final List<Slice> VALID_UTF8_SEQUENCES = ImmutableList.<Slice>builder()
            .add(EMPTY_SLICE)
            .add(STRING_APPLE)
            .add(STRING_OESTERREICH)
            .add(STRING_DULIOE_DULIOE)
            .add(STRING_FAITH_HOPE_LOVE)
            .add(STRING_NAIVE)
            .add(STRING_OO)
            .build();

    private static final List<Slice> INVALID_UTF8_SEQUENCES = ImmutableList.<Slice>builder()
            .add(INVALID_SEQUENCE_TO_LOWER_EXPANDS)
            .add(INVALID_UTF8_1)
            .add(INVALID_UTF8_2)
            .add(EM_SPACE_SURROUNDED_BY_CONTINUATION_BYTE)
            .build();

    static final List<Slice> ALL_UTF8_SEQUENCES = ImmutableList.<Slice>builder()
            .addAll(VALID_UTF8_SEQUENCES)
            .addAll(INVALID_UTF8_SEQUENCES)
            .build();

    private static final int REPLACEMENT_CHARACTER_CODE_POINT = 0xFFFD;
    static final List<Integer> TEST_CODE_POINTS = ImmutableList.<Integer>builder()
            .add(0)
            .add((int) 'a')
            .add(0xC0)
            .add(0xC1)
            .add(0xEF)
            .add(0xFE)
            .add(0xFF)
            .add(REPLACEMENT_CHARACTER_CODE_POINT - 1)
            .add(REPLACEMENT_CHARACTER_CODE_POINT)
            .add(0xFFFE)
            .add(0xFFFF)
            .add(0x10405)
            .build();

    @Test
    public void testToStringStatistics()
    {
        // ORIGINAL version only produces stats at the row group level
        assertNull(OrcMetadataReader.toStringStatistics(
                ORIGINAL,
                OrcProto.StringStatistics.newBuilder()
                        .setMinimum("ant")
                        .setMaximum("cat")
                        .setSum(44)
                        .build(),
                false));

        // if min and max are both null, no stat is ever produced
        for (HiveWriterVersion hiveWriterVersion : HiveWriterVersion.values()) {
            for (boolean isRowGroup : ImmutableList.of(true, false)) {
                assertNull(OrcMetadataReader.toStringStatistics(
                        hiveWriterVersion,
                        OrcProto.StringStatistics.newBuilder()
                                .setSum(45)
                                .build(),
                        isRowGroup));
            }
        }

        // having only a min or max should work
        assertEquals(
                OrcMetadataReader.toStringStatistics(
                        ORC_HIVE_8732,
                        OrcProto.StringStatistics.newBuilder()
                                .setMinimum("ant")
                                .build(),
                        true),
                new StringStatistics(utf8Slice("ant"), null, 0));
        assertEquals(
                OrcMetadataReader.toStringStatistics(
                        ORC_HIVE_8732,
                        OrcProto.StringStatistics.newBuilder()
                                .setMaximum("cat")
                                .build(),
                        true),
                new StringStatistics(null, utf8Slice("cat"), 0));

        // normal full stat
        assertEquals(
                OrcMetadataReader.toStringStatistics(
                        ORC_HIVE_8732,
                        OrcProto.StringStatistics.newBuilder()
                                .setMinimum("ant")
                                .setMaximum("cat")
                                .setSum(79)
                                .build(),
                        true),
                new StringStatistics(utf8Slice("ant"), utf8Slice("cat"), 79));

        for (Slice prefix : ALL_UTF8_SEQUENCES) {
            for (int testCodePoint : TEST_CODE_POINTS) {
                Slice codePoint = codePointToUtf8(testCodePoint);
                for (Slice suffix : ALL_UTF8_SEQUENCES) {
                    Slice testValue = concatSlice(prefix, codePoint, suffix);
                    testStringStatisticsTruncation(testValue, ORIGINAL);
                    testStringStatisticsTruncation(testValue, ORC_HIVE_8732);
                }
            }
        }
    }

    private static void testStringStatisticsTruncation(Slice testValue, HiveWriterVersion version)
    {
        assertEquals(
                OrcMetadataReader.toStringStatistics(
                        version,
                        OrcProto.StringStatistics.newBuilder()
                                .setMinimumBytes(ByteString.copyFrom(testValue.getBytes()))
                                .setMaximumBytes(ByteString.copyFrom(testValue.getBytes()))
                                .setSum(79)
                                .build(),
                        true),
                createExpectedStringStatistics(version, testValue, testValue, 79));
        assertEquals(
                OrcMetadataReader.toStringStatistics(
                        version,
                        OrcProto.StringStatistics.newBuilder()
                                .setMinimumBytes(ByteString.copyFrom(testValue.getBytes()))
                                .setSum(79)
                                .build(),
                        true),
                createExpectedStringStatistics(version, testValue, null, 79));
        assertEquals(
                OrcMetadataReader.toStringStatistics(
                        version,
                        OrcProto.StringStatistics.newBuilder()
                                .setMaximumBytes(ByteString.copyFrom(testValue.getBytes()))
                                .setSum(79)
                                .build(),
                        true),
                createExpectedStringStatistics(version, null, testValue, 79));
    }

    private static StringStatistics createExpectedStringStatistics(HiveWriterVersion version, Slice min, Slice max, int sum)
    {
        return new StringStatistics(
                minStringTruncateToValidRange(min, version),
                maxStringTruncateToValidRange(max, version),
                sum);
    }

    @Test
    public void testMinStringTruncateAtFirstReplacementCharacter()
            throws Exception
    {
        for (Slice prefix : VALID_UTF8_SEQUENCES) {
            for (Slice suffix : VALID_UTF8_SEQUENCES) {
                testMinStringTruncateAtFirstReplacementCharacter(prefix, suffix);
            }
        }
    }

    private static void testMinStringTruncateAtFirstReplacementCharacter(Slice prefix, Slice suffix)
    {
        for (int testCodePoint : TEST_CODE_POINTS) {
            Slice codePoint = codePointToUtf8(testCodePoint);

            Slice value = concatSlice(prefix, codePoint, suffix);

            if (testCodePoint == REPLACEMENT_CHARACTER_CODE_POINT) {
                assertEquals(minStringTruncateToValidRange(value, ORC_HIVE_8732), prefix);
            }
            else {
                assertEquals(minStringTruncateToValidRange(value, ORC_HIVE_8732), value);
            }
        }
    }

    @Test
    public void testMaxStringTruncateAtFirstReplacementCharacter()
            throws Exception
    {
        for (Slice prefix : VALID_UTF8_SEQUENCES) {
            for (Slice suffix : VALID_UTF8_SEQUENCES) {
                testMaxStringTruncateAtFirstReplacementCharacter(prefix, suffix);
            }
        }
    }

    private static void testMaxStringTruncateAtFirstReplacementCharacter(Slice prefix, Slice suffix)
    {
        Slice expectedTruncatedValue = concatSlice(prefix, wrappedBuffer((byte) 0xFF));
        for (int testCodePoint : TEST_CODE_POINTS) {
            Slice codePoint = codePointToUtf8(testCodePoint);

            Slice value = concatSlice(prefix, codePoint, suffix);

            if (testCodePoint == REPLACEMENT_CHARACTER_CODE_POINT) {
                assertEquals(maxStringTruncateToValidRange(value, ORC_HIVE_8732), expectedTruncatedValue);
            }
            else {
                assertEquals(maxStringTruncateToValidRange(value, ORC_HIVE_8732), value);
            }
        }
    }

    static Slice concatSlice(Slice... slices)
    {
        int totalLength = Arrays.stream(slices)
                .mapToInt(Slice::length)
                .sum();
        Slice value = Slices.allocate(totalLength);
        SliceOutput output = value.getOutput();
        for (Slice slice : slices) {
            output.writeBytes(slice);
        }
        return value;
    }
}
