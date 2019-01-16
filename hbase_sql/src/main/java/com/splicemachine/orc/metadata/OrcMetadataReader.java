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

import com.splicemachine.orc.metadata.ColumnEncoding.ColumnEncodingKind;
import com.splicemachine.orc.metadata.OrcType.OrcTypeKind;
import com.splicemachine.orc.metadata.PostScript.HiveWriterVersion;
import com.splicemachine.orc.metadata.Stream.StreamKind;
import com.splicemachine.orc.metadata.statistics.*;
import com.facebook.presto.orc.proto.OrcProto;
import com.facebook.presto.orc.proto.OrcProto.RowIndexEntry;
import com.facebook.presto.orc.protobuf.ByteString;
import com.facebook.presto.orc.protobuf.CodedInputStream;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceUtf8;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.splicemachine.orc.metadata.CompressionKind.*;
import static com.splicemachine.orc.metadata.PostScript.HiveWriterVersion.ORC_HIVE_8732;
import static com.splicemachine.orc.metadata.PostScript.HiveWriterVersion.ORIGINAL;
import static com.splicemachine.orc.metadata.statistics.BinaryStatistics.BINARY_VALUE_BYTES_OVERHEAD;
import static com.splicemachine.orc.metadata.statistics.BooleanStatistics.BOOLEAN_VALUE_BYTES;
import static com.splicemachine.orc.metadata.statistics.DateStatistics.DATE_VALUE_BYTES;
import static com.splicemachine.orc.metadata.statistics.DecimalStatistics.DECIMAL_VALUE_BYTES_OVERHEAD;
import static com.splicemachine.orc.metadata.statistics.DoubleStatistics.DOUBLE_VALUE_BYTES;
import static com.splicemachine.orc.metadata.statistics.IntegerStatistics.INTEGER_VALUE_BYTES;
import static com.splicemachine.orc.metadata.statistics.ShortDecimalStatisticsBuilder.SHORT_DECIMAL_VALUE_BYTES;
import static com.splicemachine.orc.metadata.statistics.StringStatistics.STRING_VALUE_BYTES_OVERHEAD;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SliceUtf8.lengthOfCodePoint;
import static io.airlift.slice.SliceUtf8.tryGetCodePointAt;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static java.lang.Character.MIN_SUPPLEMENTARY_CODE_POINT;
import static java.lang.Math.toIntExact;

public class OrcMetadataReader
        implements MetadataReader
{
    private static final int REPLACEMENT_CHARACTER_CODE_POINT = 0xFFFD;
    @VisibleForTesting
    static final Slice REPLACEMENT_CHARACTER_UTF8 = SliceUtf8.codePointToUtf8(REPLACEMENT_CHARACTER_CODE_POINT);
    private static final Slice MAX_BYTE = Slices.wrappedBuffer(new byte[] {(byte) 0xFF});
    private static final Logger log = Logger.get(OrcMetadataReader.class);

    private static final int PROTOBUF_MESSAGE_MAX_LIMIT = toIntExact(new DataSize(1, GIGABYTE).toBytes());

    @Override
    public PostScript readPostScript(ByteBuffer byteBuffer)
            throws IOException
    {
        CodedInputStream input = CodedInputStream.newInstance(byteBuffer);
        OrcProto.PostScript postScript = OrcProto.PostScript.parseFrom(input);

        return new PostScript(
                postScript.getVersionList(),
                postScript.getFooterLength(),
                postScript.getMetadataLength(),
                toCompression(postScript.getCompression()),
                postScript.getCompressionBlockSize(),
                toHiveWriterVersion(postScript.getWriterVersion()));
    }

    private static HiveWriterVersion toHiveWriterVersion(int writerVersion)
    {
        if (writerVersion >= 1) {
            return ORC_HIVE_8732;
        }
        return ORIGINAL;
    }

    @Override
    public Metadata readMetadata(HiveWriterVersion hiveWriterVersion, InputStream inputStream)
            throws IOException
    {
        CodedInputStream input = CodedInputStream.newInstance(inputStream);
        input.setSizeLimit(PROTOBUF_MESSAGE_MAX_LIMIT);
        OrcProto.Metadata metadata = OrcProto.Metadata.parseFrom(input);
        return new Metadata(toStripeStatistics(hiveWriterVersion, metadata.getStripeStatsList()));
    }

    private static List<StripeStatistics> toStripeStatistics(HiveWriterVersion hiveWriterVersion, List<OrcProto.StripeStatistics> types)
    {
        return ImmutableList.copyOf(Iterables.transform(types, stripeStatistics -> toStripeStatistics(hiveWriterVersion, stripeStatistics)));
    }

    private static StripeStatistics toStripeStatistics(HiveWriterVersion hiveWriterVersion, OrcProto.StripeStatistics stripeStatistics)
    {
        return new StripeStatistics(toColumnStatistics(hiveWriterVersion, stripeStatistics.getColStatsList(), false));
    }

    @Override
    public Footer readFooter(HiveWriterVersion hiveWriterVersion, InputStream inputStream)
            throws IOException
    {
        CodedInputStream input = CodedInputStream.newInstance(inputStream);
        input.setSizeLimit(PROTOBUF_MESSAGE_MAX_LIMIT);
        OrcProto.Footer footer = OrcProto.Footer.parseFrom(input);
        return new Footer(
                footer.getNumberOfRows(),
                footer.getRowIndexStride(),
                toStripeInformation(footer.getStripesList()),
                toType(footer.getTypesList()),
                toColumnStatistics(hiveWriterVersion, footer.getStatisticsList(), false),
                toUserMetadata(footer.getMetadataList()));
    }

    private static List<StripeInformation> toStripeInformation(List<OrcProto.StripeInformation> types)
    {
        return ImmutableList.copyOf(Iterables.transform(types, OrcMetadataReader::toStripeInformation));
    }

    private static StripeInformation toStripeInformation(OrcProto.StripeInformation stripeInformation)
    {
        return new StripeInformation(
                toIntExact(stripeInformation.getNumberOfRows()),
                stripeInformation.getOffset(),
                stripeInformation.getIndexLength(),
                stripeInformation.getDataLength(),
                stripeInformation.getFooterLength());
    }

    @Override
    public StripeFooter readStripeFooter(List<OrcType> types, InputStream inputStream)
            throws IOException
    {
        CodedInputStream input = CodedInputStream.newInstance(inputStream);
        OrcProto.StripeFooter stripeFooter = OrcProto.StripeFooter.parseFrom(input);
        return new StripeFooter(toStream(stripeFooter.getStreamsList()), toColumnEncoding(stripeFooter.getColumnsList()));
    }

    private static Stream toStream(OrcProto.Stream stream)
    {
        return new Stream(stream.getColumn(), toStreamKind(stream.getKind()), toIntExact(stream.getLength()), true);
    }

    private static List<Stream> toStream(List<OrcProto.Stream> streams)
    {
        return ImmutableList.copyOf(Iterables.transform(streams, OrcMetadataReader::toStream));
    }

    private static ColumnEncoding toColumnEncoding(OrcProto.ColumnEncoding columnEncoding)
    {
        return new ColumnEncoding(toColumnEncodingKind(columnEncoding.getKind()), columnEncoding.getDictionarySize());
    }

    private static List<ColumnEncoding> toColumnEncoding(List<OrcProto.ColumnEncoding> columnEncodings)
    {
        return ImmutableList.copyOf(Iterables.transform(columnEncodings, OrcMetadataReader::toColumnEncoding));
    }

    @Override
    public List<RowGroupIndex> readRowIndexes(HiveWriterVersion hiveWriterVersion, InputStream inputStream)
            throws IOException
    {
        CodedInputStream input = CodedInputStream.newInstance(inputStream);
        OrcProto.RowIndex rowIndex = OrcProto.RowIndex.parseFrom(input);
        return ImmutableList.copyOf(Iterables.transform(rowIndex.getEntryList(), rowIndexEntry -> toRowGroupIndex(hiveWriterVersion, rowIndexEntry)));
    }

    @Override
    public List<HiveBloomFilter> readBloomFilterIndexes(InputStream inputStream)
            throws IOException
    {
        CodedInputStream input = CodedInputStream.newInstance(inputStream);
        OrcProto.BloomFilterIndex bloomFilter = OrcProto.BloomFilterIndex.parseFrom(input);
        List<OrcProto.BloomFilter> bloomFilterList = bloomFilter.getBloomFilterList();
        ImmutableList.Builder<HiveBloomFilter> builder = ImmutableList.builder();
        for (OrcProto.BloomFilter orcBloomFilter : bloomFilterList) {
            builder.add(new HiveBloomFilter(orcBloomFilter.getBitsetList(), orcBloomFilter.getBitsetCount() * 64, orcBloomFilter.getNumHashFunctions()));
        }
        return builder.build();
    }

    private static RowGroupIndex toRowGroupIndex(HiveWriterVersion hiveWriterVersion, RowIndexEntry rowIndexEntry)
    {
        List<Long> positionsList = rowIndexEntry.getPositionsList();
        ImmutableList.Builder<Integer> positions = ImmutableList.builder();
        for (int index = 0; index < positionsList.size(); index++) {
            long longPosition = positionsList.get(index);
            int intPosition = (int) longPosition;

            checkState(intPosition == longPosition, "Expected checkpoint position %s, to be an integer", index);

            positions.add(intPosition);
        }
        return new RowGroupIndex(positions.build(), toColumnStatistics(hiveWriterVersion, rowIndexEntry.getStatistics(), true));
    }

    private static ColumnStatistics toColumnStatistics(HiveWriterVersion hiveWriterVersion, OrcProto.ColumnStatistics statistics, boolean isRowGroup)
    {
        long minAverageValueBytes;

        if (statistics.hasBucketStatistics()) {
            minAverageValueBytes = BOOLEAN_VALUE_BYTES;
        }
        else if (statistics.hasIntStatistics()) {
            minAverageValueBytes = INTEGER_VALUE_BYTES;
        }
        else if (statistics.hasDoubleStatistics()) {
            minAverageValueBytes = DOUBLE_VALUE_BYTES;
        }
        else if (statistics.hasStringStatistics()) {
            minAverageValueBytes = STRING_VALUE_BYTES_OVERHEAD;
            if (statistics.hasNumberOfValues() && statistics.getNumberOfValues() > 0) {
                minAverageValueBytes += statistics.getStringStatistics().getSum() / statistics.getNumberOfValues();
            }
        }
        else if (statistics.hasDateStatistics()) {
            minAverageValueBytes = DATE_VALUE_BYTES;
        }
        else if (statistics.hasDecimalStatistics()) {
            // could be 8 or 16; return the smaller one given it is a min average
            minAverageValueBytes = DECIMAL_VALUE_BYTES_OVERHEAD + SHORT_DECIMAL_VALUE_BYTES;
        }
        else if (statistics.hasBinaryStatistics()) {
            // offset and value length
            minAverageValueBytes = BINARY_VALUE_BYTES_OVERHEAD;
            if (statistics.hasNumberOfValues() && statistics.getNumberOfValues() > 0) {
                minAverageValueBytes += statistics.getBinaryStatistics().getSum() / statistics.getNumberOfValues();
            }
        }
        else {
            minAverageValueBytes = 0;
        }

        return new ColumnStatistics(
                statistics.getNumberOfValues(),
                minAverageValueBytes,
                toBooleanStatistics(statistics.getBucketStatistics()),
                toIntegerStatistics(statistics.getIntStatistics()),
                toDoubleStatistics(statistics.getDoubleStatistics()),
                toStringStatistics(hiveWriterVersion, statistics.getStringStatistics(), isRowGroup),
                toDateStatistics(hiveWriterVersion, statistics.getDateStatistics(), isRowGroup),
                toDecimalStatistics(statistics.getDecimalStatistics()),
                toBinaryStatistics(statistics.getBinaryStatistics()),
                null);
    }

    private static List<ColumnStatistics> toColumnStatistics(HiveWriterVersion hiveWriterVersion, List<OrcProto.ColumnStatistics> columnStatistics, boolean isRowGroup)
    {
        if (columnStatistics == null) {
            return ImmutableList.of();
        }
        return ImmutableList.copyOf(Iterables.transform(columnStatistics, statistics -> toColumnStatistics(hiveWriterVersion, statistics, isRowGroup)));
    }

    private Map<String, Slice> toUserMetadata(List<OrcProto.UserMetadataItem> metadataList)
    {
        ImmutableMap.Builder<String, Slice> mapBuilder = ImmutableMap.builder();
        for (OrcProto.UserMetadataItem item : metadataList) {
            mapBuilder.put(item.getName(), byteStringToSlice(item.getValue()));
        }
        return mapBuilder.build();
    }

    private static BooleanStatistics toBooleanStatistics(OrcProto.BucketStatistics bucketStatistics)
    {
        if (bucketStatistics.getCountCount() == 0) {
            return null;
        }

        return new BooleanStatistics(bucketStatistics.getCount(0));
    }

    private static IntegerStatistics toIntegerStatistics(OrcProto.IntegerStatistics integerStatistics)
    {
        if (!integerStatistics.hasMinimum() && !integerStatistics.hasMaximum()) {
            return null;
        }

        return new IntegerStatistics(
                integerStatistics.hasMinimum() ? integerStatistics.getMinimum() : null,
                integerStatistics.hasMaximum() ? integerStatistics.getMaximum() : null);
    }

    private static DoubleStatistics toDoubleStatistics(OrcProto.DoubleStatistics doubleStatistics)
    {
        if (!doubleStatistics.hasMinimum() && !doubleStatistics.hasMaximum()) {
            return null;
        }

        // TODO remove this when double statistics are changed to correctly deal with NaNs
        // if either min, max, or sum is NaN, ignore the stat
        if ((doubleStatistics.hasMinimum() && Double.isNaN(doubleStatistics.getMinimum())) ||
                (doubleStatistics.hasMaximum() && Double.isNaN(doubleStatistics.getMaximum())) ||
                (doubleStatistics.hasSum() && Double.isNaN(doubleStatistics.getSum()))) {
            return null;
        }

        return new DoubleStatistics(
                doubleStatistics.hasMinimum() ? doubleStatistics.getMinimum() : null,
                doubleStatistics.hasMaximum() ? doubleStatistics.getMaximum() : null);
    }

    static StringStatistics toStringStatistics(HiveWriterVersion hiveWriterVersion, OrcProto.StringStatistics stringStatistics, boolean isRowGroup)
    {
        if (hiveWriterVersion == ORIGINAL && !isRowGroup) {
            return null;
        }

        if (!stringStatistics.hasMinimum() && !stringStatistics.hasMaximum()) {
            return null;
        }

        Slice maximum = stringStatistics.hasMaximum() ? maxStringTruncateToValidRange(byteStringToSlice(stringStatistics.getMaximumBytes()), hiveWriterVersion) : null;
        Slice minimum = stringStatistics.hasMinimum() ? minStringTruncateToValidRange(byteStringToSlice(stringStatistics.getMinimumBytes()), hiveWriterVersion) : null;
        long sum = stringStatistics.hasSum() ? stringStatistics.getSum() : 0;
        return new StringStatistics(minimum, maximum, sum);
    }

    private static DecimalStatistics toDecimalStatistics(OrcProto.DecimalStatistics decimalStatistics)
    {
        if (!decimalStatistics.hasMinimum() && !decimalStatistics.hasMaximum()) {
            return null;
        }

        BigDecimal minimum = decimalStatistics.hasMinimum() ? new BigDecimal(decimalStatistics.getMinimum()) : null;
        BigDecimal maximum = decimalStatistics.hasMaximum() ? new BigDecimal(decimalStatistics.getMaximum()) : null;

        return new DecimalStatistics(minimum, maximum);
    }

    private static BinaryStatistics toBinaryStatistics(OrcProto.BinaryStatistics binaryStatistics)
    {
        if (!binaryStatistics.hasSum()) {
            return null;
        }

        return new BinaryStatistics(binaryStatistics.getSum());
    }

    static Slice byteStringToSlice(ByteString value)
    {
        return Slices.wrappedBuffer(value.toByteArray());
    }

    @VisibleForTesting
    public static Slice maxStringTruncateToValidRange(Slice value, HiveWriterVersion version)
    {
        if (value == null) {
            return null;
        }

        int index = findStringStatisticTruncationPosition(value, version);
        if (index == value.length()) {
            return value;
        }
        // Append 0xFF so that it is larger than value
        Slice newValue = Slices.copyOf(value, 0, index + 1);
        newValue.setByte(index, 0xFF);
        return newValue;
    }

    @VisibleForTesting
    public static Slice minStringTruncateToValidRange(Slice value, HiveWriterVersion version)
    {
        if (value == null) {
            return null;
        }

        int index = findStringStatisticTruncationPosition(value, version);
        if (index == value.length()) {
            return value;
        }
        return Slices.copyOf(value, 0, index);
    }

    @VisibleForTesting
    static int findStringStatisticTruncationPosition(Slice utf8, HiveWriterVersion version)
    {
        int length = utf8.length();

        int position = 0;
        while (position < length) {
            int codePoint = tryGetCodePointAt(utf8, position);

            // stop at invalid sequences
            if (codePoint < 0) {
                break;
            }

            // Currently, all versions of the ORC writer round trip string min and max values through java.lang.String which
            // replaces invalid UTF-8 sequences with the unicode replacement character.  This can cause the min value to be
            // greater than expected which can result in data sections being skipped instead of being processed. As a work around,
            // the string stats are truncated at the first replacement character.
            if (codePoint == REPLACEMENT_CHARACTER_CODE_POINT) {
                break;
            }

            // The original writer performs comparisons using java Strings to determine the minimum and maximum
            // values. This results in weird behaviors in the presence of surrogate pairs and special characters.
            //
            // For example, unicode code point 0x1D403 has the following representations:
            // UTF-16: [0xD835, 0xDC03]
            // UTF-8: [0xF0, 0x9D, 0x90, 0x83]
            //
            // while code point 0xFFFD (the replacement character) has the following representations:
            // UTF-16: [0xFFFD]
            // UTF-8: [0xEF, 0xBF, 0xBD]
            //
            // when comparisons between strings containing these characters are done with Java Strings (UTF-16),
            // 0x1D403 < 0xFFFD, but when comparisons are done using raw codepoints or UTF-8, 0x1D403 > 0xFFFD
            //
            // We use the following logic to ensure that we have a wider range of min-max
            // * if a min string has a surrogate character, the min string is truncated
            //   at the first occurrence of the surrogate character (to exclude the surrogate character)
            // * if a max string has a surrogate character, the max string is truncated
            //   at the first occurrence the surrogate character and 0xFF byte is appended to it.
            if (version == ORIGINAL && codePoint >= MIN_SUPPLEMENTARY_CODE_POINT) {
                break;
            }

            position += lengthOfCodePoint(codePoint);
        }
        return position;
    }

    private static DateStatistics toDateStatistics(HiveWriterVersion hiveWriterVersion, OrcProto.DateStatistics dateStatistics, boolean isRowGroup)
    {
        if (hiveWriterVersion == ORIGINAL && !isRowGroup) {
            return null;
        }

        if (!dateStatistics.hasMinimum() && !dateStatistics.hasMaximum()) {
            return null;
        }

        return new DateStatistics(
                dateStatistics.hasMinimum() ? dateStatistics.getMinimum() : null,
                dateStatistics.hasMaximum() ? dateStatistics.getMaximum() : null);
    }

    private static OrcType toType(OrcProto.Type type)
    {
        Optional<Integer> length = Optional.empty();
        if (type.getKind() == OrcProto.Type.Kind.VARCHAR || type.getKind() == OrcProto.Type.Kind.CHAR) {
            length = Optional.of(type.getMaximumLength());
        }
        Optional<Integer> precision = Optional.empty();
        Optional<Integer> scale = Optional.empty();
        if (type.getKind() == OrcProto.Type.Kind.DECIMAL) {
            precision = Optional.of(type.getPrecision());
            scale = Optional.of(type.getScale());
        }
        return new OrcType(toTypeKind(type.getKind()), type.getSubtypesList(), type.getFieldNamesList(), length, precision, scale);
    }

    private static List<OrcType> toType(List<OrcProto.Type> types)
    {
        return ImmutableList.copyOf(Iterables.transform(types, OrcMetadataReader::toType));
    }

    private static OrcTypeKind toTypeKind(OrcProto.Type.Kind typeKind)
    {
        switch (typeKind) {
            case BOOLEAN:
                return OrcTypeKind.BOOLEAN;
            case BYTE:
                return OrcTypeKind.BYTE;
            case SHORT:
                return OrcTypeKind.SHORT;
            case INT:
                return OrcTypeKind.INT;
            case LONG:
                return OrcTypeKind.LONG;
            case FLOAT:
                return OrcTypeKind.FLOAT;
            case DOUBLE:
                return OrcTypeKind.DOUBLE;
            case STRING:
                return OrcTypeKind.STRING;
            case BINARY:
                return OrcTypeKind.BINARY;
            case TIMESTAMP:
                return OrcTypeKind.TIMESTAMP;
            case LIST:
                return OrcTypeKind.LIST;
            case MAP:
                return OrcTypeKind.MAP;
            case STRUCT:
                return OrcTypeKind.STRUCT;
            case UNION:
                return OrcTypeKind.UNION;
            case DECIMAL:
                return OrcTypeKind.DECIMAL;
            case DATE:
                return OrcTypeKind.DATE;
            case VARCHAR:
                return OrcTypeKind.VARCHAR;
            case CHAR:
                return OrcTypeKind.CHAR;
            default:
                throw new IllegalStateException(typeKind + " stream type not implemented yet");
        }
    }

    private static StreamKind toStreamKind(OrcProto.Stream.Kind streamKind)
    {
        switch (streamKind) {
            case PRESENT:
                return StreamKind.PRESENT;
            case DATA:
                return StreamKind.DATA;
            case LENGTH:
                return StreamKind.LENGTH;
            case DICTIONARY_DATA:
                return StreamKind.DICTIONARY_DATA;
            case DICTIONARY_COUNT:
                return StreamKind.DICTIONARY_COUNT;
            case SECONDARY:
                return StreamKind.SECONDARY;
            case ROW_INDEX:
                return StreamKind.ROW_INDEX;
            case BLOOM_FILTER:
                return StreamKind.BLOOM_FILTER;
            default:
                throw new IllegalStateException(streamKind + " stream type not implemented yet");
        }
    }

    private static ColumnEncodingKind toColumnEncodingKind(OrcProto.ColumnEncoding.Kind columnEncodingKind)
    {
        switch (columnEncodingKind) {
            case DIRECT:
                return ColumnEncodingKind.DIRECT;
            case DIRECT_V2:
                return ColumnEncodingKind.DIRECT_V2;
            case DICTIONARY:
                return ColumnEncodingKind.DICTIONARY;
            case DICTIONARY_V2:
                return ColumnEncodingKind.DICTIONARY_V2;
            default:
                throw new IllegalStateException(columnEncodingKind + " stream encoding not implemented yet");
        }
    }

    private static CompressionKind toCompression(OrcProto.CompressionKind compression)
    {
        switch (compression) {
            case NONE:
                return NONE;
            case ZLIB:
                return ZLIB;
            case SNAPPY:
                return SNAPPY;
            default:
                throw new IllegalStateException(compression + " compression not implemented yet");
        }
    }
}

