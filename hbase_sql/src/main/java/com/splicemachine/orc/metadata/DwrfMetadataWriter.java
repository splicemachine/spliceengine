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
import com.splicemachine.orc.metadata.Stream.StreamKind;
import com.splicemachine.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.proto.DwrfProto;
import com.facebook.presto.orc.proto.DwrfProto.RowIndexEntry;
import com.facebook.presto.orc.proto.DwrfProto.Type;
import com.facebook.presto.orc.proto.DwrfProto.Type.Builder;
import com.facebook.presto.orc.proto.DwrfProto.UserMetadataItem;
import com.facebook.presto.orc.protobuf.ByteString;
import com.facebook.presto.orc.protobuf.MessageLite;
import com.google.common.collect.ImmutableList;
import com.google.common.io.CountingOutputStream;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import static java.lang.Math.toIntExact;

public class DwrfMetadataWriter
        implements MetadataWriter
{
    private static final int DWRF_WRITER_VERSION = 1;

    @Override
    public List<Integer> getOrcMetadataVersion()
    {
        // DWRF does not have a version field
        return ImmutableList.of();
    }

    @Override
    public int writePostscript(SliceOutput output, int footerLength, int metadataLength, CompressionKind compression, int compressionBlockSize)
            throws IOException
    {
        DwrfProto.PostScript postScriptProtobuf = DwrfProto.PostScript.newBuilder()
                .setFooterLength(footerLength)
                .setWriterVersion(DWRF_WRITER_VERSION)
                .setCompression(toCompression(compression))
                .setCompressionBlockSize(compressionBlockSize)
                .build();

        return writeProtobufObject(output, postScriptProtobuf);
    }

    @Override
    public int writeMetadata(SliceOutput output, Metadata metadata)
            throws IOException
    {
        return 0;
    }

    @Override
    public int writeFooter(SliceOutput output, Footer footer)
            throws IOException
    {
        DwrfProto.Footer footerProtobuf = DwrfProto.Footer.newBuilder()
                .setNumberOfRows(footer.getNumberOfRows())
                .setRowIndexStride(footer.getRowsInRowGroup())
                .addAllStripes(footer.getStripes().stream()
                        .map(DwrfMetadataWriter::toStripeInformation)
                        .collect(Collectors.toList()))
                .addAllTypes(footer.getTypes().stream()
                        .map(DwrfMetadataWriter::toType)
                        .collect(Collectors.toList()))
                .addAllStatistics(footer.getFileStats().stream()
                        .map(DwrfMetadataWriter::toColumnStatistics)
                        .collect(Collectors.toList()))
                .addAllMetadata(footer.getUserMetadata().entrySet().stream()
                        .map(DwrfMetadataWriter::toUserMetadata)
                        .collect(Collectors.toList()))
                .build();

        return writeProtobufObject(output, footerProtobuf);
    }

    private static DwrfProto.StripeInformation toStripeInformation(StripeInformation stripe)
    {
        return DwrfProto.StripeInformation.newBuilder()
                .setNumberOfRows(stripe.getNumberOfRows())
                .setOffset(stripe.getOffset())
                .setIndexLength(stripe.getIndexLength())
                .setDataLength(stripe.getDataLength())
                .setFooterLength(stripe.getFooterLength())
                .build();
    }

    private static Type toType(OrcType type)
    {
        Builder builder = Type.newBuilder()
                .setKind(toTypeKind(type.getOrcTypeKind()))
                .addAllSubtypes(type.getFieldTypeIndexes())
                .addAllFieldNames(type.getFieldNames());

        return builder.build();
    }

    private static Type.Kind toTypeKind(OrcTypeKind orcTypeKind)
    {
        switch (orcTypeKind) {
            case BOOLEAN:
                return Type.Kind.BOOLEAN;
            case BYTE:
                return Type.Kind.BYTE;
            case SHORT:
                return Type.Kind.SHORT;
            case INT:
                return Type.Kind.INT;
            case LONG:
                return Type.Kind.LONG;
            case FLOAT:
                return Type.Kind.FLOAT;
            case DOUBLE:
                return Type.Kind.DOUBLE;
            case STRING:
            case VARCHAR:
                return Type.Kind.STRING;
            case BINARY:
                return Type.Kind.BINARY;
            case TIMESTAMP:
                return Type.Kind.TIMESTAMP;
            case LIST:
                return Type.Kind.LIST;
            case MAP:
                return Type.Kind.MAP;
            case STRUCT:
                return Type.Kind.STRUCT;
            case UNION:
                return Type.Kind.UNION;
        }
        throw new IllegalArgumentException("Unsupported type: " + orcTypeKind);
    }

    private static DwrfProto.ColumnStatistics toColumnStatistics(ColumnStatistics columnStatistics)
    {
        DwrfProto.ColumnStatistics.Builder builder = DwrfProto.ColumnStatistics.newBuilder();

        if (columnStatistics.hasNumberOfValues()) {
            builder.setNumberOfValues(columnStatistics.getNumberOfValues());
        }

        if (columnStatistics.getBooleanStatistics() != null) {
            builder.setBucketStatistics(DwrfProto.BucketStatistics.newBuilder()
                    .addCount(columnStatistics.getBooleanStatistics().getTrueValueCount())
                    .build());
        }

        if (columnStatistics.getIntegerStatistics() != null) {
            builder.setIntStatistics(DwrfProto.IntegerStatistics.newBuilder()
                    .setMinimum(columnStatistics.getIntegerStatistics().getMin())
                    .setMaximum(columnStatistics.getIntegerStatistics().getMax())
                    .build());
        }

        if (columnStatistics.getDoubleStatistics() != null) {
            builder.setDoubleStatistics(DwrfProto.DoubleStatistics.newBuilder()
                    .setMinimum(columnStatistics.getDoubleStatistics().getMin())
                    .setMaximum(columnStatistics.getDoubleStatistics().getMax())
                    .build());
        }

        if (columnStatistics.getStringStatistics() != null) {
            builder.setStringStatistics(DwrfProto.StringStatistics.newBuilder()
                    .setMinimumBytes(ByteString.copyFrom(columnStatistics.getStringStatistics().getMin().getBytes()))
                    .setMaximumBytes(ByteString.copyFrom(columnStatistics.getStringStatistics().getMax().getBytes()))
                    .build());
        }

        return builder.build();
    }

    private static UserMetadataItem toUserMetadata(Entry<String, Slice> entry)
    {
        return UserMetadataItem.newBuilder()
                .setName(entry.getKey())
                .setValue(ByteString.copyFrom(entry.getValue().getBytes()))
                .build();
    }

    @Override
    public int writeStripeFooter(SliceOutput output, StripeFooter footer)
            throws IOException
    {
        DwrfProto.StripeFooter footerProtobuf = DwrfProto.StripeFooter.newBuilder()
                .addAllStreams(footer.getStreams().stream()
                        .map(DwrfMetadataWriter::toStream)
                        .collect(Collectors.toList()))
                .addAllColumns(footer.getColumnEncodings().stream()
                        .map(DwrfMetadataWriter::toColumnEncoding)
                        .collect(Collectors.toList()))
                .build();

        return writeProtobufObject(output, footerProtobuf);
    }

    private static DwrfProto.Stream toStream(Stream stream)
    {
        return DwrfProto.Stream.newBuilder()
                .setColumn(stream.getColumn())
                .setKind(toStreamKind(stream.getStreamKind()))
                .setLength(stream.getLength())
                .setUseVInts(stream.isUseVInts())
                .build();
    }

    private static DwrfProto.Stream.Kind toStreamKind(StreamKind streamKind)
    {
        switch (streamKind) {
            case PRESENT:
                return DwrfProto.Stream.Kind.PRESENT;
            case DATA:
                return DwrfProto.Stream.Kind.DATA;
            case SECONDARY:
                return DwrfProto.Stream.Kind.NANO_DATA;
            case LENGTH:
                return DwrfProto.Stream.Kind.LENGTH;
            case DICTIONARY_DATA:
                return DwrfProto.Stream.Kind.DICTIONARY_DATA;
            case DICTIONARY_COUNT:
                return DwrfProto.Stream.Kind.DICTIONARY_COUNT;
            case ROW_INDEX:
                return DwrfProto.Stream.Kind.ROW_INDEX;
        }
        throw new IllegalArgumentException("Unsupported stream kind: " + streamKind);
    }

    private static DwrfProto.ColumnEncoding toColumnEncoding(ColumnEncoding columnEncodings)
    {
        return DwrfProto.ColumnEncoding.newBuilder()
                .setKind(toColumnEncoding(columnEncodings.getColumnEncodingKind()))
                .setDictionarySize(columnEncodings.getDictionarySize())
                .build();
    }

    private static DwrfProto.ColumnEncoding.Kind toColumnEncoding(ColumnEncodingKind columnEncodingKind)
    {
        switch (columnEncodingKind) {
            case DIRECT:
                return DwrfProto.ColumnEncoding.Kind.DIRECT;
            case DICTIONARY:
                return DwrfProto.ColumnEncoding.Kind.DICTIONARY;
        }
        throw new IllegalArgumentException("Unsupported column encoding kind: " + columnEncodingKind);
    }

    @Override
    public int writeRowIndexes(SliceOutput output, List<RowGroupIndex> rowGroupIndexes)
            throws IOException
    {
        DwrfProto.RowIndex rowIndexProtobuf = DwrfProto.RowIndex.newBuilder()
                .addAllEntry(rowGroupIndexes.stream()
                        .map(DwrfMetadataWriter::toRowGroupIndex)
                        .collect(Collectors.toList()))
                .build();
        return writeProtobufObject(output, rowIndexProtobuf);
    }

    @Override
    public MetadataReader getMetadataReader()
    {
        return new DwrfMetadataReader();
    }

    private static RowIndexEntry toRowGroupIndex(RowGroupIndex rowGroupIndex)
    {
        return RowIndexEntry.newBuilder()
                .addAllPositions(rowGroupIndex.getPositions().stream()
                        .map(Integer::longValue)
                        .collect(Collectors.toList()))
                .setStatistics(toColumnStatistics(rowGroupIndex.getColumnStatistics()))
                .build();
    }

    private static DwrfProto.CompressionKind toCompression(CompressionKind compressionKind)
    {
        switch (compressionKind) {
            case NONE:
                return DwrfProto.CompressionKind.NONE;
            case ZLIB:
                return DwrfProto.CompressionKind.ZLIB;
            case SNAPPY:
                return DwrfProto.CompressionKind.SNAPPY;
        }
        throw new IllegalArgumentException("Unsupported compression kind: " + compressionKind);
    }

    private static int writeProtobufObject(OutputStream output, MessageLite object)
            throws IOException
    {
        CountingOutputStream countingOutput = new CountingOutputStream(output);
        object.writeTo(countingOutput);
        return toIntExact(countingOutput.getCount());
    }
}

