/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

import com.splicemachine.orc.memory.AbstractAggregatedMemoryContext;
import com.splicemachine.orc.memory.AggregatedMemoryContext;
import com.splicemachine.orc.metadata.*;
import com.splicemachine.orc.metadata.PostScript.HiveWriterVersion;
import com.splicemachine.orc.stream.OrcInputStream;
import com.google.common.base.Joiner;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import org.apache.spark.sql.types.DataType;
import org.joda.time.DateTimeZone;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class OrcReader
{
    public static final int MAX_BATCH_SIZE = 1024;

    private static final Logger log = Logger.get(OrcReader.class);

    private static final Slice MAGIC = Slices.utf8Slice("ORC");
    private static final int CURRENT_MAJOR_VERSION = 0;
    private static final int CURRENT_MINOR_VERSION = 12;
    private static final int EXPECTED_FOOTER_SIZE = 128 * 1024;

    private final OrcDataSource orcDataSource;
    private final MetadataReader metadataReader;
    private final DataSize maxMergeDistance;
    private final DataSize maxReadSize;
    private final CompressionKind compressionKind;
    private final HiveWriterVersion hiveWriterVersion;
    private final int bufferSize;
    private final Footer footer;
    private final Metadata metadata;

    // This is based on the Apache Hive ORC code
    public OrcReader(OrcDataSource orcDataSource, MetadataReader metadataReader, DataSize maxMergeDistance, DataSize maxReadSize)
            throws IOException
    {
        orcDataSource = wrapWithCacheIfTiny(requireNonNull(orcDataSource, "orcDataSource is null"), maxMergeDistance);
        this.orcDataSource = orcDataSource;
        this.metadataReader = requireNonNull(metadataReader, "metadataReader is null");
        this.maxMergeDistance = requireNonNull(maxMergeDistance, "maxMergeDistance is null");
        this.maxReadSize = requireNonNull(maxReadSize, "maxReadSize is null");

        //
        // Read the file tail:
        //
        // variable: Footer
        // variable: Metadata
        // variable: PostScript - contains length of footer and metadata
        // 3 bytes: file magic "ORC"
        // 1 byte: postScriptSize = PostScript + Magic

        // figure out the size of the file using the option or filesystem
        long size = orcDataSource.getSize();
        if (size <= 0) {
            throw new OrcCorruptionException("Malformed ORC file %s. Invalid file size %s", orcDataSource, size);
        }

        // Read the tail of the file
        byte[] buffer = new byte[toIntExact(min(size, EXPECTED_FOOTER_SIZE))];
        orcDataSource.readFully(size - buffer.length, buffer);

        // get length of PostScript - last byte of the file
        int postScriptSize = buffer[buffer.length - SIZE_OF_BYTE] & 0xff;

        // make sure this is an ORC file and not an RCFile or something else
        verifyOrcFooter(orcDataSource, postScriptSize, buffer);

        // decode the post script
        int postScriptOffset = buffer.length - SIZE_OF_BYTE - postScriptSize;
        PostScript postScript = metadataReader.readPostScript(buffer, postScriptOffset, postScriptSize);

        // verify this is a supported version
        checkOrcVersion(orcDataSource, postScript.getVersion());

        // check compression codec is supported
        this.compressionKind = postScript.getCompression();

        this.hiveWriterVersion = postScript.getHiveWriterVersion();
        this.bufferSize = toIntExact(postScript.getCompressionBlockSize());

        int footerSize = toIntExact(postScript.getFooterLength());
        int metadataSize = toIntExact(postScript.getMetadataLength());

        // check if extra bytes need to be read
        Slice completeFooterSlice;
        int completeFooterSize = footerSize + metadataSize + postScriptSize + SIZE_OF_BYTE;
        if (completeFooterSize > buffer.length) {
            // allocate a new buffer large enough for the complete footer
            byte[] newBuffer = new byte[completeFooterSize];
            completeFooterSlice = Slices.wrappedBuffer(newBuffer);

            // initial read was not large enough, so read missing section
            orcDataSource.readFully(size - completeFooterSize, newBuffer, 0, completeFooterSize - buffer.length);

            // copy already read bytes into the new buffer
            completeFooterSlice.setBytes(completeFooterSize - buffer.length, buffer);
        }
        else {
            // footer is already in the bytes in buffer, just adjust position, length
            completeFooterSlice = Slices.wrappedBuffer(buffer, buffer.length - completeFooterSize, completeFooterSize);
        }

        // read metadata
        Slice metadataSlice = completeFooterSlice.slice(0, metadataSize);
        try (InputStream metadataInputStream = new OrcInputStream(orcDataSource.toString(), metadataSlice.getInput(), compressionKind, bufferSize, new AggregatedMemoryContext())) {
            this.metadata = metadataReader.readMetadata(hiveWriterVersion, metadataInputStream);
        }

        // read footer
        Slice footerSlice = completeFooterSlice.slice(metadataSize, footerSize);
        try (InputStream footerInputStream = new OrcInputStream(orcDataSource.toString(), footerSlice.getInput(), compressionKind, bufferSize, new AggregatedMemoryContext())) {
            this.footer = metadataReader.readFooter(hiveWriterVersion, footerInputStream);
        }
    }

    public List<String> getColumnNames()
    {
        return footer.getTypes().get(0).getFieldNames();
    }

    public Footer getFooter()
    {
        return footer;
    }

    public Metadata getMetadata()
    {
        return metadata;
    }

    public CompressionKind getCompressionKind()
    {
        return compressionKind;
    }

    public int getBufferSize()
    {
        return bufferSize;
    }

    public OrcRecordReader createRecordReader(Map<Integer, DataType> includedColumns, OrcPredicate predicate, DateTimeZone hiveStorageTimeZone, AbstractAggregatedMemoryContext systemMemoryUsage,
                                              List<Integer> partitionIds, List<String> partitionValues)
            throws IOException
    {
        return createRecordReader(includedColumns, predicate, 0, orcDataSource.getSize(), hiveStorageTimeZone, systemMemoryUsage,
                partitionIds,partitionValues);
    }

    public OrcRecordReader createRecordReader(
            Map<Integer, DataType> includedColumns,
            OrcPredicate predicate,
            long offset,
            long length,
            DateTimeZone hiveStorageTimeZone,
            AbstractAggregatedMemoryContext systemMemoryUsage,
            List<Integer> partitionIds,
            List<String> partitionValues
            )
            throws IOException
    {
        return new OrcRecordReader(
                requireNonNull(includedColumns, "includedColumns is null"),
                requireNonNull(predicate, "predicate is null"),
                footer.getNumberOfRows(),
                footer.getStripes(),
                footer.getFileStats(),
                metadata.getStripeStatsList(),
                orcDataSource,
                offset,
                length,
                footer.getTypes(),
                compressionKind,
                bufferSize,
                footer.getRowsInRowGroup(),
                requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null"),
                hiveWriterVersion,
                metadataReader,
                maxMergeDistance,
                maxReadSize,
                footer.getUserMetadata(),
                systemMemoryUsage,
                partitionIds,
                partitionValues);
    }

    private static OrcDataSource wrapWithCacheIfTiny(OrcDataSource dataSource, DataSize maxCacheSize)
    {
        if (dataSource instanceof CachingOrcDataSource) {
            return dataSource;
        }
        if (dataSource.getSize() > maxCacheSize.toBytes()) {
            return dataSource;
        }
        DiskRange diskRange = new DiskRange(0, toIntExact(dataSource.getSize()));
        return new CachingOrcDataSource(dataSource, desiredOffset -> diskRange);
    }

    /**
     * Verify this is an ORC file to prevent users from trying to read text
     * files or RC files as ORC files.
     */
    // This is based on the Apache Hive ORC code
    private static void verifyOrcFooter(
            OrcDataSource source,
            int postScriptSize,
            byte[] buffer)
            throws IOException
    {
        int magicLength = MAGIC.length();
        if (postScriptSize < magicLength + 1) {
            throw new OrcCorruptionException("Malformed ORC file %s. Invalid postscript length %s", source, postScriptSize);
        }

        if (!MAGIC.equals(Slices.wrappedBuffer(buffer, buffer.length - 1 - magicLength, magicLength))) {
            // Old versions of ORC (0.11) wrote the magic to the head of the file
            byte[] headerMagic = new byte[magicLength];
            source.readFully(0, headerMagic);

            // if it isn't there, this isn't an ORC file
            if  (!MAGIC.equals(Slices.wrappedBuffer(headerMagic))) {
                throw new OrcCorruptionException("Malformed ORC file %s. Invalid postscript.", source);
            }
        }
    }

    /**
     * Check to see if this ORC file is from a future version and if so,
     * warn the user that we may not be able to read all of the column encodings.
     */
    // This is based on the Apache Hive ORC code
    private static void checkOrcVersion(OrcDataSource orcDataSource, List<Integer> version)
    {
        if (version.size() >= 1) {
            int major = version.get(0);
            int minor = 0;
            if (version.size() > 1) {
                minor = version.get(1);
            }

            if (major > CURRENT_MAJOR_VERSION || (major == CURRENT_MAJOR_VERSION && minor > CURRENT_MINOR_VERSION)) {
                log.warn("ORC file %s was written by a newer Hive version %s. This file may not be readable by this version of Hive (%s.%s).",
                        orcDataSource,
                        Joiner.on('.').join(version),
                        CURRENT_MAJOR_VERSION,
                        CURRENT_MINOR_VERSION);
            }
        }
    }
}
