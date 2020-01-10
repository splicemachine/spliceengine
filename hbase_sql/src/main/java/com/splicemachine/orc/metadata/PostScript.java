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
package com.splicemachine.orc.metadata;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class PostScript
{
    public enum HiveWriterVersion
    {
        ORIGINAL, ORC_HIVE_8732;
    }

    private final List<Integer> version;
    private final long footerLength;
    private final long metadataLength;
    private final CompressionKind compression;
    private final long compressionBlockSize;
    private final HiveWriterVersion hiveWriterVersion;

    public PostScript(List<Integer> version, long footerLength, long metadataLength, CompressionKind compression, long compressionBlockSize, HiveWriterVersion hiveWriterVersion)
    {
        this.version = ImmutableList.copyOf(requireNonNull(version, "version is null"));
        this.footerLength = footerLength;
        this.metadataLength = metadataLength;
        this.compression = requireNonNull(compression, "compressionKind is null");
        this.compressionBlockSize = compressionBlockSize;
        this.hiveWriterVersion = requireNonNull(hiveWriterVersion, "hiveWriterVersion is null");
    }

    public List<Integer> getVersion()
    {
        return version;
    }

    public long getFooterLength()
    {
        return footerLength;
    }

    public long getMetadataLength()
    {
        return metadataLength;
    }

    public CompressionKind getCompression()
    {
        return compression;
    }

    public long getCompressionBlockSize()
    {
        return compressionBlockSize;
    }

    public HiveWriterVersion getHiveWriterVersion()
    {
        return hiveWriterVersion;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("version", version)
                .add("footerLength", footerLength)
                .add("metadataLength", metadataLength)
                .add("compressionKind", compression)
                .add("compressionBlockSize", compressionBlockSize)
                .add("hiveWriterVersion", hiveWriterVersion)
                .toString();
    }
}
