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

import io.airlift.slice.SliceOutput;

import java.io.IOException;
import java.util.List;

public interface MetadataWriter
{
    List<Integer> getOrcMetadataVersion();

    int writePostscript(SliceOutput output, int footerLength, int metadataLength, CompressionKind compression, int compressionBlockSize)
            throws IOException;

    int writeMetadata(SliceOutput output, Metadata metadata)
            throws IOException;

    int writeFooter(SliceOutput output, Footer footer)
            throws IOException;

    int writeStripeFooter(SliceOutput output, StripeFooter footer)
            throws IOException;

    int writeRowIndexes(SliceOutput output, List<RowGroupIndex> rowGroupIndexes)
            throws IOException;

    MetadataReader getMetadataReader();
}

