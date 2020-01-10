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

import com.splicemachine.orc.metadata.PostScript.HiveWriterVersion;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public interface MetadataReader
{
    PostScript readPostScript(byte[] data, int offset, int length)
            throws IOException;

    Metadata readMetadata(HiveWriterVersion hiveWriterVersion, InputStream inputStream)
            throws IOException;

    Footer readFooter(HiveWriterVersion hiveWriterVersion, InputStream inputStream)
            throws IOException;

    StripeFooter readStripeFooter(HiveWriterVersion hiveWriterVersion, List<OrcType> types, InputStream inputStream)
            throws IOException;

    List<RowGroupIndex> readRowIndexes(HiveWriterVersion hiveWriterVersion, InputStream inputStream)
            throws IOException;

    List<HiveBloomFilter> readBloomFilterIndexes(InputStream inputStream)
            throws IOException;
}
