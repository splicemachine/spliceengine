/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 *  version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.compactions;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

public class SpliceCompaction {

    @SuppressFBWarnings(value="MS_MUTABLE_ARRAY")
    public static final byte[] SPLICE_COMPACTION_EVENT_KEY = Bytes.toBytes("SPLICE_COMPACTION_EVENT_KEY");

    public static Set<String> storeFilesToNames(Collection<HStoreFile> storefiles) {
        return storefiles.stream().map(sf -> sf.getPath().getName()).collect(Collectors.toSet());
    }
}
