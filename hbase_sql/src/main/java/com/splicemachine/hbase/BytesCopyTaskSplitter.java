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

package com.splicemachine.hbase;

import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.constants.SIConstants;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionUtil;
import org.apache.hadoop.hbase.regionserver.Store;
import java.io.IOException;
import java.util.*;

/**
 * @author Scott Fines
 *         Date: 4/16/14
 */
public class BytesCopyTaskSplitter {
    public static List<byte[]> getCutPoints(HRegion region, byte[] start, byte[] end,
                                            int requestedSplits, long bytesPerSplit) throws IOException {
        Store store = null;
        try {
            store = region.getStore(SIConstants.DEFAULT_FAMILY_BYTES);
            HRegionUtil.lockStore(store);
            return HRegionUtil.getCutpoints(store, start, end, requestedSplits, bytesPerSplit);
        }catch (Throwable t) {
            throw Exceptions.getIOException(t);
        }finally{
            HRegionUtil.unlockStore(store);
        }
    }

}
