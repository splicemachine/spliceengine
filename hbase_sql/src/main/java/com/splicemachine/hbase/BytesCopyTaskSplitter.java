/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
    public static List<byte[]> getCutPoints(HRegion region, byte[] start, byte[] end,byte[] expectedRegionEnd) throws IOException {
        Store store = null;
        try {
            store = region.getStore(SIConstants.DEFAULT_FAMILY_BYTES);
            HRegionUtil.lockStore(store);
            return HRegionUtil.getCutpoints(store, start, end, expectedRegionEnd);
        }catch (Throwable t) {
            throw Exceptions.getIOException(t);
        }finally{
            HRegionUtil.unlockStore(store);
        }
    }

}
