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
    private final HRegion region;
    public BytesCopyTaskSplitter(HRegion region) {
        this.region = region;
    }

    public Collection<SizedInterval> split(byte[] taskStart, byte[] taskStop) throws IOException {
        List<byte[]> splits = getCutPoints(region, taskStart, taskStop);
        List<SizedInterval> intervals =new ArrayList<>();
        int length = splits.size();
        if (length == 0)
            return Collections.singletonList(new SizedInterval(taskStart,taskStop,0));
        intervals.add(new SizedInterval(taskStart,splits.get(0),0));
        for (int i=1; i < length; i++) {
            //assert Bytes.compareTo(splits.get(i), splits.get(i-1)) > 0;
            intervals.add(new SizedInterval(splits.get(i-1),splits.get(i),0));
        }
        intervals.add(new SizedInterval(splits.get(length-1),taskStop,0));
        return intervals;
    }

    public static List<byte[]> getCutPoints(HRegion region, byte[] start, byte[] end) throws IOException {
        Store store = null;
        try {
            store = region.getStore(SIConstants.DEFAULT_FAMILY_BYTES);
            HRegionUtil.lockStore(store);
            return HRegionUtil.getCutpoints(store, start, end);
        }catch (Throwable t) {
            throw Exceptions.getIOException(t);
        }finally{
            HRegionUtil.unlockStore(store);
        }
    }

}
