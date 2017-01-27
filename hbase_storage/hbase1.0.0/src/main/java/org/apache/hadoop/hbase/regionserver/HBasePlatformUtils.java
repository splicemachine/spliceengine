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

package org.apache.hadoop.hbase.regionserver;


import org.apache.hadoop.hbase.util.Counter;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class HBasePlatformUtils{
	public static void updateWriteRequests(HRegion region, long numWrites) {
		Counter writeRequestsCount = region.writeRequestsCount;
		if (writeRequestsCount != null)
			writeRequestsCount.add(numWrites);
	}

	public static void updateReadRequests(HRegion region, long numReads) {
		Counter readRequestsCount = region.readRequestsCount;
		if (readRequestsCount != null)
			readRequestsCount.add(numReads);
	}

	public static Map<byte[],Store> getStores(HRegion region) {
		return region.getStores();
	}

    public static void flush(HRegion region) throws IOException {
        region.flushcache();
    }

    public static void bulkLoadHFiles(HRegion region, List<Pair<byte[], String>> copyPaths) throws IOException{
        region.bulkLoadHFiles(copyPaths,true);
    }

    public static long getMemstoreSize(HRegion region) {
        return region.getMemstoreSize().get();
    }

    public static long getReadpoint(HRegion region) {
        return region.getMVCC().memstoreReadPoint();
    }

    public static void validateClusterKey(String quorumAddress) throws IOException {
        ZKUtil.transformClusterKey(quorumAddress);
    }


}
