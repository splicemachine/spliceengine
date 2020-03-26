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

package com.splicemachine.access.client;

import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;

/**
 *
 * Splice Machine Custom RegionInfo that allows us to supply a comparator for the client side
 * merge of the StoreFileScanners and MemstoreKeyValueScanner.  This needs to be custom to address
 * the issues with holding the memory scanner open.
 *
 * Created by jleach on 4/12/16.
 */
public class SpliceHRegionInfo extends HRegionInfo {

    public SpliceHRegionInfo(HRegionInfo info) {
        // Set replicaId to something other than DEFAULT_REPLICA, othwerwise we might try to replay edits
        // or do some other housekeeping work on this remote region
        super(info, 1);
    }

    @Override
    /**
     * @return Comparator to use comparing {@link org.apache.hadoop.hbase.KeyValue}s.
     */
    public KeyValue.KVComparator getComparator() {
        return SpliceKVComparator.INSTANCE;
    }
}
