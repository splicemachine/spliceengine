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

package com.splicemachine.access.client;

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