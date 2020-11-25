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
 *
 */

package com.splicemachine.si.api.txn;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ActiveTxnTracker {
    private ConcurrentMap<Long, List<byte[]>> activeTxns = new ConcurrentHashMap(1024);

    public void registerActiveTxn(long id, List<byte[]> destinationTables) {
        activeTxns.put(id, destinationTables);
    }

    public void unregisterActiveTxn(long id) {
        activeTxns.remove(id);
    }

    public Set<Map.Entry<Long, List<byte[]>>> getActiveTransactions() {
        return activeTxns.entrySet();
    }

    public Long oldestActiveTransaction() {
        long oldest = Long.MAX_VALUE;
        for(Long id : activeTxns.keySet()) {
            if (id < oldest)
                oldest = id;
        }
        return oldest;
    }
}
