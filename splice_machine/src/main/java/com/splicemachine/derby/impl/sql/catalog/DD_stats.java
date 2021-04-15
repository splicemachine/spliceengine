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

package com.splicemachine.derby.impl.sql.catalog;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class DD_stats {
    private ConcurrentHashMap<Long, AtomicLong> modifiedRowsCountMap = new ConcurrentHashMap<>();
    private DataDictionary dataDictionary;

    public DD_stats(DataDictionary dataDictionary) {
        this.dataDictionary = dataDictionary;
    }

    public boolean updateModifiedRows(long heapConglom, long modifiedRowsCount) throws StandardException {
        TableDescriptor td = getTableDescriptor(heapConglom);
        if (td.getAutoAnalyze() != null) {
            AtomicLong modifiedRowCount = modifiedRowsCountMap.computeIfAbsent(heapConglom, k -> new AtomicLong());
            long count = modifiedRowCount.addAndGet(modifiedRowsCount);
            return shouldRerunAnalyze(heapConglom, count) && modifiedRowCount.compareAndSet(count, 0);
        }
        return false;
    }

    private TableDescriptor getTableDescriptor(long heapConglom) throws StandardException {
        ConglomerateDescriptor cd = dataDictionary.getConglomerateDescriptor(heapConglom);
        UUID tableID = cd.getTableID();
        return dataDictionary.getTableDescriptor(tableID);
    }

    private boolean shouldRerunAnalyze(long heapConglom, long totalModifiedRowsCount) throws StandardException {
        TableDescriptor td = getTableDescriptor(heapConglom);
        return td.getAutoAnalyze() != null && totalModifiedRowsCount > td.getAutoAnalyze();
    }
}
