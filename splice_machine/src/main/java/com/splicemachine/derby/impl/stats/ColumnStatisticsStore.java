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

package com.splicemachine.derby.impl.stats;

import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.stats.ColumnStatistics;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Date: 3/9/15
 */
public interface ColumnStatisticsStore {

    @SuppressWarnings("rawtypes")
	public Map<String,List<ColumnStatistics>> fetchColumnStats(TxnView txn, long conglomerateId, Collection<String> partitions) throws ExecutionException;

    public void invalidate(long conglomerateId,Collection<String> partitions);
    
}
