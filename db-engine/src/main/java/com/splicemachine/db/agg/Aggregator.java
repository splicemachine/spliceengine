/*
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
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.db.agg;

import java.io.Serializable;
import com.splicemachine.db.iapi.error.StandardException;

/**
 * <p>
 * Behavior of a user-defined Derby aggregator. Aggregates values
 * of type V and returns a result of type R. In addition to the methods
 * in the interface, implementing classes must have a 0-arg public
 * constructor.
 * </p>
 */
public interface Aggregator<V,R,A extends Aggregator<V,R,A>>    extends Serializable
{
    /** Initialize the Aggregator */
    void init();

    /** Accumulate the next scalar value */
    void    accumulate(V value) throws StandardException;

    /**
     * <p>
     * For merging another partial result into this Aggregator.
     * This lets the SQL interpreter divide the incoming rows into
     * subsets, aggregating each subset in isolation, and then merging
     * the partial results together. This method can be called when
     * performing a grouped aggregation with a large number of groups.
     * While processing such a query, Derby may write intermediate grouped
     * results to disk. The intermediate results may be retrieved and merged
     * with later results if Derby encounters later rows which belong to groups
     * whose intermediate results have been written to disk. This situation can
     * occur with a query like the following:
     * </p>
     *
     * <pre>
     * select a, mode( b ) from mode_inputs group by a order by a
     * </pre>
     */
    void    merge(A otherAggregator);

    /** Return the result scalar value */
    R   terminate();
}

