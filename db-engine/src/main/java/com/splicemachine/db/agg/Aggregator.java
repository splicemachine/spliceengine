/*
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */
package com.splicemachine.db.agg;

import java.io.Serializable;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

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
    public void init();

    /** Accumulate the next scalar value */
    public  void    accumulate( V value ) throws StandardException;

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
    public  void    merge( A otherAggregator );

    /** Return the result scalar value */
    public  R   terminate();
}

