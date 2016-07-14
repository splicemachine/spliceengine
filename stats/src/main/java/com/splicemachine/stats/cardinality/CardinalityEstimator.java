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

package com.splicemachine.stats.cardinality;

import com.splicemachine.stats.Mergeable;
import com.splicemachine.stats.Updateable;

/**
 * @author Scott Fines
 *         Date: 6/5/14
 */
public interface CardinalityEstimator<T> extends Updateable<T>,Mergeable<CardinalityEstimator<T>> {
		/**
		 * @return an estimate of the number of distinct items seen by this Estimate
		 */
		long getEstimate();

    CardinalityEstimator<T> getClone();
}
