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

package com.splicemachine.stats.histogram;

/**
 * @author Scott Fines
 *         Date: 4/30/14
 */
public interface IntRangeQuerySolver extends RangeQuerySolver<Integer> {
		int min();

		int max();

		long after(int value, boolean equals);

		long before(int value, boolean equals);

		long equal(int value);

		long between(int startValue, int endValue,boolean inclusiveStart,boolean inclusiveEnd);
}
