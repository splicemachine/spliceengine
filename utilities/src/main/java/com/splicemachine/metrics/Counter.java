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

package com.splicemachine.metrics;

/**
 * Represents a basic counter.
 *
 * We could of course just use longs directly, but then we'd have to
 * do boolean checks every time to determine if we should collect. This sucks,
 * when we could just do that boolean check once and use polymorphism to do
 * No-ops when we don't need to count.
 *
 * @author Scott Fines
 * Date: 1/17/14
 */
public interface Counter {

		public void increment();

		public void add(long value);

		public long getTotal();

		boolean isActive();
}
