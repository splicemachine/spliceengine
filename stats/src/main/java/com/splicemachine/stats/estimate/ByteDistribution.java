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

package com.splicemachine.stats.estimate;

/**
 * @author Scott Fines
 *         Date: 3/5/15
 */
public interface ByteDistribution extends Distribution<Byte>{

    long selectivity(byte value);

    long selectivityBefore(byte stop, boolean includeStop);

    long selectivityAfter(byte start, boolean includeStart);

    long rangeSelectivity(byte start, byte stop, boolean includeStart, boolean includeStop);

    byte min();

    byte max();
}
