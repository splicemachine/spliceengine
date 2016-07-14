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

package com.splicemachine.stats.frequency;

import com.splicemachine.stats.BooleanUpdateable;

/**
 * Interface to mark a Boolean-types Frequency counter.
 *
 * @author Scott Fines
 * Date: 3/27/14
 */
public interface BooleanFrequencyCounter extends BooleanUpdateable, FrequencyCounter<Boolean> {

    /**
     * @return the frequency estimates for both true and false. Since Booleans are so
     * limited in size, this will contain all possible elements.
     */
    BooleanFrequentElements frequencies();
}
