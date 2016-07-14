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

package com.splicemachine.test.suites;

/**
 * @author Scott Fines
 * Created on: 2/24/13
 */
public class OperationCategories {

    /**
     * Indicates that a test(or test class is transactional in nature.
     *
     * This will allow ignoring tests that are intent on transactions when
     * transactional tests are a waste of time.
     */
    public static interface Transactional{}

    /**
     * Tests which rely on inserting large volumes of data, and thus might take a very long
     * time to run. Exclude them if you want to speed up run times
     */
    public static interface LargeDataTest{}

}
