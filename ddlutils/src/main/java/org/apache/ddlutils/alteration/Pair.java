/*
 * ddlUtils is a subproject of the Apache DB project, and is licensed under
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
 */

package org.apache.ddlutils.alteration;

/**
 * Represents a pair of objects.
 *
 * @version $Revision: $
 */
public class Pair {
    /**
     * The first object.
     */
    private final Object _firstObj;
    /**
     * The first object.
     */
    private final Object _secondObj;

    /**
     * Creates a pair object.
     *
     * @param firstObj  The first object
     * @param secondObj The second object
     */
    public Pair(Object firstObj, Object secondObj) {
        _firstObj = firstObj;
        _secondObj = secondObj;
    }

    /**
     * Returns the first object of the pair.
     *
     * @return The first object
     */
    public Object getFirst() {
        return _firstObj;
    }

    /**
     * Returns the second object of the pair.
     *
     * @return The second object
     */
    public Object getSecond() {
        return _secondObj;
    }
}
