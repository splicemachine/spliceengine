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

package com.splicemachine.derby.utils.marshall.dvd;

/**
 * Given a serializer version string ("1.0", "2.0", etc), return the corresponding SerializerMap instance.
 *
 * @author Scott Fines
 *         Date: 4/3/14
 */
public class VersionedSerializers {

    private VersionedSerializers() {
    }

    public static TypeProvider typesForVersion(String version) {
        if (V2SerializerMap.VERSION.equals(version))
            return V2SerializerMap.instance(true);
        else if (V1SerializerMap.VERSION.equals(version))
            return V1SerializerMap.instance(true);
        else
            return latestTypes();
    }

    public static TypeProvider latestTypes() {
        return V2SerializerMap.instance(true);
    }

    public static SerializerMap forVersion(String version, boolean sparse) {
        /*
         * Statically defined version checked versioning
         */
        if (V2SerializerMap.VERSION.equals(version))
            return V2SerializerMap.instance(sparse);
        else if (V1SerializerMap.VERSION.equals(version))
            return V1SerializerMap.instance(sparse);

        //when in doubt, assume it's the latest version
        return latestVersion(sparse);
    }

    /**
     * Get the serializer for the latest encoding version.
     *
     * @param sparse whether or not to encode sparsely
     * @return a serializer map for the latest encoding version.
     */
    public static SerializerMap latestVersion(boolean sparse) {
        return V2SerializerMap.instance(sparse);
    }

}
