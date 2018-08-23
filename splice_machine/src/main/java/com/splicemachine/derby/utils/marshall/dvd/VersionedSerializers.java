/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
