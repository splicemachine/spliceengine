/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
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
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.util;

import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Properties;

/**
	A properties object that links two independent
	properties together. The read property set is always
	searched first, with the write property set being
	second. But any put() calls are always made directly to
	the write object.

    Only the put(), propertyNames() and getProperty() methods are supported
	by this class.
*/

public final class DoubleProperties extends Properties {

	private final Properties read;
	private final Properties write;

	public DoubleProperties(Properties read, Properties write) {
		this.read = read;
		this.write = write;
	}

	public Object put(Object key, Object value) {
		return write.put(key, value);
	}

	public String getProperty(String key) {

		return read.getProperty(key, write.getProperty(key));
	}

	public String getProperty(String key, String defaultValue) {
		return read.getProperty(key, write.getProperty(key, defaultValue));

	}

	public Enumeration propertyNames() {
        HashSet names = new HashSet();
        addAllNames(write, names);
        addAllNames(read, names);
        return Collections.enumeration(names);
	}

    /**
     * Add all property names in the Properties object {@code src} to the
     * HashSet {@code dest}.
     */
    private static void addAllNames(Properties src, HashSet dest) {
        if (src != null) {
            for (Enumeration e = src.propertyNames(); e.hasMoreElements(); ) {
                dest.add(e.nextElement());
            }
        }
    }
}
