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

package com.splicemachine.db.iapi.jdbc;

import java.util.Properties;
import java.util.Enumeration;
import java.util.Map;
import java.util.HashMap;
import java.sql.ClientInfoStatus;

    /**
     * Class <code>FailedProperties40</code> is a helper class for the
     * SQLClientInfoException. It provides convenient access to data
     * that is needed when constructing SQLClientInfoExceptions. Should
     * be kept in sync with its client side counter part
     * (com.splicemachine.db.client.am.FailedProperties40).
     * @see java.sql.SQLClientInfoException
     * @see com.splicemachine.db.client.am.FailedProperties40
     */
public class FailedProperties40 {
    private final HashMap<String,ClientInfoStatus> failedProps_ =
            new HashMap<>();
    private String firstKey_;
    private String firstValue_;

    /**
     * Helper method that creates a Propery object from the name-value
     * pair given as arguments.
     * @param name property key
     * @param value property value
     * @return the created <code>Properties</code> object
     */
    public static Properties makeProperties(String name, String value) {
	Properties p = new Properties();
	// Properties is derived from HashTable, in which both key and value cannot be null
	if (name != null && value != null)
	    p.setProperty(name, value);
	return p;
    }
    /**
     * Creates a new <code>FailedProperties40</code> instance. Since
     * Derby doesn't support any properties, all the keys from the
     * <code>props</code> parameter are added to the
     * <code>failedProps_</code> member with value
     * REASON_UNKNOWN_PROPERTY.
     *
     * @param props a <code>Properties</code> value. Can be null or empty
     */
    public FailedProperties40(Properties props) {
        if (props == null || props.isEmpty()) {
            firstKey_ = null;
            firstValue_ = null;
            return;
        }
        Enumeration e = props.keys();
        firstKey_ = (String)e.nextElement();
        firstValue_ = props.getProperty(firstKey_);
        failedProps_.put(firstKey_, ClientInfoStatus.REASON_UNKNOWN_PROPERTY);
        while (e.hasMoreElements()) {
            failedProps_.put((String)e.nextElement(), 
			     ClientInfoStatus.REASON_UNKNOWN_PROPERTY);
        }
    }

    public boolean isEmpty() {
        return failedProps_.isEmpty();
    }

    /**
     * <code>getProperties</code> provides a
     * <code>Map<String,ClientInfoStatus></code> object describing the
     * failed properties (as specified in the javadoc for
     * java.sql.SQLClientInfoException).
     *
     * @return a <code>Map<String,ClientInfoStatus></code> object with
     * the failed property keys and the reason why each failed
     */
    public Map<String,ClientInfoStatus> getProperties() { return failedProps_; }

    public boolean addProperty(String name, String value, ClientInfoStatus reason) {
        if (firstKey_ == null && firstValue_ == null) {
            assert failedProps_.isEmpty() : "corrupted structure of FailedProperties40";
            firstKey_ = name;
            firstValue_ = value;
        }
        ClientInfoStatus prevStatus = failedProps_.put(name, reason);
        return prevStatus != reason;
    }

    /**
     * <code>getFirstKey</code> returns the first property key. Used
     * when SQLClientInfoException is thrown with a parameterized error
     * message.
     *
     * @return a <code>String</code> value
     */
    public String getFirstKey() { return firstKey_; }

    /**
     * <code>getFirstValue</code> returns the first property value. Used
     * when SQLClientInfoException is thrown with a parameterized error
     * message.
     *
     * @return a <code>String</code> value
     */
    public String getFirstValue() { return firstValue_; }
}
