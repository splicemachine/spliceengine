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
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.db.impl.sql.conn;

import com.splicemachine.db.iapi.sql.conn.SessionProperties;

import static com.splicemachine.db.iapi.sql.conn.SessionProperties.PROPERTYNAME.*;

/**
 * Created by yxia on 6/1/18.
 */
public class SessionPropertiesImpl implements SessionProperties {
    Object[] properties = new Object[PROPERTYNAME.COUNT];

    public void setProperty(PROPERTYNAME property, Object value) {
        // the legal values have been checked at SetSessionpropertyNode.init(), so no need to check again
        String valString = (String)value;
        if (valString == null || valString.equals("null")) {
            properties[property.getId()] = null;
            return;
        }

        switch (property) {
            case USESPARK:
                boolean useSparkVal = Boolean.valueOf(valString);
                properties[USESPARK.getId()] = useSparkVal;
                break;
            case DEFAULTSELECTIVITYFACTOR:
                double defaultSelectivityFactor = Double.parseDouble(valString);
                properties[DEFAULTSELECTIVITYFACTOR.getId()] = defaultSelectivityFactor;
                break;
            case SKIPSTATS:
                boolean skipStatsVal = Boolean.valueOf(valString);
                properties[SKIPSTATS.getId()] = skipStatsVal;
                break;
            case OLAPQUEUE:
                properties[OLAPQUEUE.getId()] = valString;
                break;
            case RECURSIVEQUERYITERATIONLIMIT:
                int recursiveQueryIterationLimit = Integer.parseInt(valString);
                properties[RECURSIVEQUERYITERATIONLIMIT.getId()] = recursiveQueryIterationLimit;
                break;
            default:
                break;
        }
        return;
    }

    public Object getProperty(PROPERTYNAME property) {
        return properties[property.getId()];
    }

    public String getPropertyString(PROPERTYNAME property) {
        Object value = properties[property.getId()];
        return value==null?"null":value.toString();
    }

    public String getAllProperties() {
        // only return string that is not null
        StringBuilder sb = new StringBuilder();
        for (SessionProperties.PROPERTYNAME property: PROPERTYNAME.values()) {
            if (properties[property.getId()] != null)
                sb.append(property.toString()).append("=").append(properties[property.getId()]).append("; ");
        }

        return sb.toString();
    }

    public void resetAll() {
        for (int i=0; i<PROPERTYNAME.COUNT; i++)
            properties[i] = null;
    }
}
