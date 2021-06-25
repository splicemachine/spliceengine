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
package com.splicemachine.db.iapi.sql.properties;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.shared.common.reference.SQLState;

import java.util.Set;
import java.util.stream.Collectors;

public class Property<T extends Comparable<T>> {
    enum TYPE {
        INT,
        STRING,
        BOOLEAN,
        DOUBLE,
        LONG
    }

    enum QUERY_LEVEL {
        TABLE,
        FROM
    }

    PropertyRegistry.PROPERTYNAME name;
    TYPE type;
    boolean databaseLevel;
    boolean sessionLevel;
    QUERY_LEVEL queryLevel;
    Set<T> allowedValues;
    T minValue;
    T maxValue;
    boolean includeMin;
    boolean includeMax;
    T defaultValue;

    static PropertyBuilder newBuilder() {
        return new PropertyBuilder();
    }

    Property(PropertyRegistry.PROPERTYNAME name,
             TYPE type,
             boolean databaseLevel,
             boolean sessionLevel,
             QUERY_LEVEL queryLevel,
             Set<T> allowedValues,
             T minValue,
             T maxValue,
             boolean includeMin,
             boolean includeMax,
             T defaultValue) {
        this.name = name;
        this.type = type;
        this.databaseLevel = databaseLevel;
        this.sessionLevel = sessionLevel;
        this.queryLevel = queryLevel;
        this.allowedValues = allowedValues;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.includeMin = includeMin;
        this.includeMax = includeMax;
        this.defaultValue = defaultValue;
    }

    public PropertyRegistry.PROPERTYNAME getNameEnum() {
        return name;
    }

    public String getName() {
        return name.getName();
    }

    public boolean isDatabaseLevel() {
        return databaseLevel;
    }

    public boolean isSessionLevel() {
        return sessionLevel;
    }

    public QUERY_LEVEL getQueryLevel() {
        return queryLevel;
    }

    public Set<T> getAllowedValues() {
        return allowedValues;
    }

    public T getMinValue() {
        return minValue;
    }

    public T getMaxValue() {
        return maxValue;
    }

    public T getDefaultValue() {
        return defaultValue;
    }

    private void verifyValue(String valueString, String errorToThrow) throws StandardException {
        T value = convertStringToValue(valueString, errorToThrow);
        if (minValue != null) {
            int comp = value.compareTo(minValue);
            if (comp < 0 || (comp == 0 && !includeMin)) {
                throw StandardException.newException(errorToThrow,
                        value, (includeMin ? ">=" : "> ") + minValue);
            }
        }
        if (minValue != null) {
            int comp = value.compareTo(maxValue);
            if (comp > 0 || (comp == 0 && !includeMax)) {
                throw StandardException.newException(errorToThrow,
                        value, (includeMax ? "<=" : "< ") + maxValue);
            }
        }
        if (allowedValues != null && !allowedValues.contains(value)) {
            throw StandardException.newException(errorToThrow, value,
                    allowedValues.stream()
                            .map(Object::toString)
                            .collect(Collectors.joining(", ")));
        }
    }

    public T convertStringToValue(String valueString, String errorToThrow) throws StandardException {
        if (valueString == null || valueString.equalsIgnoreCase("null")) {
            return null;
        }
        T value = null;
        switch (type) {
            case INT:
                try {
                    value = (T) (Object) Integer.parseInt(valueString);
                } catch (Exception e) {
                    throw StandardException.newException(errorToThrow, valueString, "Integer or null");
                }
                break;
            case DOUBLE:
                try {
                    value = (T) (Object) Double.parseDouble(valueString);
                } catch (Exception e) {
                    throw StandardException.newException(errorToThrow, valueString, "Double or null");
                }
                break;
            case LONG:
                try {
                    value = (T) (Object) Long.parseLong(valueString);
                } catch (Exception e) {
                    throw StandardException.newException(errorToThrow, valueString, "Long or null");
                }
                break;
            case STRING:
                value = (T) valueString.toUpperCase();
                break;
            case BOOLEAN:
                try {
                    value = (T) (Object) Boolean.parseBoolean(valueString);
                } catch (Exception e) {
                    throw StandardException.newException(errorToThrow, valueString, "Boolean or null");
                }
                break;
            default:
                assert false;
        }
        return value;
    }

    public void verifySessionPropertyValue(String value) throws StandardException {
        verifyValue(value, SQLState.LANG_INVALID_SESSION_PROPERTY_VALUE);
    }
}
