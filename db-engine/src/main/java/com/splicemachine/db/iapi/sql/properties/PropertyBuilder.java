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

import splice.com.google.common.collect.Sets;

import java.util.Set;

public class PropertyBuilder {
    private PropertyRegistry.PROPERTYNAME name;
    private Property.TYPE type;
    private Integer maxInt;
    private Integer minInt;
    private Integer defaultInt;
    private Set<String> allowedStrings;
    private String defaultString;
    private Boolean defaultBool;
    private Double minDouble;
    private Double maxDouble;
    private Double defaultDouble;
    private Long minLong;
    private Long maxLong;
    private Long defaultLong;

    private boolean includeMin;
    private boolean includeMax;
    private boolean databaseLevel;
    private boolean sessionLevel;
    private Property.QUERY_LEVEL queryLevel;

    PropertyBuilder() {}

    PropertyBuilder name(String name) {
        this.name = PropertyRegistry.PROPERTYNAME.valueOf(name.toUpperCase());
        return this;
    }

    PropertyBuilder name(PropertyRegistry.PROPERTYNAME name) {
        this.name = name;
        return this;
    }

    PropertyBuilder type(Property.TYPE type) {
        this.type = type;
        return this;
    }

    PropertyBuilder allowedValues(String... values) {
        allowedStrings= Sets.newHashSet(values);
        return this;
    }

    PropertyBuilder defaultValue(String def) {
        this.defaultString = def;
        return this;
    }

    PropertyBuilder max(int max) {
        this.maxInt = max;
        this.maxDouble = (double) max;
        this.maxLong = (long) max;
        return this;
    }

    PropertyBuilder min(int min) {
        this.minInt = min;
        this.minDouble = (double) min;
        this.minLong = (long) min;
        return this;
    }

    PropertyBuilder defaultValue(int def) {
        this.defaultInt = def;
        this.defaultDouble = (double) def;
        this.defaultLong = (long) def;
        return this;
    }

    PropertyBuilder max(long max) {
        this.maxLong = max;
        return this;
    }

    PropertyBuilder min(long min) {
        this.minLong = min;
        return this;
    }

    PropertyBuilder defaultValue(long def) {
        this.defaultLong = def;
        return this;
    }

    PropertyBuilder max(double max) {
        this.maxDouble= max;
        return this;
    }

    PropertyBuilder min(double min) {
        this.minDouble = min;
        return this;
    }

    PropertyBuilder defaultValue(double def) {
        this.defaultDouble = def;
        return this;
    }

    PropertyBuilder excludeMin() {
        this.includeMin = false;
        return this;
    }

    PropertyBuilder excludeMax() {
        this.includeMax = false;
        return this;
    }

    PropertyBuilder includeMin() {
        this.includeMin = true;
        return this;
    }

    PropertyBuilder includeMax() {
        this.includeMax = true;
        return this;
    }

    PropertyBuilder defaultValue(Boolean def) {
        this.defaultBool = def;
        return this;
    }

    PropertyBuilder databaseLevel() {
        this.databaseLevel = true;
        return this;
    }

    PropertyBuilder sessionLevel() {
        this.sessionLevel = true;
        return this;
    }

    PropertyBuilder queryTableHint() {
        assert this.queryLevel == null;
        this.queryLevel = Property.QUERY_LEVEL.TABLE;
        return this;
    }

    Property build() {
        switch (type) {
            case INT:
                return new Property<>(name, type, databaseLevel, sessionLevel, queryLevel, null, minInt, maxInt, includeMin, includeMax, defaultInt);
            case LONG:
                return new Property<>(name, type, databaseLevel, sessionLevel, queryLevel, null, minLong, maxLong, includeMin, includeMax, defaultLong);
            case STRING:
                return new Property<>(name, type, databaseLevel, sessionLevel, queryLevel, allowedStrings, null, null, includeMin, includeMax, defaultString);
            case BOOLEAN:
                return new Property<>(name, type, databaseLevel, sessionLevel, queryLevel, null, null, null, includeMin, includeMax, defaultBool);
            case DOUBLE:
                return new Property<>(name, type, databaseLevel, sessionLevel, queryLevel, null, minDouble, maxDouble, includeMin, includeMax, defaultDouble);
        }
        return null;
    }
}
