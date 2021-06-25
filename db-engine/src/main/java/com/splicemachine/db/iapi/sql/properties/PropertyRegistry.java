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
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.conn.SessionProperties;

import java.util.Map;
import java.util.stream.Collectors;

public class PropertyRegistry {
    private static PropertyRegistry INSTANCE = null;

    public static PropertyRegistry getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new PropertyRegistry();
        }
        return INSTANCE;
    }

    Map<PROPERTYNAME, Property> properties;

    public void register(Property property) {
        properties.putIfAbsent(property.getNameEnum(), property);
    }

    public Property get(PROPERTYNAME name) {
        return properties.get(name);
    }

    public Property get(String name) throws IllegalArgumentException {
        if (name.equalsIgnoreCase("USESPARK")) {
            return properties.get(PROPERTYNAME.USEOLAP);
        }
        return get(PROPERTYNAME.valueOf(name));
    }

    public Property getSessionProperty(String name) throws StandardException {
        try {
            Property property = get(name);
            if (!property.isSessionLevel()) {
                throw new IllegalArgumentException("Not session level");
            }
            return property;
        } catch (IllegalArgumentException e) {
            throw StandardException.newException(SQLState.LANG_INVALID_SESSION_PROPERTY, name,
                    getListOfAvailableSessionProperties());
        }
    }

    public Property getSessionProperty(PROPERTYNAME name) throws StandardException {
        try {
            Property property = get(name);
            if (!property.isSessionLevel()) {
                throw new IllegalArgumentException("Not session level");
            }
            return property;
        } catch (IllegalArgumentException e) {
            throw StandardException.newException(SQLState.LANG_INVALID_SESSION_PROPERTY, name,
                    getListOfAvailableSessionProperties());
        }
    }

    public String getListOfAvailableSessionProperties() {
        return properties.values()
                .stream()
                .filter(Property::isSessionLevel)
                .map(Property::getName)
                .collect(Collectors.joining(", "));
    }

    public enum PROPERTYNAME{
        USEOLAP(0, "useOLAP"),
        DEFAULTSELECTIVITYFACTOR(1, "defaultSelectivityFactor"),
        SKIPSTATS(2, "skipStats"),
        RECURSIVEQUERYITERATIONLIMIT(3, "recursiveQueryIterationLimit"),
        OLAPQUEUE(4, "olapQueue"),
        SNAPSHOT_TIMESTAMP(5, "snapshot_timestamp"),
        DISABLE_TC_PUSHED_DOWN_INTO_VIEWS(6, "disable_tc_pushed_down_into_views"),
        OLAPPARALLELPARTITIONS(7, "olapParallelPartitions"),
        OLAPSHUFFLEPARTITIONS(8, "olapShufflePartitions"),
        SPARK_RESULT_STREAMING_BATCHES(9, "spark_result_streaming_batches"),
        SPARK_RESULT_STREAMING_BATCH_SIZE(10, "spark_result_streaming_batch_size"),
        TABLELIMITFOREXHAUSTIVESEARCH(11, "tableLimitForExhaustiveSearch"),
        DISABLE_NLJ_PREDICATE_PUSH_DOWN(12, "disable_nlk_predicate_push_down"),
        USE_NATIVE_SPARK(13, "use_native_spark"),
        MINPLANTIMEOUT(14, "minPlanTimeout"),
        CURRENTFUNCTIONPATH(15, "currentFunctionPath"),
        DISABLEPREDSFORINDEXORPKACCESSPATH(16, "disablePredsForIndexOrPkAccessPath"),
        ALWAYSALLOWINDEXPREFIXITERATION(17, "alwaysAllowIndexPrefixIteration"),
        OLAPALWAYSPENALIZENLJ(18, "olapAlwaysPenalizeNlj"),
        FAVORINDEXPREFIXITERATION(19, "favorIndexPrefixIteration"),
        COSTMODEL(20, "costModel"),
        JOINSTRATEGY(21, "joinStrategy");

        public static final int COUNT = PROPERTYNAME.values().length;

        private int id;
        private String name;

        public int getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        PROPERTYNAME(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    public void registerAllProperties() {
        register(Property.newBuilder()
                .name(PROPERTYNAME.USEOLAP)
                .type(Property.TYPE.BOOLEAN)
                .sessionLevel()
                .queryTableHint()
                .build());

        register(Property.newBuilder()
                .name(PROPERTYNAME.DEFAULTSELECTIVITYFACTOR)
                .type(Property.TYPE.DOUBLE)
                .min(0).includeMin()
                .max(1).excludeMax()
                .sessionLevel()
                .build());

        register(Property.newBuilder()
                .name(PROPERTYNAME.SKIPSTATS)
                .type(Property.TYPE.BOOLEAN)
                .sessionLevel()
                .build());

        register(Property.newBuilder()
                .name(PROPERTYNAME.RECURSIVEQUERYITERATIONLIMIT)
                .type(Property.TYPE.INT)
                .min(0).includeMin()
                .sessionLevel()
                .build());

        register(Property.newBuilder()
                .name(PROPERTYNAME.OLAPQUEUE)
                .type(Property.TYPE.STRING)
                .sessionLevel()
                .build());

        register(Property.newBuilder()
                .name(PROPERTYNAME.SNAPSHOT_TIMESTAMP)
                .type(Property.TYPE.LONG)
                .min(0).includeMin()
                .sessionLevel()
                .build());

        register(Property.newBuilder()
                .name(PROPERTYNAME.DISABLE_TC_PUSHED_DOWN_INTO_VIEWS)
                .type(Property.TYPE.BOOLEAN)
                .sessionLevel()
                .build());

        register(Property.newBuilder()
                .name(PROPERTYNAME.OLAPPARALLELPARTITIONS)
                .type(Property.TYPE.INT)
                .min(0).includeMin()
                .sessionLevel()
                .build());

        register(Property.newBuilder()
                .name(PROPERTYNAME.OLAPSHUFFLEPARTITIONS)
                .type(Property.TYPE.INT)
                .min(0).includeMin()
                .sessionLevel()
                .build());

        register(Property.newBuilder()
                .name(PROPERTYNAME.SPARK_RESULT_STREAMING_BATCHES)
                .type(Property.TYPE.INT)
                .min(0).includeMin()
                .sessionLevel()
                .build());

        register(Property.newBuilder()
                .name(PROPERTYNAME.SPARK_RESULT_STREAMING_BATCH_SIZE)
                .type(Property.TYPE.INT)
                .min(0).includeMin()
                .sessionLevel()
                .build());

        register(Property.newBuilder()
                .name(PROPERTYNAME.TABLELIMITFOREXHAUSTIVESEARCH)
                .type(Property.TYPE.INT)
                .min(0).includeMin()
                .sessionLevel()
                .build());

        register(Property.newBuilder()
                .name(PROPERTYNAME.DISABLE_NLJ_PREDICATE_PUSH_DOWN)
                .type(Property.TYPE.BOOLEAN)
                .sessionLevel()
                .build());

        register(Property.newBuilder()
                .name(PROPERTYNAME.USE_NATIVE_SPARK)
                .type(Property.TYPE.BOOLEAN)
                .sessionLevel()
                .build());

        register(Property.newBuilder()
                .name(PROPERTYNAME.MINPLANTIMEOUT)
                .type(Property.TYPE.LONG)
                .min(0).includeMin()
                .sessionLevel()
                .build());

        register(Property.newBuilder()
                .name(PROPERTYNAME.CURRENTFUNCTIONPATH)
                .type(Property.TYPE.STRING)
                .sessionLevel()
                .build());

        register(Property.newBuilder()
                .name(PROPERTYNAME.DISABLEPREDSFORINDEXORPKACCESSPATH)
                .type(Property.TYPE.BOOLEAN)
                .sessionLevel()
                .build());

        register(Property.newBuilder()
                .name(PROPERTYNAME.ALWAYSALLOWINDEXPREFIXITERATION)
                .type(Property.TYPE.BOOLEAN)
                .sessionLevel()
                .build());

        register(Property.newBuilder()
                .name(PROPERTYNAME.OLAPALWAYSPENALIZENLJ)
                .type(Property.TYPE.BOOLEAN)
                .sessionLevel()
                .build());

        register(Property.newBuilder()
                .name(PROPERTYNAME.FAVORINDEXPREFIXITERATION)
                .type(Property.TYPE.BOOLEAN)
                .sessionLevel()
                .build());

        register(Property.newBuilder()
                .name(PROPERTYNAME.COSTMODEL)
                .type(Property.TYPE.STRING)
                .allowedValues("V1", "V2")
                .sessionLevel()
                .build());

        register(Property.newBuilder()
                .name(PROPERTYNAME.JOINSTRATEGY)
                .type(Property.TYPE.STRING)
                .allowedValues("CROSS", "NESTEDLOOP", "MERGE", "SORTMERGE", "BROADCAST")
                .sessionLevel()
                .build());
    }
}
