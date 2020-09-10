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
package com.splicemachine.db.iapi.sql.conn;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.utils.Pair;

import java.sql.SQLException;

/**
 * Created by yxia on 6/1/18.
 */
public interface SessionProperties {
    enum PROPERTYNAME{
        USEOLAP(0),
        DEFAULTSELECTIVITYFACTOR(1),
        SKIPSTATS(2),
        RECURSIVEQUERYITERATIONLIMIT(3),
        OLAPQUEUE(4),
        SNAPSHOT_TIMESTAMP(5),
        DISABLE_TC_PUSHED_DOWN_INTO_VIEWS(6),
        OLAPPARALLELPARTITIONS(7),
        OLAPSHUFFLEPARTITIONS(8),
        SPARK_RESULT_STREAMING_BATCHES(9),
        SPARK_RESULT_STREAMING_BATCH_SIZE(10),
        TABLELIMITFOREXHAUSTIVESEARCH(11),
        DISABLE_NLJ_PREDICATE_PUSH_DOWN(12);

        public static final int COUNT = PROPERTYNAME.values().length;

        private int id;

        public int getId() {
            return id;
        }

        PROPERTYNAME(int id) {
            this.id = id;
        }
    };

    void setProperty(PROPERTYNAME property, Object value) throws SQLException;

    Object getProperty(PROPERTYNAME property);

    String getPropertyString(PROPERTYNAME property);

    String getAllProperties();

    void resetAll();

    static Pair<PROPERTYNAME, String> validatePropertyAndValue(Pair<String, String> pair) throws StandardException {
        String propertyNameString = pair.getFirst();
        SessionProperties.PROPERTYNAME property;
        if( propertyNameString.equalsIgnoreCase("useSpark" ) )
            propertyNameString = "USEOLAP";
        try {
            property = SessionProperties.PROPERTYNAME.valueOf(propertyNameString);
        } catch (IllegalArgumentException e) {
            throw StandardException.newException(SQLState.LANG_INVALID_SESSION_PROPERTY,propertyNameString,
                    "useOLAP, useSpark (deprecated), defaultSelectivityFactor, skipStats, olapQueue, recursiveQueryIterationLimit, tableLimitForExhaustiveSearch");
        }

        String valString = pair.getSecond();
        if (valString == null || valString.toLowerCase().equals("null"))
            return new Pair(property, "null");

        switch (property) {
            case USEOLAP:
            case SKIPSTATS:
            case DISABLE_TC_PUSHED_DOWN_INTO_VIEWS:
            case DISABLE_NLJ_PREDICATE_PUSH_DOWN:
                try {
                    Boolean.parseBoolean(valString);
                } catch (Exception e) {
                    throw StandardException.newException(SQLState.LANG_INVALID_SESSION_PROPERTY_VALUE, valString, "boolean or null");
                }

                break;
            case DEFAULTSELECTIVITYFACTOR:
                double defaultSelectivityFactor;
                try {
                    defaultSelectivityFactor = Double.parseDouble(valString);
                } catch (Exception parseDoubleE) {
                    throw StandardException.newException(SQLState.LANG_INVALID_SESSION_PROPERTY_VALUE, valString, "value in the range(0,1] or null");
                }
                if (defaultSelectivityFactor <= 0 || defaultSelectivityFactor > 1.0)
                    throw StandardException.newException(SQLState.LANG_INVALID_SESSION_PROPERTY_VALUE, valString, "value in the range(0,1] or null");
                break;
            case RECURSIVEQUERYITERATIONLIMIT:
            case SPARK_RESULT_STREAMING_BATCHES:
            case SPARK_RESULT_STREAMING_BATCH_SIZE:
            case OLAPPARALLELPARTITIONS:
            case OLAPSHUFFLEPARTITIONS:
            case TABLELIMITFOREXHAUSTIVESEARCH:
                int value;
                try {
                    value = Integer.parseInt(valString);
                } catch (Exception parseIntE) {
                    throw StandardException.newException(SQLState.LANG_INVALID_SESSION_PROPERTY_VALUE, valString, "value should be a positive integer or null");
                }
                if (value <= 0)
                    throw StandardException.newException(SQLState.LANG_INVALID_SESSION_PROPERTY_VALUE, valString, "value should be a positive integer or null");
                break;
            case SNAPSHOT_TIMESTAMP:
                long timestamp;
                try {
                    timestamp = Long.parseLong(valString);
                    if (timestamp <= 0)
                        throw StandardException.newException(SQLState.LANG_INVALID_SESSION_PROPERTY_VALUE, valString, "value should be a positive long");
                } catch (NumberFormatException parseIntE) {
                    throw StandardException.newException(SQLState.LANG_INVALID_SESSION_PROPERTY_VALUE, valString, "value should be a positive long");
                }
                break;
            default:
                break;
        }

        return new Pair(property, valString);
    }
}
