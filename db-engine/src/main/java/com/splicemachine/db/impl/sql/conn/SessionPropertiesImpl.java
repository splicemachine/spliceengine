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
package com.splicemachine.db.impl.sql.conn;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.conn.SessionProperties;
import com.splicemachine.db.iapi.util.StringUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.splicemachine.db.iapi.sql.conn.SessionProperties.PROPERTYNAME.*;

/**
 * Created by yxia on 6/1/18.
 */
public class SessionPropertiesImpl implements SessionProperties {
    Object[] properties = new Object[PROPERTYNAME.COUNT];

    public void setProperty(PROPERTYNAME property, Object value) {
        // the legal values have been checked at SetSessionpropertyNode.init(), so no need to check again
        String valString = String.valueOf(value);
        if (valString == null || valString.equals("null")) {
            properties[property.getId()] = null;
            return;
        }

        switch (property) {
            case USEOLAP:
                boolean useOlapVal = Boolean.parseBoolean(valString);
                properties[USEOLAP.getId()] = useOlapVal;
                break;
            case DEFAULTSELECTIVITYFACTOR:
                double defaultSelectivityFactor = Double.parseDouble(valString);
                properties[DEFAULTSELECTIVITYFACTOR.getId()] = defaultSelectivityFactor;
                break;
            case SKIPSTATS:
                boolean skipStatsVal = Boolean.parseBoolean(valString);
                properties[SKIPSTATS.getId()] = skipStatsVal;
                break;
            case OLAPQUEUE:
                properties[OLAPQUEUE.getId()] = valString;
                break;
            case RECURSIVEQUERYITERATIONLIMIT:
                int recursiveQueryIterationLimit = Integer.parseInt(valString);
                properties[RECURSIVEQUERYITERATIONLIMIT.getId()] = recursiveQueryIterationLimit;
                break;
            case OLAPPARALLELPARTITIONS:
                int parallelPartitions = Integer.parseInt(valString);
                properties[OLAPPARALLELPARTITIONS.getId()] = parallelPartitions;
                break;
            case OLAPSHUFFLEPARTITIONS:
                int shufflePartitions = Integer.parseInt(valString);
                properties[OLAPSHUFFLEPARTITIONS.getId()] = shufflePartitions;
                break;
            case SNAPSHOT_TIMESTAMP:
                long timestamp = Long.parseLong(valString);
                properties[SNAPSHOT_TIMESTAMP.getId()] = timestamp;
                break;
            case DISABLE_TC_PUSHED_DOWN_INTO_VIEWS:
                boolean disabled = Boolean.parseBoolean(valString);
                properties[DISABLE_TC_PUSHED_DOWN_INTO_VIEWS.getId()] = disabled;
                break;
            case SPARK_RESULT_STREAMING_BATCHES:
                int sparkResultStreamingBatches = Integer.parseInt(valString);
                properties[SPARK_RESULT_STREAMING_BATCHES.getId()] = sparkResultStreamingBatches;
                break;
            case SPARK_RESULT_STREAMING_BATCH_SIZE:
                int sparkResultStreamingBatchSize = Integer.parseInt(valString);
                properties[SPARK_RESULT_STREAMING_BATCH_SIZE.getId()] = sparkResultStreamingBatchSize;
                break;
            case TABLELIMITFOREXHAUSTIVESEARCH:
                int tableLimitForExhaustiveSearch = Integer.parseInt(valString);
                properties[TABLELIMITFOREXHAUSTIVESEARCH.getId()] = tableLimitForExhaustiveSearch;
                break;
            case DISABLE_NLJ_PREDICATE_PUSH_DOWN:
                boolean disablePushDown = Boolean.parseBoolean(valString);
                properties[DISABLE_NLJ_PREDICATE_PUSH_DOWN.getId()] = disablePushDown;
                break;
            case USE_NATIVE_SPARK:
                boolean useNativeSpark = Boolean.parseBoolean(valString);
                properties[USE_NATIVE_SPARK.getId()] = useNativeSpark;
                break;
            case CURRENTFUNCTIONPATH:
                properties[CURRENTFUNCTIONPATH.getId()] = parseCurrentFunctionPath(valString);
                break;
            default:
                assert false;
        }
    }

    /**
     * parses the value of CURRENTFUNCTIONPATH according to:
     * https://www.ibm.com/support/producthub/db2/docs/content/SSEPGG_11.5.0/com.ibm.db2.luw.apdv.cli.doc/doc/r0008777.html
     * @param value the user-input value
     * @return list of schemas for function lookup.
     */
    private String[] parseCurrentFunctionPath(String value) {
        if(value == null || value.isEmpty()) {
            return new String[] {"SYSIBM", "SYSFUN"};
        } else {
            // source: https://stackabuse.com/regex-splitting-by-character-unless-in-quotes/
            List<String> candidateSchemas = new ArrayList<>(Arrays.asList(value.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")));
            for(int i = 0; i < candidateSchemas.size(); ++i) {
                candidateSchemas.set(i , normalizeAndUnquoteIdentifier(candidateSchemas.get(i)));
            }
            boolean sysibmSeen = false;
            boolean sysfunSeen = false;
            for(int i = 0; i < candidateSchemas.size(); ++i) {
                String candidateSchema = candidateSchemas.get(i);
                if(i == 0 && candidateSchema.equals("SYSIBM")) {
                    sysibmSeen = true;
                } else if(i == 1 && candidateSchema.equals("SYSFUN")) {
                    sysfunSeen = true;
                } else {
                    break;
                }
            }
            if(!sysibmSeen) {
                candidateSchemas.add(0, "SYSIBM");
            }
            if(!sysfunSeen) {
                candidateSchemas.add(1, "SYSFUN");
            }
            return candidateSchemas.toArray(new String[0]);
        }
    }

    private String normalizeAndUnquoteIdentifier(String identifier) {
        String normalized = StringUtil.normalizeSQLIdentifier(identifier);
        if(!normalized.isEmpty() && normalized.charAt(0) == '"' && normalized.charAt(normalized.length() - 1) == '"') {
            normalized = normalized.substring(1, normalized.length() -1);
        }
        return normalized;
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
