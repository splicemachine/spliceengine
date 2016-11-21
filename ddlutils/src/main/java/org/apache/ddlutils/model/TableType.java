/*
 * ddlUtils is a subproject of the Apache DB project, and is licensed under
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.ddlutils.model;

import java.util.HashMap;
import java.util.Map;

/**
 * Types of Table
 */
public enum TableType {
    TABLE("TABLE"),
    VIEW("VIEW"),
    SYSTEM_TABLE("SYSTEM TABLE"),
    GLOBAL_TEMPORARY("GLOBAL TEMPORARY"),
    LOCAL_TEMPORARY("LOCAL TEMPORARY"),
    ALIAS("ALIAS"),
    SYNONYM("SYNONYM");

    private String type;

    TableType(String type) {
        this.type = type;
    }

    public String type() {
        return type;
    }

    private final static Map<String, TableType> map = new HashMap<>(TableType.values().length, 1.0f);
    static {
        for (TableType t : TableType.values()) {
            map.put(t.type, t);
        }
    }

    public static TableType of(String name) {
        TableType result = map.get(name);
        if (result == null) {
            throw new IllegalArgumentException("No TableType Exists for '"+name+"'");
        }
        return result;
    }

}
