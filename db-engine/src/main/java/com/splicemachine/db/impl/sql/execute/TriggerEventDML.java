/*
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
 *
 */

package com.splicemachine.db.impl.sql.execute;

/**
 * Indicates the type of DML statement that causes trigger to fire.
 *
 * @see TriggerEvent
 */
public enum TriggerEventDML {

    UPDATE(1),
    DELETE(2),
    INSERT(4);

    /* The actual ID values are used for serialization and storage in sys.systriggers. Use values that
     * can be stored in and retrieved from integer bitmask. */
    private int id;

    TriggerEventDML(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public static TriggerEventDML fromId(int id) {
        for (TriggerEventDML event : TriggerEventDML.values()) {
            if (id == event.getId()) {
                return event;
            }
        }
        throw new IllegalArgumentException("id=" + id);
    }
}
