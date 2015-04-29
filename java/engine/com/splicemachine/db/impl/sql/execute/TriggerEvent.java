/*

   Derby - Class org.apache.derby.impl.sql.execute.TriggerEvent

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package com.splicemachine.db.impl.sql.execute;

/**
 * This is a simple class that we use to track trigger events.  This is not expected to be used directly, instead there
 * is a static TriggerEvent in TriggerEvents for each event found in this file.
 */
public enum TriggerEvent {

    BEFORE_INSERT(true, "BEFORE INSERT"),
    BEFORE_DELETE(true, "BEFORE DELETE"),
    BEFORE_UPDATE(true, "BEFORE UPDATE"),

    AFTER_INSERT(false, "AFTER INSERT"),
    AFTER_DELETE(false, "AFTER DELETE"),
    AFTER_UPDATE(false, "AFTER UPDATE");

    private final boolean before;
    private final String name;

    /**
     * Create a trigger event of the given type
     */
    TriggerEvent(boolean before, String name) {
        this.before = before;
        this.name = name;
    }

    /**
     * Get the type number of this trigger
     */
    String getName() {
        return name;
    }

    /**
     * Is this a before trigger
     */
    boolean isBefore() {
        return before;
    }

    /**
     * Is this an after trigger
     */
    boolean isAfter() {
        return !before;
    }
}
