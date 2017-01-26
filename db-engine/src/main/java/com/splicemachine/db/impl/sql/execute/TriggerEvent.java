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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.execute;

import static com.splicemachine.db.impl.sql.execute.TriggerEventDML.*;
import static com.splicemachine.db.impl.sql.execute.TriggerEventWhen.AFTER;
import static com.splicemachine.db.impl.sql.execute.TriggerEventWhen.BEFORE;

/**
 * This is a simple class that we use to track trigger events.  This is not expected to be used directly, instead there
 * is a static TriggerEvent in TriggerEvents for each event found in this file.
 */
public enum TriggerEvent {

    BEFORE_INSERT(BEFORE, INSERT),
    BEFORE_UPDATE(BEFORE, UPDATE),
    BEFORE_DELETE(BEFORE, DELETE),

    AFTER_INSERT(AFTER, INSERT),
    AFTER_UPDATE(AFTER, UPDATE),
    AFTER_DELETE(AFTER, DELETE);

    private TriggerEventWhen when;
    private TriggerEventDML dml;

    /**
     * Create a trigger event of the given type
     */
    TriggerEvent(TriggerEventWhen when, TriggerEventDML dml) {
        this.when = when;
        this.dml = dml;
    }

    /**
     * Get the type number of this trigger
     */
    public String getName() {
        return when + " " + dml;
    }

    /**
     * Is this a before trigger
     */
    public boolean isBefore() {
        return when == BEFORE;
    }

    /**
     * Is this an after trigger
     */
    public boolean isAfter() {
        return when == AFTER;
    }

    /**
     * Was it an update event that caused this trigger?
     * @return true if was an update.
     */
    public boolean isUpdate() {
        return dml == TriggerEventDML.UPDATE;
    }

    public TriggerEventDML getDml() {
        return dml;
    }
}
