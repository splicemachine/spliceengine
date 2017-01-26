/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.impl.sql.execute.TriggerEvent;
import org.spark_project.guava.collect.ImmutableMap;
import java.util.Map;

/**
 * Maps our DML operations to trigger events.
 */
public class DMLTriggerEventMapper {

    private static final Map<Class<? extends DMLWriteOperation>, TriggerEvent> beforeMap = ImmutableMap.<Class<? extends DMLWriteOperation>, TriggerEvent>of(
            InsertOperation.class, TriggerEvent.BEFORE_INSERT,
            UpdateOperation.class, TriggerEvent.BEFORE_UPDATE,
            DeleteOperation.class, TriggerEvent.BEFORE_DELETE
    );

    private static final Map<Class<? extends DMLWriteOperation>, TriggerEvent> afterMap = ImmutableMap.<Class<? extends DMLWriteOperation>, TriggerEvent>of(
            InsertOperation.class, TriggerEvent.AFTER_INSERT,
            UpdateOperation.class, TriggerEvent.AFTER_UPDATE,
            DeleteOperation.class, TriggerEvent.AFTER_DELETE
    );

    public static TriggerEvent getBeforeEvent(Class<? extends DMLWriteOperation> operationClass) {
        for (Map.Entry<Class<? extends DMLWriteOperation>, TriggerEvent> entry : beforeMap.entrySet()) {
            if (entry.getKey().isAssignableFrom(operationClass)) {
                return entry.getValue();
            }
        }
        throw new IllegalArgumentException("could not find trigger event for operation = " + operationClass);
    }

    public static TriggerEvent getAfterEvent(Class<? extends DMLWriteOperation> operationClass) {
        for (Map.Entry<Class<? extends DMLWriteOperation>, TriggerEvent> entry : afterMap.entrySet()) {
            if (entry.getKey().isAssignableFrom(operationClass)) {
                return entry.getValue();
            }
        }
        throw new IllegalArgumentException("could not find trigger event for operation = " + operationClass);
    }

}
