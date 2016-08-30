/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.impl.sql.execute.TriggerEvent;
import org.sparkproject.guava.collect.ImmutableMap;
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
