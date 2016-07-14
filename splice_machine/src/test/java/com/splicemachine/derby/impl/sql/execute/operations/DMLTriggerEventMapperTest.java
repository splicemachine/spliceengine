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
import com.splicemachine.si.testenv.ArchitectureIndependent;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(ArchitectureIndependent.class)
public class DMLTriggerEventMapperTest {

    @Test
    public void testGetBeforeEvent() throws Exception {
        assertEquals(TriggerEvent.BEFORE_DELETE, DMLTriggerEventMapper.getBeforeEvent(DeleteOperation.class));

        assertEquals(TriggerEvent.BEFORE_INSERT, DMLTriggerEventMapper.getBeforeEvent(InsertOperation.class));
        assertEquals(TriggerEvent.BEFORE_UPDATE, DMLTriggerEventMapper.getBeforeEvent(UpdateOperation.class));
    }

    @Test
    public void testGetAfterEvent() throws Exception {
        assertEquals(TriggerEvent.AFTER_DELETE, DMLTriggerEventMapper.getAfterEvent(DeleteOperation.class));

        assertEquals(TriggerEvent.AFTER_INSERT, DMLTriggerEventMapper.getAfterEvent(InsertOperation.class));
        assertEquals(TriggerEvent.AFTER_UPDATE, DMLTriggerEventMapper.getAfterEvent(UpdateOperation.class));
    }
}