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