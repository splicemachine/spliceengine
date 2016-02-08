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
        assertEquals(TriggerEvent.BEFORE_DELETE, DMLTriggerEventMapper.getBeforeEvent(DeleteCascadeOperation.class));
        assertEquals(TriggerEvent.BEFORE_DELETE, DMLTriggerEventMapper.getBeforeEvent(DeleteOperation.class));

        assertEquals(TriggerEvent.BEFORE_INSERT, DMLTriggerEventMapper.getBeforeEvent(InsertOperation.class));
        assertEquals(TriggerEvent.BEFORE_UPDATE, DMLTriggerEventMapper.getBeforeEvent(UpdateOperation.class));
    }

    @Test
    public void testGetAfterEvent() throws Exception {
        assertEquals(TriggerEvent.AFTER_DELETE, DMLTriggerEventMapper.getAfterEvent(DeleteCascadeOperation.class));
        assertEquals(TriggerEvent.AFTER_DELETE, DMLTriggerEventMapper.getAfterEvent(DeleteOperation.class));

        assertEquals(TriggerEvent.AFTER_INSERT, DMLTriggerEventMapper.getAfterEvent(InsertOperation.class));
        assertEquals(TriggerEvent.AFTER_UPDATE, DMLTriggerEventMapper.getAfterEvent(UpdateOperation.class));
    }
}