package com.splicemachine.db.iapi.sql.compile;

import static com.splicemachine.db.iapi.sql.compile.DataSetProcessorType.FORCED_SPARK;
import static com.splicemachine.db.iapi.sql.compile.DataSetProcessorType.FORCED_CONTROL;
import static com.splicemachine.db.iapi.sql.compile.DataSetProcessorType.QUERY_HINTED_SPARK;
import static com.splicemachine.db.iapi.sql.compile.DataSetProcessorType.QUERY_HINTED_CONTROL;
import static com.splicemachine.db.iapi.sql.compile.DataSetProcessorType.SESSION_HINTED_SPARK;
import static com.splicemachine.db.iapi.sql.compile.DataSetProcessorType.SESSION_HINTED_CONTROL;
import static com.splicemachine.db.iapi.sql.compile.DataSetProcessorType.COST_SUGGESTED_SPARK;
import static com.splicemachine.db.iapi.sql.compile.DataSetProcessorType.DEFAULT_CONTROL;

import com.splicemachine.db.iapi.error.StandardException;
import org.junit.Test;

import static org.junit.Assert.*;

public class DataSetProcessorTypeTest {

    @Test
    public void isSpark() {
        assertTrue(COST_SUGGESTED_SPARK.isSpark());
        assertTrue(SESSION_HINTED_SPARK.isSpark());
        assertTrue(QUERY_HINTED_SPARK.isSpark());
        assertTrue(FORCED_SPARK.isSpark());

        assertFalse(DEFAULT_CONTROL.isSpark());
        assertFalse(SESSION_HINTED_CONTROL.isSpark());
        assertFalse(QUERY_HINTED_CONTROL.isSpark());
        assertFalse(FORCED_CONTROL.isSpark());
    }

    @Test
    public void isForced() {
        assertTrue(FORCED_SPARK.isForced());
        assertTrue(FORCED_CONTROL.isForced());

        assertFalse(COST_SUGGESTED_SPARK.isForced());
        assertFalse(SESSION_HINTED_SPARK.isForced());
        assertFalse(QUERY_HINTED_SPARK.isForced());
        assertFalse(DEFAULT_CONTROL.isForced());
        assertFalse(SESSION_HINTED_CONTROL.isForced());
        assertFalse(QUERY_HINTED_CONTROL.isForced());
    }

    @Test
    public void isQueryHinted() {
        assertTrue(QUERY_HINTED_SPARK.isQueryHinted());
        assertTrue(QUERY_HINTED_CONTROL.isQueryHinted());

        assertFalse(SESSION_HINTED_CONTROL.isQueryHinted());
        assertFalse(SESSION_HINTED_SPARK.isQueryHinted());
        assertFalse(FORCED_SPARK.isQueryHinted());
        assertFalse(FORCED_CONTROL.isQueryHinted());
        assertFalse(COST_SUGGESTED_SPARK.isQueryHinted());
        assertFalse(DEFAULT_CONTROL.isQueryHinted());
    }

    @Test
    public void isSessionHinted() {
        assertTrue(SESSION_HINTED_CONTROL.isSessionHinted());
        assertTrue(SESSION_HINTED_SPARK.isSessionHinted());

        assertFalse(QUERY_HINTED_SPARK.isSessionHinted());
        assertFalse(QUERY_HINTED_CONTROL.isSessionHinted());
        assertFalse(FORCED_SPARK.isSessionHinted());
        assertFalse(FORCED_CONTROL.isSessionHinted());
        assertFalse(COST_SUGGESTED_SPARK.isSessionHinted());
        assertFalse(DEFAULT_CONTROL.isSessionHinted());
    }

    @Test
    public void isHinted() {
        assertTrue(SESSION_HINTED_SPARK.isHinted());
        assertTrue(QUERY_HINTED_SPARK.isHinted());
        assertTrue(SESSION_HINTED_CONTROL.isHinted());
        assertTrue(QUERY_HINTED_CONTROL.isHinted());

        assertFalse(FORCED_SPARK.isHinted());
        assertFalse(FORCED_CONTROL.isHinted());
        assertFalse(COST_SUGGESTED_SPARK.isHinted());
        assertFalse(DEFAULT_CONTROL.isHinted());
    }

    @Test
    public void isDefaultControl() {
        assertTrue(DEFAULT_CONTROL.isDefaultControl());

        assertFalse(SESSION_HINTED_SPARK.isDefaultControl());
        assertFalse(QUERY_HINTED_SPARK.isDefaultControl());
        assertFalse(SESSION_HINTED_CONTROL.isDefaultControl());
        assertFalse(QUERY_HINTED_CONTROL.isDefaultControl());
        assertFalse(FORCED_SPARK.isDefaultControl());
        assertFalse(FORCED_CONTROL.isDefaultControl());
        assertFalse(COST_SUGGESTED_SPARK.isDefaultControl());
    }

    void shouldFail(DataSetProcessorType a, DataSetProcessorType b) {
        try {
            a.combine(b);
            fail("Combining " + a + " and " + b + "should have failed but didn't");
        } catch (StandardException e) {
            // pass
        }
    }

    @Test
    public void combine() throws StandardException {
        assertEquals(FORCED_SPARK, FORCED_SPARK.combine(FORCED_SPARK));
        shouldFail(FORCED_SPARK, FORCED_CONTROL);
        assertEquals(FORCED_SPARK, FORCED_SPARK.combine(QUERY_HINTED_SPARK));
        assertEquals(FORCED_SPARK, FORCED_SPARK.combine(QUERY_HINTED_CONTROL));
        assertEquals(FORCED_SPARK, FORCED_SPARK.combine(SESSION_HINTED_SPARK));
        assertEquals(FORCED_SPARK, FORCED_SPARK.combine(SESSION_HINTED_CONTROL));
        assertEquals(FORCED_SPARK, FORCED_SPARK.combine(COST_SUGGESTED_SPARK));
        assertEquals(FORCED_SPARK, FORCED_SPARK.combine(DEFAULT_CONTROL));
        assertEquals(FORCED_SPARK, FORCED_SPARK.combine(null));

        shouldFail(FORCED_CONTROL, FORCED_SPARK);
        assertEquals(FORCED_CONTROL, FORCED_CONTROL.combine(FORCED_CONTROL));
        assertEquals(FORCED_CONTROL, FORCED_CONTROL.combine(QUERY_HINTED_SPARK));
        assertEquals(FORCED_CONTROL, FORCED_CONTROL.combine(QUERY_HINTED_CONTROL));
        assertEquals(FORCED_CONTROL, FORCED_CONTROL.combine(SESSION_HINTED_SPARK));
        assertEquals(FORCED_CONTROL, FORCED_CONTROL.combine(SESSION_HINTED_CONTROL));
        assertEquals(FORCED_CONTROL, FORCED_CONTROL.combine(COST_SUGGESTED_SPARK));
        assertEquals(FORCED_CONTROL, FORCED_CONTROL.combine(DEFAULT_CONTROL));
        assertEquals(FORCED_CONTROL, FORCED_CONTROL.combine(null));

        assertEquals(FORCED_SPARK, QUERY_HINTED_SPARK.combine(FORCED_SPARK));
        assertEquals(FORCED_CONTROL, QUERY_HINTED_SPARK.combine(FORCED_CONTROL));
        assertEquals(QUERY_HINTED_SPARK, QUERY_HINTED_SPARK.combine(QUERY_HINTED_SPARK));
        shouldFail(QUERY_HINTED_SPARK, QUERY_HINTED_CONTROL);
        assertEquals(QUERY_HINTED_SPARK, QUERY_HINTED_SPARK.combine(SESSION_HINTED_SPARK));
        assertEquals(QUERY_HINTED_SPARK, QUERY_HINTED_SPARK.combine(SESSION_HINTED_CONTROL));
        assertEquals(QUERY_HINTED_SPARK, QUERY_HINTED_SPARK.combine(COST_SUGGESTED_SPARK));
        assertEquals(QUERY_HINTED_SPARK, QUERY_HINTED_SPARK.combine(DEFAULT_CONTROL));
        assertEquals(QUERY_HINTED_SPARK, QUERY_HINTED_SPARK.combine(null));

        assertEquals(FORCED_SPARK, QUERY_HINTED_CONTROL.combine(FORCED_SPARK));
        assertEquals(FORCED_CONTROL, QUERY_HINTED_CONTROL.combine(FORCED_CONTROL));
        shouldFail(QUERY_HINTED_CONTROL, QUERY_HINTED_SPARK);
        assertEquals(QUERY_HINTED_CONTROL, QUERY_HINTED_CONTROL.combine(QUERY_HINTED_CONTROL));
        assertEquals(QUERY_HINTED_CONTROL, QUERY_HINTED_CONTROL.combine(SESSION_HINTED_SPARK));
        assertEquals(QUERY_HINTED_CONTROL, QUERY_HINTED_CONTROL.combine(SESSION_HINTED_CONTROL));
        assertEquals(QUERY_HINTED_CONTROL, QUERY_HINTED_CONTROL.combine(COST_SUGGESTED_SPARK));
        assertEquals(QUERY_HINTED_CONTROL, QUERY_HINTED_CONTROL.combine(DEFAULT_CONTROL));
        assertEquals(QUERY_HINTED_CONTROL, QUERY_HINTED_CONTROL.combine(null));

        assertEquals(FORCED_SPARK, SESSION_HINTED_SPARK.combine(FORCED_SPARK));
        assertEquals(FORCED_CONTROL, SESSION_HINTED_SPARK.combine(FORCED_CONTROL));
        assertEquals(QUERY_HINTED_SPARK, SESSION_HINTED_SPARK.combine(QUERY_HINTED_SPARK));
        assertEquals(QUERY_HINTED_CONTROL, SESSION_HINTED_SPARK.combine(QUERY_HINTED_CONTROL));
        assertEquals(SESSION_HINTED_SPARK, SESSION_HINTED_SPARK.combine(SESSION_HINTED_SPARK));
        shouldFail(SESSION_HINTED_SPARK, SESSION_HINTED_CONTROL);
        assertEquals(SESSION_HINTED_SPARK, SESSION_HINTED_SPARK.combine(COST_SUGGESTED_SPARK));
        assertEquals(SESSION_HINTED_SPARK, SESSION_HINTED_SPARK.combine(DEFAULT_CONTROL));
        assertEquals(SESSION_HINTED_SPARK, SESSION_HINTED_SPARK.combine(null));

        assertEquals(FORCED_SPARK, SESSION_HINTED_CONTROL.combine(FORCED_SPARK));
        assertEquals(FORCED_CONTROL, SESSION_HINTED_CONTROL.combine(FORCED_CONTROL));
        assertEquals(QUERY_HINTED_SPARK, SESSION_HINTED_CONTROL.combine(QUERY_HINTED_SPARK));
        assertEquals(QUERY_HINTED_CONTROL, SESSION_HINTED_CONTROL.combine(QUERY_HINTED_CONTROL));
        shouldFail(SESSION_HINTED_CONTROL, SESSION_HINTED_SPARK);
        assertEquals(SESSION_HINTED_CONTROL, SESSION_HINTED_CONTROL.combine(SESSION_HINTED_CONTROL));
        assertEquals(SESSION_HINTED_CONTROL, SESSION_HINTED_CONTROL.combine(COST_SUGGESTED_SPARK));
        assertEquals(SESSION_HINTED_CONTROL, SESSION_HINTED_CONTROL.combine(DEFAULT_CONTROL));
        assertEquals(SESSION_HINTED_CONTROL, SESSION_HINTED_CONTROL.combine(null));

        assertEquals(FORCED_SPARK, COST_SUGGESTED_SPARK.combine(FORCED_SPARK));
        assertEquals(FORCED_CONTROL, COST_SUGGESTED_SPARK.combine(FORCED_CONTROL));
        assertEquals(QUERY_HINTED_SPARK, COST_SUGGESTED_SPARK.combine(QUERY_HINTED_SPARK));
        assertEquals(QUERY_HINTED_CONTROL, COST_SUGGESTED_SPARK.combine(QUERY_HINTED_CONTROL));
        assertEquals(SESSION_HINTED_SPARK, COST_SUGGESTED_SPARK.combine(SESSION_HINTED_SPARK));
        assertEquals(SESSION_HINTED_CONTROL, COST_SUGGESTED_SPARK.combine(SESSION_HINTED_CONTROL));
        assertEquals(COST_SUGGESTED_SPARK, COST_SUGGESTED_SPARK.combine(COST_SUGGESTED_SPARK));
        assertEquals(COST_SUGGESTED_SPARK, COST_SUGGESTED_SPARK.combine(DEFAULT_CONTROL));
        assertEquals(COST_SUGGESTED_SPARK, COST_SUGGESTED_SPARK.combine(null));

        assertEquals(FORCED_SPARK, DEFAULT_CONTROL.combine(FORCED_SPARK));
        assertEquals(FORCED_CONTROL, DEFAULT_CONTROL.combine(FORCED_CONTROL));
        assertEquals(QUERY_HINTED_SPARK, DEFAULT_CONTROL.combine(QUERY_HINTED_SPARK));
        assertEquals(QUERY_HINTED_CONTROL, DEFAULT_CONTROL.combine(QUERY_HINTED_CONTROL));
        assertEquals(SESSION_HINTED_SPARK, DEFAULT_CONTROL.combine(SESSION_HINTED_SPARK));
        assertEquals(SESSION_HINTED_CONTROL, DEFAULT_CONTROL.combine(SESSION_HINTED_CONTROL));
        assertEquals(COST_SUGGESTED_SPARK, DEFAULT_CONTROL.combine(COST_SUGGESTED_SPARK));
        assertEquals(DEFAULT_CONTROL, DEFAULT_CONTROL.combine(DEFAULT_CONTROL));
        assertEquals(DEFAULT_CONTROL, DEFAULT_CONTROL.combine(null));
    }
}