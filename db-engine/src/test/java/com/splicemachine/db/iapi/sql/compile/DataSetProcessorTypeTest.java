package com.splicemachine.db.iapi.sql.compile;

import static com.splicemachine.db.iapi.sql.compile.DataSetProcessorType.FORCED_OLAP;
import static com.splicemachine.db.iapi.sql.compile.DataSetProcessorType.FORCED_OLTP;
import static com.splicemachine.db.iapi.sql.compile.DataSetProcessorType.QUERY_HINTED_OLAP;
import static com.splicemachine.db.iapi.sql.compile.DataSetProcessorType.QUERY_HINTED_OLTP;
import static com.splicemachine.db.iapi.sql.compile.DataSetProcessorType.SESSION_HINTED_OLAP;
import static com.splicemachine.db.iapi.sql.compile.DataSetProcessorType.SESSION_HINTED_OLTP;
import static com.splicemachine.db.iapi.sql.compile.DataSetProcessorType.COST_SUGGESTED_OLAP;
import static com.splicemachine.db.iapi.sql.compile.DataSetProcessorType.DEFAULT_OLTP;

import com.splicemachine.db.iapi.error.StandardException;
import org.junit.Test;

import static org.junit.Assert.*;

public class DataSetProcessorTypeTest {

    @Test
    public void isSpark() {
        assertTrue(COST_SUGGESTED_OLAP.isOlap());
        assertTrue(SESSION_HINTED_OLAP.isOlap());
        assertTrue(QUERY_HINTED_OLAP.isOlap());
        assertTrue(FORCED_OLAP.isOlap());

        assertFalse(DEFAULT_OLTP.isOlap());
        assertFalse(SESSION_HINTED_OLTP.isOlap());
        assertFalse(QUERY_HINTED_OLTP.isOlap());
        assertFalse(FORCED_OLTP.isOlap());
    }

    @Test
    public void isForced() {
        assertTrue(FORCED_OLAP.isForced());
        assertTrue(FORCED_OLTP.isForced());

        assertFalse(COST_SUGGESTED_OLAP.isForced());
        assertFalse(SESSION_HINTED_OLAP.isForced());
        assertFalse(QUERY_HINTED_OLAP.isForced());
        assertFalse(DEFAULT_OLTP.isForced());
        assertFalse(SESSION_HINTED_OLTP.isForced());
        assertFalse(QUERY_HINTED_OLTP.isForced());
    }

    @Test
    public void isQueryHinted() {
        assertTrue(QUERY_HINTED_OLAP.isQueryHinted());
        assertTrue(QUERY_HINTED_OLTP.isQueryHinted());

        assertFalse(SESSION_HINTED_OLTP.isQueryHinted());
        assertFalse(SESSION_HINTED_OLAP.isQueryHinted());
        assertFalse(FORCED_OLAP.isQueryHinted());
        assertFalse(FORCED_OLTP.isQueryHinted());
        assertFalse(COST_SUGGESTED_OLAP.isQueryHinted());
        assertFalse(DEFAULT_OLTP.isQueryHinted());
    }

    @Test
    public void isSessionHinted() {
        assertTrue(SESSION_HINTED_OLTP.isSessionHinted());
        assertTrue(SESSION_HINTED_OLAP.isSessionHinted());

        assertFalse(QUERY_HINTED_OLAP.isSessionHinted());
        assertFalse(QUERY_HINTED_OLTP.isSessionHinted());
        assertFalse(FORCED_OLAP.isSessionHinted());
        assertFalse(FORCED_OLTP.isSessionHinted());
        assertFalse(COST_SUGGESTED_OLAP.isSessionHinted());
        assertFalse(DEFAULT_OLTP.isSessionHinted());
    }

    @Test
    public void isHinted() {
        assertTrue(SESSION_HINTED_OLAP.isHinted());
        assertTrue(QUERY_HINTED_OLAP.isHinted());
        assertTrue(SESSION_HINTED_OLTP.isHinted());
        assertTrue(QUERY_HINTED_OLTP.isHinted());

        assertFalse(FORCED_OLAP.isHinted());
        assertFalse(FORCED_OLTP.isHinted());
        assertFalse(COST_SUGGESTED_OLAP.isHinted());
        assertFalse(DEFAULT_OLTP.isHinted());
    }

    @Test
    public void isDefaultControl() {
        assertTrue(DEFAULT_OLTP.isDefaultOltp());

        assertFalse(SESSION_HINTED_OLAP.isDefaultOltp());
        assertFalse(QUERY_HINTED_OLAP.isDefaultOltp());
        assertFalse(SESSION_HINTED_OLTP.isDefaultOltp());
        assertFalse(QUERY_HINTED_OLTP.isDefaultOltp());
        assertFalse(FORCED_OLAP.isDefaultOltp());
        assertFalse(FORCED_OLTP.isDefaultOltp());
        assertFalse(COST_SUGGESTED_OLAP.isDefaultOltp());
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
        assertEquals(FORCED_OLAP, FORCED_OLAP.combine(FORCED_OLAP));
        shouldFail(FORCED_OLAP, FORCED_OLTP);
        assertEquals(FORCED_OLAP, FORCED_OLAP.combine(QUERY_HINTED_OLAP));
        assertEquals(FORCED_OLAP, FORCED_OLAP.combine(QUERY_HINTED_OLTP));
        assertEquals(FORCED_OLAP, FORCED_OLAP.combine(SESSION_HINTED_OLAP));
        assertEquals(FORCED_OLAP, FORCED_OLAP.combine(SESSION_HINTED_OLTP));
        assertEquals(FORCED_OLAP, FORCED_OLAP.combine(COST_SUGGESTED_OLAP));
        assertEquals(FORCED_OLAP, FORCED_OLAP.combine(DEFAULT_OLTP));
        assertEquals(FORCED_OLAP, FORCED_OLAP.combine(null));

        shouldFail(FORCED_OLTP, FORCED_OLAP);
        assertEquals(FORCED_OLTP, FORCED_OLTP.combine(FORCED_OLTP));
        assertEquals(FORCED_OLTP, FORCED_OLTP.combine(QUERY_HINTED_OLAP));
        assertEquals(FORCED_OLTP, FORCED_OLTP.combine(QUERY_HINTED_OLTP));
        assertEquals(FORCED_OLTP, FORCED_OLTP.combine(SESSION_HINTED_OLAP));
        assertEquals(FORCED_OLTP, FORCED_OLTP.combine(SESSION_HINTED_OLTP));
        assertEquals(FORCED_OLTP, FORCED_OLTP.combine(COST_SUGGESTED_OLAP));
        assertEquals(FORCED_OLTP, FORCED_OLTP.combine(DEFAULT_OLTP));
        assertEquals(FORCED_OLTP, FORCED_OLTP.combine(null));

        assertEquals(FORCED_OLAP, QUERY_HINTED_OLAP.combine(FORCED_OLAP));
        assertEquals(FORCED_OLTP, QUERY_HINTED_OLAP.combine(FORCED_OLTP));
        assertEquals(QUERY_HINTED_OLAP, QUERY_HINTED_OLAP.combine(QUERY_HINTED_OLAP));
        shouldFail(QUERY_HINTED_OLAP, QUERY_HINTED_OLTP);
        assertEquals(QUERY_HINTED_OLAP, QUERY_HINTED_OLAP.combine(SESSION_HINTED_OLAP));
        assertEquals(QUERY_HINTED_OLAP, QUERY_HINTED_OLAP.combine(SESSION_HINTED_OLTP));
        assertEquals(QUERY_HINTED_OLAP, QUERY_HINTED_OLAP.combine(COST_SUGGESTED_OLAP));
        assertEquals(QUERY_HINTED_OLAP, QUERY_HINTED_OLAP.combine(DEFAULT_OLTP));
        assertEquals(QUERY_HINTED_OLAP, QUERY_HINTED_OLAP.combine(null));

        assertEquals(FORCED_OLAP, QUERY_HINTED_OLTP.combine(FORCED_OLAP));
        assertEquals(FORCED_OLTP, QUERY_HINTED_OLTP.combine(FORCED_OLTP));
        shouldFail(QUERY_HINTED_OLTP, QUERY_HINTED_OLAP);
        assertEquals(QUERY_HINTED_OLTP, QUERY_HINTED_OLTP.combine(QUERY_HINTED_OLTP));
        assertEquals(QUERY_HINTED_OLTP, QUERY_HINTED_OLTP.combine(SESSION_HINTED_OLAP));
        assertEquals(QUERY_HINTED_OLTP, QUERY_HINTED_OLTP.combine(SESSION_HINTED_OLTP));
        assertEquals(QUERY_HINTED_OLTP, QUERY_HINTED_OLTP.combine(COST_SUGGESTED_OLAP));
        assertEquals(QUERY_HINTED_OLTP, QUERY_HINTED_OLTP.combine(DEFAULT_OLTP));
        assertEquals(QUERY_HINTED_OLTP, QUERY_HINTED_OLTP.combine(null));

        assertEquals(FORCED_OLAP, SESSION_HINTED_OLAP.combine(FORCED_OLAP));
        assertEquals(FORCED_OLTP, SESSION_HINTED_OLAP.combine(FORCED_OLTP));
        assertEquals(QUERY_HINTED_OLAP, SESSION_HINTED_OLAP.combine(QUERY_HINTED_OLAP));
        assertEquals(QUERY_HINTED_OLTP, SESSION_HINTED_OLAP.combine(QUERY_HINTED_OLTP));
        assertEquals(SESSION_HINTED_OLAP, SESSION_HINTED_OLAP.combine(SESSION_HINTED_OLAP));
        shouldFail(SESSION_HINTED_OLAP, SESSION_HINTED_OLTP);
        assertEquals(SESSION_HINTED_OLAP, SESSION_HINTED_OLAP.combine(COST_SUGGESTED_OLAP));
        assertEquals(SESSION_HINTED_OLAP, SESSION_HINTED_OLAP.combine(DEFAULT_OLTP));
        assertEquals(SESSION_HINTED_OLAP, SESSION_HINTED_OLAP.combine(null));

        assertEquals(FORCED_OLAP, SESSION_HINTED_OLTP.combine(FORCED_OLAP));
        assertEquals(FORCED_OLTP, SESSION_HINTED_OLTP.combine(FORCED_OLTP));
        assertEquals(QUERY_HINTED_OLAP, SESSION_HINTED_OLTP.combine(QUERY_HINTED_OLAP));
        assertEquals(QUERY_HINTED_OLTP, SESSION_HINTED_OLTP.combine(QUERY_HINTED_OLTP));
        shouldFail(SESSION_HINTED_OLTP, SESSION_HINTED_OLAP);
        assertEquals(SESSION_HINTED_OLTP, SESSION_HINTED_OLTP.combine(SESSION_HINTED_OLTP));
        assertEquals(SESSION_HINTED_OLTP, SESSION_HINTED_OLTP.combine(COST_SUGGESTED_OLAP));
        assertEquals(SESSION_HINTED_OLTP, SESSION_HINTED_OLTP.combine(DEFAULT_OLTP));
        assertEquals(SESSION_HINTED_OLTP, SESSION_HINTED_OLTP.combine(null));

        assertEquals(FORCED_OLAP, COST_SUGGESTED_OLAP.combine(FORCED_OLAP));
        assertEquals(FORCED_OLTP, COST_SUGGESTED_OLAP.combine(FORCED_OLTP));
        assertEquals(QUERY_HINTED_OLAP, COST_SUGGESTED_OLAP.combine(QUERY_HINTED_OLAP));
        assertEquals(QUERY_HINTED_OLTP, COST_SUGGESTED_OLAP.combine(QUERY_HINTED_OLTP));
        assertEquals(SESSION_HINTED_OLAP, COST_SUGGESTED_OLAP.combine(SESSION_HINTED_OLAP));
        assertEquals(SESSION_HINTED_OLTP, COST_SUGGESTED_OLAP.combine(SESSION_HINTED_OLTP));
        assertEquals(COST_SUGGESTED_OLAP, COST_SUGGESTED_OLAP.combine(COST_SUGGESTED_OLAP));
        assertEquals(COST_SUGGESTED_OLAP, COST_SUGGESTED_OLAP.combine(DEFAULT_OLTP));
        assertEquals(COST_SUGGESTED_OLAP, COST_SUGGESTED_OLAP.combine(null));

        assertEquals(FORCED_OLAP, DEFAULT_OLTP.combine(FORCED_OLAP));
        assertEquals(FORCED_OLTP, DEFAULT_OLTP.combine(FORCED_OLTP));
        assertEquals(QUERY_HINTED_OLAP, DEFAULT_OLTP.combine(QUERY_HINTED_OLAP));
        assertEquals(QUERY_HINTED_OLTP, DEFAULT_OLTP.combine(QUERY_HINTED_OLTP));
        assertEquals(SESSION_HINTED_OLAP, DEFAULT_OLTP.combine(SESSION_HINTED_OLAP));
        assertEquals(SESSION_HINTED_OLTP, DEFAULT_OLTP.combine(SESSION_HINTED_OLTP));
        assertEquals(COST_SUGGESTED_OLAP, DEFAULT_OLTP.combine(COST_SUGGESTED_OLAP));
        assertEquals(DEFAULT_OLTP, DEFAULT_OLTP.combine(DEFAULT_OLTP));
        assertEquals(DEFAULT_OLTP, DEFAULT_OLTP.combine(null));
    }
}