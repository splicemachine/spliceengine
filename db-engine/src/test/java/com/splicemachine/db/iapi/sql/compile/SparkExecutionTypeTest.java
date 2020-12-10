package com.splicemachine.db.iapi.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import org.junit.Test;

import static com.splicemachine.db.iapi.sql.compile.SparkExecutionType.*;
import static org.junit.Assert.*;

public class SparkExecutionTypeTest {

    @Test
    public void isNative() {
        assertTrue(SESSION_HINTED_NATIVE.isNative());
        assertTrue(QUERY_HINTED_NATIVE.isNative());
        assertTrue(FORCED_NATIVE.isNative());

        assertFalse(UNSPECIFIED.isNative());
        assertFalse(SESSION_HINTED_NON_NATIVE.isNative());
        assertFalse(QUERY_HINTED_NON_NATIVE.isNative());
        assertFalse(FORCED_NON_NATIVE.isNative());
    }

    @Test
    public void isNonNative() {
        assertTrue(SESSION_HINTED_NON_NATIVE.isNonNative());
        assertTrue(QUERY_HINTED_NON_NATIVE.isNonNative());
        assertTrue(FORCED_NON_NATIVE.isNonNative());

        assertFalse(UNSPECIFIED.isNonNative());
        assertFalse(SESSION_HINTED_NATIVE.isNonNative());
        assertFalse(QUERY_HINTED_NATIVE.isNonNative());
        assertFalse(FORCED_NATIVE.isNonNative());
    }

    @Test
    public void isForced() {
        assertTrue(FORCED_NATIVE.isForced());
        assertTrue(FORCED_NON_NATIVE.isForced());

        assertFalse(SESSION_HINTED_NATIVE.isForced());
        assertFalse(SESSION_HINTED_NON_NATIVE.isForced());
        assertFalse(QUERY_HINTED_NATIVE.isForced());
        assertFalse(QUERY_HINTED_NON_NATIVE.isForced());
        assertFalse(UNSPECIFIED.isForced());
    }

    @Test
    public void isQueryHinted() {
        assertTrue(QUERY_HINTED_NON_NATIVE.isQueryHinted());
        assertTrue(QUERY_HINTED_NATIVE.isQueryHinted());

        assertFalse(SESSION_HINTED_NON_NATIVE.isQueryHinted());
        assertFalse(SESSION_HINTED_NATIVE.isQueryHinted());
        assertFalse(FORCED_NON_NATIVE.isQueryHinted());
        assertFalse(FORCED_NATIVE.isQueryHinted());
        assertFalse(UNSPECIFIED.isQueryHinted());
    }

    @Test
    public void isSessionHinted() {
        assertTrue(SESSION_HINTED_NATIVE.isSessionHinted());
        assertTrue(SESSION_HINTED_NON_NATIVE.isSessionHinted());

        assertFalse(QUERY_HINTED_NATIVE.isSessionHinted());
        assertFalse(QUERY_HINTED_NON_NATIVE.isSessionHinted());
        assertFalse(FORCED_NATIVE.isSessionHinted());
        assertFalse(FORCED_NON_NATIVE.isSessionHinted());
        assertFalse(UNSPECIFIED.isSessionHinted());
    }

    @Test
    public void isHinted() {
        assertTrue(SESSION_HINTED_NON_NATIVE.isHinted());
        assertTrue(SESSION_HINTED_NATIVE.isHinted());
        assertTrue(QUERY_HINTED_NON_NATIVE.isHinted());
        assertTrue(QUERY_HINTED_NATIVE.isHinted());

        assertFalse(FORCED_NON_NATIVE.isHinted());
        assertFalse(FORCED_NATIVE.isHinted());
        assertFalse(UNSPECIFIED.isHinted());
    }

    void shouldFail(SparkExecutionType a, SparkExecutionType b) {
        try {
            a.combine(b);
            fail("Combining " + a + " and " + b + "should have failed but didn't");
        } catch (StandardException e) {
            // pass
        }
    }

    @Test
    public void combine() throws StandardException {
        assertEquals(FORCED_NATIVE, FORCED_NATIVE.combine(FORCED_NATIVE));
        shouldFail(FORCED_NATIVE, FORCED_NON_NATIVE);
        assertEquals(FORCED_NATIVE, FORCED_NATIVE.combine(QUERY_HINTED_NATIVE));
        assertEquals(FORCED_NATIVE, FORCED_NATIVE.combine(QUERY_HINTED_NON_NATIVE));
        assertEquals(FORCED_NATIVE, FORCED_NATIVE.combine(SESSION_HINTED_NATIVE));
        assertEquals(FORCED_NATIVE, FORCED_NATIVE.combine(SESSION_HINTED_NON_NATIVE));
        assertEquals(FORCED_NATIVE, FORCED_NATIVE.combine(UNSPECIFIED));
        assertEquals(FORCED_NATIVE, FORCED_NATIVE.combine(null));

        shouldFail(FORCED_NON_NATIVE, FORCED_NATIVE);
        assertEquals(FORCED_NON_NATIVE, FORCED_NON_NATIVE.combine(FORCED_NON_NATIVE));
        assertEquals(FORCED_NON_NATIVE, FORCED_NON_NATIVE.combine(QUERY_HINTED_NATIVE));
        assertEquals(FORCED_NON_NATIVE, FORCED_NON_NATIVE.combine(QUERY_HINTED_NON_NATIVE));
        assertEquals(FORCED_NON_NATIVE, FORCED_NON_NATIVE.combine(SESSION_HINTED_NATIVE));
        assertEquals(FORCED_NON_NATIVE, FORCED_NON_NATIVE.combine(SESSION_HINTED_NON_NATIVE));
        assertEquals(FORCED_NON_NATIVE, FORCED_NON_NATIVE.combine(UNSPECIFIED));
        assertEquals(FORCED_NON_NATIVE, FORCED_NON_NATIVE.combine(null));

        assertEquals(FORCED_NATIVE, QUERY_HINTED_NATIVE.combine(FORCED_NATIVE));
        assertEquals(FORCED_NON_NATIVE, QUERY_HINTED_NATIVE.combine(FORCED_NON_NATIVE));
        assertEquals(QUERY_HINTED_NATIVE, QUERY_HINTED_NATIVE.combine(QUERY_HINTED_NATIVE));
        shouldFail(QUERY_HINTED_NATIVE, QUERY_HINTED_NON_NATIVE);
        assertEquals(QUERY_HINTED_NATIVE, QUERY_HINTED_NATIVE.combine(SESSION_HINTED_NATIVE));
        assertEquals(QUERY_HINTED_NATIVE, QUERY_HINTED_NATIVE.combine(SESSION_HINTED_NON_NATIVE));
        assertEquals(QUERY_HINTED_NATIVE, QUERY_HINTED_NATIVE.combine(UNSPECIFIED));
        assertEquals(QUERY_HINTED_NATIVE, QUERY_HINTED_NATIVE.combine(null));

        assertEquals(FORCED_NATIVE, QUERY_HINTED_NON_NATIVE.combine(FORCED_NATIVE));
        assertEquals(FORCED_NON_NATIVE, QUERY_HINTED_NON_NATIVE.combine(FORCED_NON_NATIVE));
        shouldFail(QUERY_HINTED_NON_NATIVE, QUERY_HINTED_NATIVE);
        assertEquals(QUERY_HINTED_NON_NATIVE, QUERY_HINTED_NON_NATIVE.combine(QUERY_HINTED_NON_NATIVE));
        assertEquals(QUERY_HINTED_NON_NATIVE, QUERY_HINTED_NON_NATIVE.combine(SESSION_HINTED_NATIVE));
        assertEquals(QUERY_HINTED_NON_NATIVE, QUERY_HINTED_NON_NATIVE.combine(SESSION_HINTED_NON_NATIVE));
        assertEquals(QUERY_HINTED_NON_NATIVE, QUERY_HINTED_NON_NATIVE.combine(UNSPECIFIED));
        assertEquals(QUERY_HINTED_NON_NATIVE, QUERY_HINTED_NON_NATIVE.combine(null));

        assertEquals(FORCED_NATIVE, SESSION_HINTED_NATIVE.combine(FORCED_NATIVE));
        assertEquals(FORCED_NON_NATIVE, SESSION_HINTED_NATIVE.combine(FORCED_NON_NATIVE));
        assertEquals(QUERY_HINTED_NATIVE, SESSION_HINTED_NATIVE.combine(QUERY_HINTED_NATIVE));
        assertEquals(QUERY_HINTED_NON_NATIVE, SESSION_HINTED_NATIVE.combine(QUERY_HINTED_NON_NATIVE));
        assertEquals(SESSION_HINTED_NATIVE, SESSION_HINTED_NATIVE.combine(SESSION_HINTED_NATIVE));
        shouldFail(SESSION_HINTED_NATIVE, SESSION_HINTED_NON_NATIVE);
        assertEquals(SESSION_HINTED_NATIVE, SESSION_HINTED_NATIVE.combine(UNSPECIFIED));
        assertEquals(SESSION_HINTED_NATIVE, SESSION_HINTED_NATIVE.combine(null));

        assertEquals(FORCED_NATIVE, SESSION_HINTED_NON_NATIVE.combine(FORCED_NATIVE));
        assertEquals(FORCED_NON_NATIVE, SESSION_HINTED_NON_NATIVE.combine(FORCED_NON_NATIVE));
        assertEquals(QUERY_HINTED_NATIVE, SESSION_HINTED_NON_NATIVE.combine(QUERY_HINTED_NATIVE));
        assertEquals(QUERY_HINTED_NON_NATIVE, SESSION_HINTED_NON_NATIVE.combine(QUERY_HINTED_NON_NATIVE));
        shouldFail(SESSION_HINTED_NON_NATIVE, SESSION_HINTED_NATIVE);
        assertEquals(SESSION_HINTED_NON_NATIVE, SESSION_HINTED_NON_NATIVE.combine(SESSION_HINTED_NON_NATIVE));
        assertEquals(SESSION_HINTED_NON_NATIVE, SESSION_HINTED_NON_NATIVE.combine(UNSPECIFIED));
        assertEquals(SESSION_HINTED_NON_NATIVE, SESSION_HINTED_NON_NATIVE.combine(null));

        assertEquals(FORCED_NATIVE, UNSPECIFIED.combine(FORCED_NATIVE));
        assertEquals(FORCED_NON_NATIVE, UNSPECIFIED.combine(FORCED_NON_NATIVE));
        assertEquals(QUERY_HINTED_NATIVE, UNSPECIFIED.combine(QUERY_HINTED_NATIVE));
        assertEquals(QUERY_HINTED_NON_NATIVE, UNSPECIFIED.combine(QUERY_HINTED_NON_NATIVE));
        assertEquals(SESSION_HINTED_NATIVE, UNSPECIFIED.combine(SESSION_HINTED_NATIVE));
        assertEquals(SESSION_HINTED_NON_NATIVE, UNSPECIFIED.combine(SESSION_HINTED_NON_NATIVE));
        assertEquals(UNSPECIFIED, UNSPECIFIED.combine(UNSPECIFIED));
        assertEquals(UNSPECIFIED, UNSPECIFIED.combine(null));
    }
}