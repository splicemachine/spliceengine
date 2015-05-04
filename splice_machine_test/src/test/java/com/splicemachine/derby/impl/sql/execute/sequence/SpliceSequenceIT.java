package com.splicemachine.derby.impl.sql.execute.sequence;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SpliceSequenceIT {

    private static final String SCHEMA = SpliceSequenceIT.class.getSimpleName().toUpperCase();

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @Test
    public void testShortSequence() throws Exception {
        methodWatcher.executeUpdate("create sequence SMALLSEQ AS smallint");

        Integer first = methodWatcher.query("values (next value for SMALLSEQ)");

        assertEquals(first + 1, methodWatcher.query("values (next value for SMALLSEQ)"));
        assertEquals(first + 2, methodWatcher.query("values (next value for SMALLSEQ)"));
        assertEquals(first + 3, methodWatcher.query("values (next value for SMALLSEQ)"));
        assertEquals(first + 4, methodWatcher.query("values (next value for SMALLSEQ)"));
        assertEquals(first + 5, methodWatcher.query("values (next value for SMALLSEQ)"));
        assertEquals(first + 6, methodWatcher.query("values (next value for SMALLSEQ)"));

        assertTrue(first >= Short.MIN_VALUE && first <= Short.MAX_VALUE);

        methodWatcher.executeUpdate("drop sequence SMALLSEQ restrict");
    }

}