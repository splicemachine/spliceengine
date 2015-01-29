package com.splicemachine.pipeline.exception;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class SpliceDoNotRetryIOExceptionWrappingTest {

    @Test
    public void testUnwrap() throws Exception {

        // given
        StandardException stdExceptionIn = StandardException.newException(SQLState.LANG_COLUMN_ID, "arg1", "arg2", "arg3", "arg4");

        // when
        SpliceDoNotRetryIOException wrapped = SpliceDoNotRetryIOExceptionWrapping.wrap(stdExceptionIn);
        StandardException stdExceptionOut = (StandardException) SpliceDoNotRetryIOExceptionWrapping.unwrap(wrapped);

        // then
        assertEquals("Column Id", stdExceptionOut.getMessage());
        assertArrayEquals(new String[]{"arg1", "arg2", "arg3", "arg4"}, stdExceptionOut.getArguments());
        assertEquals("42Z42", stdExceptionOut.getSqlState());

    }

}