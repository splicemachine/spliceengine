package com.splicemachine.derby.utils;

import com.splicemachine.derby.error.SpliceDoNotRetryIOException;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.hadoop.hbase.ipc.RemoteWithExtrasException;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class ExceptionsTest {

    @Test
    public void testParseStandardException() throws Exception {
        StandardException se = StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "SMALLINT");
        IOException spliceDoNotRetryIOException = Exceptions.getIOException(se);

        //check that it can be turned back into a StandardException
        StandardException converted = Exceptions.parseException(spliceDoNotRetryIOException);
        assertEquals("Error codes incorrect!", se.getErrorCode(), converted.getErrorCode());
        assertEquals("Message incorrect!", se.getMessage(), converted.getMessage());
    }

    @Test
    public void parseException_convertsRemoteExceptionInStandardWithCorrectMessage() {

        String noRetryClassName = SpliceDoNotRetryIOException.class.getName();
        String json = "{'severity':20000,'textMessage':'Scalar subquery is only allowed to return a single row.','sqlState':'21000','messageId':'21000'}";
        String msg = "org.apache.derby.iapi.error.StandardException" + json;

        RemoteWithExtrasException remoteWithExtrasException = new RemoteWithExtrasException(noRetryClassName, msg, true);

        Exception result = Exceptions.parseException(remoteWithExtrasException);

        assertEquals(StandardException.class, result.getClass());
        assertEquals("Scalar subquery is only allowed to return a single row.", result.getMessage());
    }

}
